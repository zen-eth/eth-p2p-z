const std = @import("std");

pub const PROTOCOL_ID = "/multistream/1.0.0";
const MESSAGE_SUFFIX = "\n";
const NA = "na";
const MAX_MESSAGE_LENGTH = 1024;

pub const Error = error{
    ProtocolIdTooLong,
    InvalidMultistreamSuffix,
    FirstLineShouldBeMultistream,
    AllProposedProtocolsRejected,
    NoSupportedProtocols,
    UnexpectedEof,
    InvalidLength,
};

/// Write a multistream-select message: length-prefixed, newline-terminated.
fn writeMessage(stream: anytype, msg: []const u8) !void {
    // Write varint length (msg + newline)
    var len_buf: [10]u8 = undefined;
    const len_bytes = encodeUvarint(msg.len + 1, &len_buf);
    try writeAllGeneric(stream, len_buf[0..len_bytes]);
    try writeAllGeneric(stream, msg);
    try writeAllGeneric(stream, MESSAGE_SUFFIX);
}

/// Read a multistream-select message: length-prefixed, newline-terminated.
fn readMessage(stream: anytype, buf: []u8) ![]const u8 {
    // Read varint length
    var len: usize = 0;
    var shift: u6 = 0;
    while (true) {
        var byte_buf: [1]u8 = undefined;
        const n = try stream.read(&byte_buf);
        if (n == 0) return Error.UnexpectedEof;
        const b = byte_buf[0];
        len |= @as(usize, b & 0x7f) << shift;
        if (b & 0x80 == 0) break;
        shift +|= 7;
        if (shift >= 64) return Error.InvalidLength;
    }

    if (len == 0 or len > MAX_MESSAGE_LENGTH) return Error.InvalidLength;
    if (len > buf.len) return Error.ProtocolIdTooLong;

    // Read exactly `len` bytes
    var total: usize = 0;
    while (total < len) {
        const n = try stream.read(buf[total..len]);
        if (n == 0) return Error.UnexpectedEof;
        total += n;
    }

    // Verify and strip newline suffix
    if (buf[len - 1] != '\n') return Error.InvalidMultistreamSuffix;
    return buf[0 .. len - 1];
}

/// Negotiate as initiator: propose protocols, return the selected one.
pub fn negotiateOutbound(
    stream: anytype,
    proposed_protocols: []const []const u8,
) ![]const u8 {
    var buf: [MAX_MESSAGE_LENGTH]u8 = undefined;

    // Send multistream header
    try writeMessage(stream, PROTOCOL_ID);

    // Read multistream header response
    const header = try readMessage(stream, &buf);
    if (!std.mem.eql(u8, header, PROTOCOL_ID)) {
        return Error.FirstLineShouldBeMultistream;
    }

    // Propose each protocol until one is accepted
    for (proposed_protocols) |proto| {
        try writeMessage(stream, proto);
        const response = try readMessage(stream, &buf);
        if (std.mem.eql(u8, response, proto)) {
            return proto; // Accepted!
        }
        // "na" means rejected, try next
    }

    return Error.AllProposedProtocolsRejected;
}

/// Negotiate as responder: wait for proposals, accept if supported.
pub fn negotiateInbound(
    stream: anytype,
    supported_protocols: []const []const u8,
) ![]const u8 {
    var buf: [MAX_MESSAGE_LENGTH]u8 = undefined;

    // Read multistream header
    const header = try readMessage(stream, &buf);
    if (!std.mem.eql(u8, header, PROTOCOL_ID)) {
        return Error.FirstLineShouldBeMultistream;
    }

    // Send multistream header
    try writeMessage(stream, PROTOCOL_ID);

    // Process proposals
    while (true) {
        const proposal = try readMessage(stream, &buf);

        for (supported_protocols) |supported| {
            if (std.mem.eql(u8, proposal, supported)) {
                try writeMessage(stream, supported);
                return supported;
            }
        }

        // Reject unsupported protocol
        try writeMessage(stream, NA);
    }
}

fn encodeUvarint(value: usize, buf: []u8) usize {
    var v = value;
    var i: usize = 0;
    while (v >= 0x80) : (i += 1) {
        buf[i] = @intCast((v & 0x7f) | 0x80);
        v >>= 7;
    }
    buf[i] = @intCast(v);
    return i + 1;
}

fn writeAllGeneric(stream: anytype, data: []const u8) !void {
    var total: usize = 0;
    while (total < data.len) {
        const n = try stream.write(data[total..]);
        if (n == 0) return error.BrokenPipe;
        total += n;
    }
}

// --- Tests ---

test "encodeUvarint" {
    var buf: [10]u8 = undefined;
    const n = encodeUvarint(300, &buf);
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqual(@as(u8, 0xAC), buf[0]);
    try std.testing.expectEqual(@as(u8, 0x02), buf[1]);
}

test "encodeUvarint single byte" {
    var buf: [10]u8 = undefined;
    const n = encodeUvarint(21, &buf);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(u8, 21), buf[0]);
}

/// A mock stream backed by two fixed buffers, used for testing multistream negotiation.
const MockStream = struct {
    /// Data to be returned by read() calls — simulates incoming data from a remote peer.
    read_buf: []const u8,
    read_pos: usize = 0,
    /// Data written via write() calls — captures outgoing data.
    write_buf: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, read_data: []const u8) MockStream {
        return .{
            .read_buf = read_data,
            .write_buf = .empty,
            .allocator = allocator,
        };
    }

    fn deinit(self: *MockStream) void {
        self.write_buf.deinit(self.allocator);
    }

    pub fn read(self: *MockStream, buf: []u8) !usize {
        if (self.read_pos >= self.read_buf.len) return 0;
        const available = self.read_buf.len - self.read_pos;
        const to_read = @min(buf.len, available);
        @memcpy(buf[0..to_read], self.read_buf[self.read_pos..][0..to_read]);
        self.read_pos += to_read;
        return to_read;
    }

    pub fn write(self: *MockStream, data: []const u8) !usize {
        self.write_buf.appendSlice(self.allocator, data) catch return error.BrokenPipe;
        return data.len;
    }
};

/// Encode a multistream message (varint length + payload + newline) into a buffer.
fn encodeMessage(allocator: std.mem.Allocator, msg: []const u8) ![]const u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    var len_buf: [10]u8 = undefined;
    const len_bytes = encodeUvarint(msg.len + 1, &len_buf);
    try out.appendSlice(allocator, len_buf[0..len_bytes]);
    try out.appendSlice(allocator, msg);
    try out.appendSlice(allocator, "\n");
    return out.toOwnedSlice(allocator);
}

test "negotiateOutbound succeeds on first protocol" {
    const allocator = std.testing.allocator;

    // Build the responder's replies: multistream header + accepted protocol
    const header_msg = try encodeMessage(allocator, PROTOCOL_ID);
    defer allocator.free(header_msg);
    const proto_msg = try encodeMessage(allocator, "/ipfs/ping/1.0.0");
    defer allocator.free(proto_msg);

    const read_data = try std.mem.concat(allocator, u8, &.{ header_msg, proto_msg });
    defer allocator.free(read_data);

    var stream = MockStream.init(allocator, read_data);
    defer stream.deinit();

    const proposed = [_][]const u8{"/ipfs/ping/1.0.0"};
    const result = try negotiateOutbound(&stream, &proposed);
    try std.testing.expectEqualStrings("/ipfs/ping/1.0.0", result);
}

test "negotiateOutbound falls back to second protocol" {
    const allocator = std.testing.allocator;

    // Built responder replies: header + na (reject first) + accept second
    const header_msg = try encodeMessage(allocator, PROTOCOL_ID);
    defer allocator.free(header_msg);
    const na_msg = try encodeMessage(allocator, NA);
    defer allocator.free(na_msg);
    const proto_msg = try encodeMessage(allocator, "/ipfs/id/1.0.0");
    defer allocator.free(proto_msg);

    const read_data = try std.mem.concat(allocator, u8, &.{ header_msg, na_msg, proto_msg });
    defer allocator.free(read_data);

    var stream = MockStream.init(allocator, read_data);
    defer stream.deinit();

    const proposed = [_][]const u8{ "/ipfs/ping/1.0.0", "/ipfs/id/1.0.0" };
    const result = try negotiateOutbound(&stream, &proposed);
    try std.testing.expectEqualStrings("/ipfs/id/1.0.0", result);
}

test "negotiateOutbound all rejected" {
    const allocator = std.testing.allocator;

    const header_msg = try encodeMessage(allocator, PROTOCOL_ID);
    defer allocator.free(header_msg);
    const na_msg = try encodeMessage(allocator, NA);
    defer allocator.free(na_msg);

    const read_data = try std.mem.concat(allocator, u8, &.{ header_msg, na_msg });
    defer allocator.free(read_data);

    var stream = MockStream.init(allocator, read_data);
    defer stream.deinit();

    const proposed = [_][]const u8{"/ipfs/ping/1.0.0"};
    const result = negotiateOutbound(&stream, &proposed);
    try std.testing.expectError(Error.AllProposedProtocolsRejected, result);
}

test "negotiateInbound accepts supported protocol" {
    const allocator = std.testing.allocator;

    // Build the initiator's messages: header + protocol proposal
    const header_msg = try encodeMessage(allocator, PROTOCOL_ID);
    defer allocator.free(header_msg);
    const proto_msg = try encodeMessage(allocator, "/ipfs/ping/1.0.0");
    defer allocator.free(proto_msg);

    const read_data = try std.mem.concat(allocator, u8, &.{ header_msg, proto_msg });
    defer allocator.free(read_data);

    var stream = MockStream.init(allocator, read_data);
    defer stream.deinit();

    const supported = [_][]const u8{"/ipfs/ping/1.0.0"};
    const result = try negotiateInbound(&stream, &supported);
    try std.testing.expectEqualStrings("/ipfs/ping/1.0.0", result);
}
