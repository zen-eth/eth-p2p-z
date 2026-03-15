const std = @import("std");
const Io = std.Io;
const stream_util = @import("../util/stream.zig");

pub const protocol_id = "/multistream/1.0.0";
const message_suffix = "\n";
const na_response = "na";
const max_message_length: u32 = 1024;
const max_varint_bytes: u32 = 9;

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
fn writeMessage(io: Io, stream: anytype, msg: []const u8) Error!void {
    var len_buf: [max_varint_bytes + 1]u8 = undefined;
    const len_bytes = encodeUvarint(msg.len + 1, &len_buf);
    writeAllGeneric(io, stream, len_buf[0..len_bytes]) catch return Error.UnexpectedEof;
    writeAllGeneric(io, stream, msg) catch return Error.UnexpectedEof;
    writeAllGeneric(io, stream, message_suffix) catch return Error.UnexpectedEof;
}

fn writeAllGeneric(io: Io, stream: anytype, data: []const u8) !void {
    var total: usize = 0;
    while (total < data.len) {
        const n = stream.write(io, data[total..]) catch return error.BrokenPipe;
        if (n == 0) return error.BrokenPipe;
        total += n;
    }
}

/// Read a multistream-select message: length-prefixed, newline-terminated.
fn readMessage(io: Io, stream: anytype, buf: []u8) Error![]const u8 {
    var len: usize = 0;
    var bytes_read: usize = 0;
    while (bytes_read < max_varint_bytes) : (bytes_read += 1) {
        var byte_buf: [1]u8 = undefined;
        const n = stream.read(io, &byte_buf) catch return Error.UnexpectedEof;
        if (n == 0) return Error.UnexpectedEof;
        const b = byte_buf[0];
        const shift: u6 = std.math.cast(u6, bytes_read * 7) orelse return Error.InvalidLength;
        len |= @as(usize, b & 0x7f) << shift;
        if (b & 0x80 == 0) break;
    } else {
        return Error.InvalidLength;
    }

    if (len == 0 or len > max_message_length) return Error.InvalidLength;
    if (len > buf.len) return Error.ProtocolIdTooLong;

    var total: usize = 0;
    while (total < len) {
        const n = stream.read(io, buf[total..len]) catch return Error.UnexpectedEof;
        if (n == 0) return Error.UnexpectedEof;
        total += n;
    }

    if (buf[len - 1] != '\n') return Error.InvalidMultistreamSuffix;
    return buf[0 .. len - 1];
}

/// Negotiate as initiator: propose protocols, return the selected one.
pub fn negotiateOutbound(
    io: Io,
    stream: anytype,
    proposed_protocols: []const []const u8,
) Error![]const u8 {
    var buf: [max_message_length]u8 = undefined;

    try writeMessage(io, stream, protocol_id);

    const header = try readMessage(io, stream, &buf);
    if (!std.mem.eql(u8, header, protocol_id)) {
        return Error.FirstLineShouldBeMultistream;
    }

    for (proposed_protocols) |proto| {
        try writeMessage(io, stream, proto);
        const response = try readMessage(io, stream, &buf);
        if (std.mem.eql(u8, response, proto)) {
            return proto;
        }
    }

    return Error.AllProposedProtocolsRejected;
}

/// Negotiate as responder: wait for proposals, accept if supported.
pub fn negotiateInbound(
    io: Io,
    stream: anytype,
    supported_protocols: []const []const u8,
) Error![]const u8 {
    var buf: [max_message_length]u8 = undefined;

    const header = try readMessage(io, stream, &buf);
    if (!std.mem.eql(u8, header, protocol_id)) {
        return Error.FirstLineShouldBeMultistream;
    }

    try writeMessage(io, stream, protocol_id);

    while (true) {
        const proposal = try readMessage(io, stream, &buf);

        for (supported_protocols) |supported| {
            if (std.mem.eql(u8, proposal, supported)) {
                try writeMessage(io, stream, supported);
                return supported;
            }
        }

        try writeMessage(io, stream, na_response);
    }
}

fn encodeUvarint(value: usize, buf: []u8) usize {
    std.debug.assert(buf.len >= max_varint_bytes + 1);
    var v = value;
    var i: usize = 0;
    while (v >= 0x80) : (i += 1) {
        buf[i] = @intCast((v & 0x7f) | 0x80);
        v >>= 7;
    }
    buf[i] = @intCast(v);
    return i + 1;
}

// --- Tests ---

const MockStream = stream_util.MockStream;

test "encodeUvarint multi-byte" {
    var buf: [max_varint_bytes + 1]u8 = undefined;
    const n = encodeUvarint(300, &buf);
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqual(@as(u8, 0xAC), buf[0]);
    try std.testing.expectEqual(@as(u8, 0x02), buf[1]);
}

test "encodeUvarint single byte" {
    var buf: [max_varint_bytes + 1]u8 = undefined;
    const n = encodeUvarint(21, &buf);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(u8, 21), buf[0]);
}

fn encodeMessage(allocator: std.mem.Allocator, msg: []const u8) ![]const u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    var len_buf: [max_varint_bytes + 1]u8 = undefined;
    const len_bytes = encodeUvarint(msg.len + 1, &len_buf);
    try out.appendSlice(allocator, len_buf[0..len_bytes]);
    try out.appendSlice(allocator, msg);
    try out.appendSlice(allocator, "\n");
    return out.toOwnedSlice(allocator);
}

test "negotiateOutbound succeeds on first protocol" {
    const allocator = std.testing.allocator;

    const header_msg = try encodeMessage(allocator, protocol_id);
    defer allocator.free(header_msg);
    const proto_msg = try encodeMessage(allocator, "/ipfs/ping/1.0.0");
    defer allocator.free(proto_msg);

    const read_data = try std.mem.concat(allocator, u8, &.{ header_msg, proto_msg });
    defer allocator.free(read_data);

    var stream = MockStream.init(allocator, read_data);
    defer stream.deinit();

    const proposed = [_][]const u8{"/ipfs/ping/1.0.0"};
    const result = try negotiateOutbound(undefined, &stream, &proposed);
    try std.testing.expectEqualStrings("/ipfs/ping/1.0.0", result);
}

test "negotiateOutbound falls back to second protocol" {
    const allocator = std.testing.allocator;

    const header_msg = try encodeMessage(allocator, protocol_id);
    defer allocator.free(header_msg);
    const na_msg = try encodeMessage(allocator, na_response);
    defer allocator.free(na_msg);
    const proto_msg = try encodeMessage(allocator, "/ipfs/id/1.0.0");
    defer allocator.free(proto_msg);

    const read_data = try std.mem.concat(allocator, u8, &.{ header_msg, na_msg, proto_msg });
    defer allocator.free(read_data);

    var stream = MockStream.init(allocator, read_data);
    defer stream.deinit();

    const proposed = [_][]const u8{ "/ipfs/ping/1.0.0", "/ipfs/id/1.0.0" };
    const result = try negotiateOutbound(undefined, &stream, &proposed);
    try std.testing.expectEqualStrings("/ipfs/id/1.0.0", result);
}

test "negotiateOutbound all rejected" {
    const allocator = std.testing.allocator;

    const header_msg = try encodeMessage(allocator, protocol_id);
    defer allocator.free(header_msg);
    const na_msg = try encodeMessage(allocator, na_response);
    defer allocator.free(na_msg);

    const read_data = try std.mem.concat(allocator, u8, &.{ header_msg, na_msg });
    defer allocator.free(read_data);

    var stream = MockStream.init(allocator, read_data);
    defer stream.deinit();

    const proposed = [_][]const u8{"/ipfs/ping/1.0.0"};
    const result = negotiateOutbound(undefined, &stream, &proposed);
    try std.testing.expectError(Error.AllProposedProtocolsRejected, result);
}

test "negotiateInbound accepts supported protocol" {
    const allocator = std.testing.allocator;

    const header_msg = try encodeMessage(allocator, protocol_id);
    defer allocator.free(header_msg);
    const proto_msg = try encodeMessage(allocator, "/ipfs/ping/1.0.0");
    defer allocator.free(proto_msg);

    const read_data = try std.mem.concat(allocator, u8, &.{ header_msg, proto_msg });
    defer allocator.free(read_data);

    var stream = MockStream.init(allocator, read_data);
    defer stream.deinit();

    const supported = [_][]const u8{"/ipfs/ping/1.0.0"};
    const result = try negotiateInbound(undefined, &stream, &supported);
    try std.testing.expectEqualStrings("/ipfs/ping/1.0.0", result);
}

test "readMessage rejects overlong varint" {
    const bad_varint = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80 };
    var stream = MockStream.init(std.testing.allocator, &bad_varint);
    defer stream.deinit();

    var buf: [max_message_length]u8 = undefined;
    const result = readMessage(undefined, &stream, &buf);
    try std.testing.expectError(Error.InvalidLength, result);
}
