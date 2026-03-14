const std = @import("std");

/// Protocol identifier for libp2p ping.
pub const id = "/ipfs/ping/1.0.0";

/// Length of a ping payload in bytes.
pub const payload_length: usize = 32;

pub const Error = error{
    UnexpectedEof,
    PayloadMismatch,
};

/// Handle an inbound ping: read 32 bytes from the stream, echo them back.
pub fn handleInbound(stream: anytype) Error!void {
    var buf: [payload_length]u8 = undefined;
    readExact(stream, &buf) catch return Error.UnexpectedEof;
    writeAll(stream, &buf) catch return Error.UnexpectedEof;
}

/// Handle an outbound ping: send the given payload, wait for echo, verify match.
/// Caller provides the payload (typically 32 random bytes) and handles RTT measurement.
pub fn handleOutbound(stream: anytype, payload: *const [payload_length]u8) Error!void {
    writeAll(stream, payload) catch return Error.UnexpectedEof;

    var response: [payload_length]u8 = undefined;
    readExact(stream, &response) catch return Error.UnexpectedEof;

    if (!std.mem.eql(u8, payload, &response)) {
        return Error.PayloadMismatch;
    }
}

fn readExact(stream: anytype, buf: []u8) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const n = try stream.read(buf[total..]);
        if (n == 0) return error.UnexpectedEof;
        total += n;
    }
}

fn writeAll(stream: anytype, data: []const u8) !void {
    var total: usize = 0;
    while (total < data.len) {
        const n = try stream.write(data[total..]);
        if (n == 0) return error.BrokenPipe;
        total += n;
    }
}

// --- Tests ---

const MockStream = struct {
    read_buf: []const u8,
    read_pos: usize = 0,
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
        try self.write_buf.appendSlice(self.allocator, data);
        return data.len;
    }
};

test "handleInbound echoes payload" {
    const allocator = std.testing.allocator;

    const payload = [_]u8{0x42} ** payload_length;
    var stream = MockStream.init(allocator, &payload);
    defer stream.deinit();

    try handleInbound(&stream);

    try std.testing.expectEqual(payload_length, stream.write_buf.items.len);
    try std.testing.expectEqualSlices(u8, &payload, stream.write_buf.items);
}

test "handleInbound returns error on short read" {
    const allocator = std.testing.allocator;

    const short_payload = [_]u8{0x42} ** 16; // Only 16 bytes, need 32
    var stream = MockStream.init(allocator, &short_payload);
    defer stream.deinit();

    const result = handleInbound(&stream);
    try std.testing.expectError(Error.UnexpectedEof, result);
}

test "handleOutbound succeeds with matching echo" {
    const allocator = std.testing.allocator;

    const payload = [_]u8{0xAB} ** payload_length;
    var stream = MockStream.init(allocator, &payload);
    defer stream.deinit();

    try handleOutbound(&stream, &payload);
    try std.testing.expectEqualSlices(u8, &payload, stream.write_buf.items);
}

test "handleOutbound detects payload mismatch" {
    const allocator = std.testing.allocator;

    // Respond with wrong data
    const wrong_response = [_]u8{0x00} ** payload_length;
    var stream = MockStream.init(allocator, &wrong_response);
    defer stream.deinit();

    const payload = [_]u8{0xFF} ** payload_length;
    const result = handleOutbound(&stream, &payload);
    try std.testing.expectError(Error.PayloadMismatch, result);
}
