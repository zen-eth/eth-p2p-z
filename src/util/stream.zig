const std = @import("std");

/// Read exactly `buf.len` bytes from a stream.
/// Returns `UnexpectedEof` if the stream ends before the buffer is filled.
pub fn readExact(stream: anytype, buf: []u8) error{UnexpectedEof}!void {
    var total: usize = 0;
    while (total < buf.len) {
        const n = stream.read(buf[total..]) catch return error.UnexpectedEof;
        if (n == 0) return error.UnexpectedEof;
        total += n;
    }
}

/// Write all bytes to a stream.
/// Returns `BrokenPipe` if the stream refuses data before all bytes are written.
pub fn writeAll(stream: anytype, data: []const u8) error{BrokenPipe}!void {
    var total: usize = 0;
    while (total < data.len) {
        const n = stream.write(data[total..]) catch return error.BrokenPipe;
        if (n == 0) return error.BrokenPipe;
        total += n;
    }
}

/// A bidirectional in-memory stream for testing protocol implementations.
/// Reads from a pre-loaded buffer, writes to a growable ArrayList.
pub const MockStream = struct {
    read_buf: []const u8,
    read_pos: usize = 0,
    write_buf: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, read_data: []const u8) MockStream {
        return .{
            .read_buf = read_data,
            .write_buf = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *MockStream) void {
        self.write_buf.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn read(self: *MockStream, buf: []u8) error{}!usize {
        if (self.read_pos >= self.read_buf.len) return 0;
        const available = self.read_buf.len - self.read_pos;
        const to_read = @min(buf.len, available);
        @memcpy(buf[0..to_read], self.read_buf[self.read_pos..][0..to_read]);
        self.read_pos += to_read;
        return to_read;
    }

    pub fn write(self: *MockStream, data: []const u8) error{OutOfMemory}!usize {
        self.write_buf.appendSlice(self.allocator, data) catch return error.OutOfMemory;
        return data.len;
    }
};

test "readExact reads full buffer" {
    const data = "hello world";
    var stream = MockStream.init(std.testing.allocator, data);
    defer stream.deinit();

    var buf: [5]u8 = undefined;
    try readExact(&stream, &buf);
    try std.testing.expectEqualStrings("hello", &buf);
}

test "readExact returns error on short stream" {
    const data = "hi";
    var stream = MockStream.init(std.testing.allocator, data);
    defer stream.deinit();

    var buf: [5]u8 = undefined;
    const result = readExact(&stream, &buf);
    try std.testing.expectError(error.UnexpectedEof, result);
}

test "writeAll writes all data" {
    var stream = MockStream.init(std.testing.allocator, &.{});
    defer stream.deinit();

    try writeAll(&stream, "hello");
    try std.testing.expectEqualStrings("hello", stream.write_buf.items);
}

test "MockStream read returns 0 at end" {
    var stream = MockStream.init(std.testing.allocator, "ab");
    defer stream.deinit();

    var buf: [2]u8 = undefined;
    const n = try stream.read(&buf);
    try std.testing.expectEqual(@as(usize, 2), n);

    const n2 = try stream.read(&buf);
    try std.testing.expectEqual(@as(usize, 0), n2);
}
