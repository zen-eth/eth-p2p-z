const std = @import("std");

/// A simple FIFO buffer over an externally-owned byte slice.
/// Uses std slice operations for all functionality.
pub const SliceBuffer = struct {
    buf: []u8,
    head: usize = 0,
    count: usize = 0,

    const Self = @This();

    pub fn init(buffer: []u8) Self {
        return .{ .buf = buffer, .head = 0, .count = 0 };
    }

    pub fn deinit(self: *Self) void {
        self.* = undefined;
    }

    pub fn readableLength(self: *const Self) usize {
        return self.count;
    }

    pub fn readableSlice(self: *const Self, offset: usize) []const u8 {
        if (offset >= self.count) return &.{};
        const start = self.head + offset;
        return self.buf[start .. start + self.count - offset];
    }

    pub fn read(self: *Self, dest: []u8) usize {
        const n = @min(dest.len, self.count);
        if (n > 0) {
            @memcpy(dest[0..n], self.buf[self.head .. self.head + n]);
            self.head += n;
            self.count -= n;
        }
        return n;
    }

    pub fn discard(self: *Self, num: usize) void {
        const n = @min(num, self.count);
        self.head += n;
        self.count -= n;
    }

    pub fn unget(self: *Self, data: []const u8) error{OutOfMemory}!void {
        if (data.len == 0) return;
        if (self.head >= data.len) {
            self.head -= data.len;
        } else {
            if (self.count + data.len > self.buf.len) return error.OutOfMemory;
            if (self.count > 0) {
                std.mem.copyBackwards(u8, self.buf[data.len .. data.len + self.count], self.buf[self.head .. self.head + self.count]);
            }
            self.head = 0;
        }
        @memcpy(self.buf[self.head .. self.head + data.len], data);
        self.count += data.len;
    }

    pub fn write(self: *Self, data: []const u8) error{OutOfMemory}!void {
        if (data.len == 0) return;
        const end = self.head + self.count;
        if (end + data.len > self.buf.len) {
            if (self.count > 0 and self.head > 0) {
                std.mem.copyForwards(u8, self.buf[0..self.count], self.buf[self.head .. self.head + self.count]);
            }
            self.head = 0;
            if (self.count + data.len > self.buf.len) {
                return error.OutOfMemory;
            }
        }
        @memcpy(self.buf[self.head + self.count .. self.head + self.count + data.len], data);
        self.count += data.len;
    }

    pub fn reset(self: *Self) void {
        self.head = 0;
        self.count = 0;
    }
};

/// A growable FIFO buffer using std.ArrayList(u8).
/// Provides the same interface as SliceBuffer but can grow dynamically.
pub const DynamicBuffer = struct {
    list: std.ArrayList(u8) = .empty,
    allocator: std.mem.Allocator,
    head: usize = 0,

    const Self = @This();
    const compaction_threshold: usize = 64;

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .list = .empty,
            .allocator = allocator,
            .head = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.list.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn readableLength(self: *const Self) usize {
        return self.list.items.len - self.head;
    }

    pub fn readableSlice(self: *const Self, offset: usize) []const u8 {
        const readable = self.readableLength();
        if (offset >= readable) return &.{};
        const start = self.head + offset;
        return self.list.items[start..];
    }

    pub fn read(self: *Self, dest: []u8) usize {
        const available = self.readableLength();
        const n = @min(dest.len, available);
        if (n > 0) {
            @memcpy(dest[0..n], self.list.items[self.head .. self.head + n]);
            self.head += n;
            self.maybeCompact();
        }
        return n;
    }

    pub fn discard(self: *Self, num: usize) void {
        const n = @min(num, self.readableLength());
        self.head += n;
        self.maybeCompact();
    }

    pub fn unget(self: *Self, data: []const u8) error{OutOfMemory}!void {
        if (data.len == 0) return;
        if (self.head >= data.len) {
            self.head -= data.len;
            @memcpy(self.list.items[self.head .. self.head + data.len], data);
        } else {
            const readable = self.readableLength();
            const new_len = data.len + readable;

            const new_buf = try self.allocator.alloc(u8, new_len);
            @memcpy(new_buf[0..data.len], data);
            if (readable > 0) {
                @memcpy(new_buf[data.len .. data.len + readable], self.list.items[self.head .. self.head + readable]);
            }
            self.list.deinit(self.allocator);
            self.list = std.ArrayList(u8).fromOwnedSlice(new_buf);
            self.head = 0;
        }
    }

    pub fn write(self: *Self, data: []const u8) error{OutOfMemory}!void {
        if (data.len == 0) return;
        try self.list.appendSlice(self.allocator, data);
    }

    pub fn reset(self: *Self) void {
        self.list.clearRetainingCapacity();
        self.head = 0;
    }

    fn maybeCompact(self: *Self) void {
        if (self.head > self.list.items.len / 2 and self.head > compaction_threshold) {
            // Use replaceRange to remove consumed bytes from the front
            // This never allocates (shrinking), so error is unreachable
            self.list.replaceRange(self.allocator, 0, self.head, &.{}) catch unreachable;
            self.head = 0;
        }
    }
};

// Unit tests
test "SliceBuffer: basic write and read" {
    var backing: [64]u8 = undefined;
    var buf = SliceBuffer.init(&backing);

    try buf.write("hello");
    try std.testing.expectEqual(@as(usize, 5), buf.readableLength());

    var dest: [10]u8 = undefined;
    const n = buf.read(&dest);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("hello", dest[0..n]);
    try std.testing.expectEqual(@as(usize, 0), buf.readableLength());
}

test "SliceBuffer: readableSlice with offset" {
    var backing: [64]u8 = undefined;
    var buf = SliceBuffer.init(&backing);

    try buf.write("hello world");
    try std.testing.expectEqualStrings("hello world", buf.readableSlice(0));
    try std.testing.expectEqualStrings("world", buf.readableSlice(6));
    try std.testing.expectEqualStrings("", buf.readableSlice(100));
}

test "SliceBuffer: unget" {
    var backing: [64]u8 = undefined;
    var buf = SliceBuffer.init(&backing);

    try buf.write("world");
    var dest: [2]u8 = undefined;
    _ = buf.read(&dest);
    try buf.unget(&dest);
    try std.testing.expectEqualStrings("world", buf.readableSlice(0));
}

test "SliceBuffer: discard" {
    var backing: [64]u8 = undefined;
    var buf = SliceBuffer.init(&backing);

    try buf.write("hello world");
    buf.discard(6);
    try std.testing.expectEqualStrings("world", buf.readableSlice(0));
}

test "SliceBuffer: write compaction" {
    var backing: [16]u8 = undefined;
    var buf = SliceBuffer.init(&backing);

    try buf.write("12345678");
    buf.discard(6);
    try buf.write("abcdefgh");
    try std.testing.expectEqualStrings("78abcdefgh", buf.readableSlice(0));
}

test "SliceBuffer: overflow returns error" {
    var backing: [8]u8 = undefined;
    var buf = SliceBuffer.init(&backing);

    try buf.write("12345678");
    try std.testing.expectError(error.OutOfMemory, buf.write("9"));
}

test "DynamicBuffer: basic write and read" {
    var buf = DynamicBuffer.init(std.testing.allocator);
    defer buf.deinit();

    try buf.write("hello");
    try std.testing.expectEqual(@as(usize, 5), buf.readableLength());

    var dest: [10]u8 = undefined;
    const n = buf.read(&dest);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("hello", dest[0..n]);
}

test "DynamicBuffer: readableSlice with offset bounds" {
    var buf = DynamicBuffer.init(std.testing.allocator);
    defer buf.deinit();

    try buf.write("hello");
    try std.testing.expectEqualStrings("hello", buf.readableSlice(0));
    try std.testing.expectEqualStrings("lo", buf.readableSlice(3));
    try std.testing.expectEqualStrings("", buf.readableSlice(100));
}

test "DynamicBuffer: unget with space in head" {
    var buf = DynamicBuffer.init(std.testing.allocator);
    defer buf.deinit();

    try buf.write("world");
    var dest: [2]u8 = undefined;
    _ = buf.read(&dest);
    try buf.unget(&dest);
    try std.testing.expectEqualStrings("world", buf.readableSlice(0));
}

test "DynamicBuffer: unget requiring expansion" {
    var buf = DynamicBuffer.init(std.testing.allocator);
    defer buf.deinit();

    try buf.write("cd");
    try buf.unget("ab");
    try std.testing.expectEqualStrings("abcd", buf.readableSlice(0));
}

test "DynamicBuffer: grows dynamically" {
    var buf = DynamicBuffer.init(std.testing.allocator);
    defer buf.deinit();

    const data = "a" ** 1000;
    try buf.write(data);
    try std.testing.expectEqual(@as(usize, 1000), buf.readableLength());
}

test "DynamicBuffer: reset clears buffer" {
    var buf = DynamicBuffer.init(std.testing.allocator);
    defer buf.deinit();

    try buf.write("hello");
    buf.reset();
    try std.testing.expectEqual(@as(usize, 0), buf.readableLength());
}
