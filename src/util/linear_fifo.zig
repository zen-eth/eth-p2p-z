const std = @import("std");

/// A minimal FIFO buffer equivalent to the removed std.fifo.LinearFifo.
/// Supports both .Slice (fixed-size, externally owned buffer) and .Dynamic (allocator-backed, growable) modes.
pub fn LinearFifo(comptime T: type, comptime buffer_type: BufferType) type {
    return struct {
        buf: []T,
        head: usize = 0,
        count: usize = 0,
        allocator: if (buffer_type == .Dynamic) std.mem.Allocator else void =
            if (buffer_type == .Dynamic) undefined else {},

        pub fn init(arg: if (buffer_type == .Dynamic) std.mem.Allocator else []T) @This() {
            if (buffer_type == .Dynamic) {
                return .{ .buf = &.{}, .head = 0, .count = 0, .allocator = arg };
            } else {
                return .{ .buf = arg, .head = 0, .count = 0 };
            }
        }

        pub fn deinit(self: *@This()) void {
            if (buffer_type == .Dynamic) {
                if (self.buf.len > 0) {
                    self.allocator.free(self.buf);
                }
            }
            self.* = undefined;
        }

        pub fn readableLength(self: *const @This()) usize {
            return self.count;
        }

        pub fn readableSlice(self: *const @This(), offset: usize) []const T {
            std.debug.assert(offset <= self.count);
            const start = self.head + offset;
            return self.buf[start .. start + self.count - offset];
        }

        pub fn read(self: *@This(), dest: []T) usize {
            const n = @min(dest.len, self.count);
            @memcpy(dest[0..n], self.buf[self.head .. self.head + n]);
            self.head += n;
            self.count -= n;
            return n;
        }

        pub fn discard(self: *@This(), num: usize) void {
            const n = @min(num, self.count);
            self.head += n;
            self.count -= n;
        }

        pub fn unget(self: *@This(), data: []const T) !void {
            if (self.head < data.len) {
                if (buffer_type == .Dynamic) {
                    try self.ensureCapacity(self.count + data.len);
                } else {
                    if (self.count + data.len > self.buf.len) return error.OutOfMemory;
                }
                std.mem.copyBackwards(T, self.buf[data.len .. data.len + self.count], self.buf[self.head .. self.head + self.count]);
                self.head = 0;
            } else {
                self.head -= data.len;
            }
            @memcpy(self.buf[self.head .. self.head + data.len], data);
            self.count += data.len;
        }

        pub fn write(self: *@This(), data: []const T) !void {
            const end = self.head + self.count;
            if (end + data.len > self.buf.len) {
                if (self.count > 0 and self.head > 0) {
                    std.mem.copyForwards(T, self.buf[0..self.count], self.buf[self.head .. self.head + self.count]);
                }
                self.head = 0;
                if (self.count + data.len > self.buf.len) {
                    if (buffer_type == .Dynamic) {
                        try self.ensureCapacity(self.count + data.len);
                    } else {
                        return error.OutOfMemory;
                    }
                }
            }
            const new_end = self.head + self.count;
            @memcpy(self.buf[new_end .. new_end + data.len], data);
            self.count += data.len;
        }

        fn ensureCapacity(self: *@This(), needed: usize) !void {
            if (buffer_type != .Dynamic) return;
            if (self.buf.len >= needed) return;
            var new_cap = if (self.buf.len == 0) @as(usize, 64) else self.buf.len;
            while (new_cap < needed) {
                new_cap = std.math.mul(usize, new_cap, 2) catch return error.OutOfMemory;
            }
            const new_buf = try self.allocator.alloc(T, new_cap);
            if (self.count > 0) {
                @memcpy(new_buf[0..self.count], self.buf[self.head .. self.head + self.count]);
            }
            if (self.buf.len > 0) {
                self.allocator.free(self.buf);
            }
            self.buf = new_buf;
            self.head = 0;
        }
    };
}

pub const BufferType = enum {
    Slice,
    Dynamic,
};

test "LinearFifo Slice basic read/write" {
    var backing: [64]u8 = undefined;
    var fifo = LinearFifo(u8, .Slice).init(&backing);

    try fifo.write("hello");
    try std.testing.expectEqual(@as(usize, 5), fifo.readableLength());

    var dest: [5]u8 = undefined;
    const n = fifo.read(&dest);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("hello", &dest);
    try std.testing.expectEqual(@as(usize, 0), fifo.readableLength());
}

test "LinearFifo Dynamic grow" {
    var fifo = LinearFifo(u8, .Dynamic).init(std.testing.allocator);
    defer fifo.deinit();

    // Write more than initial capacity
    const data = "a" ** 128;
    try fifo.write(data);
    try std.testing.expectEqual(@as(usize, 128), fifo.readableLength());

    var dest: [128]u8 = undefined;
    const n = fifo.read(&dest);
    try std.testing.expectEqual(@as(usize, 128), n);
    try std.testing.expectEqualStrings(data, &dest);
}

test "LinearFifo unget" {
    var backing: [64]u8 = undefined;
    var fifo = LinearFifo(u8, .Slice).init(&backing);

    try fifo.write("world");
    var dest: [2]u8 = undefined;
    _ = fifo.read(&dest);
    // Read "wo", remaining "rld"

    try fifo.unget("wo");
    try std.testing.expectEqual(@as(usize, 5), fifo.readableLength());
    try std.testing.expectEqualStrings("world", fifo.readableSlice(0));
}
