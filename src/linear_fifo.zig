const std = @import("std");

/// A minimal FIFO buffer equivalent to the removed std.fifo.LinearFifo.
/// Supports both .Slice (fixed-size, externally owned buffer) and .Dynamic (allocator-backed, growable) modes.
pub fn LinearFifo(comptime T: type, comptime buffer_type: BufferType) type {
    return struct {
        const Self = @This();

        buf: []T,
        head: usize = 0,
        count: usize = 0,
        allocator: if (buffer_type == .Dynamic) std.mem.Allocator else void =
            if (buffer_type == .Dynamic) undefined else {},

        pub fn init(arg: if (buffer_type == .Dynamic) std.mem.Allocator else []T) Self {
            if (buffer_type == .Dynamic) {
                return .{ .buf = &.{}, .head = 0, .count = 0, .allocator = arg };
            } else {
                return .{ .buf = arg, .head = 0, .count = 0 };
            }
        }

        pub fn deinit(self: *Self) void {
            if (buffer_type == .Dynamic) {
                if (self.buf.len > 0) {
                    self.allocator.free(self.buf);
                }
            }
            self.* = undefined;
        }

        pub fn readableLength(self: *const Self) usize {
            return self.count;
        }

        pub fn readableSlice(self: *const Self, offset: usize) []const T {
            const start = self.head + offset;
            return self.buf[start .. start + self.count - offset];
        }

        pub fn read(self: *Self, dest: []T) usize {
            const n = @min(dest.len, self.count);
            @memcpy(dest[0..n], self.buf[self.head .. self.head + n]);
            self.head += n;
            self.count -= n;
            return n;
        }

        pub fn discard(self: *Self, num: usize) void {
            const n = @min(num, self.count);
            self.head += n;
            self.count -= n;
        }

        pub fn unget(self: *Self, data: []const T) !void {
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

        pub fn write(self: *Self, data: []const T) !void {
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

        fn ensureCapacity(self: *Self, needed: usize) !void {
            if (buffer_type != .Dynamic) return;
            if (self.buf.len >= needed) return;
            var new_cap = if (self.buf.len == 0) @as(usize, 64) else self.buf.len;
            while (new_cap < needed) {
                new_cap *= 2;
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
