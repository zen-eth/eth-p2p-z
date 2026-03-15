/// A replacement for std.io.fixedBufferStream that works with Zig 0.16.
/// Provides writer() and reader() methods for use with anytype writer/reader patterns.

const std = @import("std");

/// Writer adapter for std.ArrayList(u8) — replaces ArrayList.writer().
pub const ArrayListWriter = struct {
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn writeAll(self: *ArrayListWriter, data: []const u8) !void {
        try self.list.appendSlice(self.allocator, data);
    }

    pub fn writeByteNTimes(self: *ArrayListWriter, byte: u8, n: usize) !void {
        var i: usize = 0;
        while (i < n) : (i += 1) {
            try self.list.append(self.allocator, byte);
        }
    }
};

pub fn fixedBufferStream(buffer: anytype) FixedBufferStream(BufferType(@TypeOf(buffer))) {
    return .{ .buf = buffer, .pos = 0 };
}

fn BufferType(comptime T: type) type {
    if (T == []u8 or T == *[]u8) return []u8;
    if (T == []const u8 or T == *[]const u8) return []const u8;

    const info = @typeInfo(T);
    if (info == .pointer) {
        if (info.pointer.size == .one) {
            const child = @typeInfo(info.pointer.child);
            if (child == .array and child.array.child == u8) {
                return []u8;
            }
        }
    }
    @compileError("unsupported buffer type");
}

pub fn FixedBufferStream(comptime BufferT: type) type {
    return struct {
        const Self = @This();

        buf: BufferT,
        pos: usize = 0,

        pub const Writer = struct {
            stream: *Self,

            pub fn writeAll(self: *Writer, data: []const u8) !void {
                if (self.stream.pos + data.len > self.stream.buf.len) return error.NoSpaceLeft;
                @memcpy(self.stream.buf[self.stream.pos..][0..data.len], data);
                self.stream.pos += data.len;
            }

            pub fn writeByteNTimes(self: *Writer, byte: u8, n: usize) !void {
                var i: usize = 0;
                while (i < n) : (i += 1) {
                    if (self.stream.pos >= self.stream.buf.len) return error.NoSpaceLeft;
                    self.stream.buf[self.stream.pos] = byte;
                    self.stream.pos += 1;
                }
            }
        };

        pub const Reader = struct {
            stream: *Self,

            pub fn readByte(self: *Reader) !u8 {
                if (self.stream.pos >= self.stream.buf.len) return error.EndOfStream;
                const byte = self.stream.buf[self.stream.pos];
                self.stream.pos += 1;
                return byte;
            }

            pub fn readNoEof(self: *Reader, dest: []u8) !void {
                if (self.stream.pos + dest.len > self.stream.buf.len) return error.EndOfStream;
                @memcpy(dest, self.stream.buf[self.stream.pos..][0..dest.len]);
                self.stream.pos += dest.len;
            }
        };

        pub fn writer(self: *Self) Writer {
            return .{ .stream = self };
        }

        pub fn reader(self: *Self) Reader {
            return .{ .stream = self };
        }

        pub fn getWritten(self: *const Self) []const u8 {
            return self.buf[0..self.pos];
        }
    };
}
