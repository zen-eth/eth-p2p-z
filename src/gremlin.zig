const std = @import("std");

pub const ProtoWireNumber = i32;

pub const ProtoWireType = enum(u64) {
    varint = 0,
    fixed64 = 1,
    bytes = 2,
    startGroup = 3,
    endGroup = 4,
    fixed32 = 5,
};

pub const Error = error{
    InvalidVarInt,
    InvalidTag,
    InvalidData,
    OutOfMemory,
};

pub const ProtoTag = struct {
    number: ProtoWireNumber,
    wire: ProtoWireType,
    size: usize,
};

fn Sized(comptime T: type) type {
    return struct {
        size: usize,
        value: T,
    };
}

pub const SizedU32 = Sized(u32);
pub const SizedU64 = Sized(u64);
pub const SizedI32 = Sized(i32);
pub const SizedI64 = Sized(i64);
pub const SizedBool = Sized(bool);
pub const SizedBytes = Sized([]const u8);

pub const sizes = struct {
    pub fn sizeVarInt(value: u64) u32 {
        if (value < 1 << 7) return 1;
        if (value < 1 << 14) return 2;
        if (value < 1 << 21) return 3;
        if (value < 1 << 28) return 4;
        if (value < 1 << 35) return 5;
        if (value < 1 << 42) return 6;
        if (value < 1 << 49) return 7;
        if (value < 1 << 56) return 8;
        if (value < 1 << 63) return 9;
        return 10;
    }

    pub fn sizeBool(_: bool) u32 {
        return 1;
    }

    pub fn sizeI32(value: i32) u32 {
        return sizeVarInt(@as(u64, @bitCast(@as(i64, value))));
    }

    pub fn sizeU64(value: u64) u32 {
        return sizeVarInt(value);
    }

    pub fn sizeUsize(value: usize) u32 {
        return sizeVarInt(@intCast(value));
    }

    pub fn sizeWireNumber(comptime tag: ProtoWireNumber) usize {
        return sizeVarInt((@as(u64, @intCast(tag)) << 3) | @intFromEnum(ProtoWireType.varint));
    }
};

pub const Writer = struct {
    buf: []u8,
    pos: usize,

    pub fn init(buf: []u8) Writer {
        return .{ .buf = buf, .pos = 0 };
    }

    pub fn appendBytes(self: *Writer, tag: ProtoWireNumber, data: []const u8) void {
        self.appendTag(tag, .bytes);
        self.appendVarInt(data.len);
        self.writeBytes(data);
    }

    pub fn appendBytesTag(self: *Writer, tag: ProtoWireNumber, len: usize) void {
        self.appendTag(tag, .bytes);
        self.appendVarInt(len);
    }

    pub fn appendBool(self: *Writer, tag: ProtoWireNumber, data: bool) void {
        self.appendTag(tag, .varint);
        self.appendVarInt(if (data) 1 else 0);
    }

    pub fn appendInt32(self: *Writer, tag: ProtoWireNumber, data: i32) void {
        self.appendTag(tag, .varint);
        self.appendVarInt(@as(u64, @bitCast(@as(i64, data))));
    }

    pub fn appendUint64(self: *Writer, tag: ProtoWireNumber, data: u64) void {
        self.appendTag(tag, .varint);
        self.appendVarInt(data);
    }

    fn appendTag(self: *Writer, tag: ProtoWireNumber, wire_type: ProtoWireType) void {
        self.appendVarInt((@as(u64, @intCast(tag)) << 3) | @intFromEnum(wire_type));
    }

    fn appendVarInt(self: *Writer, v: u64) void {
        var value = v;
        while (value >= 0x80) {
            self.writeByte(@as(u8, @truncate(value)) | 0x80);
            value >>= 7;
        }
        self.writeByte(@truncate(value));
    }

    fn writeByte(self: *Writer, byte: u8) void {
        self.buf[self.pos] = byte;
        self.pos += 1;
    }

    fn writeBytes(self: *Writer, bytes: []const u8) void {
        @memcpy(self.buf[self.pos..][0..bytes.len], bytes);
        self.pos += bytes.len;
    }
};

pub const Reader = struct {
    buf: []const u8,

    pub fn init(data: []const u8) Reader {
        return .{ .buf = data };
    }

    pub fn hasNext(self: Reader, offset: usize, size: usize) bool {
        return offset <= self.buf.len and size < self.buf.len - offset;
    }

    pub fn readTagAt(self: Reader, offset: usize) Error!ProtoTag {
        const tag_data = try self.readVarIntAt(offset);
        if (tag_data.value >> 3 > std.math.maxInt(i32)) return error.InvalidTag;
        const wire_int = tag_data.value & 0x7;
        if (wire_int > @intFromEnum(ProtoWireType.fixed32)) return error.InvalidTag;
        return .{
            .number = @intCast(tag_data.value >> 3),
            .wire = @enumFromInt(wire_int),
            .size = tag_data.size,
        };
    }

    pub fn skipData(self: Reader, offset: usize, wire: ProtoWireType) Error!usize {
        return switch (wire) {
            .varint => offset + try self.getVarIntSize(offset),
            .fixed32 => checkedEnd(self.buf.len, offset, 4),
            .fixed64 => checkedEnd(self.buf.len, offset, 8),
            .bytes => blk: {
                const size_data = try self.readVarIntAt(offset);
                break :blk checkedEnd(self.buf.len, offset + size_data.size, @intCast(size_data.value));
            },
            .startGroup => {
                var current_offset = offset;
                while (true) {
                    const tag = try self.readTagAt(current_offset);
                    current_offset += tag.size;
                    if (tag.wire == .endGroup) return current_offset;
                    current_offset = try self.skipData(current_offset, tag.wire);
                }
            },
            .endGroup => error.InvalidTag,
        };
    }

    pub fn readBytes(self: Reader, offset: usize) Error!SizedBytes {
        const size_data = try self.readVarIntAt(offset);
        const start = offset + size_data.size;
        const len: usize = @intCast(size_data.value);
        const end = try checkedEnd(self.buf.len, start, len);
        return .{
            .value = self.buf[start..end],
            .size = size_data.size + len,
        };
    }

    pub fn readBool(self: Reader, offset: usize) Error!SizedBool {
        const result = try self.readVarIntAt(offset);
        return .{ .value = result.value != 0, .size = result.size };
    }

    pub fn readInt32(self: Reader, offset: usize) Error!SizedI32 {
        const result = try self.readVarIntAt(offset);
        return .{
            .value = @as(i32, @bitCast(@as(u32, @truncate(result.value)))),
            .size = result.size,
        };
    }

    pub fn readUInt64(self: Reader, offset: usize) Error!SizedU64 {
        return self.readVarIntAt(offset);
    }

    fn getVarIntSize(self: Reader, offset: usize) Error!usize {
        var i: usize = 0;
        while (i < 10) : (i += 1) {
            if (!self.hasNext(offset, i)) return error.InvalidVarInt;
            if (self.buf[offset + i] < 0x80) return i + 1;
        }
        return error.InvalidVarInt;
    }

    fn readVarIntAt(self: Reader, offset: usize) Error!SizedU64 {
        var value: u64 = 0;
        var shift: u6 = 0;
        var i: usize = 0;

        while (i < 10) : (i += 1) {
            if (!self.hasNext(offset, i)) return error.InvalidVarInt;

            const byte = self.buf[offset + i];
            value |= @as(u64, byte & 0x7f) << shift;

            if (byte < 0x80) {
                return .{ .value = value, .size = i + 1 };
            }

            if (shift > 57) return error.InvalidVarInt;
            shift += 7;
        }

        return error.InvalidVarInt;
    }
};

fn checkedEnd(limit: usize, start: usize, len: usize) Error!usize {
    if (start > limit) return error.InvalidData;
    if (len > limit - start) return error.InvalidData;
    return start + len;
}

test "generated protobuf runtime smoke test" {
    var buf: [8]u8 = undefined;
    var writer = Writer.init(&buf);
    writer.appendBool(1, true);

    const reader = Reader.init(buf[0..writer.pos]);
    const tag = try reader.readTagAt(0);
    try std.testing.expectEqual(@as(ProtoWireNumber, 1), tag.number);
    const value = try reader.readBool(tag.size);
    try std.testing.expect(value.value);
}
