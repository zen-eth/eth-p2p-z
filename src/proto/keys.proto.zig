const std = @import("std");
const gremlin = @import("gremlin");

// enums
pub const KeyType = enum(i32) {
    RSA = 0,
    ED25519 = 1,
    SECP256K1 = 2,
    ECDSA = 3,
    CURVE25519 = 4,
};


// structs
const PublicKeyWire = struct {
    const TYPE_WIRE: gremlin.ProtoWireNumber = 1;
    const DATA_WIRE: gremlin.ProtoWireNumber = 2;
};

pub const PublicKey = struct {
    // fields
    type: KeyType = @enumFromInt(0),
    data: ?[]const u8 = null,

    pub fn calcProtobufSize(self: *const PublicKey) usize {
        var res: usize = 0;
        if (@intFromEnum(self.type) != 0) { res += gremlin.sizes.sizeWireNumber(PublicKeyWire.TYPE_WIRE) + gremlin.sizes.sizeI32(@intFromEnum(self.type)); }
        if (self.data) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(PublicKeyWire.DATA_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        return res;
    }

    pub fn encode(self: *const PublicKey, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const PublicKey, target: *gremlin.Writer) void {
        if (@intFromEnum(self.type) != 0) { target.appendInt32(PublicKeyWire.TYPE_WIRE, @intFromEnum(self.type)); }
        if (self.data) |v| {
            if (v.len > 0) {
                target.appendBytes(PublicKeyWire.DATA_WIRE, v);
            }
        }
    }
};

pub const PublicKeyReader = struct {
    _type: KeyType = @enumFromInt(0),
    _data: ?[]const u8 = null,

    pub fn init(_: std.mem.Allocator, src: []const u8) gremlin.Error!PublicKeyReader {
        var buf = gremlin.Reader.init(src);
        var res = PublicKeyReader{};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                PublicKeyWire.TYPE_WIRE => {
                  const result = try buf.readInt32(offset);
                  offset += result.size;
                  res._type = @enumFromInt(result.value);
                },
                PublicKeyWire.DATA_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._data = result.value;
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(_: *const PublicKeyReader) void { }
    
    pub inline fn getType(self: *const PublicKeyReader) KeyType { return self._type; }
    pub inline fn getData(self: *const PublicKeyReader) []const u8 { return self._data orelse &[_]u8{}; }
};

const PrivateKeyWire = struct {
    const TYPE_WIRE: gremlin.ProtoWireNumber = 1;
    const DATA_WIRE: gremlin.ProtoWireNumber = 2;
};

pub const PrivateKey = struct {
    // fields
    type: KeyType = @enumFromInt(0),
    data: ?[]const u8 = null,

    pub fn calcProtobufSize(self: *const PrivateKey) usize {
        var res: usize = 0;
        if (@intFromEnum(self.type) != 0) { res += gremlin.sizes.sizeWireNumber(PrivateKeyWire.TYPE_WIRE) + gremlin.sizes.sizeI32(@intFromEnum(self.type)); }
        if (self.data) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(PrivateKeyWire.DATA_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        return res;
    }

    pub fn encode(self: *const PrivateKey, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const PrivateKey, target: *gremlin.Writer) void {
        if (@intFromEnum(self.type) != 0) { target.appendInt32(PrivateKeyWire.TYPE_WIRE, @intFromEnum(self.type)); }
        if (self.data) |v| {
            if (v.len > 0) {
                target.appendBytes(PrivateKeyWire.DATA_WIRE, v);
            }
        }
    }
};

pub const PrivateKeyReader = struct {
    _type: KeyType = @enumFromInt(0),
    _data: ?[]const u8 = null,

    pub fn init(_: std.mem.Allocator, src: []const u8) gremlin.Error!PrivateKeyReader {
        var buf = gremlin.Reader.init(src);
        var res = PrivateKeyReader{};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                PrivateKeyWire.TYPE_WIRE => {
                  const result = try buf.readInt32(offset);
                  offset += result.size;
                  res._type = @enumFromInt(result.value);
                },
                PrivateKeyWire.DATA_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._data = result.value;
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(_: *const PrivateKeyReader) void { }
    
    pub inline fn getType(self: *const PrivateKeyReader) KeyType { return self._type; }
    pub inline fn getData(self: *const PrivateKeyReader) []const u8 { return self._data orelse &[_]u8{}; }
};

