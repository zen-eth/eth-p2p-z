const std = @import("std");
const base58_alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub const keys = struct {
    pub const KeyType = enum(i32) {
        RSA = 0,
        ED25519 = 1,
        SECP256K1 = 2,
        ECDSA = 3,
    };

    pub const PublicKey = struct {
        type: keys.KeyType = .RSA,
        data: ?[]const u8 = null,

        pub fn calcProtobufSize(self: *const @This()) usize {
            var size: usize = 0;
            if (self.type != .RSA) size += 2;
            if (self.data) |bytes| {
                if (bytes.len > 0) {
                    size += 1 + varintLen(bytes.len) + bytes.len;
                }
            }
            return size;
        }

        pub fn encode(self: *const @This(), allocator: std.mem.Allocator) ![]const u8 {
            const out = try allocator.alloc(u8, self.calcProtobufSize());
            var pos: usize = 0;
            if (self.type != .RSA) {
                out[pos] = 0x08;
                pos += 1;
                pos += writeVarint(out[pos..], @intCast(@intFromEnum(self.type)));
            }
            if (self.data) |bytes| {
                if (bytes.len > 0) {
                    out[pos] = 0x12;
                    pos += 1;
                    pos += writeVarint(out[pos..], bytes.len);
                    @memcpy(out[pos..][0..bytes.len], bytes);
                }
            }
            return out;
        }
    };

    pub const PublicKeyReader = struct {
        key_type: keys.KeyType = .RSA,
        key_data: ?[]const u8 = null,

        pub fn init(bytes: []const u8) !@This() {
            var reader: @This() = .{};
            var pos: usize = 0;
            while (pos < bytes.len) {
                const tag = bytes[pos];
                pos += 1;
                switch (tag) {
                    0x08 => {
                        const value = try readVarint(bytes, &pos);
                        reader.key_type = @enumFromInt(@as(i32, @intCast(value)));
                    },
                    0x12 => {
                        const len: usize = @intCast(try readVarint(bytes, &pos));
                        if (len > bytes.len - pos) return error.InvalidData;
                        reader.key_data = bytes[pos..][0..len];
                        pos += len;
                    },
                    else => return error.InvalidTag,
                }
            }
            return reader;
        }

        pub fn getType(self: *const @This()) keys.KeyType {
            return self.key_type;
        }

        pub fn getData(self: *const @This()) []const u8 {
            return self.key_data orelse &.{};
        }
    };

    pub const PrivateKey = struct {};
    pub const PrivateKeyReader = struct {};
};

pub const PublicKey = keys.PublicKey;
pub const PrivateKey = keys.PrivateKey;
pub const KeyType = keys.KeyType;
pub const PublicKeyReader = keys.PublicKeyReader;
pub const PrivateKeyReader = keys.PrivateKeyReader;

pub const PeerId = struct {
    bytes: [64]u8,
    len: usize,

    pub fn fromPublicKey(allocator: std.mem.Allocator, public_key: *keys.PublicKey) !PeerId {
        const encoded = try public_key.encode(allocator);
        defer allocator.free(encoded);

        if (encoded.len <= 42) {
            var out: [64]u8 = undefined;
            out[0] = 0x00;
            out[1] = @intCast(encoded.len);
            @memcpy(out[2..][0..encoded.len], encoded);
            return .{ .bytes = out, .len = 2 + encoded.len };
        }

        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});

        var out: [64]u8 = undefined;
        out[0] = 0x12;
        out[1] = 0x20;
        @memcpy(out[2..34], &digest);
        return .{ .bytes = out, .len = 34 };
    }

    pub fn fromBytes(bytes: []const u8) !PeerId {
        if (bytes.len > 64) return error.InvalidSize;
        var out: [64]u8 = undefined;
        @memcpy(out[0..bytes.len], bytes);
        return .{ .bytes = out, .len = bytes.len };
    }

    pub fn fromString(allocator: std.mem.Allocator, text: []const u8) !PeerId {
        const bytes = try base58Decode(allocator, text);
        defer allocator.free(bytes);
        return fromBytes(bytes);
    }

    pub fn random() !PeerId {
        var out: [64]u8 = undefined;
        out[0] = 0x00;
        out[1] = 32;
        // Fill the 32-byte identity-hash portion with real OS entropy so each
        // call returns a DISTINCT peer id. arc4random_buf is in libc on macOS
        // and glibc Linux (this build links libc), and cannot fail.
        std.c.arc4random_buf(out[2..34].ptr, 32);
        return .{ .bytes = out, .len = 34 };
    }

    pub fn toBytesLen(self: *const PeerId) usize {
        return self.len;
    }

    pub fn toBytes(self: *const PeerId, dest: []u8) ![]const u8 {
        if (dest.len < self.len) return error.NoSpaceLeft;
        @memcpy(dest[0..self.len], self.bytes[0..self.len]);
        return dest[0..self.len];
    }

    pub fn toString(self: *const PeerId, allocator: std.mem.Allocator) ![]u8 {
        return base58Encode(allocator, self.bytes[0..self.len]);
    }

    pub fn eql(self: *const PeerId, other: *const PeerId) bool {
        return std.mem.eql(u8, self.bytes[0..self.len], other.bytes[0..other.len]);
    }
};

fn varintLen(value: usize) usize {
    var v = value;
    var len: usize = 1;
    while (v >= 0x80) : (len += 1) v >>= 7;
    return len;
}

fn writeVarint(dest: []u8, value: u64) usize {
    var v = value;
    var pos: usize = 0;
    while (v >= 0x80) {
        dest[pos] = @as(u8, @truncate(v)) | 0x80;
        pos += 1;
        v >>= 7;
    }
    dest[pos] = @truncate(v);
    return pos + 1;
}

fn readVarint(bytes: []const u8, pos: *usize) !u64 {
    var value: u64 = 0;
    var shift: u6 = 0;
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        if (pos.* >= bytes.len) return error.InvalidVarInt;
        const byte = bytes[pos.*];
        pos.* += 1;
        value |= @as(u64, byte & 0x7f) << shift;
        if (byte < 0x80) return value;
        if (shift > 57) return error.InvalidVarInt;
        shift += 7;
    }
    return error.InvalidVarInt;
}

fn base58Encode(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    if (input.len == 0) return allocator.alloc(u8, 0);

    var zeros: usize = 0;
    while (zeros < input.len and input[zeros] == 0) : (zeros += 1) {}

    const size = ((input.len - zeros) * 138 / 100) + 1;
    var digits = try allocator.alloc(u8, size);
    defer allocator.free(digits);
    @memset(digits, 0);

    var length: usize = 0;
    for (input[zeros..]) |byte| {
        var carry: usize = byte;
        var i: usize = 0;
        while (carry != 0 or i < length) : (i += 1) {
            const index = size - 1 - i;
            carry += @as(usize, digits[index]) << 8;
            digits[index] = @intCast(carry % 58);
            carry /= 58;
        }
        length = i;
    }

    const out = try allocator.alloc(u8, zeros + length);
    @memset(out[0..zeros], '1');
    const start = size - length;
    for (digits[start..], 0..) |digit, i| out[zeros + i] = base58_alphabet[digit];
    return out;
}

fn base58Decode(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    if (text.len == 0) return allocator.alloc(u8, 0);

    var zeros: usize = 0;
    while (zeros < text.len and text[zeros] == '1') : (zeros += 1) {}

    const size = ((text.len - zeros) * 733 / 1000) + 1;
    var bytes = try allocator.alloc(u8, size);
    defer allocator.free(bytes);
    @memset(bytes, 0);

    var length: usize = 0;
    for (text[zeros..]) |char| {
        const value = base58Value(char) orelse return error.InvalidBase58;
        var carry: usize = value;
        var i: usize = 0;
        while (carry != 0 or i < length) : (i += 1) {
            const index = size - 1 - i;
            carry += @as(usize, bytes[index]) * 58;
            bytes[index] = @intCast(carry & 0xff);
            carry >>= 8;
        }
        length = i;
    }

    const out = try allocator.alloc(u8, zeros + length);
    @memset(out[0..zeros], 0);
    @memcpy(out[zeros..], bytes[size - length ..]);
    return out;
}

fn base58Value(char: u8) ?u8 {
    for (base58_alphabet, 0..) |candidate, i| {
        if (candidate == char) return @intCast(i);
    }
    return null;
}

test "public key peer id round trip" {
    const data = [_]u8{ 1, 2, 3 };
    var pk = keys.PublicKey{ .type = .ED25519, .data = &data };
    const id = try PeerId.fromPublicKey(std.testing.allocator, &pk);
    var buf: [64]u8 = undefined;
    const bytes = try id.toBytes(&buf);
    const decoded = try PeerId.fromBytes(bytes);
    try std.testing.expect(id.eql(&decoded));
}

test "peer id base58 string round trip" {
    const data = [_]u8{ 1, 2, 3 };
    var pk = keys.PublicKey{ .type = .ED25519, .data = &data };
    const id = try PeerId.fromPublicKey(std.testing.allocator, &pk);
    const text = try id.toString(std.testing.allocator);
    defer std.testing.allocator.free(text);
    const decoded = try PeerId.fromString(std.testing.allocator, text);
    try std.testing.expect(id.eql(&decoded));
}
