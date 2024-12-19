const std = @import("std");
const multibase = @import("multiformats-zig").multibase;
const base58 = multibase.MultiBaseCodec.base58;
const multihash = @import("multiformats-zig").multihash;
const Multicodec = @import("multiformats-zig").multicodec.Multicodec;
const VarintError = @import("multiformats-zig").uvarint.VarintError;
// const sha2 = @import("sha2");
// const PublicKey = @import("keypair.zig").PublicKey;

const MAX_INLINE_KEY_LENGTH: usize = 42;
const MULTIHASH_IDENTITY_CODE: u64 = 0;
const MULTIHASH_SHA256_CODE: u64 = 0x12;

pub const ParseError = error{
    B58DecodeError,
    UnsupportedCode,
    InvalidMultihash,
};

pub const PeerId = struct {
    multihash: multihash.Multihash(64),

    // pub fn fromPublicKey(key: *const PublicKey) !PeerId {
    //     const key_enc = try key.encodeProtobuf();
    //
    //     const hash = if (key_enc.len <= MAX_INLINE_KEY_LENGTH) {
    //         try multihash.Multihash(64).wrap(MULTIHASH_IDENTITY_CODE, key_enc)
    //     } else {
    //         var hasher = sha2.Sha256.init();
    //         hasher.update(key_enc);
    //         const digest = hasher.finalResult();
    //         try multihash.Multihash(64).wrap(MULTIHASH_SHA256_CODE, &digest)
    //     };
    //
    //     return PeerId{ .multihash = hash };
    // }

    pub fn fromBytes(data: []const u8) !PeerId {
        const hash = try multihash.Multihash(64).readBytes(data);
        return fromMultihash(hash);
    }

    pub fn fromMultihash(hash: multihash.Multihash(64)) !PeerId {
        switch (hash.getCode()) {
            Multicodec.SHA2_256 => return PeerId{ .multihash = hash },
            Multicodec.IDENTITY => {
                if (hash.getDigest().len <= MAX_INLINE_KEY_LENGTH) {
                    return PeerId{ .multihash = hash };
                }
                return ParseError.UnsupportedCode;
            },
            else => return ParseError.UnsupportedCode,
        }
    }

    pub fn random(rand: std.Random) !PeerId {
        var bytes: [32]u8 = undefined;
        rand.bytes(&bytes);
        const hash = try multihash.Multihash(64).wrap(Multicodec.IDENTITY, &bytes);
        return PeerId{ .multihash = hash };
    }

    pub fn toBytes(self: *const PeerId, allocator: std.mem.Allocator) ![]const u8 {
        return self.multihash.toBytes(allocator);
    }

    pub fn toBase58(self: *const PeerId, allocator: std.mem.Allocator) ![]const u8 {
        const bytes = try self.toBytes(allocator);
        const needed_size = multibase.MultiBaseCodec.Base58Btc.calcSize(bytes) - 1; // -1 for remove the multibase prefix 'z'
        const dest = try allocator.alloc(u8, needed_size);
        return base58.encodeBtc(dest, bytes);
    }
};

test "PeerId - random generation and bytes conversion" {
    const testing = std.testing;
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    // Test multiple random peer IDs
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        const peer_id = try PeerId.random(random);
        const bytes = try peer_id.toBytes(testing.allocator);
        defer testing.allocator.free(bytes);
        const decoded = try PeerId.fromBytes(bytes);
        try testing.expectEqualDeep(peer_id, decoded);
    }
}

// test "PeerId - base58 encoding and decoding" {
//     const testing = std.testing;
//     var prng = std.rand.DefaultPrng.init(0);
//     const random = prng.random();
//
//     const peer_id = try PeerId.random(random);
//     const base58_str = try peer_id.toBase58(testing.allocator);
//     defer testing.allocator.free(base58_str);
//
//     // TODO: Add fromBase58 implementation and test round-trip conversion
//     // const decoded = try PeerId.fromBase58(testing.allocator, base58_str);
//     // try testing.expectEqualDeep(peer_id, decoded);
// }
//
test "PeerId - invalid multihash code" {
    const testing = std.testing;

    // Create an invalid multihash with unsupported code
    var invalid_bytes = [_]u8{0xFF} ** 32;
    try testing.expectError(VarintError.Overflow, PeerId.fromBytes(&invalid_bytes));
}

test "PeerId - unsupported code error" {
    const testing = std.testing;

    // Create a valid multihash but with an unsupported code (e.g. not SHA2_256 or IDENTITY)
    const hash = try multihash.Multihash(64).wrap(Multicodec.AES_128, &[_]u8{1} ** 32);
    try testing.expectError(error.UnsupportedCode, PeerId.fromMultihash(hash));
}
