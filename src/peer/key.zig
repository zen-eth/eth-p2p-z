const std = @import("std");
const crypto = std.crypto;
const ecdsa = std.crypto.sign.ecdsa;
const Ed25519 = std.crypto.sign.Ed25519;

pub const KeyType = enum {
    EcdsaP256Sha256,
    Ed25519,
    Secp256k1,
};

pub const PublicKey = union(KeyType) {
    EcdsaP256Sha256: ecdsa.EcdsaP256Sha256.PublicKey,
    Ed25519: Ed25519.PublicKey,
    Secp256k1: ecdsa.EcdsaSecp256k1Sha256oSha256.PublicKey,

    pub fn toBytesSize(self: PublicKey, is_compressed: bool) usize {
        return switch (self) {
            .EcdsaP256Sha256 => |pk| {
                const PkType = @TypeOf(pk);
                return if (is_compressed) PkType.compressed_sec1_encoded_length else PkType.uncompressed_sec1_encoded_length;
            },
            .Ed25519 => |pk| {
                const PkType = @TypeOf(pk);
                return PkType.encoded_length;
            },
            .Secp256k1 => |pk| {
                const PkType = @TypeOf(pk);
                return if (is_compressed) PkType.compressed_sec1_encoded_length else PkType.uncompressed_sec1_encoded_length;
            },
        };
    }

    pub fn toBytes(self: PublicKey, is_compressed: bool, buffer: []u8) []u8 {
        const written_len = switch (self) {
            .EcdsaP256Sha256 => |pk| blk: {
                if (is_compressed) {
                    const data = pk.toCompressedSec1();
                    @memcpy(buffer[0..data.len], &data);
                    break :blk data.len;
                } else {
                    const data = pk.toUncompressedSec1();
                    @memcpy(buffer[0..data.len], &data);
                    break :blk data.len;
                }
            },
            .Secp256k1 => |pk| blk: {
                if (is_compressed) {
                    const data = pk.toCompressedSec1();
                    @memcpy(buffer[0..data.len], &data);
                    break :blk data.len;
                } else {
                    const data = pk.toUncompressedSec1();
                    @memcpy(buffer[0..data.len], &data);
                    break :blk data.len;
                }
            },
            .Ed25519 => |pk| blk: {
                const data = pk.toBytes();
                @memcpy(buffer[0..data.len], &data);
                break :blk data.len;
            },
        };
        return buffer[0..written_len];
    }
};

pub const SecretKey = union(KeyType) {
    EcdsaP256Sha256: ecdsa.EcdsaP256Sha256.SecretKey,
    Ed25519: Ed25519.SecretKey,
    Secp256k1: ecdsa.EcdsaSecp256k1Sha256oSha256.SecretKey,

    pub fn toBytesSize(self: SecretKey) usize {
        return switch (self) {
            .EcdsaP256Sha256 => |sk| {
                const SkType = @TypeOf(sk);
                return SkType.encoded_length;
            },
            .Ed25519 => |sk| {
                const SkType = @TypeOf(sk);
                return SkType.encoded_length;
            },
            .Secp256k1 => |sk| {
                const SkType = @TypeOf(sk);
                return SkType.encoded_length;
            },
        };
    }

    pub fn toBytes(self: SecretKey, buffer: []u8) []u8 {
        const written_len = switch (self) {
            .EcdsaP256Sha256 => |sk| blk: {
                const data = sk.toBytes();
                @memcpy(buffer[0..data.len], &data);
                break :blk data.len;
            },
            .Ed25519 => |sk| blk: {
                const data = sk.toBytes();
                @memcpy(buffer[0..data.len], &data);
                break :blk data.len;
            },
            .Secp256k1 => |sk| blk: {
                const data = sk.toBytes();
                @memcpy(buffer[0..data.len], &data);
                break :blk data.len;
            },
        };
        return buffer[0..written_len];
    }
};

test "SecretKey and PublicKey toBytes" {
    const key_pair = Ed25519.KeyPair.generate();
    const secret_key = SecretKey{
        .Ed25519 = key_pair.secret_key,
    };
    const public_key = PublicKey{
        .Ed25519 = key_pair.public_key,
    };

    const secret_key_size = secret_key.toBytesSize();
    const secret_key_buffer = try std.testing.allocator.alloc(u8, secret_key_size);
    defer std.testing.allocator.free(secret_key_buffer);
    const secret_key_bytes = secret_key.toBytes(secret_key_buffer);
    try std.testing.expectEqualSlices(u8, secret_key_bytes, secret_key_buffer[0..secret_key_size]);

    const public_key_size = public_key.toBytesSize(false);
    const public_key_buffer = try std.testing.allocator.alloc(u8, public_key_size);
    defer std.testing.allocator.free(public_key_buffer);
    const public_key_bytes = public_key.toBytes(false, public_key_buffer);
    try std.testing.expectEqualSlices(u8, public_key_bytes, public_key_buffer[0..public_key_size]);
}
