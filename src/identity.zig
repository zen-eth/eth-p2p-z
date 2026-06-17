const std = @import("std");
const ssl = @import("ssl").c;
const tls = @import("security/tls.zig");
const secp = @import("secp256k1");
const secp_context = @import("secp_context.zig");
const keys = @import("peer_id").keys;
const PeerId = @import("peer_id").PeerId;

const Allocator = std.mem.Allocator;

pub const Error = tls.SignCallbackError;

pub fn signWithKeyPair(ctx: ?*anyopaque, allocator: Allocator, data: []const u8) Error![]u8 {
    const key_pair_ptr: *const KeyPair = @ptrCast(@alignCast(ctx orelse return error.KeyMaterialReleased));
    return key_pair_ptr.signData(allocator, data);
}

pub const Backend = enum {
    tls,
    secp256k1,
};

pub const KeyPair = struct {
    key_type: keys.KeyType,
    backend: Backend,
    storage: Storage,

    const Self = @This();

    pub const Storage = union(enum) {
        tls: ?*ssl.EVP_PKEY,
        secp256k1: Secp256k1Storage,
    };

    const Secp256k1Storage = struct {
        secret: [32]u8,
        released: bool = false,

        fn markReleased(self: *Secp256k1Storage) void {
            if (!self.released) {
                @memset(self.secret[0..], 0);
                self.released = true;
            }
        }
    };

    fn generateSecp256k1Storage() Secp256k1Storage {
        const secret = secp.SecretKey.generate();
        return .{ .secret = secret.secretBytes() };
    }

    fn createSecp256k1PublicKey(allocator: Allocator, secret: [32]u8) Error!keys.PublicKey {
        const sk = secp.SecretKey.fromSlice(&secret) catch return error.InvalidData;

        const context = secp_context.get();
        const pk = secp.PublicKey.fromSecretKey(context.*, sk);
        const compressed = pk.serialize();

        const data = try allocator.alloc(u8, compressed.len);
        @memcpy(data, compressed[0..]);

        return .{
            .type = .SECP256K1,
            .data = data,
        };
    }

    fn signWithSecp256k1(allocator: Allocator, secret: [32]u8, data: []const u8) Error![]u8 {
        var secret_key = secp.SecretKey.fromSlice(&secret) catch return error.InvalidData;

        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update(data);
        const digest = hasher.finalResult();

        var message = secp.Message{ .inner = digest };

        const context = secp_context.get();
        const signature = context.signEcdsa(&message, &secret_key);
        const serialized = signature.serializeDer();

        const buf = try allocator.alloc(u8, serialized.len);
        @memcpy(buf, serialized.data[0..serialized.len]);
        return buf;
    }

    /// Generates a new key pair for the requested key type using the
    /// appropriate backend implementation.
    pub fn generate(key_type: keys.KeyType) Error!Self {
        return switch (key_type) {
            .SECP256K1 => .{
                .key_type = key_type,
                .backend = .secp256k1,
                .storage = .{ .secp256k1 = generateSecp256k1Storage() },
            },
            else => blk: {
                const tls_pkey = try tls.generateKeyPair(key_type);
                break :blk .{
                    .key_type = key_type,
                    .backend = .tls,
                    .storage = .{ .tls = tls_pkey },
                };
            },
        };
    }

    /// Deterministically derives an Ed25519 KeyPair from a raw 32-byte seed, so
    /// nodes agree on each other's peer-id without coordination. Byte-identical
    /// to go-libp2p (`ed25519.NewKeyFromSeed(seed)`) for the same seed.
    pub fn fromEd25519Seed(seed: []const u8) Error!Self {
        const pkey = tls.ed25519KeyFromSeed(seed) catch return error.OpenSSLFailed;
        return .{
            .key_type = .ED25519,
            .backend = .tls,
            .storage = .{ .tls = pkey },
        };
    }

    /// Creates a KeyPair that takes ownership of the provided TLS key.
    pub fn fromTlsOwned(key_type: keys.KeyType, pkey: *ssl.EVP_PKEY) Self {
        return .{
            .key_type = key_type,
            .backend = .tls,
            .storage = .{ .tls = pkey },
        };
    }

    /// Releases any resources associated with the key pair.
    pub fn deinit(self: *Self) void {
        const storage_ptr = &self.storage;
        switch (storage_ptr.*) {
            .tls => |maybe_key| {
                if (maybe_key) |pkey| {
                    ssl.EVP_PKEY_free(pkey);
                }
                storage_ptr.* = .{ .tls = null };
            },
            .secp256k1 => |*secp_storage| {
                secp_storage.markReleased();
            },
        }
    }

    /// Returns the underlying TLS key pointer without transferring ownership.
    pub fn tlsKey(self: *const Self) Error!*ssl.EVP_PKEY {
        return switch (self.storage) {
            .tls => |maybe_key| maybe_key orelse return error.KeyMaterialReleased,
            .secp256k1 => return error.UnsupportedBackend,
        };
    }

    /// Encodes the public key as a libp2p PublicKey protobuf structure.
    pub fn publicKey(self: *const Self, allocator: Allocator) Error!keys.PublicKey {
        return switch (self.storage) {
            .tls => |maybe_key| blk: {
                const pkey = maybe_key orelse return error.KeyMaterialReleased;
                break :blk try tls.createProtobufEncodedPublicKey(allocator, pkey);
            },
            .secp256k1 => |secp_storage| blk: {
                if (secp_storage.released) {
                    return error.KeyMaterialReleased;
                }

                break :blk try createSecp256k1PublicKey(allocator, secp_storage.secret);
            },
        };
    }

    /// Encodes the public key as protobuf bytes. The caller owns the returned slice.
    pub fn publicKeyBytes(self: *const Self, allocator: Allocator) Error![]const u8 {
        var public_key = try self.publicKey(allocator);
        defer if (public_key.data) |data| allocator.free(data);

        return public_key.encode(allocator);
    }

    /// Signs opaque data using the active backend.
    pub fn signData(self: *const Self, allocator: Allocator, data: []const u8) Error![]u8 {
        return switch (self.storage) {
            .tls => |maybe_key| blk: {
                const pkey = maybe_key orelse return error.KeyMaterialReleased;
                break :blk try tls.signData(allocator, pkey, data);
            },
            .secp256k1 => |secp_storage| blk: {
                if (secp_storage.released) {
                    return error.KeyMaterialReleased;
                }

                break :blk try signWithSecp256k1(allocator, secp_storage.secret, data);
            },
        };
    }

    /// Derives a PeerId from the key pair's public key.
    pub fn peerId(self: *const Self, allocator: Allocator) Error!PeerId {
        var public_key = try self.publicKey(allocator);
        defer if (public_key.data) |data| allocator.free(data);

        return PeerId.fromPublicKey(allocator, &public_key);
    }
};

test "deterministic Ed25519 seed peer-id matches go-libp2p" {
    // Seed is little-endian(nodeId) in the first 8 bytes, zero after. go-libp2p's
    // own test pins the SHA-256 of the `>nodeId:peerIdBase58\n` lines for nodeId
    // 0..9999 to this hash; reproducing it proves our seed->peer-id path matches.
    const allocator = std.testing.allocator;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var line_buf: [128]u8 = undefined;
    var node_id: u64 = 0;
    while (node_id < 10_000) : (node_id += 1) {
        var seed = [_]u8{0} ** 32;
        std.mem.writeInt(u64, seed[0..8], node_id, .little);
        var kp = try KeyPair.fromEd25519Seed(&seed);
        defer kp.deinit();
        const pid = try kp.peerId(allocator);
        const pid_str = try pid.toString(allocator);
        defer allocator.free(pid_str);
        const line = try std.fmt.bufPrint(&line_buf, ">{d}:{s}\n", .{ node_id, pid_str });
        hasher.update(line);
    }
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    var hex_buf: [64]u8 = undefined;
    const hex = try std.fmt.bufPrint(&hex_buf, "{x}", .{&digest});
    try std.testing.expectEqualStrings(
        "11395ea896d00ca25f7f648ebb336488ee092096a5498d90d76b92eaec27867a",
        hex,
    );
}
