//! libp2p pubsub message signing + verification (the StrictSign policy).
//!
//! Sign: clear `signature`/`key`, marshal to protobuf, prepend ASCII
//! `libp2p-pubsub:`, sign with the host private key. The signature, the
//! marshaled libp2p `PublicKey` protobuf, and the marshaled peer-id go in
//! `Message.signature`/`.key`/`.from`. Verify reverses it: get the pubkey,
//! confirm `from` is its derived peer-id, reconstruct the exact signing input,
//! and check the signature; any mismatch rejects.
//!
//! Wire-parity anchor: go-libp2p OMITS `Message.key` when the pubkey is
//! recoverable from `from` (Ed25519 identity-multihash peer-ids) and includes
//! it otherwise. We always include it on send (spec-valid, simpler) but accept
//! its absence on receive.
//!
//! This is the ONLY place that knows the signing byte layout; it reuses the
//! identity / peer-id / TLS crypto primitives rather than hand-rolling crypto.

const std = @import("std");
const gremlin = @import("gremlin");
const rpc_pb = @import("../../protobuf.zig").rpc;
const identity = @import("../../identity.zig");
const tls = @import("../../security/tls.zig");
const keys = @import("peer_id").keys;
const PeerId = @import("peer_id").PeerId;

const Allocator = std.mem.Allocator;

/// Errors building the signing input: protobuf marshaling (gremlin, which
/// already includes OutOfMemory) plus the prefix allocation.
pub const SigningInputError = gremlin.Error || Allocator.Error;

/// The fixed prefix prepended to a message's marshaled bytes before signing.
/// These are the raw ASCII bytes of "libp2p-pubsub:" (14 bytes, no terminator).
pub const signing_prefix = "libp2p-pubsub:";

/// Builds the exact signing input: `signing_prefix` followed by the protobuf
/// marshaling of the message WITH `signature`/`key` absent (the encoder omits
/// null fields). Caller owns the returned slice.
pub fn signingInput(
    allocator: Allocator,
    from: []const u8,
    seqno: []const u8,
    topic: []const u8,
    data: []const u8,
) SigningInputError![]u8 {
    const msg = rpc_pb.Message{
        .from = from,
        .data = data,
        .seqno = seqno,
        .topic = topic,
        .signature = null,
        .key = null,
    };
    const marshaled = try msg.encode(allocator);
    defer if (marshaled.len > 0) allocator.free(marshaled);

    const out = try allocator.alloc(u8, signing_prefix.len + marshaled.len);
    @memcpy(out[0..signing_prefix.len], signing_prefix);
    @memcpy(out[signing_prefix.len..], marshaled);
    return out;
}

/// If `from` is an identity-multihash peer-id (`0x00 <len> <pubkey-proto>`,
/// the form used for small keys like Ed25519), return the embedded marshaled
/// libp2p PublicKey protobuf. Otherwise (e.g. a sha256-hashed peer-id,
/// `0x12 0x20 <digest>`) the pubkey is NOT recoverable, so return null — such a
/// message must carry `key` to be verifiable.
fn recoverKeyFromPeerId(from: []const u8) ?[]const u8 {
    // Identity multihash: code 0x00, then a varint length, then the digest which
    // (for an inlined key) IS the marshaled PublicKey proto. The lengths libp2p
    // inlines are well under 0x80, so the length is a single byte.
    if (from.len < 2 or from[0] != 0x00) return null;
    const len: usize = from[1];
    if (len == 0 or 2 + len != from.len) return null;
    return from[2 .. 2 + len];
}

/// Verify a signed message per the StrictSign scheme. Returns true only when:
///   - the publisher's libp2p PublicKey protobuf is available (from the `key`
///     field, or recovered from `from` when `key` is absent),
///   - the peer-id derived from that pubkey equals `from`,
///   - and the signature checks out over `signing_prefix ++ marshal(msg)`.
/// Fail-closed: any parse/derivation/crypto failure returns false (reject), so a
/// malformed or unsigned (empty-signature) message is simply dropped. An empty
/// `key` is acceptable only when the pubkey is recoverable from `from`.
pub fn verifyMessage(
    allocator: Allocator,
    from: []const u8,
    seqno: []const u8,
    topic: []const u8,
    data: []const u8,
    signature: []const u8,
    key: []const u8,
) bool {
    if (signature.len == 0 or from.len == 0) return false;

    // The PublicKey protobuf comes from `key` if present, else is recovered from
    // an identity-multihash `from`.
    const key_proto: []const u8 = if (key.len > 0) key else (recoverKeyFromPeerId(from) orelse return false);

    // Parse the libp2p PublicKey protobuf.
    const reader = keys.PublicKeyReader.init(key_proto) catch return false;
    var pubkey = keys.PublicKey{
        .type = reader.getType(),
        .data = allocator.dupe(u8, reader.getData()) catch return false,
    };
    defer if (pubkey.data) |d| allocator.free(d);
    if (pubkey.data.?.len == 0) return false;

    // The `from` must be the peer-id derived from this pubkey (binds the key to
    // the claimed sender; otherwise anyone could sign as anyone).
    const derived = PeerId.fromPublicKey(allocator, &pubkey) catch return false;
    if (!std.mem.eql(u8, derived.bytes[0..derived.len], from)) return false;

    const input = signingInput(allocator, from, seqno, topic, data) catch return false;
    defer allocator.free(input);

    return tls.verifyHostSignature(&pubkey, input, signature) catch false;
}

/// Holds a publisher's key plus the once-computed `Message.key`/`.from` bytes,
/// so per-publish signing avoids re-marshaling the pubkey. `key` is borrowed
/// (the host KeyPair must outlive the Signer); the cached pubkey bytes are owned
/// and freed on `deinit`.
pub const Signer = struct {
    allocator: Allocator,
    key: *const identity.KeyPair,
    /// Marshaled libp2p PublicKey protobuf for `key`. Owned; freed on deinit.
    pubkey_proto: []const u8,
    /// This node's peer-id; `from_bytes` slices into it.
    from_peer: PeerId,

    pub fn init(allocator: Allocator, key: *const identity.KeyPair) !Signer {
        const pubkey_proto = try key.publicKeyBytes(allocator);
        errdefer allocator.free(pubkey_proto);
        const from_peer = try key.peerId(allocator);
        return .{
            .allocator = allocator,
            .key = key,
            .pubkey_proto = pubkey_proto,
            .from_peer = from_peer,
        };
    }

    pub fn deinit(self: *Signer) void {
        self.allocator.free(self.pubkey_proto);
        self.* = undefined;
    }

    /// The marshaled peer-id bytes used as `Message.from`. Valid for the Signer's
    /// lifetime (slices the cached `from_peer`).
    pub fn fromBytes(self: *const Signer) []const u8 {
        return self.from_peer.bytes[0..self.from_peer.len];
    }

    /// The marshaled libp2p PublicKey protobuf used as `Message.key`. Valid for
    /// the Signer's lifetime (the cached `pubkey_proto`).
    pub fn keyBytes(self: *const Signer) []const u8 {
        return self.pubkey_proto;
    }

    /// Produce the signature for a message with the given fields (signature/key
    /// absent). The caller owns the returned slice and must free it. `from`
    /// should be `fromBytes()` (the publisher's peer-id bytes).
    pub fn sign(
        self: *const Signer,
        from: []const u8,
        seqno: []const u8,
        topic: []const u8,
        data: []const u8,
    ) ![]u8 {
        const input = try signingInput(self.allocator, from, seqno, topic, data);
        defer self.allocator.free(input);
        return self.key.signData(self.allocator, input);
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "signing input is the prefix followed by the marshaled message" {
    const allocator = std.testing.allocator;
    const from = "\x00\x24from-peer-id-bytes";
    const seqno = "\x00\x00\x00\x00\x00\x00\x00\x01";
    const topic = "t";
    const data = "hello";

    const input = try signingInput(allocator, from, seqno, topic, data);
    defer allocator.free(input);

    // Prefix is the literal ASCII bytes, no terminator.
    try std.testing.expectEqualSlices(u8, signing_prefix, input[0..signing_prefix.len]);

    // The tail must equal marshal(Message{from,data,seqno,topic}) with sig/key nil.
    const expected_msg = rpc_pb.Message{ .from = from, .data = data, .seqno = seqno, .topic = topic };
    const expected_tail = try expected_msg.encode(allocator);
    defer allocator.free(expected_tail);
    try std.testing.expectEqualSlices(u8, expected_tail, input[signing_prefix.len..]);
}

test "sign then verify round trips; tampering rejects" {
    const allocator = std.testing.allocator;

    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();

    var signer = try Signer.init(allocator, &key);
    defer signer.deinit();

    const from = signer.fromBytes();
    const seqno = "\x00\x00\x00\x00\x00\x00\x00\x07";
    const topic = "topic-a";
    const data = "payload";

    const sig = try signer.sign(from, seqno, topic, data);
    defer allocator.free(sig);

    // A correctly signed message verifies against the publisher's key bytes.
    try std.testing.expect(verifyMessage(allocator, from, seqno, topic, data, sig, signer.keyBytes()));

    // Tampered data must NOT verify (signature is over the original bytes).
    try std.testing.expect(!verifyMessage(allocator, from, seqno, topic, "PAYLOAD", sig, signer.keyBytes()));

    // Tampered signature (flip a byte) must not verify.
    var bad_sig = try allocator.dupe(u8, sig);
    defer allocator.free(bad_sig);
    bad_sig[0] ^= 0xff;
    try std.testing.expect(!verifyMessage(allocator, from, seqno, topic, data, bad_sig, signer.keyBytes()));

    // A wrong `from` (not the key's peer-id) must not verify.
    try std.testing.expect(!verifyMessage(allocator, "\x00\x04nope", seqno, topic, data, sig, signer.keyBytes()));

    // Empty signature (an unsigned message) must not verify.
    try std.testing.expect(!verifyMessage(allocator, from, seqno, topic, data, "", signer.keyBytes()));

    // Empty `key` still verifies for an Ed25519 identity-multihash peer-id: the
    // pubkey is recovered from `from`.
    try std.testing.expect(verifyMessage(allocator, from, seqno, topic, data, sig, ""));
    // ...but with a tampered signature, recovery does not save it.
    try std.testing.expect(!verifyMessage(allocator, from, seqno, topic, data, bad_sig, ""));
}

test "a message signed by one key does not verify under another key's bytes" {
    const allocator = std.testing.allocator;

    var key_a = try identity.KeyPair.generate(.ED25519);
    defer key_a.deinit();
    var key_b = try identity.KeyPair.generate(.ED25519);
    defer key_b.deinit();

    var signer_a = try Signer.init(allocator, &key_a);
    defer signer_a.deinit();
    var signer_b = try Signer.init(allocator, &key_b);
    defer signer_b.deinit();

    const seqno = "\x00\x00\x00\x00\x00\x00\x00\x01";
    const sig = try signer_a.sign(signer_a.fromBytes(), seqno, "t", "d");
    defer allocator.free(sig);

    // Presenting B's from+key with A's signature must fail the from/key bind.
    try std.testing.expect(!verifyMessage(allocator, signer_b.fromBytes(), seqno, "t", "d", sig, signer_b.keyBytes()));
}
