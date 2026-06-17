//! libp2p signed Envelope + PeerRecord codec — advertises a node's listen
//! addresses. Verification is stateless: the embedded public key checks the
//! signature AND must hash to the peer-id named inside the record, so an
//! attacker cannot forge addresses for a peer it has no key for. Carried by
//! gossipsub peer-exchange (PRUNE).
//!
//! Wire format must byte-match go-libp2p / rust-libp2p:
//!
//!   Envelope protobuf (proto3, all `bytes` except the nested PublicKey message):
//!     field 1 (public_key): the libp2p crypto.PublicKey protobuf {Type, Data}
//!     field 2 (payload_type): the payload's multicodec (peer-record: 0x03 0x01)
//!     field 3 (payload): the marshaled PeerRecord
//!     field 5 (signature): host-key signature over the signing input below
//!
//!   PeerRecord protobuf (proto3):
//!     field 1 (peer_id): the peer-id in its binary multihash form
//!     field 2 (seq): a monotonic uint64 ordering records in time
//!     field 3 (addresses): repeated AddressInfo { field 1 (multiaddr): bytes }
//!
//!   Signing input (`makeUnsigned`): for each of [domain, payload_type, payload],
//!   uvarint length prefix || raw bytes, concatenated in order; `domain` is the
//!   constant ASCII "libp2p-peer-record".

const std = @import("std");
const gremlin = @import("gremlin");
const identity = @import("identity.zig");
const tls = @import("security/tls.zig");
const keys = @import("peer_id").keys;
const PeerId = @import("peer_id").PeerId;

const Allocator = std.mem.Allocator;

/// The domain string mixed into the signing input for peer records. Verifying a
/// signature requires knowing this domain, so a signature is only valid in the
/// peer-record context (a record cannot be replayed as a different envelope type).
pub const peer_record_domain = "libp2p-peer-record";

/// The Envelope `payload_type` for a peer record: the multiformats multicodec
/// table entry named "libp2p-peer-record".
pub const peer_record_payload_type = [_]u8{ 0x03, 0x01 };

/// Envelope protobuf field numbers (proto3).
const env_public_key_field: gremlin.ProtoWireNumber = 1;
const env_payload_type_field: gremlin.ProtoWireNumber = 2;
const env_payload_field: gremlin.ProtoWireNumber = 3;
const env_signature_field: gremlin.ProtoWireNumber = 5;

/// PeerRecord protobuf field numbers (proto3).
const rec_peer_id_field: gremlin.ProtoWireNumber = 1;
const rec_seq_field: gremlin.ProtoWireNumber = 2;
const rec_addresses_field: gremlin.ProtoWireNumber = 3;
/// AddressInfo (the nested message inside `addresses`) field number.
const addr_multiaddr_field: gremlin.ProtoWireNumber = 1;

pub const SealError = error{NoPeerIdInRecord} || tls.SignCallbackError || Allocator.Error || gremlin.Error;

pub const ConsumeError = error{
    InvalidSignature,
    PayloadTypeMismatch,
    PeerIdMismatch,
    MissingPublicKey,
    MissingPeerId,
} || gremlin.Error || Allocator.Error;

/// A peer record ready to be marshaled and signed. `peer_id` and `addrs` are
/// borrowed by the encode/seal helpers (not owned); `addrs` are already-marshaled
/// multiaddr bytes (each entry is one multiaddr in its binary form).
pub const PeerRecord = struct {
    peer_id: PeerId,
    seq: u64,
    addrs: []const []const u8,

    /// Marshals the PeerRecord to its protobuf bytes (caller owns the slice).
    /// `seq` is omitted when zero, per proto3.
    pub fn encode(self: *const PeerRecord, allocator: Allocator) Allocator.Error![]u8 {
        const id_bytes = self.peer_id.bytes[0..self.peer_id.len];

        var size: usize = 0;
        size += tagSize(rec_peer_id_field) + lenDelimSize(id_bytes.len);
        if (self.seq != 0) {
            size += tagSize(rec_seq_field) + gremlin.sizes.sizeU64(self.seq);
        }
        for (self.addrs) |addr| {
            const inner = addrInfoSize(addr);
            size += tagSize(rec_addresses_field) + lenDelimSize(inner);
        }

        const out = try allocator.alloc(u8, size);
        var w = gremlin.Writer.init(out);
        w.appendBytes(rec_peer_id_field, id_bytes);
        if (self.seq != 0) {
            w.appendUint64(rec_seq_field, self.seq);
        }
        for (self.addrs) |addr| {
            // AddressInfo is a length-delimited submessage wrapping one
            // `multiaddr` bytes field; write its length then its single field.
            w.appendBytesTag(rec_addresses_field, addrInfoSize(addr));
            w.appendBytes(addr_multiaddr_field, addr);
        }
        return out;
    }
};

/// A verified peer record returned by `consumeEnvelope`. Owns its address slices
/// and backing buffer; call `deinit` to free. `seq` and `peer_id` are copied out;
/// `addrs[i]` are marshaled-multiaddr byte slices (borrowing `backing`).
pub const ConsumedRecord = struct {
    peer_id: PeerId,
    seq: u64,
    addrs: [][]const u8,
    /// The address slices point into this owned copy of the record payload.
    backing: []u8,

    pub fn deinit(self: *ConsumedRecord, allocator: Allocator) void {
        allocator.free(self.addrs);
        allocator.free(self.backing);
        self.* = undefined;
    }
};

/// Marshals and signs `record` into an Envelope (caller owns the bytes).
/// `record.peer_id` should be `key`'s own peer-id, else the envelope fails the
/// consumer's key↔peer-id binding; only a non-empty id is enforced here.
pub fn sealPeerRecord(allocator: Allocator, key: *const identity.KeyPair, record: PeerRecord) SealError![]u8 {
    if (record.peer_id.len == 0) return error.NoPeerIdInRecord;

    const payload = try record.encode(allocator);
    defer allocator.free(payload);

    const unsigned = try makeUnsigned(allocator, peer_record_domain, &peer_record_payload_type, payload);
    defer allocator.free(unsigned);

    const signature = try key.signData(allocator, unsigned);
    defer allocator.free(signature);

    const pubkey_proto = try key.publicKeyBytes(allocator);
    defer allocator.free(pubkey_proto);

    return encodeEnvelope(allocator, pubkey_proto, &peer_record_payload_type, payload, signature);
}

/// Parses, verifies, and returns the peer record inside `envelope_bytes`
/// (owned by the caller; free with `deinit`). Rejected unless: (a) `payload_type`
/// is the peer-record codec, (b) the signature verifies against the embedded
/// public key, (c) that key hashes to the record's `peer_id` (key↔peer-id binding).
pub fn consumeEnvelope(allocator: Allocator, envelope_bytes: []const u8) ConsumeError!ConsumedRecord {
    const env = try parseEnvelope(envelope_bytes);

    // The payload must be a peer record (constant 2-byte codec).
    if (!std.mem.eql(u8, env.payload_type, &peer_record_payload_type)) {
        return error.PayloadTypeMismatch;
    }
    if (env.public_key.len == 0) return error.MissingPublicKey;

    // Recover the libp2p PublicKey from the embedded marshaled protobuf.
    const reader = keys.PublicKeyReader.init(env.public_key) catch return error.InvalidData;
    var pubkey = keys.PublicKey{
        .type = reader.getType(),
        .data = try allocator.dupe(u8, reader.getData()),
    };
    defer if (pubkey.data) |d| allocator.free(d);
    if (pubkey.data.?.len == 0) return error.MissingPublicKey;

    // Verify the signature over the EXACT received payload bytes (re-marshaling
    // could differ; the signature is over what was sent).
    const unsigned = try makeUnsigned(allocator, peer_record_domain, env.payload_type, env.payload);
    defer allocator.free(unsigned);
    const valid = tls.verifyHostSignature(&pubkey, unsigned, env.signature) catch return error.InvalidSignature;
    if (!valid) return error.InvalidSignature;

    // Parse the inner record (signature already proven over these bytes).
    var parsed = try parseRecord(allocator, env.payload);
    errdefer parsed.deinit(allocator);

    // Bind the key to the claimed peer: the public key must hash to the record's
    // peer-id, otherwise a valid signer could impersonate another peer's id.
    const derived = PeerId.fromPublicKey(allocator, &pubkey) catch return error.PeerIdMismatch;
    if (!std.mem.eql(u8, derived.bytes[0..derived.len], parsed.peer_id.bytes[0..parsed.peer_id.len])) {
        return error.PeerIdMismatch;
    }

    return parsed;
}

// ---------------------------------------------------------------------------
// Envelope encode / parse
// ---------------------------------------------------------------------------

const ParsedEnvelope = struct {
    public_key: []const u8,
    payload_type: []const u8,
    payload: []const u8,
    signature: []const u8,
};

fn encodeEnvelope(
    allocator: Allocator,
    public_key: []const u8,
    payload_type: []const u8,
    payload: []const u8,
    signature: []const u8,
) Allocator.Error![]u8 {
    var size: usize = 0;
    size += tagSize(env_public_key_field) + lenDelimSize(public_key.len);
    size += tagSize(env_payload_type_field) + lenDelimSize(payload_type.len);
    size += tagSize(env_payload_field) + lenDelimSize(payload.len);
    size += tagSize(env_signature_field) + lenDelimSize(signature.len);

    const out = try allocator.alloc(u8, size);
    var w = gremlin.Writer.init(out);
    w.appendBytes(env_public_key_field, public_key);
    w.appendBytes(env_payload_type_field, payload_type);
    w.appendBytes(env_payload_field, payload);
    w.appendBytes(env_signature_field, signature);
    return out;
}

fn parseEnvelope(bytes: []const u8) gremlin.Error!ParsedEnvelope {
    var env = ParsedEnvelope{ .public_key = &.{}, .payload_type = &.{}, .payload = &.{}, .signature = &.{} };
    const buf = gremlin.Reader.init(bytes);
    var offset: usize = 0;
    while (buf.hasNext(offset, 0)) {
        const tag = try buf.readTagAt(offset);
        offset += tag.size;
        switch (tag.number) {
            env_public_key_field => {
                const r = try buf.readBytes(offset);
                offset += r.size;
                env.public_key = r.value;
            },
            env_payload_type_field => {
                const r = try buf.readBytes(offset);
                offset += r.size;
                env.payload_type = r.value;
            },
            env_payload_field => {
                const r = try buf.readBytes(offset);
                offset += r.size;
                env.payload = r.value;
            },
            env_signature_field => {
                const r = try buf.readBytes(offset);
                offset += r.size;
                env.signature = r.value;
            },
            else => offset = try buf.skipData(offset, tag.wire),
        }
    }
    return env;
}

// ---------------------------------------------------------------------------
// PeerRecord parse
// ---------------------------------------------------------------------------

/// Parses a marshaled PeerRecord. Allocates an owned copy of `payload` so the
/// returned address slices stay valid independent of the caller's buffer.
fn parseRecord(allocator: Allocator, payload: []const u8) ConsumeError!ConsumedRecord {
    const backing = try allocator.dupe(u8, payload);
    errdefer allocator.free(backing);

    var peer_id_bytes: []const u8 = &.{};
    var seq: u64 = 0;
    var addr_count: usize = 0;

    // First pass: read scalar fields and count addresses (to size the slice).
    {
        const buf = gremlin.Reader.init(backing);
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                rec_peer_id_field => {
                    const r = try buf.readBytes(offset);
                    offset += r.size;
                    peer_id_bytes = r.value;
                },
                rec_seq_field => {
                    const r = try buf.readUInt64(offset);
                    offset += r.size;
                    seq = r.value;
                },
                rec_addresses_field => {
                    const r = try buf.readBytes(offset);
                    offset += r.size;
                    addr_count += 1;
                },
                else => offset = try buf.skipData(offset, tag.wire),
            }
        }
    }

    if (peer_id_bytes.len == 0) return error.MissingPeerId;
    const peer_id = PeerId.fromBytes(peer_id_bytes) catch return error.InvalidData;

    const addrs = try allocator.alloc([]const u8, addr_count);
    errdefer allocator.free(addrs);

    // Second pass: extract each AddressInfo's inner multiaddr bytes.
    {
        const buf = gremlin.Reader.init(backing);
        var offset: usize = 0;
        var i: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            if (tag.number == rec_addresses_field) {
                const r = try buf.readBytes(offset);
                offset += r.size;
                addrs[i] = try parseAddressInfo(r.value);
                i += 1;
            } else {
                offset = try buf.skipData(offset, tag.wire);
            }
        }
    }

    return .{ .peer_id = peer_id, .seq = seq, .addrs = addrs, .backing = backing };
}

/// Extracts the `multiaddr` bytes (field 1) from a marshaled AddressInfo
/// submessage. Unknown fields are skipped. Returns an empty slice if absent.
fn parseAddressInfo(bytes: []const u8) gremlin.Error![]const u8 {
    const buf = gremlin.Reader.init(bytes);
    var offset: usize = 0;
    var multiaddr: []const u8 = &.{};
    while (buf.hasNext(offset, 0)) {
        const tag = try buf.readTagAt(offset);
        offset += tag.size;
        if (tag.number == addr_multiaddr_field) {
            const r = try buf.readBytes(offset);
            offset += r.size;
            multiaddr = r.value;
        } else {
            offset = try buf.skipData(offset, tag.wire);
        }
    }
    return multiaddr;
}

// ---------------------------------------------------------------------------
// Signing-input construction + size helpers
// ---------------------------------------------------------------------------

/// Builds the byte string a peer record signs/verifies (see the module header
/// for the layout). Caller owns the returned slice.
fn makeUnsigned(
    allocator: Allocator,
    domain: []const u8,
    payload_type: []const u8,
    payload: []const u8,
) Allocator.Error![]u8 {
    const fields = [_][]const u8{ domain, payload_type, payload };
    var size: usize = 0;
    for (fields) |f| size += gremlin.sizes.sizeVarInt(f.len) + f.len;

    const out = try allocator.alloc(u8, size);
    var pos: usize = 0;
    for (fields) |f| {
        pos += writeUvarint(out[pos..], f.len);
        @memcpy(out[pos..][0..f.len], f);
        pos += f.len;
    }
    std.debug.assert(pos == size);
    return out;
}

/// Writes `value` as an unsigned LEB128 varint into `dest`, returning the count.
fn writeUvarint(dest: []u8, value: usize) usize {
    var v: u64 = value;
    var pos: usize = 0;
    while (v >= 0x80) {
        dest[pos] = @as(u8, @truncate(v)) | 0x80;
        pos += 1;
        v >>= 7;
    }
    dest[pos] = @truncate(v);
    return pos + 1;
}

/// Wire size of a field tag (the field number + wire type varint).
fn tagSize(field: gremlin.ProtoWireNumber) usize {
    return gremlin.sizes.sizeVarInt((@as(u64, @intCast(field)) << 3) | 2);
}

/// Wire size of a length-delimited field's `len`-prefix plus its `len` bytes.
fn lenDelimSize(len: usize) usize {
    return gremlin.sizes.sizeVarInt(len) + len;
}

/// Wire size of an AddressInfo submessage carrying `addr` as its multiaddr field.
fn addrInfoSize(addr: []const u8) usize {
    return tagSize(addr_multiaddr_field) + lenDelimSize(addr.len);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

fn fixtureAddrs() [2][]const u8 {
    // Two arbitrary marshaled-multiaddr byte strings (content is opaque to the
    // codec, which treats each address as raw bytes).
    return .{
        "\x04\x7f\x00\x00\x01\x91\x02\x11\xd9\x03", // /ip4/127.0.0.1/udp/4561/quic-v1 (illustrative bytes)
        "\x04\xc0\xa8\x00\x0a\x91\x02\x22\xb8\x03",
    };
}

test "seal then consume round-trips peer-id, seq, and addresses" {
    const allocator = testing.allocator;

    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();
    const peer_id = try key.peerId(allocator);

    const addrs = fixtureAddrs();
    const record = PeerRecord{ .peer_id = peer_id, .seq = 0x0102_0304_0506_0708, .addrs = &addrs };

    const envelope = try sealPeerRecord(allocator, &key, record);
    defer allocator.free(envelope);

    var consumed = try consumeEnvelope(allocator, envelope);
    defer consumed.deinit(allocator);

    // peer-id matches both the key's derived id and the record we sealed.
    try testing.expect(consumed.peer_id.eql(@constCast(&peer_id)));
    try testing.expectEqual(record.seq, consumed.seq);
    try testing.expectEqual(addrs.len, consumed.addrs.len);
    for (addrs, consumed.addrs) |want, got| {
        try testing.expectEqualSlices(u8, want, got);
    }
}

test "round-trip works with no addresses and zero seq" {
    const allocator = testing.allocator;

    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();
    const peer_id = try key.peerId(allocator);

    const record = PeerRecord{ .peer_id = peer_id, .seq = 0, .addrs = &.{} };
    const envelope = try sealPeerRecord(allocator, &key, record);
    defer allocator.free(envelope);

    var consumed = try consumeEnvelope(allocator, envelope);
    defer consumed.deinit(allocator);

    try testing.expectEqual(@as(u64, 0), consumed.seq);
    try testing.expectEqual(@as(usize, 0), consumed.addrs.len);
    try testing.expect(consumed.peer_id.eql(@constCast(&peer_id)));
}

test "round-trip works for a secp256k1 host key" {
    const allocator = testing.allocator;

    var key = try identity.KeyPair.generate(.SECP256K1);
    defer key.deinit();
    const peer_id = try key.peerId(allocator);

    const addrs = fixtureAddrs();
    const record = PeerRecord{ .peer_id = peer_id, .seq = 42, .addrs = &addrs };
    const envelope = try sealPeerRecord(allocator, &key, record);
    defer allocator.free(envelope);

    var consumed = try consumeEnvelope(allocator, envelope);
    defer consumed.deinit(allocator);

    try testing.expectEqual(@as(u64, 42), consumed.seq);
    try testing.expect(consumed.peer_id.eql(@constCast(&peer_id)));
}

test "tampering with the payload is rejected as an invalid signature" {
    const allocator = testing.allocator;

    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();
    const peer_id = try key.peerId(allocator);

    const addrs = fixtureAddrs();
    const record = PeerRecord{ .peer_id = peer_id, .seq = 7, .addrs = &addrs };
    const envelope = try sealPeerRecord(allocator, &key, record);
    defer allocator.free(envelope);

    // Flip a byte inside the payload region. The payload is the largest field; a
    // byte near the end lands inside the address bytes, leaving the protobuf
    // structurally parseable but signature-invalid.
    const tampered = try allocator.dupe(u8, envelope);
    defer allocator.free(tampered);
    tampered[tampered.len - 10] ^= 0xff;

    try testing.expectError(error.InvalidSignature, consumeEnvelope(allocator, tampered));
}

test "tampering with the signature is rejected as an invalid signature" {
    const allocator = testing.allocator;

    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();
    const peer_id = try key.peerId(allocator);

    const record = PeerRecord{ .peer_id = peer_id, .seq = 1, .addrs = &.{} };
    const envelope = try sealPeerRecord(allocator, &key, record);
    defer allocator.free(envelope);

    // The signature is the last field; flipping its final byte breaks it.
    const tampered = try allocator.dupe(u8, envelope);
    defer allocator.free(tampered);
    tampered[tampered.len - 1] ^= 0xff;

    try testing.expectError(error.InvalidSignature, consumeEnvelope(allocator, tampered));
}

test "an envelope whose public key does not match the record peer-id is rejected" {
    const allocator = testing.allocator;

    // Sign a record that names a DIFFERENT peer's id than the signing key. The
    // signature is valid (so we get past the signature check) but the key↔peer-id
    // binding must fail.
    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();
    var other = try identity.KeyPair.generate(.ED25519);
    defer other.deinit();
    const other_id = try other.peerId(allocator);

    const record = PeerRecord{ .peer_id = other_id, .seq = 3, .addrs = &.{} };
    const envelope = try sealPeerRecord(allocator, &key, record);
    defer allocator.free(envelope);

    try testing.expectError(error.PeerIdMismatch, consumeEnvelope(allocator, envelope));
}

test "a wrong payload type is rejected" {
    const allocator = testing.allocator;

    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();
    const peer_id = try key.peerId(allocator);

    const record = PeerRecord{ .peer_id = peer_id, .seq = 5, .addrs = &.{} };
    const payload = try record.encode(allocator);
    defer allocator.free(payload);

    // Build an envelope by hand with a non-peer-record payload type, signed
    // correctly over the bogus type so only the type check can reject it.
    const wrong_type = [_]u8{ 0x99, 0x99 };
    const unsigned = try makeUnsigned(allocator, peer_record_domain, &wrong_type, payload);
    defer allocator.free(unsigned);
    const signature = try key.signData(allocator, unsigned);
    defer allocator.free(signature);
    const pubkey_proto = try key.publicKeyBytes(allocator);
    defer allocator.free(pubkey_proto);
    const envelope = try encodeEnvelope(allocator, pubkey_proto, &wrong_type, payload, signature);
    defer allocator.free(envelope);

    try testing.expectError(error.PayloadTypeMismatch, consumeEnvelope(allocator, envelope));
}

test "the signing input matches the libp2p makeUnsigned layout" {
    const allocator = testing.allocator;

    const domain = peer_record_domain;
    const ptype = &peer_record_payload_type;
    const payload = "PAYLOAD";

    const out = try makeUnsigned(allocator, domain, ptype, payload);
    defer allocator.free(out);

    // Manually assemble: uvarint(len)||bytes for each of domain, ptype, payload.
    // All three lengths here are < 0x80, so each prefix is a single byte.
    var expected: std.ArrayList(u8) = .empty;
    defer expected.deinit(allocator);
    try expected.append(allocator, @intCast(domain.len));
    try expected.appendSlice(allocator, domain);
    try expected.append(allocator, @intCast(ptype.len));
    try expected.appendSlice(allocator, ptype);
    try expected.append(allocator, @intCast(payload.len));
    try expected.appendSlice(allocator, payload);

    try testing.expectEqualSlices(u8, expected.items, out);
}

test "the payload type constant is the libp2p peer-record codec 0x03 0x01" {
    try testing.expectEqualSlices(u8, &[_]u8{ 0x03, 0x01 }, &peer_record_payload_type);
    try testing.expectEqualStrings("libp2p-peer-record", peer_record_domain);
}

test "consumes a real go-libp2p signed peer-record envelope (wire-compat)" {
    const allocator = testing.allocator;

    // Fixture produced by go-libp2p `record.Seal` (Ed25519 fixed seed,
    // seq=0x1122334455667788, two quic-v1 multiaddrs). Decoding it proves our
    // envelope/record/signing-input byte layout matches the reference exactly.
    const envelope = try hexDecode(allocator, "0a240801122079b5562e8fe654f94078b112e8a98ba7901f853ae695bed7e0e3910bad049664120203011a500a2600240801122079b5562e8fe654f94078b112e8a98ba7901f853ae695bed7e0e3910bad0496641088ef99abc5e88c91111a0d0a0b047f00000191020fa1cd031a0d0a0b04c0a8000a91022328cd032a40d85c6ac219c546773786025106fb199fa63b456e808af9cdbed155efd4dc1114cf7eb912a0b1cc2adfcb129f4fe54bdc47fb03987733be3107fed37c6495e30a");
    defer allocator.free(envelope);

    var consumed = try consumeEnvelope(allocator, envelope);
    defer consumed.deinit(allocator);

    const expected_peer_id = try hexDecode(allocator, "00240801122079b5562e8fe654f94078b112e8a98ba7901f853ae695bed7e0e3910bad049664");
    defer allocator.free(expected_peer_id);

    try testing.expectEqual(@as(u64, 0x1122334455667788), consumed.seq);
    try testing.expectEqualSlices(u8, expected_peer_id, consumed.peer_id.bytes[0..consumed.peer_id.len]);
    try testing.expectEqual(@as(usize, 2), consumed.addrs.len);

    // The addresses are binary multiaddrs; `fromBytes` must yield the exact
    // string forms go advertised, proving we read the binary encoding correctly.
    const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;
    const expected_strs = [_][]const u8{
        "/ip4/127.0.0.1/udp/4001/quic-v1",
        "/ip4/192.168.0.10/udp/9000/quic-v1",
    };
    for (consumed.addrs, expected_strs) |bin, want| {
        var decoded = try Multiaddr.fromBytes(allocator, bin);
        defer decoded.deinit(allocator);
        try testing.expectEqualStrings(want, decoded.bytes);
    }
}

fn hexDecode(allocator: Allocator, hex: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, hex.len / 2);
    errdefer allocator.free(out);
    return std.fmt.hexToBytes(out, hex);
}
