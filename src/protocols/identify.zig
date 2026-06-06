const std = @import("std");
const identify_pb = @import("../protobuf.zig").identify;
const Stream = @import("../quic.zig").Stream;
const identity = @import("../identity.zig");
const peer_record = @import("../peer_record.zig");
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;
const PeerId = @import("peer_id").PeerId;

pub const protocol_id = "/ipfs/id/1.0.0";
const max_identify_message_len = 1024 * 1024;
const max_varint_len = 10;

pub const Options = struct {
    protocol_version: []const u8 = "ipfs/0.1.0",
    agent_version: []const u8 = "eth-p2p-z/0.1.0",
    public_key: ?[]const u8 = null,
    listen_addrs: []const []const u8 = &.{},
    observed_addr: ?[]const u8 = null,
    protocols: []const []const u8 = &.{},
    /// The marshaled signed Envelope advertising our own PeerRecord (libp2p
    /// identify field 8, `signedPeerRecord`). When set, peers can verify and
    /// certify our addresses (and forward us in peer-exchange). Built by
    /// `IdentifyHandler` from a host key; null leaves the field off the wire
    /// (a plain identify, exactly as before this field existed).
    signed_peer_record: ?[]const u8 = null,
};

pub const IdentifyHandler = struct {
    allocator: std.mem.Allocator,
    options: Options = .{},
    /// Our sealed peer record, owned and freed on `deinit` when present. Built
    /// once at construction from the host key (the listen addrs are fixed in
    /// `options`), then served verbatim on every inbound identify — the record
    /// is immutable, so one shared handler can answer concurrent streams without
    /// synchronisation. Mirrors `options.signed_peer_record` (a borrowed view of
    /// these bytes).
    owned_record: ?[]u8 = null,

    pub fn init(allocator: std.mem.Allocator) IdentifyHandler {
        return .{ .allocator = allocator };
    }

    pub fn initWithOptions(allocator: std.mem.Allocator, options: Options) IdentifyHandler {
        return .{ .allocator = allocator, .options = options };
    }

    /// Like `initWithOptions`, but also seals a PeerRecord with `host_key` and
    /// advertises it as the identify `signedPeerRecord` (libp2p field 8). The
    /// record names `host_key`'s peer-id, `seq` (a monotonic version the caller
    /// supplies — pass a counter, larger meaning newer), and `options.listen_addrs`
    /// as its addresses — the SAME string-form listen addrs the plain identify
    /// already sends, so the record stays consistent with the `listenAddrs` field.
    /// Each listen addr is encoded to its BINARY multiaddr form for the record (and
    /// the on-wire listen-addrs), matching go-libp2p / rust-libp2p so they parse our
    /// addresses; the plain `listenAddrs` field is converted separately in
    /// `writeIdentify`. The returned handler owns the sealed bytes (freed by
    /// `deinit`). On a seal failure the error is propagated rather than silently
    /// dropping the record.
    pub fn initWithSignedRecord(
        allocator: std.mem.Allocator,
        options: Options,
        host_key: *const identity.KeyPair,
        seq: u64,
    ) !IdentifyHandler {
        const peer_id = try host_key.peerId(allocator);

        // The PeerRecord carries BINARY multiaddrs (the libp2p wire form). Encode
        // each string listen addr to bytes, seal, then free the temporaries — the
        // sealed envelope owns its own copy.
        const bin_addrs = try encodeListenAddrs(allocator, options.listen_addrs);
        defer freeAddrList(allocator, bin_addrs);

        const record = peer_record.PeerRecord{
            .peer_id = peer_id,
            .seq = seq,
            .addrs = bin_addrs,
        };
        const sealed = try peer_record.sealPeerRecord(allocator, host_key, record);
        errdefer allocator.free(sealed);

        var opts = options;
        opts.signed_peer_record = sealed;
        return .{ .allocator = allocator, .options = opts, .owned_record = sealed };
    }

    pub fn deinit(self: *IdentifyHandler) void {
        if (self.owned_record) |bytes| self.allocator.free(bytes);
        self.owned_record = null;
        self.options.signed_peer_record = null;
    }

    pub fn run(self: *IdentifyHandler, io: std.Io, stream: *Stream) !void {
        try writeIdentify(self.allocator, io, stream, self.options);
        try stream.closeWrite(io);
    }
};

pub const OwnedIdentify = struct {
    bytes: []const u8,
    reader: identify_pb.IdentifyReader,

    pub fn deinit(self: *OwnedIdentify, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub fn writeIdentify(allocator: std.mem.Allocator, io: std.Io, stream: *Stream, options: Options) !void {
    // `listenAddrs` go on the wire as BINARY multiaddrs (so go/rust parse them).
    const bin_addrs = try encodeListenAddrs(allocator, options.listen_addrs);
    defer freeAddrList(allocator, bin_addrs);
    const listen_addrs = try nullableSliceList(allocator, bin_addrs);
    defer allocator.free(listen_addrs);
    const protocols = try nullableSliceList(allocator, options.protocols);
    defer allocator.free(protocols);

    const msg = identify_pb.Identify{
        .protocol_version = options.protocol_version,
        .agent_version = options.agent_version,
        .public_key = options.public_key,
        .listen_addrs = if (listen_addrs.len == 0) null else listen_addrs,
        .observed_addr = options.observed_addr,
        .protocols = if (protocols.len == 0) null else protocols,
        .signed_peer_record = options.signed_peer_record,
    };
    const payload = try msg.encode(allocator);
    defer allocator.free(payload);

    var len_buf: [max_varint_len]u8 = undefined;
    const len_len = encodeUvarint(&len_buf, payload.len);
    try stream.writeAll(io, len_buf[0..len_len], .{});
    try stream.writeAll(io, payload, .{});
}

pub fn readIdentify(allocator: std.mem.Allocator, io: std.Io, stream: *Stream) !OwnedIdentify {
    const len = try readUvarint(io, stream);
    if (len > max_identify_message_len) return error.MessageTooLarge;

    const bytes = try allocator.alloc(u8, len);
    errdefer allocator.free(bytes);
    try stream.readAll(io, bytes, .{});

    return .{
        .bytes = bytes,
        .reader = try identify_pb.IdentifyReader.init(bytes),
    };
}

/// Verify the `signedPeerRecord` (libp2p identify field 8) inside a received
/// identify and return the validated record for `expected_peer`. Mirrors
/// go-libp2p `consumeSignedPeerRecord`: the envelope's signature must verify
/// against its embedded public key, that key must hash to the record's peer-id
/// (the key↔peer-id binding, enforced inside `consumeEnvelope`), AND the record's
/// peer-id must equal the peer we completed the connection with — so a peer cannot
/// advertise another node's record. Returns null when the message carries no
/// record (a plain identify); returns an error when a record is present but
/// invalid or names the wrong peer.
///
/// The returned `ConsumedRecord` owns its address slices and backing buffer
/// (free with `deinit`). To populate the gossipsub certified-record store the
/// caller hands the ORIGINAL envelope bytes (`reader.getSignedPeerRecord()`,
/// which `putRecord` re-verifies and stores verbatim) to the binding; this helper
/// proves they verify and bind to the expected peer first.
pub fn consumeSignedPeerRecord(
    allocator: std.mem.Allocator,
    reader: *const identify_pb.IdentifyReader,
    expected_peer: PeerId,
) (peer_record.ConsumeError || error{PeerIdMismatch})!?peer_record.ConsumedRecord {
    const envelope = reader.getSignedPeerRecord();
    if (envelope.len == 0) return null;

    var consumed = try peer_record.consumeEnvelope(allocator, envelope);
    errdefer consumed.deinit(allocator);

    var expected = expected_peer;
    if (!consumed.peer_id.eql(&expected)) return error.PeerIdMismatch;
    return consumed;
}

/// Encode each string-form listen multiaddr into its BINARY multiaddr bytes (the
/// libp2p wire form). Returns a freshly allocated list of owned byte slices (free
/// with `freeAddrList`). An addr that fails to encode (an unsupported protocol)
/// propagates the error rather than being silently dropped, so a misconfigured
/// listen addr surfaces instead of going on the wire malformed.
fn encodeListenAddrs(allocator: std.mem.Allocator, addrs: []const []const u8) ![][]u8 {
    const out = try allocator.alloc([]u8, addrs.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |a| allocator.free(a);
        allocator.free(out);
    }
    for (addrs) |addr| {
        const ma = Multiaddr{ .bytes = addr };
        out[filled] = try ma.toBytes(allocator);
        filled += 1;
    }
    return out;
}

fn freeAddrList(allocator: std.mem.Allocator, addrs: [][]u8) void {
    for (addrs) |a| allocator.free(a);
    allocator.free(addrs);
}

fn nullableSliceList(allocator: std.mem.Allocator, values: []const []const u8) std.mem.Allocator.Error![]const ?[]const u8 {
    if (values.len == 0) return &.{};
    const out = try allocator.alloc(?[]const u8, values.len);
    for (values, 0..) |value, i| out[i] = value;
    return out;
}

fn encodeUvarint(out: *[max_varint_len]u8, value: usize) usize {
    var remaining = value;
    var i: usize = 0;
    while (remaining >= 0x80) {
        out[i] = @as(u8, @intCast(remaining & 0x7f)) | 0x80;
        remaining >>= 7;
        i += 1;
    }
    out[i] = @intCast(remaining);
    return i + 1;
}

fn readUvarint(io: std.Io, stream: *Stream) !usize {
    var result: usize = 0;
    var shift: usize = 0;
    var i: usize = 0;
    while (i < max_varint_len) : (i += 1) {
        var byte: [1]u8 = undefined;
        try stream.readAll(io, &byte, .{});
        result |= (@as(usize, byte[0] & 0x7f) << @intCast(shift));
        if ((byte[0] & 0x80) == 0) return result;
        shift += 7;
    }
    return error.VarintTooLong;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Encode an identify message from `options` (the same field assembly
/// `writeIdentify` performs) and hand back a reader over the wire bytes, so a
/// test can exercise the build → encode → parse round-trip without a live Stream.
fn encodeAndRead(allocator: std.mem.Allocator, options: Options) !OwnedIdentify {
    const bin_addrs = try encodeListenAddrs(allocator, options.listen_addrs);
    defer freeAddrList(allocator, bin_addrs);
    const listen_addrs = try nullableSliceList(allocator, bin_addrs);
    defer allocator.free(listen_addrs);
    const protocols = try nullableSliceList(allocator, options.protocols);
    defer allocator.free(protocols);

    const msg = identify_pb.Identify{
        .protocol_version = options.protocol_version,
        .agent_version = options.agent_version,
        .public_key = options.public_key,
        .listen_addrs = if (listen_addrs.len == 0) null else listen_addrs,
        .observed_addr = options.observed_addr,
        .protocols = if (protocols.len == 0) null else protocols,
        .signed_peer_record = options.signed_peer_record,
    };
    const bytes = try msg.encode(allocator);
    errdefer allocator.free(bytes);
    return .{ .bytes = bytes, .reader = try identify_pb.IdentifyReader.init(bytes) };
}

test "identify response built with a signed record verifies to our peer-id and listen addrs" {
    const allocator = testing.allocator;

    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();
    const peer_id = try key.peerId(allocator);

    // The string-form listen addrs identify already sends; the sealed record
    // carries the SAME bytes (consistent with the `listenAddrs` field).
    const listen_addrs = [_][]const u8{
        "/ip4/127.0.0.1/udp/4001/quic-v1",
        "/ip4/10.0.0.5/udp/4002/quic-v1",
    };

    var handler = try IdentifyHandler.initWithSignedRecord(
        allocator,
        .{ .listen_addrs = &listen_addrs },
        &key,
        7,
    );
    defer handler.deinit();

    // The handler set signed_peer_record on its options; the wire message carries
    // it as field 8.
    try testing.expect(handler.options.signed_peer_record != null);
    var owned = try encodeAndRead(allocator, handler.options);
    defer owned.deinit(allocator);
    try testing.expect(owned.reader.getSignedPeerRecord().len > 0);

    // Verify it as a peer would: the record must bind to our peer-id and carry our
    // listen addrs verbatim.
    var consumed = (try consumeSignedPeerRecord(allocator, &owned.reader, peer_id)).?;
    defer consumed.deinit(allocator);

    try testing.expect(consumed.peer_id.eql(@constCast(&peer_id)));
    try testing.expectEqual(@as(u64, 7), consumed.seq);
    try testing.expectEqual(listen_addrs.len, consumed.addrs.len);
    // The record's addresses are BINARY multiaddrs on the wire; decode each back
    // to its string form and confirm it matches the listen addr we advertised.
    for (listen_addrs, consumed.addrs) |want, got| {
        var decoded = try Multiaddr.fromBytes(allocator, got);
        defer decoded.deinit(allocator);
        try testing.expectEqualStrings(want, decoded.bytes);
    }
}

test "an identify with no signed record consumes to null" {
    const allocator = testing.allocator;

    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();
    const peer_id = try key.peerId(allocator);

    // A plain identify (no record) — exactly the pre-existing behaviour.
    var owned = try encodeAndRead(allocator, .{ .listen_addrs = &.{"/ip4/127.0.0.1/udp/4001/quic-v1"} });
    defer owned.deinit(allocator);

    try testing.expectEqual(@as(usize, 0), owned.reader.getSignedPeerRecord().len);
    const consumed = try consumeSignedPeerRecord(allocator, &owned.reader, peer_id);
    try testing.expect(consumed == null);
}

test "a signed record for a different peer than the connection is rejected" {
    const allocator = testing.allocator;

    // The record is sealed by `key` (and so names key's peer-id), but we consume it
    // as if the connection were to `other` — a peer-id mismatch.
    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();
    var other = try identity.KeyPair.generate(.ED25519);
    defer other.deinit();
    const other_id = try other.peerId(allocator);

    var handler = try IdentifyHandler.initWithSignedRecord(allocator, .{}, &key, 1);
    defer handler.deinit();

    var owned = try encodeAndRead(allocator, handler.options);
    defer owned.deinit(allocator);

    try testing.expectError(error.PeerIdMismatch, consumeSignedPeerRecord(allocator, &owned.reader, other_id));
}
