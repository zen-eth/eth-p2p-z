const std = @import("std");
const gremlin = @import("gremlin");
const PeerId = @import("peer_id").PeerId;
const rpc_pb = @import("../../protobuf.zig").rpc;
const Stream = @import("../../quic.zig").Stream;

pub const rpc = @import("rpc.zig");

pub const protocol_id = "/meshsub/1.1.0";
pub const protocol_id_v1_0 = "/meshsub/1.0.0";
pub const protocol_id_v1_2 = "/meshsub/1.2.0";

/// The /meshsub protocol versions this node speaks, in descending preference
/// order. The dialer proposes these to a peer (best first) and uses whichever
/// the peer accepts; the listener registers its inbound service for all of them.
/// 1.2.0 adds IDONTWANT, 1.1.0 adds the gossip/scoring control messages, 1.0.0
/// is the original floodsub-style mesh — all are wire-compatible up to the
/// control messages each version introduces.
pub const supported_protocols = [_][]const u8{ protocol_id_v1_2, protocol_id, protocol_id_v1_0 };

/// The gossipsub protocol version in effect for one peer. Tracked per peer so
/// version-gated features (e.g. IDONTWANT, which needs 1.2) only fire toward
/// peers that negotiated a high enough version. `v1_1` is the baseline default
/// for a peer whose version is not yet known.
pub const Version = enum {
    v1_0,
    v1_1,
    v1_2,

    /// Map a negotiated /meshsub protocol id to its Version. An unrecognised id
    /// (should not happen — we only ever negotiate from `supported_protocols`)
    /// falls back to the 1.1 baseline.
    pub fn fromProtocolId(id: []const u8) Version {
        if (std.mem.eql(u8, id, protocol_id_v1_2)) return .v1_2;
        if (std.mem.eql(u8, id, protocol_id)) return .v1_1;
        if (std.mem.eql(u8, id, protocol_id_v1_0)) return .v1_0;
        return .v1_1;
    }
};

const max_rpc_message_len = 1024 * 1024;
const max_varint_len = 10;

pub const PubSubMessage = rpc_pb.Message;
pub const PublishResult = []PeerId;

pub const OwnedRpc = struct {
    bytes: []u8,
    reader: rpc_pb.RPCReader,

    pub fn deinit(self: *OwnedRpc, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub const Gossipsub = struct {
    allocator: std.mem.Allocator,
    inbox: *std.Io.Queue(OwnedRpc),

    pub fn init(allocator: std.mem.Allocator, inbox: *std.Io.Queue(OwnedRpc)) Gossipsub {
        return .{ .allocator = allocator, .inbox = inbox };
    }

    pub fn run(self: *Gossipsub, io: std.Io, stream: *Stream) !void {
        while (true) {
            var msg = readRpc(self.allocator, io, stream) catch |err| switch (err) {
                error.EndOfStream, error.StreamShutdown, error.ConnectionClosed => return,
                else => |e| return e,
            };
            errdefer msg.deinit(self.allocator);
            try self.inbox.putOne(io, msg);
        }
    }
};

pub fn writePublish(allocator: std.mem.Allocator, io: std.Io, stream: *Stream, message: rpc_pb.Message) !void {
    const publish = [_]?rpc_pb.Message{message};
    const frame = rpc_pb.RPC{ .publish = &publish };
    try writeRpc(allocator, io, stream, frame);
}

/// Errors returned by readRpc: the read-side I/O of the stream, the varint
/// framing checks, plus allocation and protobuf-reader init.
pub const ReadRpcError = Stream.ReadError || error{ VarintTooLong, MessageTooLarge } || gremlin.Error;

/// Errors returned by writeRpc: encoding/framing (gremlin.Error, which includes
/// OutOfMemory) plus the write-side I/O of the stream.
pub const WriteRpcError = gremlin.Error || Stream.WriteError;

/// Encodes `msg` and returns owned, length-prefixed wire bytes (uvarint length || payload).
/// Caller owns the returned slice and must free it. gremlin.Error already
/// includes OutOfMemory, so it covers both encode and the framing allocation.
pub fn frameRpc(allocator: std.mem.Allocator, msg: rpc_pb.RPC) gremlin.Error![]u8 {
    // encode returns a comptime empty slice (not heap-backed) when the message
    // is empty, so only free a non-empty payload.
    const payload = try msg.encode(allocator);
    defer if (payload.len > 0) allocator.free(payload);

    var len_buf: [max_varint_len]u8 = undefined;
    const len_len = encodeUvarint(&len_buf, payload.len);

    const framed = try allocator.alloc(u8, len_len + payload.len);
    @memcpy(framed[0..len_len], len_buf[0..len_len]);
    @memcpy(framed[len_len..], payload);
    return framed;
}

pub fn writeRpc(allocator: std.mem.Allocator, io: std.Io, stream: *Stream, frame: rpc_pb.RPC) WriteRpcError!void {
    const framed = try frameRpc(allocator, frame);
    defer allocator.free(framed);
    try stream.writeAll(io, framed, .{});
}

pub fn readRpc(allocator: std.mem.Allocator, io: std.Io, stream: *Stream) ReadRpcError!OwnedRpc {
    const len = try readUvarint(io, stream);
    if (len > max_rpc_message_len) return error.MessageTooLarge;

    const bytes = try allocator.alloc(u8, len);
    errdefer allocator.free(bytes);
    try stream.readAll(io, bytes, .{});

    // RPCReader borrows `bytes`; OwnedRpc keeps `bytes` alive for the reader's
    // lifetime. errdefer covers an init failure (malformed frame) before the
    // owned struct is returned.
    return .{
        .bytes = bytes,
        .reader = try rpc_pb.RPCReader.init(bytes),
    };
}

pub const ReadRpcBufferedError = error{ EndOfStream, ReadFailed, VarintTooLong, MessageTooLarge } || gremlin.Error;

/// Like `readRpc` but over a persistent buffered `std.Io.Reader`: one stream
/// read per buffer refill instead of one mutex-guarded `readAll` per varint
/// byte. Over-read bytes stay buffered for the next frame, so the reader must
/// outlive the stream (one per inbound handler).
pub fn readRpcBuffered(allocator: std.mem.Allocator, r: *std.Io.Reader) ReadRpcBufferedError!OwnedRpc {
    const len = try readUvarintBuffered(r);
    if (len > max_rpc_message_len) return error.MessageTooLarge;

    const bytes = try allocator.alloc(u8, len);
    errdefer allocator.free(bytes);
    r.readSliceAll(bytes) catch |err| switch (err) {
        error.EndOfStream => return error.EndOfStream,
        error.ReadFailed => return error.ReadFailed,
    };
    return .{
        .bytes = bytes,
        .reader = try rpc_pb.RPCReader.init(bytes),
    };
}

fn readUvarintBuffered(r: *std.Io.Reader) error{ EndOfStream, ReadFailed, VarintTooLong }!usize {
    var result: usize = 0;
    var shift: usize = 0;
    var i: usize = 0;
    while (i < max_varint_len) : (i += 1) {
        const byte = r.takeByte() catch |err| switch (err) {
            error.EndOfStream => return error.EndOfStream,
            error.ReadFailed => return error.ReadFailed,
        };
        result |= @as(usize, byte & 0x7f) << @intCast(shift);
        if ((byte & 0x80) == 0) return result;
        shift += 7;
    }
    return error.VarintTooLong;
}

/// Write `value` as an unsigned LEB128 varint into `out`, returning the byte
/// count written. A usize varint is at most max_varint_len bytes, so the buffer
/// is always large enough.
fn encodeUvarint(out: *[max_varint_len]u8, value: usize) usize {
    var remaining = value;
    var i: usize = 0;
    while (remaining >= 0x80) : (i += 1) {
        std.debug.assert(i < max_varint_len - 1); // room for the final byte
        out[i] = @as(u8, @intCast(remaining & 0x7f)) | 0x80;
        remaining >>= 7;
    }
    out[i] = @intCast(remaining);
    return i + 1;
}

/// Read an unsigned LEB128 varint from the stream, one byte at a time. Rejects
/// varints longer than max_varint_len (the most a usize needs) as malformed.
fn readUvarint(io: std.Io, stream: *Stream) (Stream.ReadError || error{VarintTooLong})!usize {
    var result: usize = 0;
    var shift: usize = 0;
    var i: usize = 0;
    while (i < max_varint_len) : (i += 1) {
        var byte: [1]u8 = undefined;
        try stream.readAll(io, &byte, .{});
        result |= @as(usize, byte[0] & 0x7f) << @intCast(shift);
        if ((byte[0] & 0x80) == 0) return result;
        shift += 7;
    }
    return error.VarintTooLong;
}

// Aggregate test blocks for all files in this subdirectory so that their
// test {} blocks are reachable from the root test aggregator. Add each new
// pubsub source file here as `_ = @import("filename.zig");`.
test {
    _ = @import("rpc.zig");
    _ = @import("peer_io.zig");
    _ = @import("mcache.zig");
    _ = @import("score.zig");
    _ = @import("signing.zig");
    // intern.zig has no test blocks and is already analyzed via mcache.zig, so it
    // needs no aggregator entry; signing.zig above does (it has tests).
}

test "supported_protocols lists meshsub versions newest-first" {
    // The dialer proposes these in order and takes the first a peer accepts, so
    // 1.2.0 must come before 1.1.0 before 1.0.0 for best-version negotiation.
    try std.testing.expectEqual(@as(usize, 3), supported_protocols.len);
    try std.testing.expectEqualStrings(protocol_id_v1_2, supported_protocols[0]);
    try std.testing.expectEqualStrings(protocol_id, supported_protocols[1]);
    try std.testing.expectEqualStrings(protocol_id_v1_0, supported_protocols[2]);
}

test "Version.fromProtocolId maps each meshsub id and defaults to v1_1" {
    try std.testing.expectEqual(Version.v1_2, Version.fromProtocolId(protocol_id_v1_2));
    try std.testing.expectEqual(Version.v1_1, Version.fromProtocolId(protocol_id));
    try std.testing.expectEqual(Version.v1_0, Version.fromProtocolId(protocol_id_v1_0));
    // An unknown id falls back to the 1.1 baseline rather than crashing.
    try std.testing.expectEqual(Version.v1_1, Version.fromProtocolId("/meshsub/9.9.9"));
}

test "frameRpc length prefix matches payload length" {
    const allocator = std.testing.allocator;
    const sub = rpc_pb.RPC{ .subscriptions = &[_]?rpc_pb.RPC.SubOpts{.{ .subscribe = true, .topicid = "t" }} };

    const framed = try frameRpc(allocator, sub);
    defer allocator.free(framed);

    // The first bytes must be a valid uvarint that equals the remaining payload length.
    var shift: usize = 0;
    var prefix_len: usize = 0;
    var i: usize = 0;
    while (i < framed.len) : (i += 1) {
        prefix_len |= (@as(usize, framed[i] & 0x7f) << @intCast(shift));
        shift += 7;
        if ((framed[i] & 0x80) == 0) {
            i += 1;
            break;
        }
    }
    const payload_slice = framed[i..];
    try std.testing.expectEqual(prefix_len, payload_slice.len);

    // The payload must round-trip: parse it and find the subscription.
    var reader = try rpc_pb.RPCReader.init(payload_slice);
    const got = reader.subscriptionsNext() orelse return error.MissingSub;
    try std.testing.expect(got.getSubscribe());
    try std.testing.expectEqualSlices(u8, "t", got.getTopicid());
}

/// Decode an unsigned LEB128 varint from `buf`, returning the value and the
/// number of bytes consumed. Mirrors readUvarint without the stream I/O so the
/// encoder can be checked in isolation.
fn decodeUvarint(buf: []const u8) struct { value: usize, len: usize } {
    var result: usize = 0;
    var shift: usize = 0;
    var i: usize = 0;
    while (i < buf.len) : (i += 1) {
        result |= @as(usize, buf[i] & 0x7f) << @intCast(shift);
        if ((buf[i] & 0x80) == 0) return .{ .value = result, .len = i + 1 };
        shift += 7;
    }
    unreachable; // caller passes a complete, well-formed varint
}

test "encodeUvarint produces canonical LEB128 at byte-length boundaries" {
    // (value, expected encoded length) pairs straddling each 7-bit boundary.
    const cases = [_]struct { value: usize, len: usize }{
        .{ .value = 0, .len = 1 },
        .{ .value = 1, .len = 1 },
        .{ .value = 127, .len = 1 }, // last 1-byte value
        .{ .value = 128, .len = 2 }, // first 2-byte value
        .{ .value = 16_383, .len = 2 }, // last 2-byte value
        .{ .value = 16_384, .len = 3 }, // first 3-byte value
        .{ .value = 1 << 21, .len = 4 },
        .{ .value = max_rpc_message_len, .len = 3 },
    };
    for (cases) |c| {
        var buf: [max_varint_len]u8 = undefined;
        const n = encodeUvarint(&buf, c.value);
        try std.testing.expectEqual(c.len, n);
        // Continuation bit set on every byte but the last; clear on the last.
        for (buf[0 .. n - 1]) |b| try std.testing.expect((b & 0x80) != 0);
        try std.testing.expect((buf[n - 1] & 0x80) == 0);
        // And it must decode back to the original value, consuming exactly n bytes.
        const decoded = decodeUvarint(buf[0..n]);
        try std.testing.expectEqual(c.value, decoded.value);
        try std.testing.expectEqual(n, decoded.len);
    }
}

test "frameRpc on an empty RPC emits a single zero-length prefix" {
    const allocator = std.testing.allocator;
    // An RPC with all fields null encodes to zero payload bytes; the frame is
    // then just the uvarint 0 (one byte) and nothing else.
    const empty = rpc_pb.RPC{};
    const framed = try frameRpc(allocator, empty);
    defer allocator.free(framed);

    const decoded = decodeUvarint(framed);
    try std.testing.expectEqual(@as(usize, 0), decoded.value);
    try std.testing.expectEqual(framed.len, decoded.len);
}
