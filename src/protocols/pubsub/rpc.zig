const std = @import("std");
const rpc_pb = @import("../../protobuf.zig").rpc;

// MessageId holds the bytes of a computed message identifier. Call deinit to
// free the allocation when it is no longer needed.
pub const MessageId = struct {
    bytes: []u8,

    pub fn deinit(self: *MessageId, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

/// Default libp2p message id: from ++ seqno. Takes raw slices (not a *Message) so
/// callers can pass either an outbound Message's fields (`msg.from orelse &.{}`) or
/// an inbound MessageReader's getters (`reader.getFrom()`) without a temporary.
/// Empty `from` and `seqno` are valid and yield a zero-length id.
pub fn messageId(allocator: std.mem.Allocator, from: []const u8, seqno: []const u8) std.mem.Allocator.Error!MessageId {
    const buf = try allocator.alloc(u8, from.len + seqno.len);
    @memcpy(buf[0..from.len], from);
    @memcpy(buf[from.len..], seqno);
    return .{ .bytes = buf };
}

// buildGraft returns a ControlGraft for the given topic, ready to encode.
pub fn buildGraft(topic: []const u8) rpc_pb.ControlGraft {
    return .{ .topic_i_d = topic };
}

// buildPrune returns a ControlPrune for the given topic. backoff=0 is the
// protobuf default and is omitted from the wire.
pub fn buildPrune(topic: []const u8, px_peers: []const ?rpc_pb.PeerInfo, backoff: u64) rpc_pb.ControlPrune {
    return .{
        .topic_i_d = topic,
        .peers = if (px_peers.len > 0) px_peers else null,
        .backoff = backoff,
    };
}

// buildIHave returns a ControlIHave advertising a set of message-ids for
// the given topic.
pub fn buildIHave(topic: []const u8, message_ids: []const ?[]const u8) rpc_pb.ControlIHave {
    return .{
        .topic_i_d = topic,
        .message_i_ds = if (message_ids.len > 0) message_ids else null,
    };
}

// buildIWant returns a ControlIWant requesting the listed message-ids.
pub fn buildIWant(message_ids: []const ?[]const u8) rpc_pb.ControlIWant {
    return .{ .message_i_ds = if (message_ids.len > 0) message_ids else null };
}

// buildIDontWant returns a ControlIDontWant announcing message-ids the
// local node has already seen (gossipsub v1.2).
pub fn buildIDontWant(message_ids: []const ?[]const u8) rpc_pb.ControlIDontWant {
    return .{ .message_i_ds = if (message_ids.len > 0) message_ids else null };
}

// buildSubscription returns a SubOpts for a single topic subscription or
// unsubscription.
pub fn buildSubscription(topic: []const u8, subscribe: bool) rpc_pb.RPC.SubOpts {
    return .{ .subscribe = subscribe, .topicid = topic };
}

// RpcOut is the unit of work the per-peer writer sends to the wire. Each
// variant maps directly to one of the three top-level RPC shapes.
pub const RpcOut = union(enum) {
    subscriptions: []const ?rpc_pb.RPC.SubOpts,
    publish: []const ?rpc_pb.Message,
    forward: []const ?rpc_pb.Message, // relayed messages; same wire shape as publish, separate lane
    control: rpc_pb.ControlMessage,

    // toRpc assembles an rpc_pb.RPC from this variant. The returned struct
    // borrows all slice/byte data from self — no allocation.
    pub fn toRpc(self: RpcOut) rpc_pb.RPC {
        return switch (self) {
            .subscriptions => |subs| .{ .subscriptions = subs },
            .publish, .forward => |msgs| .{ .publish = msgs },
            .control => |ctrl| .{ .control = ctrl },
        };
    }
};

test "messageId concatenates from and seqno" {
    const allocator = std.testing.allocator;
    const from = "peer1";
    const seqno = "\x00\x01\x02\x03";
    var id = try messageId(allocator, from, seqno);
    defer id.deinit(allocator);
    try std.testing.expectEqualSlices(u8, "peer1\x00\x01\x02\x03", id.bytes);
}

test "messageId empty inputs" {
    const allocator = std.testing.allocator;
    var id = try messageId(allocator, "", "");
    defer id.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), id.bytes.len);
}

test "graft round-trip" {
    const allocator = std.testing.allocator;
    const topic = "test-topic";
    const graft = buildGraft(topic);
    const ctrl = rpc_pb.ControlMessage{ .graft = &[_]?rpc_pb.ControlGraft{graft} };
    const frame = rpc_pb.RPC{ .control = ctrl };

    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);

    var reader = try rpc_pb.RPCReader.init(encoded);
    var ctrl_reader = try reader.getControl();
    const got = ctrl_reader.graftNext() orelse return error.MissingGraft;
    try std.testing.expectEqualSlices(u8, topic, got.getTopicID());
}

test "prune round-trip" {
    const allocator = std.testing.allocator;
    const topic = "prune-topic";
    const px = [_]?rpc_pb.PeerInfo{.{ .peer_i_d = "abc", .signed_peer_record = null }};
    const prune = buildPrune(topic, &px, 42);
    const ctrl = rpc_pb.ControlMessage{ .prune = &[_]?rpc_pb.ControlPrune{prune} };
    const frame = rpc_pb.RPC{ .control = ctrl };

    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);

    var reader = try rpc_pb.RPCReader.init(encoded);
    var ctrl_reader = try reader.getControl();
    var got = ctrl_reader.pruneNext() orelse return error.MissingPrune;
    try std.testing.expectEqualSlices(u8, topic, got.getTopicID());
    try std.testing.expectEqual(@as(u64, 42), got.getBackoff());
    const peer = got.peersNext() orelse return error.MissingPeer;
    try std.testing.expectEqualSlices(u8, "abc", peer.getPeerID());
}

test "ihave round-trip" {
    const allocator = std.testing.allocator;
    const topic = "ihave-topic";
    const ids = [_]?[]const u8{ "id1", "id2" };
    const ihave = buildIHave(topic, &ids);
    const ctrl = rpc_pb.ControlMessage{ .ihave = &[_]?rpc_pb.ControlIHave{ihave} };
    const frame = rpc_pb.RPC{ .control = ctrl };

    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);

    var reader = try rpc_pb.RPCReader.init(encoded);
    var ctrl_reader = try reader.getControl();
    var got = ctrl_reader.ihaveNext() orelse return error.MissingIHave;
    try std.testing.expectEqualSlices(u8, topic, got.getTopicID());
    try std.testing.expectEqual(@as(usize, 2), got.messageIDsCount());
    const m1 = got.messageIDsNext() orelse return error.MissingId1;
    try std.testing.expectEqualSlices(u8, "id1", m1);
    const m2 = got.messageIDsNext() orelse return error.MissingId2;
    try std.testing.expectEqualSlices(u8, "id2", m2);
}

test "iwant round-trip" {
    const allocator = std.testing.allocator;
    const ids = [_]?[]const u8{"want-id"};
    const iwant = buildIWant(&ids);
    const ctrl = rpc_pb.ControlMessage{ .iwant = &[_]?rpc_pb.ControlIWant{iwant} };
    const frame = rpc_pb.RPC{ .control = ctrl };

    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);

    var reader = try rpc_pb.RPCReader.init(encoded);
    var ctrl_reader = try reader.getControl();
    var got = ctrl_reader.iwantNext() orelse return error.MissingIWant;
    try std.testing.expectEqual(@as(usize, 1), got.messageIDsCount());
    const m = got.messageIDsNext() orelse return error.MissingId;
    try std.testing.expectEqualSlices(u8, "want-id", m);
}

test "idontwant round-trip" {
    const allocator = std.testing.allocator;
    const ids = [_]?[]const u8{"seen-id"};
    const idontwant = buildIDontWant(&ids);
    const ctrl = rpc_pb.ControlMessage{ .idontwant = &[_]?rpc_pb.ControlIDontWant{idontwant} };
    const frame = rpc_pb.RPC{ .control = ctrl };

    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);

    var reader = try rpc_pb.RPCReader.init(encoded);
    var ctrl_reader = try reader.getControl();
    var got = ctrl_reader.idontwantNext() orelse return error.MissingIDontWant;
    try std.testing.expectEqual(@as(usize, 1), got.messageIDsCount());
    const m = got.messageIDsNext() orelse return error.MissingId;
    try std.testing.expectEqualSlices(u8, "seen-id", m);
}

test "subscription round-trip" {
    const allocator = std.testing.allocator;
    const sub = buildSubscription("my-topic", true);
    const frame = rpc_pb.RPC{ .subscriptions = &[_]?rpc_pb.RPC.SubOpts{sub} };

    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);

    var reader = try rpc_pb.RPCReader.init(encoded);
    const got = reader.subscriptionsNext() orelse return error.MissingSub;
    try std.testing.expect(got.getSubscribe());
    try std.testing.expectEqualSlices(u8, "my-topic", got.getTopicid());
}

test "message round-trip via RpcOut.publish" {
    const allocator = std.testing.allocator;
    const msg = rpc_pb.Message{
        .from = "sender",
        .seqno = "\x00\x01",
        .data = "hello",
        .topic = "topic-x",
    };
    const out = RpcOut{ .publish = &[_]?rpc_pb.Message{msg} };
    const frame = out.toRpc();

    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);

    var reader = try rpc_pb.RPCReader.init(encoded);
    const got = reader.publishNext() orelse return error.MissingMessage;
    try std.testing.expectEqualSlices(u8, "sender", got.getFrom());
    try std.testing.expectEqualSlices(u8, "\x00\x01", got.getSeqno());
    try std.testing.expectEqualSlices(u8, "hello", got.getData());
    try std.testing.expectEqualSlices(u8, "topic-x", got.getTopic());
}

test "RpcOut.subscriptions via toRpc" {
    const allocator = std.testing.allocator;
    const sub = buildSubscription("net", false);
    const out = RpcOut{ .subscriptions = &[_]?rpc_pb.RPC.SubOpts{sub} };
    const frame = out.toRpc();

    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);

    var reader = try rpc_pb.RPCReader.init(encoded);
    // `subscribe=false` is the protobuf default and is omitted from the field; assert the
    // SubOpts message itself is still present on the wire (an unsubscribe is a real message).
    try std.testing.expectEqual(@as(usize, 1), reader.subscriptionsCount());
    const got = reader.subscriptionsNext() orelse return error.MissingSub;
    try std.testing.expect(!got.getSubscribe());
    try std.testing.expectEqualSlices(u8, "net", got.getTopicid());
}

test "RpcOut.control via toRpc" {
    const allocator = std.testing.allocator;
    const graft = buildGraft("ctrl-topic");
    const ctrl = rpc_pb.ControlMessage{ .graft = &[_]?rpc_pb.ControlGraft{graft} };
    const out = RpcOut{ .control = ctrl };
    const frame = out.toRpc();

    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);

    var reader = try rpc_pb.RPCReader.init(encoded);
    var ctrl_reader = try reader.getControl();
    const got = ctrl_reader.graftNext() orelse return error.MissingGraft;
    try std.testing.expectEqualSlices(u8, "ctrl-topic", got.getTopicID());
}

test "message round-trip via RpcOut.forward" {
    const allocator = std.testing.allocator;
    const msg = rpc_pb.Message{
        .from = "relay-sender",
        .seqno = "\x0a\x0b",
        .data = "relayed",
        .topic = "topic-y",
    };
    const out = RpcOut{ .forward = &[_]?rpc_pb.Message{msg} };
    const frame = out.toRpc();

    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);

    var reader = try rpc_pb.RPCReader.init(encoded);
    const got = reader.publishNext() orelse return error.MissingMessage;
    try std.testing.expectEqualSlices(u8, "relay-sender", got.getFrom());
    try std.testing.expectEqualSlices(u8, "\x0a\x0b", got.getSeqno());
    try std.testing.expectEqualSlices(u8, "relayed", got.getData());
    try std.testing.expectEqualSlices(u8, "topic-y", got.getTopic());
}
