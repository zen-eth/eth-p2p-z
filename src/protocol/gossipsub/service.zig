const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const router_mod = @import("router.zig");
const codec_mod = @import("codec.zig");
const config_mod = @import("config.zig");
const transport_mod = @import("../../transport/transport.zig");
const rpc = @import("../../proto/rpc.proto.zig");
const AnyStream = transport_mod.AnyStream;

const Config = config_mod.Config;
const Event = config_mod.Event;
const FrameDecoder = codec_mod.FrameDecoder;

const log = std.log.scoped(.gossipsub_service);

/// GossipSub Service — wraps the comptime-generic Router and satisfies
/// both the Switch protocol Handler interface and the Router's Handler interface.
///
/// The Service uses a pending-sends queue for outbound RPCs: when the Router
/// calls `sendRpc`, the data is enqueued. The integration layer (or test harness)
/// drains the queue via `drainPendingSends` and writes to actual streams.
///
/// ## Usage
///
/// ```zig
/// const svc = try Service.init(allocator, .{});
/// defer svc.deinit();
///
/// svc.subscribe("my-topic");
/// _ = svc.publish("my-topic", "hello world");
///
/// // Drain pending sends and write to peer streams
/// const sends = svc.drainPendingSends();
/// for (sends) |s| {
///     // write s.data to s.peer's outbound stream...
///     allocator.free(s.peer);
///     allocator.free(s.data);
/// }
/// allocator.free(sends);
/// ```
pub const Service = struct {
    const Self = @This();
    const RouterType = router_mod.Router(Self);

    allocator: Allocator,
    router: RouterType,
    /// Pending outbound RPC data per peer.
    /// Populated by the Router via sendRpc; drained by the integration layer.
    pending_sends: std.ArrayList(PendingRpc),
    /// PRNG state for randomU64 (xorshift64).
    rng_state: u64,
    /// Current time in milliseconds, set externally via setTime.
    time_ms: u64,
    /// Io instance, stored when handling inbound/outbound streams.
    io: ?Io,
    /// Outbound streams keyed by peer ID (owned keys).
    outbound_streams: std.StringHashMap(AnyStream),
    /// Topics we are subscribed to (owned keys), for announcing to new peers.
    tracked_subscriptions: std.StringHashMap(void),

    /// A pending outbound RPC message to a specific peer.
    pub const PendingRpc = struct {
        /// Peer identifier (owned copy).
        peer: []const u8,
        /// Encoded RPC data (owned copy).
        data: []const u8,
    };

    /// Protocol identifier for Switch integration.
    pub const id = config_mod.protocol_ids.v1_2;

    /// Create a new heap-allocated Service.
    ///
    /// The Service is heap-allocated because it contains a self-referential
    /// pointer: Router stores `*Handler` which points back to the Service.
    pub fn init(allocator: Allocator, gs_config: Config) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .router = undefined,
            .pending_sends = .empty,
            .rng_state = 12345,
            .time_ms = 0,
            .io = null,
            .outbound_streams = std.StringHashMap(AnyStream).init(allocator),
            .tracked_subscriptions = std.StringHashMap(void).init(allocator),
        };
        self.router = RouterType.init(allocator, gs_config, self) catch |e| {
            allocator.destroy(self);
            return e;
        };
        return self;
    }

    /// Release all resources owned by this Service.
    pub fn deinit(self: *Self) void {
        for (self.pending_sends.items) |p| {
            self.allocator.free(p.peer);
            self.allocator.free(p.data);
        }
        self.pending_sends.deinit(self.allocator);

        // Clean up outbound streams
        var os_iter = self.outbound_streams.iterator();
        while (os_iter.next()) |entry| {
            if (self.io) |io| {
                entry.value_ptr.*.close(io);
            }
            self.allocator.free(entry.key_ptr.*);
        }
        self.outbound_streams.deinit();

        // Clean up tracked subscriptions
        var ts_iter = self.tracked_subscriptions.iterator();
        while (ts_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.tracked_subscriptions.deinit();

        self.router.deinit();
        self.allocator.destroy(self);
    }

    // ---------------------------------------------------------------
    // Switch Protocol Handler interface
    // ---------------------------------------------------------------

    /// Handle an inbound gossipsub stream from a remote peer.
    /// Reads varint-length-prefixed protobuf frames from the stream using
    /// FrameDecoder and passes each decoded RPC to the Router.
    /// Pure reader — peer lifecycle is managed by the Switch via
    /// onPeerDisconnected (rust-libp2p pattern).
    pub fn handleInbound(self: *Self, io: Io, stream: anytype, ctx: anytype) !void {
        const peer_id: []const u8 = if (@hasField(@TypeOf(ctx), "peer_id"))
            (ctx.peer_id orelse return)
        else
            return;

        self.io = io;

        var decoder = FrameDecoder.init(self.allocator);
        defer decoder.deinit();

        var buf: [4096]u8 = undefined;
        while (true) {
            const n = stream.read(io, &buf) catch break;
            if (n == 0) break;
            decoder.feed(buf[0..n]) catch break;
            while (decoder.next() catch null) |frame| {
                defer self.allocator.free(frame);
                self.router.handleRpc(peer_id, frame) catch {};
            }
        }
    }

    /// Handle an outbound gossipsub stream.
    ///
    /// Stores the stream as an AnyStream keyed by peer ID so that sendRpc
    /// can write directly to it. Announces current subscriptions to the
    /// new peer.
    pub fn handleOutbound(self: *Self, io: Io, stream: anytype, ctx: anytype) !void {
        const peer_id: []const u8 = if (@hasField(@TypeOf(ctx), "peer_id"))
            (ctx.peer_id orelse return)
        else
            return;

        self.io = io;

        const any = AnyStream.wrap(@TypeOf(stream.*), stream);

        // If peer already has an outbound stream, close the old one and free the old key
        if (self.outbound_streams.fetchRemove(peer_id)) |old| {
            old.value.close(io);
            self.allocator.free(old.key);
        }

        const peer_copy = try self.allocator.dupe(u8, peer_id);
        self.outbound_streams.put(peer_copy, any) catch {
            self.allocator.free(peer_copy);
            return;
        };

        self.router.addPeer(peer_id) catch {};
        self.sendSubscriptionAnnouncement(peer_id);
    }

    /// Send a subscription announcement to a peer for all tracked subscriptions.
    fn sendSubscriptionAnnouncement(self: *Self, peer_id: []const u8) void {
        const count = self.tracked_subscriptions.count();
        if (count == 0) return;

        // Build subscription list from tracked_subscriptions
        // Use a stack buffer for SubOpts (up to 64 topics)
        var sub_opts: [64]?rpc.RPC.SubOpts = undefined;
        var i: usize = 0;
        var iter = self.tracked_subscriptions.keyIterator();
        while (iter.next()) |key| {
            if (i >= 64) break;
            sub_opts[i] = .{ .subscribe = true, .topicid = key.* };
            i += 1;
        }
        if (i == 0) return;

        var rpc_msg = rpc.RPC{ .subscriptions = sub_opts[0..i] };
        const frame = codec_mod.encodeRpc(self.allocator, &rpc_msg) catch return;
        defer self.allocator.free(frame);
        _ = self.sendRpc(peer_id, frame);
    }

    // ---------------------------------------------------------------
    // Router Handler interface (called by Router internally)
    // ---------------------------------------------------------------

    /// Send raw RPC bytes to a peer. Called by the Router.
    /// Tries direct write to outbound stream first; falls back to
    /// pending-sends queue for unit tests and unconnected peers.
    pub fn sendRpc(self: *Self, peer: []const u8, data: []const u8) bool {
        // Try direct write to outbound stream
        if (self.io) |io| {
            if (self.outbound_streams.get(peer)) |any_stream| {
                var total: usize = 0;
                while (total < data.len) {
                    const n = any_stream.write(io, data[total..]) catch return false;
                    if (n == 0) return false;
                    total += n;
                }
                return true;
            }
        }
        // Fallback: enqueue for external draining (unit tests, unconnected peers)
        const peer_copy = self.allocator.dupe(u8, peer) catch return false;
        const data_copy = self.allocator.dupe(u8, data) catch {
            self.allocator.free(peer_copy);
            return false;
        };
        self.pending_sends.append(self.allocator, .{
            .peer = peer_copy,
            .data = data_copy,
        }) catch {
            self.allocator.free(peer_copy);
            self.allocator.free(data_copy);
            return false;
        };
        return true;
    }

    /// Return a pseudo-random u64 using xorshift64.
    pub fn randomU64(self: *Self) u64 {
        var x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        return x;
    }

    /// Return the current time in milliseconds.
    pub fn currentTimeMs(self: *Self) u64 {
        return self.time_ms;
    }

    /// Check if a peer is currently connected.
    pub fn isPeerConnected(self: *Self, peer: []const u8) bool {
        _ = self;
        _ = peer;
        // TODO: track connected peers via addPeer/removePeer
        return true;
    }

    // ---------------------------------------------------------------
    // Public API — delegates to Router
    // ---------------------------------------------------------------

    /// Subscribe to a topic. Joins the mesh for this topic.
    pub fn subscribe(self: *Self, topic: []const u8) !void {
        try self.router.subscribe(topic);
        if (!self.tracked_subscriptions.contains(topic)) {
            const topic_copy = try self.allocator.dupe(u8, topic);
            self.tracked_subscriptions.put(topic_copy, {}) catch {
                self.allocator.free(topic_copy);
            };
        }
    }

    /// Unsubscribe from a topic. Leaves the mesh for this topic.
    pub fn unsubscribe(self: *Self, topic: []const u8) !void {
        try self.router.unsubscribe(topic);
        if (self.tracked_subscriptions.fetchRemove(topic)) |kv| {
            self.allocator.free(kv.key);
        }
    }

    /// Publish a message to a topic.
    /// Returns the number of peers the message was sent to.
    pub fn publish(self: *Self, topic: []const u8, data: []const u8) !u32 {
        return try self.router.publish(topic, data);
    }

    /// Notify the Router that a new peer has connected.
    pub fn addPeer(self: *Self, peer_id: []const u8) !void {
        try self.router.addPeer(peer_id);
    }

    /// Notify the Router that a peer has disconnected.
    pub fn removePeer(self: *Self, peer_id: []const u8) void {
        if (self.outbound_streams.fetchRemove(peer_id)) |entry| {
            if (self.io) |io| {
                entry.value.close(io);
            }
            self.allocator.free(entry.key);
        }
        self.router.removePeer(peer_id);
    }

    /// Execute one heartbeat tick. Should be called periodically
    /// (e.g., every Config.heartbeat_interval_ms milliseconds).
    pub fn heartbeat(self: *Self) !void {
        try self.router.heartbeat();
    }

    /// Drain accumulated events from the Router.
    /// Caller owns the returned slice and must free it.
    pub fn drainEvents(self: *Self) ![]Event {
        return try self.router.drainEvents();
    }

    /// Pass a received RPC (protobuf bytes, without varint length prefix) to
    /// the Router for processing.
    pub fn handleRpc(self: *Self, from_peer: []const u8, rpc_bytes: []const u8) !void {
        try self.router.handleRpc(from_peer, rpc_bytes);
    }

    /// Drain all pending outbound RPCs. Caller owns the returned slice
    /// and must free each entry's `peer` and `data` slices, plus the slice itself.
    pub fn drainPendingSends(self: *Self) []PendingRpc {
        return self.pending_sends.toOwnedSlice(self.allocator) catch return &.{};
    }

    /// Set the current time (for testing or external time source).
    pub fn setTime(self: *Self, ms: u64) void {
        self.time_ms = ms;
    }

    /// Set the PRNG seed.
    pub fn setSeed(self: *Self, seed: u64) void {
        self.rng_state = seed;
    }
};

/// Handler wraps a heap-allocated Service for embedding in Switch's HandlerTuple.
/// The Service is heap-allocated (self-referential: Router stores *Handler -> *Service).
/// This thin wrapper stores a pointer to the Service and is safe to embed by value.
pub const Handler = struct {
    svc: *Service,

    pub const id = Service.id;

    pub fn handleInbound(self: *Handler, io: Io, stream: anytype, ctx: anytype) !void {
        try self.svc.handleInbound(io, stream, ctx);
    }

    pub fn handleOutbound(self: *Handler, io: Io, stream: anytype, ctx: anytype) !void {
        try self.svc.handleOutbound(io, stream, ctx);
    }
};

// --- Tests ---

/// A message ID function that uses topic + data instead of from + seqno.
/// Suitable for testing with strict_no_sign / anonymous policies where
/// the router does not populate from/seqno on publish.
fn testMsgId(allocator: Allocator, msg: *const rpc.Message) anyerror![]const u8 {
    return std.mem.concat(allocator, u8, &.{ msg.topic orelse "", msg.data orelse "" });
}

/// Test config that does not require from/seqno for message IDs.
const test_config: Config = .{
    .signature_policy = .strict_no_sign,
    .publish_policy = .anonymous,
    .msg_id_fn = testMsgId,
};

test "Service init and deinit" {
    const svc = try Service.init(std.testing.allocator, .{});
    defer svc.deinit();
}

test "Service subscribe and unsubscribe" {
    const svc = try Service.init(std.testing.allocator, .{});
    defer svc.deinit();

    try svc.subscribe("test-topic");
    try svc.unsubscribe("test-topic");
}

test "Service subscribe, publish, heartbeat" {
    const svc = try Service.init(std.testing.allocator, test_config);
    defer svc.deinit();

    try svc.subscribe("test-topic");

    // Add a peer and publish
    try svc.addPeer("peer-1");

    // Run heartbeat to establish mesh
    svc.setTime(1000);
    try svc.heartbeat();

    // Publish a message
    _ = try svc.publish("test-topic", "hello");

    // Check pending sends
    const sends = svc.drainPendingSends();
    defer {
        for (sends) |s| {
            svc.allocator.free(s.peer);
            svc.allocator.free(s.data);
        }
        svc.allocator.free(sends);
    }

    try svc.unsubscribe("test-topic");
    svc.removePeer("peer-1");
}

test "Service drainPendingSends returns empty when nothing pending" {
    const svc = try Service.init(std.testing.allocator, .{});
    defer svc.deinit();

    const sends = svc.drainPendingSends();
    try std.testing.expectEqual(@as(usize, 0), sends.len);
    svc.allocator.free(sends);
}

test "Service setTime and setSeed" {
    const svc = try Service.init(std.testing.allocator, .{});
    defer svc.deinit();

    svc.setTime(42000);
    try std.testing.expectEqual(@as(u64, 42000), svc.currentTimeMs());

    svc.setSeed(99);
    const r1 = svc.randomU64();
    const r2 = svc.randomU64();
    try std.testing.expect(r1 != r2);
}

test "Service randomU64 is deterministic for same seed" {
    const svc = try Service.init(std.testing.allocator, .{});
    defer svc.deinit();

    svc.setSeed(12345);
    const a1 = svc.randomU64();
    const a2 = svc.randomU64();

    svc.setSeed(12345);
    const b1 = svc.randomU64();
    const b2 = svc.randomU64();

    try std.testing.expectEqual(a1, b1);
    try std.testing.expectEqual(a2, b2);
}

test "Service protocol id matches meshsub v1.2" {
    try std.testing.expectEqualStrings("/meshsub/1.2.0", Service.id);
}

test "Service publish generates pending sends to mesh peers" {
    const svc = try Service.init(std.testing.allocator, test_config);
    defer svc.deinit();

    try svc.subscribe("topic-a");
    try svc.addPeer("peer-1");
    try svc.addPeer("peer-2");

    // Subscribe peers to the topic so they are eligible for mesh
    var subs = [_]?rpc.RPC.SubOpts{
        .{ .subscribe = true, .topicid = "topic-a" },
    };
    var rpc_msg = rpc.RPC{ .subscriptions = &subs };
    const encoded = rpc_msg.encode(std.testing.allocator) catch unreachable;
    defer std.testing.allocator.free(encoded);
    try svc.handleRpc("peer-1", encoded);
    try svc.handleRpc("peer-2", encoded);

    // Heartbeat to graft peers into mesh
    svc.setTime(1000);
    try svc.heartbeat();

    // Clear any sends from heartbeat (GRAFT messages)
    const heartbeat_sends = svc.drainPendingSends();
    for (heartbeat_sends) |s| {
        svc.allocator.free(s.peer);
        svc.allocator.free(s.data);
    }
    svc.allocator.free(heartbeat_sends);

    // Publish a message
    const sent_count = try svc.publish("topic-a", "test-data");
    try std.testing.expect(sent_count > 0);

    // Verify pending sends were generated
    const sends = svc.drainPendingSends();
    defer {
        for (sends) |s| {
            svc.allocator.free(s.peer);
            svc.allocator.free(s.data);
        }
        svc.allocator.free(sends);
    }
    try std.testing.expect(sends.len > 0);
}
