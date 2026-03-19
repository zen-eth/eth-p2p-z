const std = @import("std");
const libp2p = @import("../../../root.zig");
const multiformats = @import("multiformats");
const Multiaddr = multiformats.multiaddr.Multiaddr;
const Switch = libp2p.swarm.Switch;
const quic = libp2p.transport.quic;
const ProtocolId = libp2p.protocols.ProtocolId;
const PeerId = @import("peer_id").PeerId;
const Allocator = std.mem.Allocator;
const io_loop = libp2p.thread_event_loop;
const xev = libp2p.xev;
const tls = libp2p.security.tls;
const identity = libp2p.identity;
const keys = @import("peer_id").keys;
const ssl = @import("ssl");
const swarm = libp2p.swarm;
const ping = libp2p.protocols.ping;
const rpc = libp2p.protobuf.rpc;
const uvarint = multiformats.uvarint;
const event = libp2p.event;
const pubsub = @import("../pubsub.zig");
const cache = @import("cache");
const p2p_conn = libp2p.conn;

const sign_prefix: []const u8 = "libp2p-pubsub:";

pub const semiduplex = @import("../semiduplex.zig");
pub const Semiduplex = semiduplex.Semiduplex;
pub const PubSubPeerInitiator = semiduplex.PubSubPeerInitiator;
pub const PubSubPeerResponder = semiduplex.PubSubPeerResponder;
pub const PubSubPeerProtocolHandler = semiduplex.PubSubPeerProtocolHandler;
pub const mcache = @import("mcache.zig");

pub const v1_id: ProtocolId = "/meshsub/1.0.0";
pub const v1_1_id: ProtocolId = "/meshsub/1.1.0";
pub const v1_2_id: ProtocolId = "/meshsub/1.2.0";

pub const protocols: []const ProtocolId = &.{ v1_id, v1_1_id };

/// DataTransformVTable defines the function pointers for transforming message data
/// before sending and after receiving. This allows for custom processing such as
/// encryption, compression, or other modifications to the message payload.
pub const DataTransformVTable = struct {
    inboundTransformFn: *const fn (instance: *anyopaque, allocator: Allocator, topic: []const u8, data: []const u8) anyerror![]const u8,
    outboundTransformFn: *const fn (instance: *anyopaque, allocator: Allocator, topic: []const u8, data: []const u8) anyerror![]const u8,
};

pub const AnyDataTransform = struct {
    instance: *anyopaque,
    vtable: *const DataTransformVTable,

    const Self = @This();

    pub fn inboundTransform(self: Self, allocator: Allocator, topic: []const u8, data: []const u8) ![]const u8 {
        return self.vtable.inboundTransformFn(self.instance, allocator, topic, data);
    }

    pub fn outboundTransform(self: Self, allocator: Allocator, topic: []const u8, data: []const u8) ![]const u8 {
        return self.vtable.outboundTransformFn(self.instance, allocator, topic, data);
    }
};

const FilterCtx = struct {
    mesh_peers: *std.AutoHashMapUnmanaged(PeerId, void),

    fn filter(ctx: ?*anyopaque, peer: PeerId) bool {
        const filter_ctx: *@This() = @ptrCast(@alignCast(ctx.?));
        return filter_ctx.mesh_peers.contains(peer);
    }
};

const GossipFilterCtx = struct {
    exclude_a: ?*std.AutoHashMapUnmanaged(PeerId, void) = null,
    exclude_b: ?*std.AutoHashMapUnmanaged(PeerId, void) = null,

    fn filter(ctx: ?*anyopaque, peer: PeerId) bool {
        const self: *@This() = @ptrCast(@alignCast(ctx.?));
        if (self.exclude_a) |map| {
            if (map.contains(peer)) return true;
        }
        if (self.exclude_b) |map| {
            if (map.contains(peer)) return true;
        }
        return false;
    }
};

const SelectPeersResult = struct {
    const SelectPeersCounts = struct {
        direct: u64,
        flood: u64,
        mesh: u64,
        fanout: u64,
    };

    to_send: []PeerId,
    to_send_count: SelectPeersCounts,
};

fn deinitControlIHaveOptional(allocator: Allocator, ihave_opt: ?rpc.ControlIHave) void {
    const ihave = ihave_opt orelse return;

    if (ihave.topic_i_d) |topic_id| {
        allocator.free(topic_id);
    }

    if (ihave.message_i_ds) |msg_ids| {
        for (msg_ids) |maybe_msg_id| {
            if (maybe_msg_id) |msg_id| {
                allocator.free(msg_id);
            }
        }
        allocator.free(msg_ids);
    }
}

fn deinitControlIHaveList(allocator: Allocator, list: *std.ArrayListUnmanaged(?rpc.ControlIHave)) void {
    for (list.items) |item| {
        deinitControlIHaveOptional(allocator, item);
    }
    list.deinit(allocator);
}

pub const SendingRPCContext = struct {
    gossipsub: *Gossipsub,
    control: ?rpc.ControlMessage,
    ihave: ?std.ArrayListUnmanaged(?rpc.ControlIHave),
    to: PeerId,

    pub fn callback(ctx: ?*anyopaque, res: anyerror!usize) void {
        const self: *SendingRPCContext = @ptrCast(@alignCast(ctx.?));
        defer self.gossipsub.allocator.destroy(self);

        if (res) |_| {
            // nothing to do
        } else |err| {
            std.log.warn("Failed to send RPC message to peer {}: {}", .{ self.gossipsub.peer_id, err });
            if (self.gossipsub.control.put(self.gossipsub.allocator, self.to, self.control)) |_| {
                self.control = null;
            } else |e| {
                std.log.warn("Failed to cache control message for peer {}: {}", .{ self.gossipsub.peer_id, e });
                pubsub.deinitControl(&self.control, self.gossipsub.allocator);
                self.control = null;
            }

            if (self.ihave) |*ihave_ptr| {
                if (self.gossipsub.gossip.put(self.gossipsub.allocator, self.to, ihave_ptr.*)) |_| {
                    self.ihave = null;
                } else |put_err| {
                    std.log.warn("Failed to cache IHAVE message for peer {}: {}", .{ self.gossipsub.peer_id, put_err });
                    deinitControlIHaveList(self.gossipsub.allocator, ihave_ptr);
                    self.ihave = null;
                }
            }
        }

        self.deinit(self.gossipsub.allocator);
    }

    pub fn deinit(self: *SendingRPCContext, allocator: std.mem.Allocator) void {
        pubsub.deinitControl(&self.control, allocator);
        self.control = null;
        if (self.ihave) |*ihave_ptr| {
            deinitControlIHaveList(allocator, ihave_ptr);
            self.ihave = null;
        }
    }
};

pub const Subscription = struct {
    topic: []const u8,
    subscribe: bool,
};

pub const AppMessage = struct {
    from: ?PeerId = null,
    data: []const u8,
    seqno: ?u64 = null,
    topic: []const u8,
    signature: ?[]const u8 = null,
    key: ?keys.PublicKey = null,
};

pub const MessageStatus = enum {
    Valid,
    Invalid,
    Duplicate,
};

pub const MessageValidationResult = union(enum) {
    valid: struct {
        message_id: []const u8,
        message: AppMessage,
    },
    invalid: struct {
        reason: RejectReason,
        err: ValidateError,
    },
    duplicate: struct {
        message_id: []const u8,
    },
};

pub const RejectReason = enum {
    Error,
    Ignore,
    Reject,
    Blacklisted,
};

pub const ValidateError = error{
    InvalidSignature,
    InvalidSeqno,
    InvalidPeerId,
    InvalidPubkey,
    SignaturePresent,
    SeqnoPresent,
    FromPresent,
    KeyPresent,
    TransformFailed,
    MessageIdGenerationFailed,
} || Allocator.Error;

pub const SignaturePolicy = enum {
    StrictSign,
    StrictNoSign,
};

pub const Event = union(enum) {
    subscription_changed: struct {
        peer: PeerId,
        subscriptions: []Subscription,
    },
    gossipsub_graft: struct {
        peer: PeerId,
        topic: []const u8,
        direction: p2p_conn.Direction,
    },
    gossipsub_prune: struct {
        peer: PeerId,
        topic: []const u8,
        direction: p2p_conn.Direction,
    },
    message: struct {
        propagation_source: PeerId,
        message_id: []const u8,
        message: *const AppMessage,
    },
    heartbeat: struct {},

    pub fn hash(self: Event, hasher: anytype) void {
        const tag = std.meta.activeTag(self);
        hasher.update(std.mem.asBytes(&tag));

        switch (self) {
            .subscription_changed => |sub_change| {
                var peer_buf: [128]u8 = undefined; // this is enough space for a PeerId
                const peer_bytes = sub_change.peer.toBytes(&peer_buf) catch unreachable;
                hasher.update(peer_bytes);

                hasher.update(std.mem.asBytes(&sub_change.subscriptions.len));

                for (sub_change.subscriptions) |subscription| {
                    hasher.update(std.mem.asBytes(&subscription.topic.len));
                    hasher.update(subscription.topic);

                    hasher.update(std.mem.asBytes(&subscription.subscribe));
                }
            },
            .gossipsub_graft, .gossipsub_prune => |graft_or_prune| {
                var peer_buf: [128]u8 = undefined; // this is enough space for a PeerId
                const peer_bytes = graft_or_prune.peer.toBytes(&peer_buf) catch unreachable;
                hasher.update(peer_bytes);

                hasher.update(std.mem.asBytes(&graft_or_prune.topic.len));
                hasher.update(graft_or_prune.topic);

                hasher.update(std.mem.asBytes(&graft_or_prune.direction));
            },
            .message => |msg_event| {
                hasher.update(std.mem.asBytes(&msg_event.message_id.len));
                hasher.update(msg_event.message_id);
            },
            .heartbeat => {},
        }
    }

    pub fn eql(self: Event, other: Event) bool {
        const self_tag = std.meta.activeTag(self);
        const other_tag = std.meta.activeTag(other);

        if (self_tag != other_tag) return false;

        switch (self) {
            .subscription_changed => |self_sub| {
                const other_sub = other.subscription_changed;

                if (!self_sub.peer.eql(&other_sub.peer)) {
                    return false;
                }

                if (self_sub.subscriptions.len != other_sub.subscriptions.len) {
                    return false;
                }

                for (self_sub.subscriptions, other_sub.subscriptions) |self_subscription, other_subscription| {
                    if (!std.mem.eql(u8, self_subscription.topic, other_subscription.topic) or
                        self_subscription.subscribe != other_subscription.subscribe)
                    {
                        return false;
                    }
                }

                return true;
            },
            .gossipsub_graft => |self_graft| {
                const other_graft = other.gossipsub_graft;

                return self_graft.peer.eql(&other_graft.peer) and
                    std.mem.eql(u8, self_graft.topic, other_graft.topic) and
                    self_graft.direction == other_graft.direction;
            },
            .gossipsub_prune => |self_prune| {
                const other_prune = other.gossipsub_prune;

                return self_prune.peer.eql(&other_prune.peer) and
                    std.mem.eql(u8, self_prune.topic, other_prune.topic) and
                    self_prune.direction == other_prune.direction;
            },
            .message => |self_msg| {
                const other_msg = other.message;

                return std.mem.eql(u8, self_msg.message_id, other_msg.message_id);
            },
            .heartbeat => return true,
        }
    }
};

/// Gossipsub is a PubSub router implementation that follows the Gossipsub protocol.
/// It maintains a list of peers and topics, and handles incoming and outgoing messages.
/// It uses the Semiduplex struct to manage bidirectional communication channels.
/// It also provides methods to add and remove peers, and to handle incoming RPC messages.
///
const heartbeat_seed_salt: u64 = 0x9e3779b97f4a7c15;

fn heartbeatSeed(now_ms: i64, ticks: u64) u64 {
    const now_bits: u64 = @bitCast(now_ms);
    return now_bits ^ (ticks ^ heartbeat_seed_salt);
}

pub const Gossipsub = struct {
    peers: std.AutoHashMap(PeerId, Semiduplex),

    swarm: *Switch,

    peer: Multiaddr,

    peer_id: PeerId,

    peer_id_bytes: []const u8,

    sign_key: *identity.KeyPair,

    allocator: Allocator,

    // map of topics peers are interested in
    topics: std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)),

    // set of topics we are subscribed to
    subscriptions: std.StringHashMapUnmanaged(void),

    mesh: std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)),

    fanout: std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)),

    fanout_last_pub: std.StringHashMapUnmanaged(i64),

    peer_have: std.AutoHashMapUnmanaged(PeerId, usize),

    gossip: std.AutoHashMapUnmanaged(PeerId, std.ArrayListUnmanaged(?rpc.ControlIHave)),

    control: std.AutoHashMapUnmanaged(PeerId, ?rpc.ControlMessage),

    iasked: std.AutoArrayHashMapUnmanaged(PeerId, usize),

    event_emitter: event.EventEmitter(Event),

    seen_cache: cache.Cache(void),

    mcache: mcache.MessageCache,

    opts: Options,

    heartbeat_interval_ns: u64,

    heartbeat_ticks: u64,

    heartbeat_timer: xev.Timer,
    heartbeat_completion: xev.Completion,
    heartbeat_cancel_completion: xev.Completion,
    heartbeat_running: bool,
    heartbeat_stop_event: std.Thread.ResetEvent = .{},

    counter: std.atomic.Value(u64),

    topic_pool: std.StringHashMapUnmanaged(usize),

    protocols: []const ProtocolId = &.{ v1_id, v1_1_id },

    const Options = struct {
        seen_ttl_s: u32 = 120,
        max_messages_per_rpc: ?usize = null,
        global_signature_policy: SignaturePolicy = .StrictSign,
        publish_policy: pubsub.PublishPolicy = .signing,
        data_transform: ?AnyDataTransform = null,
        msg_id_fn: pubsub.MessageIdFn = pubsub.defaultMsgId,
        flood_publish: bool = true,
        emit_self: bool = false,
        idontwant_min_size: usize = 512,
        idontwant_message_size_threshold: usize = 1024,
        max_ihave_messages: usize = 10,
        max_ihave_len: usize = 5000,
        history_length: usize = 5,
        history_gossip: usize = 3,
        gossip_retransmission: usize = 3,
        D: usize = 6,
        D_lo: usize = 4,
        D_hi: usize = 12,
        D_lazy: usize = 6,
        heartbeat_interval_ms: u64 = 1_000,
        fanout_ttl_ms: i64 = 60_000,
        gossip_factor: f32 = 0.25,
    };

    const AddPeerCtx = struct {
        pubsub: *Gossipsub,
        semiduplex: ?*Semiduplex,
        callback_ctx: ?*anyopaque,
        callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,

        fn onOutgoingNewStream(callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void {
            const self: *AddPeerCtx = @ptrCast(@alignCast(callback_ctx.?));
            defer self.pubsub.allocator.destroy(self);
            const initiator = controller catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };
            const stream_initiator: *PubSubPeerInitiator = @ptrCast(@alignCast(initiator.?));
            const peer_id = stream_initiator.stream.conn.security_session.?.remote_id;

            if (self.semiduplex) |semi_duplex| {
                semi_duplex.initiator = stream_initiator;
            } else {
                const result = self.pubsub.peers.getOrPut(peer_id) catch |err| {
                    self.callback(self.callback_ctx, err);
                    return;
                };

                if (result.found_existing) {
                    result.value_ptr.initiator = stream_initiator;
                } else {
                    result.value_ptr.* = Semiduplex{
                        .initiator = stream_initiator,
                        .responder = null,
                        .allocator = self.pubsub.allocator,
                    };
                }
            }

            stream_initiator.stream.close_ctx = .{
                .active_callback_ctx = null,
                .active_callback = null,
                .callback_ctx = self.pubsub,
                .callback = Gossipsub.onStreamClose,
            };

            self.pubsub.sendExistingSubscriptionsToPeer(peer_id) catch |err| {
                std.log.warn("failed to send existing subscriptions to peer {}: {}", .{ peer_id, err });
            };

            self.callback(self.callback_ctx, {});
        }
    };

    const RemovePeerCtx = struct {
        pubsub: *Gossipsub,
        peer: PeerId,
        callback_ctx: ?*anyopaque,
        callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,

        fn onCloseSemiduplex(ctx: ?*anyopaque, _: anyerror!*Semiduplex) void {
            const self: *RemovePeerCtx = @ptrCast(@alignCast(ctx.?));
            defer self.pubsub.allocator.destroy(self);

            _ = self.pubsub.peers.remove(self.peer);
            self.callback(self.callback_ctx, {});
        }
    };

    const Self = @This();

    pub fn init(self: *Self, allocator: Allocator, peer: Multiaddr, peer_id: PeerId, sign_key: *identity.KeyPair, network_swarm: *Switch, opts: Options) !void {
        var seen_cache = try cache.Cache(void).init(allocator, .{});
        errdefer seen_cache.deinit();

        var msg_cache = try mcache.MessageCache.init(allocator, opts.history_gossip, opts.history_length, opts.msg_id_fn);
        errdefer msg_cache.deinit();

        var peer_id_buf: [128]u8 = undefined; // this is enough space for a PeerId
        const peer_id_bytes_src = try peer_id.toBytes(&peer_id_buf);
        const peer_id_bytes = try allocator.dupe(u8, peer_id_bytes_src);
        errdefer allocator.free(peer_id_bytes);

        var heartbeat_timer = try xev.Timer.init();
        errdefer heartbeat_timer.deinit();

        self.* = .{
            .allocator = allocator,
            .peer = peer,
            .peer_id = peer_id,
            .peer_id_bytes = peer_id_bytes,
            .sign_key = sign_key,
            .swarm = network_swarm,
            .peers = std.AutoHashMap(PeerId, Semiduplex).init(allocator),
            .topics = std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)).empty,
            .subscriptions = std.StringHashMapUnmanaged(void).empty,
            .mesh = std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)).empty,
            .fanout = std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)).empty,
            .fanout_last_pub = std.StringHashMapUnmanaged(i64).empty,
            .peer_have = std.AutoHashMapUnmanaged(PeerId, usize).empty,
            .gossip = std.AutoHashMapUnmanaged(PeerId, std.ArrayListUnmanaged(?rpc.ControlIHave)).empty,
            .control = std.AutoHashMapUnmanaged(PeerId, ?rpc.ControlMessage).empty,
            .iasked = std.AutoArrayHashMapUnmanaged(PeerId, usize).empty,
            .event_emitter = event.EventEmitter(Event).init(allocator),
            .seen_cache = seen_cache,
            .mcache = msg_cache,
            .opts = opts,
            .heartbeat_interval_ns = opts.heartbeat_interval_ms * std.time.ns_per_ms,
            .heartbeat_ticks = 0,
            .counter = std.atomic.Value(u64).init(0),
            .topic_pool = std.StringHashMapUnmanaged(usize).empty,
            .heartbeat_timer = heartbeat_timer,
            .heartbeat_completion = .{},
            .heartbeat_cancel_completion = .{},
            .heartbeat_running = false,
            .heartbeat_stop_event = .{},
        };

        self.startHeartbeatTimer();
    }

    pub fn deinit(self: *Self) void {
        self.stopHeartbeatTimer();

        self.peers.deinit();

        var topic_iter = self.topics.iterator();
        while (topic_iter.next()) |entry| {
            self.unrefTopic(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.topics.deinit(self.allocator);

        var my_topic_iter = self.subscriptions.iterator();
        while (my_topic_iter.next()) |entry| {
            self.unrefTopic(entry.key_ptr.*);
        }
        self.subscriptions.deinit(self.allocator);
        var mesh_iter = self.mesh.iterator();
        while (mesh_iter.next()) |entry| {
            self.unrefTopic(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.mesh.deinit(self.allocator);
        self.peer_have.deinit(self.allocator);
        var fanout_iter = self.fanout.iterator();
        while (fanout_iter.next()) |entry| {
            self.unrefTopic(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.fanout.deinit(self.allocator);

        var fanout_last_pub_iter = self.fanout_last_pub.iterator();
        while (fanout_last_pub_iter.next()) |entry| {
            self.unrefTopic(entry.key_ptr.*);
        }
        self.fanout_last_pub.deinit(self.allocator);

        var control_iter = self.control.iterator();
        while (control_iter.next()) |entry| {
            pubsub.deinitControl(entry.value_ptr, self.allocator);
        }
        self.control.deinit(self.allocator);
        var gossip_iter = self.gossip.iterator();
        while (gossip_iter.next()) |entry| {
            deinitControlIHaveList(self.allocator, entry.value_ptr);
        }
        self.gossip.deinit(self.allocator);
        self.iasked.deinit(self.allocator);
        self.event_emitter.deinit();
        self.seen_cache.deinit();
        self.mcache.deinit();
        var topic_pool_iter = self.topic_pool.iterator();
        while (topic_pool_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.allocator.free(self.peer_id_bytes);
        self.topic_pool.deinit(self.allocator);
    }

    fn startHeartbeatTimer(self: *Self) void {
        if (self.swarm.transport.io_event_loop.inEventLoopThread()) {
            self.startHeartbeatTimerOnLoop();
        } else {
            io_loop.ThreadEventLoop.PubsubTasks.queuePubsubStartHeartbeat(
                self.swarm.transport.io_event_loop,
                self.any(),
            ) catch unreachable;
        }
    }

    fn startHeartbeatTimerOnLoop(self: *Self) void {
        if (self.heartbeat_running) return;
        self.heartbeat_running = true;
        self.scheduleHeartbeat();
    }

    fn stopHeartbeatTimer(self: *Self) void {
        if (self.swarm.transport.io_event_loop.inEventLoopThread()) {
            self.stopHeartbeatTimerOnLoop();
        } else {
            self.heartbeat_stop_event.reset();
            io_loop.ThreadEventLoop.PubsubTasks.queuePubsubStopHeartbeat(
                self.swarm.transport.io_event_loop,
                self.any(),
            ) catch unreachable;
            self.heartbeat_stop_event.wait();
        }
    }

    fn stopHeartbeatTimerOnLoop(self: *Self) void {
        if (!self.heartbeat_running) {
            self.heartbeat_stop_event.set();
            return;
        }
        self.heartbeat_running = false;
        self.heartbeat_timer.cancel(
            &self.swarm.transport.io_event_loop.loop,
            &self.heartbeat_completion,
            &self.heartbeat_cancel_completion,
            Self,
            self,
            heartbeatCancelCallback,
        );
    }

    fn scheduleHeartbeat(self: *Self) void {
        if (!self.heartbeat_running) return;
        const interval_ms = if (self.opts.heartbeat_interval_ms == 0) 1 else self.opts.heartbeat_interval_ms;
        self.heartbeat_timer.reset(
            &self.swarm.transport.io_event_loop.loop,
            &self.heartbeat_completion,
            &self.heartbeat_cancel_completion,
            interval_ms,
            Self,
            self,
            heartbeatTimerCallback,
        );
    }

    fn heartbeatTimerCallback(
        ctx: ?*Self,
        loop: *xev.Loop,
        _: *xev.Completion,
        r: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        if (loop.stopped()) return .disarm;

        const self = ctx.?;

        _ = r catch |err| {
            if (err == error.Canceled) return .disarm;
            std.log.warn("heartbeat timer failed: {}", .{err});
            return .disarm;
        };

        if (!self.heartbeat_running) return .disarm;

        self.doHeartbeat();

        if (!self.heartbeat_running) return .disarm;

        self.scheduleHeartbeat();

        return .disarm;
    }

    fn heartbeatCancelCallback(
        ctx: ?*Self,
        loop: *xev.Loop,
        _: *xev.Completion,
        _: xev.Timer.CancelError!void,
    ) xev.CallbackAction {
        if (ctx) |self| {
            self.heartbeat_stop_event.set();
        }
        if (loop.stopped()) return .disarm;
        return .disarm;
    }

    pub fn acceptFrom(self: *Self, peer_id: PeerId) bool {
        // TODO: Implement peer acceptance logic
        _ = self;
        _ = peer_id;
        return true;
    }

    pub fn handleRPC(self: *Self, arena: Allocator, rpc_message: *const pubsub.RPC) !void {
        if (!self.acceptFrom(rpc_message.from)) {
            std.log.warn("Rejected RPC message from peer: {}", .{rpc_message.from});
            return;
        }

        const subs = try rpc_message.rpc_reader.getSubscriptions(arena);

        //TODO:Implement subscription filtering

        var subscriptions = std.ArrayListUnmanaged(Subscription).empty;

        for (subs) |sub| {
            const topic_id = sub.getTopicid();
            const subscribe_msg = sub.getSubscribe();

            try self.handleSubscription(&rpc_message.from, topic_id, subscribe_msg);

            try subscriptions.append(arena, .{ .topic = topic_id, .subscribe = subscribe_msg });
        }

        self.event_emitter.emit(.{ .subscription_changed = .{
            .peer = rpc_message.from,
            .subscriptions = subscriptions.items,
        } });

        // Handle publish
        const msgs = try rpc_message.rpc_reader.getPublish(arena);

        if (self.opts.max_messages_per_rpc) |max| if (msgs.len > max) {
            std.log.warn("Received {} messages, exceeding limit of {}", .{ msgs.len, max });
            return;
        };

        for (msgs) |*msg| {
            try self.handleMessage(arena, &rpc_message.from, msg);
        }

        const control = try rpc_message.rpc_reader.getControl(arena);
        defer control.deinit();
        if (control.buf.bytes().len > 0) {
            try self.handleControl(arena, &control, &rpc_message.from);
        }
    }

    pub fn removePeer(self: *Self, peer: PeerId, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        if (self.swarm.transport.io_event_loop.inEventLoopThread()) {
            self.doRemovePeer(peer, callback_ctx, callback);
        } else {
            io_loop.ThreadEventLoop.PubsubTasks.queuePubsubRemovePeer(
                self.swarm.transport.io_event_loop,
                self.any(),
                peer,
                callback_ctx,
                callback,
            ) catch unreachable;
        }
    }

    pub fn addPeer(self: *Self, peer: Multiaddr, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        if (self.swarm.transport.io_event_loop.inEventLoopThread()) {
            self.doAddPeer(peer, callback_ctx, callback);
        } else {
            io_loop.ThreadEventLoop.PubsubTasks.queuePubsubAddPeer(
                self.swarm.transport.io_event_loop,
                self.any(),
                peer,
                callback_ctx,
                callback,
            ) catch unreachable;
        }
    }

    pub fn doAddPeer(self: *Self, peer: Multiaddr, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        // TODO: Make `maToStdAddrAndPeerId` more generic
        const addr_and_peer_id = quic.maToStdAddrAndPeerId(peer) catch |err| {
            std.log.warn("Failed to convert Multiaddr to standard address and peer ID: {}", .{err});
            callback(callback_ctx, err);
            return;
        };

        if (self.peers.getEntry(addr_and_peer_id.peer_id.?)) |entry| {
            if (entry.value_ptr.initiator == null) {
                const add_peer_ctx = self.allocator.create(AddPeerCtx) catch unreachable;
                add_peer_ctx.* = AddPeerCtx{
                    .pubsub = self,
                    .semiduplex = entry.value_ptr,
                    .callback_ctx = callback_ctx,
                    .callback = callback,
                };
                self.swarm.newStream(peer, self.protocols, add_peer_ctx, AddPeerCtx.onOutgoingNewStream);
                return;
            }
        } else {
            const add_peer_ctx = self.allocator.create(AddPeerCtx) catch unreachable;
            add_peer_ctx.* = AddPeerCtx{
                .pubsub = self,
                .semiduplex = null,
                .callback_ctx = callback_ctx,
                .callback = callback,
            };
            self.swarm.newStream(peer, self.protocols, add_peer_ctx, AddPeerCtx.onOutgoingNewStream);
        }
    }

    pub fn doRemovePeer(self: *Self, peer: PeerId, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        if (!self.peers.contains(peer)) {
            return;
        }

        const remove_peer_ctx = self.allocator.create(RemovePeerCtx) catch unreachable;
        remove_peer_ctx.* = RemovePeerCtx{
            .pubsub = self,
            .peer = peer,
            .callback_ctx = callback_ctx,
            .callback = callback,
        };
        self.peers.getPtr(peer).?.close(remove_peer_ctx, RemovePeerCtx.onCloseSemiduplex);
    }

    pub fn onIncomingNewStream(ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        const resp = controller catch unreachable;
        const responder: *PubSubPeerResponder = @ptrCast(@alignCast(resp.?));
        responder.pubsub = self.any();
        const peer_id = responder.stream.conn.security_session.?.remote_id;

        const result = self.peers.getOrPut(peer_id) catch unreachable;

        if (result.found_existing) {
            if (result.value_ptr.responder != null) {
                result.value_ptr.replace_stream = true;
                const old_responder = result.value_ptr.responder.?;
                old_responder.stream.close(null, struct {
                    fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
                }.callback);
            }
            result.value_ptr.responder = responder;
        } else {
            result.value_ptr.* = Semiduplex{
                .initiator = null,
                .responder = responder,
                .allocator = self.allocator,
            };
        }

        responder.stream.close_ctx = .{
            .active_callback_ctx = null,
            .active_callback = null,
            .callback_ctx = self,
            .callback = Self.onStreamClose,
        };
    }

    pub fn publish(self: *Self, topic: []const u8, data: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void) void {
        const copied_topic = self.allocator.dupe(u8, topic) catch |err| {
            std.log.warn("Failed to copy topic string: {}", .{err});
            callback(callback_ctx, err);
            return;
        };

        const copied_data = self.allocator.dupe(u8, data) catch |err| {
            self.allocator.free(copied_topic);
            std.log.warn("Failed to copy data buffer: {}", .{err});
            callback(callback_ctx, err);
            return;
        };

        if (self.swarm.transport.io_event_loop.inEventLoopThread()) {
            self.publishOwnedInternal(copied_topic, copied_data, callback_ctx, callback);
        } else {
            io_loop.ThreadEventLoop.PubsubTasks.queuePubsubPublish(
                self.swarm.transport.io_event_loop,
                self.any(),
                copied_topic,
                copied_data,
                callback_ctx,
                callback,
            ) catch |err| {
                self.allocator.free(copied_data);
                self.allocator.free(copied_topic);
                callback(callback_ctx, err);
                return;
            };
        }
    }

    fn publishOwnedInternal(self: *Self, topic: []const u8, data: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void) void {
        std.debug.assert(self.swarm.transport.io_event_loop.inEventLoopThread());
        self.doPublish(topic, data, callback_ctx, callback);
    }

    pub fn getAllocator(self: *Self) Allocator {
        return self.allocator;
    }

    pub fn doPublish(self: *Self, topic: []const u8, data: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void) void {
        defer self.allocator.free(topic);
        defer self.allocator.free(data);
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        const transformed_data = if (self.opts.data_transform) |transform| blk: {
            break :blk transform.outboundTransform(arena_allocator, topic, data) catch |err| {
                std.log.warn("Failed to transform outbound message for topic {s}: {}", .{ topic, err });
                callback(callback_ctx, err);
                return;
            };
        } else blk: {
            //TODO: Copy data to allocator memory?
            break :blk data;
        };

        var publish_msg: rpc.Message = undefined;
        var app_msg: AppMessage = undefined;
        var msg_id: []const u8 = undefined;

        self.buildRPCandMsgId(arena_allocator, topic, data, transformed_data, &publish_msg, &app_msg, &msg_id) catch |err| {
            std.log.warn("Failed to build RPC message for topic {s}: {}", .{ topic, err });
            callback(callback_ctx, err);
            return;
        };

        if (self.seen_cache.contains(msg_id)) {
            // Message already seen, do not publish again
            callback(callback_ctx, error.PublishDuplicate);
            return;
        }

        const selected_result = self.selectPeersToPublish(arena_allocator, topic) catch |err| {
            std.log.warn("Failed to select peers to publish for topic {s}: {}", .{ topic, err });
            callback(callback_ctx, err);
            return;
        };

        const will_send_to_self = self.subscriptions.contains(topic) and self.opts.emit_self;

        if (selected_result.to_send.len == 0 and !will_send_to_self) {
            // No peers to send to, return early
            callback(callback_ctx, error.NoPeersSubscribedToTopic);
            return;
        }

        self.seen_cache.put(msg_id, {}, .{
            .ttl = self.opts.seen_ttl_s,
        }) catch |err| {
            std.log.warn("Failed to cache seen message ID for topic {s}: {}", .{ topic, err });
            callback(callback_ctx, err);
            return;
        };

        self.mcache.putWithId(msg_id, &publish_msg) catch |err| {
            std.log.warn("Failed to cache message in message cache for topic {s}: {}", .{ topic, err });
            callback(callback_ctx, err);
            return;
        };

        // TODO: should we support batch publish?
        // rust-libp2p send idontwant if the message is large enough
        var receipents = std.ArrayListUnmanaged(PeerId).empty;
        errdefer receipents.deinit(self.allocator);
        for (selected_result.to_send) |*to| {
            const rpc_msg = rpc.RPC{
                .publish = &.{publish_msg},
            };
            const sent = self.sendRPC(arena_allocator, to, &rpc_msg);
            if (sent) {
                receipents.append(self.allocator, to.*) catch |err| {
                    std.log.warn("Failed to record sent peer {} for topic {s}: {}", .{ to.*, topic, err });
                    callback(callback_ctx, err);
                    return;
                };
            }
        }

        if (will_send_to_self) {
            receipents.append(self.allocator, self.peer_id) catch |err| {
                std.log.warn("Failed to record sent peer {} for topic {s}: {}", .{ self.peer_id, topic, err });
                callback(callback_ctx, err);
                return;
            };
            self.event_emitter.emit(.{ .message = .{
                .propagation_source = self.peer_id,
                .message_id = msg_id,
                .message = &app_msg,
            } });
        }

        const receipents_slice = receipents.toOwnedSlice(self.allocator) catch |err| {
            std.log.warn("Failed to finalize receipents list for topic {s}: {}", .{ topic, err });
            callback(callback_ctx, err);
            return;
        };
        callback(callback_ctx, receipents_slice);
    }

    pub fn subscribe(self: *Self, topic: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        if (self.swarm.transport.io_event_loop.inEventLoopThread()) {
            const copied_topic = self.allocator.dupe(u8, topic) catch |err| {
                std.log.warn("Failed to copy topic string: {}", .{err});
                callback(callback_ctx, err);
                return;
            };
            self.doSubscribe(copied_topic, callback_ctx, callback);
        } else {
            io_loop.ThreadEventLoop.PubsubTasks.queuePubsubSubscribe(
                self.swarm.transport.io_event_loop,
                self.any(),
                topic,
                callback_ctx,
                callback,
            ) catch unreachable;
        }
    }

    fn doSubscribe(self: *Self, topic: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        defer self.allocator.free(topic);
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        if (!self.subscriptions.contains(topic)) {
            const interned_topic = self.refTopic(topic) catch |err| {
                callback(callback_ctx, err);
                return;
            };
            self.subscriptions.put(self.allocator, interned_topic, {}) catch |err| {
                std.log.warn("Failed to add subscription for topic {s}: {}", .{ topic, err });
                self.unrefTopic(interned_topic);
                callback(callback_ctx, err);
                return;
            };

            var peers_iter = self.peers.iterator();
            while (peers_iter.next()) |entry| {
                const peer_id = entry.key_ptr;
                self.sendSubscriptions(arena_allocator, peer_id, &.{interned_topic}, true) catch |err| {
                    std.log.warn("Failed to send subscription for topic {s} to peer {}: {}", .{ topic, peer_id, err });
                    callback(callback_ctx, err);
                    continue;
                };
            }
        }

        self.join(arena_allocator, topic) catch |err| {
            std.log.warn("Failed to join mesh for topic {s}: {}", .{ topic, err });
            callback(callback_ctx, err);
            return;
        };

        callback(callback_ctx, {});
    }

    pub fn unsubscribe(self: *Self, topic: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        if (self.swarm.transport.io_event_loop.inEventLoopThread()) {
            const copied_topic = self.allocator.dupe(u8, topic) catch |err| {
                std.log.warn("Failed to copy topic string: {}", .{err});
                callback(callback_ctx, err);
                return;
            };
            self.doUnsubscribe(copied_topic, callback_ctx, callback);
        } else {
            io_loop.ThreadEventLoop.PubsubTasks.queuePubsubUnsubscribe(
                self.swarm.transport.io_event_loop,
                self.any(),
                topic,
                callback_ctx,
                callback,
            ) catch unreachable;
        }
    }

    fn doUnsubscribe(self: *Self, topic: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        defer self.allocator.free(topic);
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        if (self.subscriptions.fetchRemove(topic)) |removed| {
            const interned_topic = removed.key;
            var peers_iter = self.peers.iterator();
            while (peers_iter.next()) |entry| {
                const peer_id = entry.key_ptr;
                self.sendSubscriptions(arena_allocator, peer_id, &.{interned_topic}, false) catch |err| {
                    std.log.warn("Failed to send unsubscription for topic {s} to peer {}: {}", .{ topic, peer_id, err });
                    callback(callback_ctx, err);
                    continue;
                };
            }
            self.unrefTopic(interned_topic);
        }

        self.leave(arena_allocator, topic);

        callback(callback_ctx, {});
    }

    fn forwardMessage(self: *Self, arena: Allocator, msg_id: []const u8, rpc_msg: *const rpc.Message, propagation_source: ?PeerId, exclude: ?[]PeerId) !void {
        // TODO: Implement peer scoring
        _ = msg_id;
        const selected_peers = try self.selectPeersToForward(arena, rpc_msg.topic.?, propagation_source, exclude);

        for (selected_peers) |*to| {
            var publish_storage: [1]?rpc.Message = undefined;
            publish_storage[0] = rpc_msg.*;

            const rpc_to_send = rpc.RPC{
                .publish = publish_storage[0..],
            };
            _ = self.sendRPC(arena, to, &rpc_to_send);
        }
    }

    fn selectPeersToForward(self: *Self, arena: Allocator, topic: []const u8, propagation_source: ?PeerId, exclude: ?[]PeerId) ![]PeerId {
        var selected_peers = std.ArrayListUnmanaged(PeerId).empty;

        if (self.topics.getPtr(topic)) |peers_map| {
            _ = peers_map;
            //TODO: implement direct peers, floodsub peers
        }

        if (self.mesh.getPtr(topic)) |mesh_peers| {
            var it = mesh_peers.iterator();
            while (it.next()) |entry| {
                if (propagation_source) |*source| {
                    if (entry.key_ptr.*.eql(source)) continue;
                }
                if (exclude) |ex| {
                    var found = false;
                    for (ex) |*e| {
                        if (entry.key_ptr.*.eql(e)) {
                            found = true;
                            break;
                        }
                    }
                    if (found) continue;
                }
                try selected_peers.append(arena, entry.key_ptr.*);
            }
        }

        return try selected_peers.toOwnedSlice(arena);
    }

    fn selectPeersToPublish(self: *Self, arena: Allocator, topic: []const u8) !SelectPeersResult {
        var selected_peers = std.ArrayListUnmanaged(PeerId).empty;

        var to_send_count: SelectPeersResult.SelectPeersCounts = .{
            .direct = 0,
            .flood = 0,
            .mesh = 0,
            .fanout = 0,
        };

        if (self.topics.getPtr(topic)) |peers_map| {
            if (self.opts.flood_publish) {
                var it = peers_map.iterator();
                while (it.next()) |entry| {
                    // TODO: support direct peers and peer scoring
                    to_send_count.flood += 1;
                    try selected_peers.append(arena, entry.key_ptr.*);
                }
            } else {
                // TODO: support direct peers and floodsub peers
                if (self.mesh.getPtr(topic)) |mesh_peers| {
                    var it = mesh_peers.iterator();
                    while (it.next()) |entry| {
                        to_send_count.mesh += 1;
                        try selected_peers.append(arena, entry.key_ptr.*);
                    }

                    if (mesh_peers.count() < self.opts.D) {
                        var filter_ctx: FilterCtx = .{
                            .mesh_peers = mesh_peers,
                        };
                        var more_peers = try self.getRandomGossipPeers(topic, self.opts.D - mesh_peers.count(), &filter_ctx, FilterCtx.filter);
                        defer more_peers.deinit(self.allocator);
                        try selected_peers.appendSlice(arena, more_peers.items);
                        to_send_count.fanout += more_peers.items.len;
                    }
                } else {
                    if (self.fanout.getPtr(topic)) |fanout_peers| {
                        var it = fanout_peers.iterator();
                        while (it.next()) |entry| {
                            to_send_count.fanout += 1;
                            try selected_peers.append(arena, entry.key_ptr.*);
                        }
                    } else {
                        var more_peers = try self.getRandomGossipPeers(topic, self.opts.D, null, struct {
                            fn filter(_: ?*anyopaque, _: PeerId) bool {
                                return false;
                            }
                        }.filter);
                        defer more_peers.deinit(self.allocator);
                        if (more_peers.items.len > 0) {
                            var new_fanout: std.AutoHashMapUnmanaged(PeerId, void) = .empty;
                            errdefer new_fanout.deinit(self.allocator);
                            for (more_peers.items) |p| {
                                try new_fanout.put(self.allocator, p, {});
                                try selected_peers.append(arena, p);
                                to_send_count.fanout += 1;
                            }
                            try self.fanout.put(self.allocator, topic, new_fanout);
                        }

                        try self.fanout_last_pub.put(self.allocator, topic, std.time.milliTimestamp());
                    }
                }
            }
        }

        return SelectPeersResult{
            .to_send = try selected_peers.toOwnedSlice(arena),
            .to_send_count = to_send_count,
        };
    }

    fn buildRPCandMsgId(self: *Self, arena: Allocator, topic: []const u8, original_data: []const u8, transformed_data: []const u8, rpc_msg: *rpc.Message, app_msg: *AppMessage, msg_id: *[]const u8) !void {
        switch (self.opts.publish_policy) {
            .anonymous => {
                rpc_msg.* = .{
                    .data = original_data,
                    .topic = topic,
                };
                app_msg.* = .{
                    .data = original_data,
                    .topic = topic,
                };
                msg_id.* = try self.opts.msg_id_fn(arena, rpc_msg);
                rpc_msg.data = transformed_data;
            },
            .signing => {
                const seq_no = try self.nextSeqno(arena);

                rpc_msg.* = .{
                    .from = self.peer_id_bytes,
                    .seqno = seq_no.@"1",
                    .data = transformed_data,
                    .topic = topic,
                };

                const encoded_rpc_msg = try rpc_msg.encode(arena);

                const data_to_sign = try std.mem.concat(arena, u8, &.{ sign_prefix, encoded_rpc_msg });

                const signature = try self.sign_key.signData(arena, data_to_sign);

                const host_pubkey = try self.sign_key.publicKey(arena);

                const host_pubkey_proto = try host_pubkey.encode(arena);

                rpc_msg.signature = signature;
                rpc_msg.key = host_pubkey_proto;
                rpc_msg.data = original_data;

                msg_id.* = try self.opts.msg_id_fn(arena, rpc_msg);
                rpc_msg.data = transformed_data;

                app_msg.* = .{
                    .from = self.peer_id,
                    .seqno = seq_no.@"0",
                    .data = original_data,
                    .topic = topic,
                    .signature = signature,
                    .key = host_pubkey,
                };
            },
        }
    }

    // This function is called when a stream is closed. It is set when the stream is created.
    fn onStreamClose(ctx: ?*anyopaque, stream: anyerror!*libp2p.QuicStream) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        const s = stream catch unreachable;
        const remote_peer_id = s.conn.security_session.?.remote_id;

        if (!self.peers.contains(remote_peer_id)) {
            // This should not be reached
            std.log.warn("Stream closed for unknown peer: {}", .{s.conn.security_session.?.remote_id});
            return;
        }

        const semi_duplex = self.peers.getPtr(remote_peer_id).?;
        if (semi_duplex.replace_stream) {
            semi_duplex.replace_stream = false;
            return;
        }

        if (semi_duplex.initiator) |initiator| {
            if (initiator.stream == s) {
                semi_duplex.initiator = null;
            }
        }
        if (semi_duplex.responder) |resp| {
            if (resp.stream == s) {
                semi_duplex.responder = null;
            }
        }

        // If the close operation is not initiated by the application layer, we will try to close the other direction stream as well.
        // If both directions are closed, we will remove the peer from the peer list.
        // If the close operation is initiated by the application layer, we will not do anything here.
        // Because the application layer will handle the removal of the peer.
        if (!semi_duplex.active_close) {
            if (semi_duplex.initiator == null and semi_duplex.responder == null) {
                _ = self.peers.remove(remote_peer_id);
            } else {
                semi_duplex.close(null, struct {
                    fn callback(_: ?*anyopaque, _: anyerror!*Semiduplex) void {}
                }.callback);
            }
        }
    }

    fn sendRPC(self: *Self, arena: Allocator, to: *const PeerId, rpc_msg_in: *const rpc.RPC) bool {
        // TODO: Use a pool of pre-allocated contexts?
        var sending_ctx = self.allocator.create(SendingRPCContext) catch |err| {
            std.log.err("Failed to allocate RPC message: {}", .{err});
            return false;
        };
        var handing_off = false;
        defer {
            if (!handing_off) {
                sending_ctx.deinit(self.allocator);
                self.allocator.destroy(sending_ctx);
            }
        }
        sending_ctx.* = .{
            .gossipsub = self,
            .control = null,
            .ihave = null,
            .to = to.*,
        };

        // TODO: should we dial the peer if not connected? should we return an error?
        const semi_duplex = self.peers.getPtr(to.*) orelse {
            std.log.warn("Attempted to send RPC to unknown peer: {}", .{to.*});
            return false;
        };

        const initiator = semi_duplex.initiator orelse {
            std.log.warn("No outgoing stream to peer: {}", .{to.*});
            return false;
        };

        const has_control = self.control.contains(to.*);
        const has_gossip = self.gossip.contains(to.*);
        const needs_piggyback = has_control or has_gossip;

        var rpc_storage: ?rpc.RPC = null;
        var rpc_msg: *const rpc.RPC = rpc_msg_in;

        if (needs_piggyback) {
            rpc_storage = rpc_msg_in.*;
            rpc_msg = &rpc_storage.?;
        }

        if (has_control) {
            const entry = self.control.fetchRemove(to.*).?;
            sending_ctx.control = entry.value;
            self.piggybackControl(arena, to, @constCast(rpc_msg), &sending_ctx.control.?) catch |err| {
                std.log.warn("Failed to piggyback control message to peer {}: {}", .{ to.*, err });
                if (self.control.put(self.allocator, to.*, sending_ctx.control.?)) |_| {
                    sending_ctx.control = null;
                } else |put_err| {
                    std.log.err("Failed to restore control message for peer {}: {}", .{ to.*, put_err });
                }
                return false;
            };
        }

        if (has_gossip) {
            const entry = self.gossip.fetchRemove(to.*).?;
            sending_ctx.ihave = entry.value;

            const ihave_ptr = &sending_ctx.ihave.?;
            if (piggybackGossip(@constCast(rpc_msg), ihave_ptr)) |_| {
                // ok
            } else |err| {
                std.log.warn("Failed to piggyback gossip message to peer {}: {}", .{ to.*, err });

                if (self.gossip.put(self.allocator, to.*, ihave_ptr.*)) |_| {
                    sending_ctx.ihave = null;
                } else |put_err| {
                    std.log.err("Failed to restore gossip message for peer {}: {}", .{ to.*, put_err });
                    deinitControlIHaveList(self.allocator, ihave_ptr);
                    sending_ctx.ihave = null;
                }
                return false;
            }
        }

        const rpc_bytes = rpc_msg.encode(arena) catch |err| {
            std.log.warn("Failed to encode RPC message for peer {}: {}", .{ to.*, err });
            if (sending_ctx.control) |ctrl| {
                if (self.control.put(self.allocator, to.*, ctrl)) |_| {
                    sending_ctx.control = null;
                } else |put_err| {
                    std.log.warn("Failed to restore control message for peer {}: {}", .{ to.*, put_err });
                }
            }
            if (sending_ctx.ihave) |*ihave_ptr| {
                if (self.gossip.put(self.allocator, to.*, ihave_ptr.*)) |_| {
                    sending_ctx.ihave = null;
                } else |put_err| {
                    std.log.warn("Failed to restore gossip message for peer {}: {}", .{ to.*, put_err });
                    deinitControlIHaveList(self.allocator, ihave_ptr);
                    sending_ctx.ihave = null;
                }
            }
            return false;
        };

        const encoded_size = rpc_msg.calcProtobufSize();
        var size_buffer: [10]u8 = undefined;
        const size_bytes = uvarint.encode(usize, encoded_size, &size_buffer);

        const encoded_rpc_msg = std.mem.concat(arena, u8, &.{ size_bytes, rpc_bytes }) catch |err| {
            std.log.warn("Failed to prepare encoded RPC message for peer {}: {}", .{ to.*, err });
            if (sending_ctx.control) |ctrl| {
                if (self.control.put(self.allocator, to.*, ctrl)) |_| {
                    sending_ctx.control = null;
                } else |put_err| {
                    std.log.warn("Failed to restore control message for peer {}: {}", .{ to.*, put_err });
                }
            }
            if (sending_ctx.ihave) |*ihave_ptr| {
                if (self.gossip.put(self.allocator, to.*, ihave_ptr.*)) |_| {
                    sending_ctx.ihave = null;
                } else |put_err| {
                    std.log.warn("Failed to restore gossip message for peer {}: {}", .{ to.*, put_err });
                    deinitControlIHaveList(self.allocator, ihave_ptr);
                    sending_ctx.ihave = null;
                }
            }
            return false;
        };

        initiator.stream.write(encoded_rpc_msg, sending_ctx, SendingRPCContext.callback);
        handing_off = true;

        if (rpc_msg.control) |ctrl| {
            if (ctrl.graft) |graft| {
                for (graft) |g_opt| {
                    const g = g_opt orelse continue;
                    const topic_id = g.topic_i_d orelse continue;

                    self.event_emitter.emit(.{ .gossipsub_graft = .{
                        .peer = to.*,
                        .topic = topic_id,
                        .direction = .OUTBOUND,
                    } });
                }
            }
            if (ctrl.prune) |prune| {
                for (prune) |p_opt| {
                    const p = p_opt orelse continue;
                    const topic_id = p.topic_i_d orelse continue;

                    self.event_emitter.emit(.{ .gossipsub_prune = .{
                        .peer = to.*,
                        .topic = topic_id,
                        .direction = .OUTBOUND,
                    } });
                }
            }
        }

        return true;
    }

    fn piggybackControl(self: *Self, arena: Allocator, to: *const PeerId, rpc_msg: *rpc.RPC, ctrl: *const rpc.ControlMessage) !void {
        var graft_list: std.ArrayListUnmanaged(?rpc.ControlGraft) = .empty;
        if (ctrl.graft) |grafts| {
            for (grafts) |graft_opt| {
                const graft = graft_opt orelse continue;
                const topic_id = graft.topic_i_d orelse continue;
                const peers = self.mesh.get(topic_id) orelse continue;

                if (peers.contains(to.*)) {
                    try graft_list.append(arena, graft);
                }
            }
        }

        var prune_list: std.ArrayListUnmanaged(?rpc.ControlPrune) = .empty;
        if (ctrl.prune) |prunes| {
            for (prunes) |prune_opt| {
                const prune = prune_opt orelse continue;
                const topic_id = prune.topic_i_d orelse continue;

                if (self.mesh.get(topic_id)) |peers| {
                    if (!peers.contains(to.*)) {
                        try prune_list.append(arena, prune);
                    }
                } else {
                    try prune_list.append(arena, prune);
                }
            }
        }

        if (graft_list.items.len > 0 or prune_list.items.len > 0) {
            if (rpc_msg.control == null) {
                rpc_msg.control = .{};
            }

            if (graft_list.items.len > 0) {
                rpc_msg.control.?.graft = try graft_list.toOwnedSlice(arena);
            }
            if (prune_list.items.len > 0) {
                rpc_msg.control.?.prune = try prune_list.toOwnedSlice(arena);
            }
        }
    }

    fn piggybackGossip(rpc_msg: *rpc.RPC, ihave: *const std.ArrayListUnmanaged(?rpc.ControlIHave)) !void {
        if (rpc_msg.control == null) {
            rpc_msg.control = .{};
        }
        rpc_msg.control.?.ihave = ihave.items;
    }

    fn handleSubscription(self: *Self, from: *const PeerId, topic: []const u8, is_subscribe: bool) !void {
        if (is_subscribe) {
            if (!self.topics.contains(topic)) {
                const copied_topic_id = try self.refTopic(topic);
                errdefer self.unrefTopic(copied_topic_id);
                var tmap = std.AutoHashMapUnmanaged(PeerId, void).empty;
                errdefer tmap.deinit(self.allocator);
                try self.topics.put(self.allocator, copied_topic_id, tmap);
            }

            const pgop = try self.topics.getPtr(topic).?.getOrPut(self.allocator, from.*);
            if (!pgop.found_existing) {
                pgop.value_ptr.* = {};
            }
        } else {
            if (!self.topics.contains(topic)) {
                return;
            } else {
                if (self.topics.getPtr(topic)) |peer_set| {
                    _ = peer_set.remove(from.*);
                }
            }
        }
    }

    fn validateAndToAppMessage(self: *Self, arena: Allocator, msg: *const rpc.MessageReader, policy: SignaturePolicy, app_msg: *AppMessage) ValidateError!void {
        switch (policy) {
            SignaturePolicy.StrictNoSign => {
                if (msg.getSignature().len != 0) {
                    return error.SignaturePresent;
                }
                if (msg.getSeqno().len != 0) {
                    return error.SeqnoPresent;
                }
                if (msg.getFrom().len != 0) {
                    return error.FromPresent;
                }

                app_msg.* = .{
                    .from = null,
                    .data = msg.getData(),
                    .seqno = null,
                    .topic = msg.getTopic(),
                    .signature = null,
                    .key = null,
                };

                return;
            },
            SignaturePolicy.StrictSign => {
                if (msg.getSeqno().len == 0) {
                    return error.InvalidSeqno;
                }
                if (msg.getSeqno().len != 8) {
                    return error.InvalidSeqno;
                }
                if (msg.getSignature().len == 0) {
                    return error.InvalidSignature;
                }
                if (msg.getFrom().len == 0) {
                    return error.InvalidPeerId;
                }
                const from_peer_id = PeerId.fromBytes(msg.getFrom()) catch {
                    return error.InvalidPeerId;
                };

                var pubkey_bytes_buf: [128]u8 = undefined; // this is enough space for a PeerId
                const pubkey_bytes = if (msg._key) |k| k else (from_peer_id.toBytes(&pubkey_bytes_buf) catch {
                    return error.InvalidPeerId;
                })[2..];
                // This actually no allocates, just use the slice directly
                const proto_pubkey_reader = keys.PublicKeyReader.init(self.allocator, pubkey_bytes) catch {
                    return error.InvalidPeerId;
                };

                // This will be used in app_msg, so we need to allocate as pubkey_bytes_buf is stack memory
                var proto_pubkey = keys.PublicKey{
                    .type = proto_pubkey_reader.getType(),
                    .data = proto_pubkey_reader.getData(),
                };

                const evp_key = tls.reconstructEvpKeyFromPublicKey(&proto_pubkey) catch {
                    return error.InvalidPubkey;
                };
                defer ssl.EVP_PKEY_free(evp_key);

                if (msg._key != null) {
                    const key_peer_id = PeerId.fromPublicKey(self.allocator, &proto_pubkey) catch {
                        return error.InvalidPubkey;
                    };

                    if (!from_peer_id.eql(&key_peer_id)) {
                        return error.InvalidPeerId;
                    }
                }

                const data_to_sign: rpc.Message = .{
                    .seqno = msg._seqno,
                    .data = msg._data,
                    .from = msg._from,
                    .topic = msg._topic,
                    .signature = null,
                    .key = null,
                };
                const encoded_data = data_to_sign.encode(arena) catch {
                    return error.InvalidSignature;
                };
                const prefixed_data = std.mem.concat(arena, u8, &.{ sign_prefix, encoded_data }) catch {
                    return error.InvalidSignature;
                };
                const is_valid = tls.verifySignature(evp_key, prefixed_data, msg.getSignature()) catch {
                    return error.InvalidSignature;
                };
                if (!is_valid) {
                    return error.InvalidSignature;
                }

                app_msg.* = .{
                    .from = from_peer_id,
                    .data = msg.getData(),
                    .seqno = std.mem.readInt(u64, msg.getSeqno()[0..8], .big),
                    .topic = msg.getTopic(),
                    .signature = msg.getSignature(),
                    .key = proto_pubkey,
                };
                return;
            },
        }
    }

    fn validateReceivedMessage(self: *Self, arena: Allocator, msg: *const rpc.MessageReader, propagation_source: PeerId) !MessageValidationResult {
        var m: AppMessage = undefined;
        self.validateAndToAppMessage(arena, msg, self.opts.global_signature_policy, &m) catch |err| {
            return MessageValidationResult{
                .invalid = .{
                    .reason = RejectReason.Error,
                    .err = err,
                },
            };
        };

        if (self.opts.data_transform) |dt| {
            const transformed_data = dt.inboundTransform(arena, m.topic, m.data) catch |err| {
                std.log.warn("Inbound data transformation failed for message from peer {}: {}", .{ propagation_source, err });
                return MessageValidationResult{
                    .invalid = .{
                        .reason = RejectReason.Error,
                        .err = error.TransformFailed,
                    },
                };
            };
            m.data = transformed_data;
        }

        // use a copy of the message with transformed data to generate message ID
        // TODO: maybe change msg_id_fn to take data as parameter `Message` not `rpc.Message`
        const msg_for_id = rpc.Message{
            .from = msg._from,
            .data = m.data,
            .seqno = msg._seqno,
            .topic = msg._topic,
            .signature = msg._signature,
            .key = msg._key,
        };
        // Generate message ID using the provided function and the copied message with transformed data
        const message_id = self.opts.msg_id_fn(arena, &msg_for_id) catch |err| {
            std.log.warn("Failed to generate message ID for message from peer {}: {}", .{ propagation_source, err });
            return MessageValidationResult{
                .invalid = .{
                    .reason = RejectReason.Error,
                    .err = error.MessageIdGenerationFailed,
                },
            };
        };

        if (self.seen_cache.contains(message_id)) {
            return MessageValidationResult{
                .duplicate = .{
                    .message_id = message_id,
                },
            };
        } else {
            // seen_cache clone the message_id internally
            try self.seen_cache.put(message_id, {}, .{
                .ttl = self.opts.seen_ttl_s,
            });
        }

        // Here use the origin message's data length to determine if we should send IDontWant
        if (msg._data) |data| {
            if (data.len >= self.opts.idontwant_message_size_threshold) {
                try self.sendIDontWants(m.topic, propagation_source, message_id);
            }
        }

        return MessageValidationResult{
            .valid = .{
                .message_id = message_id,
                .message = m,
            },
        };
    }

    fn sendIDontWants(self: *Self, topic: []const u8, source: PeerId, message_id: []const u8) !void {
        const peer_set = self.mesh.get(topic) orelse return;

        var iter = peer_set.keyIterator();
        while (iter.next()) |peer_id| {
            if (self.shouldSendIDontWant(peer_id.*, source)) {
                std.log.debug("Sending IDontWant for message ID {any} to peer {}", .{ message_id, peer_id });
                // TODO: Here we would normally send the IDontWant message to the peer.
                // For brevity, this is omitted.
            }
        }
    }

    fn shouldSendIDontWant(self: *Self, peer_id: PeerId, source: PeerId) bool {
        if (peer_id.eql(&source)) return false;

        const semi_duplex = self.peers.getPtr(peer_id) orelse return false;
        const initiator = semi_duplex.initiator orelse return false;

        return std.mem.eql(u8, initiator.stream.negotiated_protocol.?, v1_2_id);
    }

    fn handleMessage(self: *Self, arena: Allocator, from: *const PeerId, publish_msg: *const rpc.MessageReader) !void {
        const validation_result = try self.validateReceivedMessage(arena, publish_msg, from.*);

        switch (validation_result) {
            .valid => |valid_msg| {
                // TODO: Change the mcache to store `rpc.MessageReader` to avoid copying data again.
                // Store the original publish_msg from network, not the transformed one.
                const rpc_msg = rpc.Message{
                    .from = publish_msg._from,
                    .data = publish_msg._data,
                    .seqno = publish_msg._seqno,
                    .topic = publish_msg._topic,
                    .signature = publish_msg._signature,
                    .key = publish_msg._key,
                };
                try self.mcache.putWithId(valid_msg.message_id, &rpc_msg);

                if (self.subscriptions.contains(valid_msg.message.topic)) {
                    const is_from_self = from.*.eql(&self.peer_id);
                    if (!is_from_self or self.opts.emit_self) {
                        self.event_emitter.emit(.{ .message = .{
                            .propagation_source = from.*,
                            .message_id = valid_msg.message_id,
                            .message = &valid_msg.message,
                        } });
                    }
                }

                try self.forwardMessage(arena, valid_msg.message_id, &rpc_msg, from.*, null);
            },
            .invalid => |invalid_msg| {
                // TODO: We don't have topic validator so that no msg_id in invalid message, should we add it?
                std.log.warn("Invalid message from peer {}: {}", .{ from.*, invalid_msg.err });
            },
            .duplicate => |dup_msg| {
                // TODO: When change the `mcache` to store unvalidated message, we can use it here to avoid re-validation.
                std.log.debug("Duplicate message received from peer {}: ID {any}", .{ from.*, dup_msg.message_id });
            },
        }
    }

    fn handleControl(self: *Self, arena: Allocator, control: *const rpc.ControlMessageReader, from: *const PeerId) !void {
        const ihave_messages = try control.getIhave(arena);

        const iwant = if (ihave_messages.len > 0) blk: {
            const result = try self.handleIHave(arena, from, ihave_messages);
            break :blk result;
        } else blk: {
            break :blk &.{};
        };

        std.log.debug("Sending IWANT with {d} message IDs to peer {}", .{ iwant.len, from.* });

        const iwant_messages = try control.getIwant(arena);

        const ihave = if (iwant_messages.len > 0) blk: {
            const result = try self.handleIWant(arena, from, iwant_messages);
            break :blk result;
        } else blk: {
            break :blk &.{};
        };
        std.log.debug("Sending {d} messages to peer {}", .{ ihave.len, from.* });

        const graft_messages = try control.getGraft(arena);

        const prune = if (graft_messages.len > 0) blk: {
            const result = try self.handleGraft(arena, from, graft_messages);
            break :blk result;
        } else blk: {
            break :blk &.{};
        };
        std.log.debug("Sending {d} PRUNE messages to peer {}", .{ prune.len, from.* });

        const prune_messages = try control.getPrune(self.allocator);
        defer if (prune_messages.len > 0) self.allocator.free(prune_messages);

        if (prune_messages.len > 0) {
            try self.handlePrune(arena, from, prune_messages);
        }
    }

    fn handleIWant(self: *Self, arena: Allocator, from: *const PeerId, iwant: []const rpc.ControlIWantReader) ![]*rpc.Message {
        var ihave: std.StringHashMapUnmanaged(*rpc.Message) = .empty;

        var iwant_by_topic: std.StringHashMapUnmanaged(usize) = .empty;
        var iwant_dont_have: usize = 0;

        for (iwant) |*iwant_msg| {
            for (iwant_msg.getMessageIDs()) |msg_id| {
                const cached = try self.mcache.getForPeer(msg_id, from.*) orelse {
                    iwant_dont_have += 1;
                    continue;
                };

                // TODO: used by metrics
                try iwant_by_topic.put(arena, cached.msg.topic.?, (iwant_by_topic.get(cached.msg.topic.?) orelse 0) + 1);

                if (cached.count > self.opts.gossip_retransmission) {
                    std.log.debug("Not sending message ID {any} to peer {} as it has been sent {d} times (limit {d})", .{ msg_id, from.*, cached.count, self.opts.gossip_retransmission });
                    continue;
                }

                try ihave.put(arena, msg_id, cached.msg);
            }
        }

        if (ihave.count() == 0) {
            return &.{};
        }

        var ihave_list = try std.ArrayListUnmanaged(*rpc.Message).initCapacity(arena, ihave.count());

        var it = ihave.valueIterator();
        while (it.next()) |msg| {
            try ihave_list.append(arena, msg.*);
        }

        std.log.debug("Sending {d} messages to peer {}, {d} IWANT IDs not found", .{ ihave_list.items.len, from.*, iwant_dont_have });

        return try ihave_list.toOwnedSlice(arena);
    }

    fn handleIHave(self: *Self, arena: Allocator, from: *const PeerId, ihave: []const rpc.ControlIHaveReader) ![]const rpc.ControlIWant {
        const peer_have = (self.peer_have.get(from.*) orelse 0) + 1;
        try self.peer_have.put(self.allocator, from.*, peer_have);
        if (peer_have > self.opts.max_ihave_messages) {
            std.log.warn("Peer {} sent too many {d} IHAVE messages within this heartbeat, ignoring further IHAVE messages.", .{ from.*, peer_have });
            return &.{};
        }

        const iasked = self.iasked.get(from.*) orelse 0;
        if (iasked > self.opts.max_ihave_len) {
            std.log.warn("Peer {} has asked for too many {d} IHAVE message IDs within this heartbeat, ignoring further IHAVE messages.", .{ from.*, iasked });
            return &.{};
        }

        var iwant: std.StringHashMapUnmanaged(void) = .empty;

        for (ihave) |*ihave_msg| {
            if (ihave_msg.getTopicID().len == 0 or ihave_msg.getMessageIDs().len == 0 or !self.mesh.contains(ihave_msg.getTopicID())) {
                continue;
            }

            var idonthave: usize = 0;
            for (ihave_msg.getMessageIDs()) |msg_id| {
                if (!self.seen_cache.contains(msg_id)) {
                    try iwant.put(arena, msg_id, {});
                    idonthave += 1;
                }
            }
        }

        if (iwant.count() == 0) {
            return &.{};
        }

        var iask: usize = iwant.count();
        if (iask + iasked > self.opts.max_ihave_len) {
            iask = self.opts.max_ihave_len - iasked;
        }

        std.log.debug("Asking for {d} out of {d} messages from peer {}", .{ iask, iwant.count(), from.* });

        var iwant_list = try std.ArrayListUnmanaged([]const u8).initCapacity(arena, iwant.count());

        var it = iwant.keyIterator();
        while (it.next()) |id| {
            try iwant_list.append(arena, id.*);
        }

        shuffleWithEntropy([]const u8, iwant_list.items);
        // This is not temporary, we need to keep track of how many IDs we asked for from this peer, so use `self.allocator`
        try self.iasked.put(self.allocator, from.*, iask + iasked);

        const result = try arena.alloc(rpc.ControlIWant, 1);

        var selected_ids = try arena.alloc(?[]const u8, iask);

        for (iwant_list.items[0..iask], 0..) |id, idx| {
            selected_ids[idx] = id;
        }

        result[0] = .{
            .message_i_ds = selected_ids,
        };

        return result;
    }

    fn handleGraft(self: *Self, arena: Allocator, from: *const PeerId, graft: []const rpc.ControlGraftReader) ![]rpc.ControlPrune {
        var prune: std.StringHashMapUnmanaged(void) = .empty;

        for (graft) |graft_msg| {
            const topic = graft_msg.getTopicID();
            if (topic.len == 0) continue;

            const peers = self.mesh.getPtr(topic) orelse continue;
            // This should use `self.allocator` because `self.mesh` is long-lived.
            if (peers.contains(from.*)) continue;

            const has_outgoing = if (self.peers.getPtr(from.*)) |semi| semi.initiator != null else false;

            if (peers.count() >= self.opts.D_hi and !has_outgoing) {
                try prune.put(arena, topic, {});
            } else {
                try peers.put(self.allocator, from.*, {});
            }

            self.event_emitter.emit(.{ .gossipsub_graft = .{
                .peer = from.*,
                .topic = topic,
                .direction = .INBOUND,
            } });
        }

        if (prune.count() == 0) {
            return &.{};
        }

        var prune_list = try std.ArrayListUnmanaged(rpc.ControlPrune).initCapacity(arena, prune.count());

        var it = prune.keyIterator();
        // TODO: only support v1.0 now, need to support v1.1 later
        while (it.next()) |topic| {
            try prune_list.append(arena, .{ .topic_i_d = topic.* });
        }

        return prune_list.toOwnedSlice(arena);
    }

    fn handlePrune(self: *Self, arena: Allocator, from: *const PeerId, prune: []const rpc.ControlPruneReader) !void {
        _ = arena;
        for (prune) |prune_msg| {
            const topic = prune_msg.getTopicID();
            if (topic.len == 0) continue;

            var peer_set = self.mesh.getPtr(topic) orelse continue;
            _ = peer_set.remove(from.*);

            self.event_emitter.emit(.{ .gossipsub_prune = .{
                .peer = from.*,
                .topic = topic,
                .direction = .INBOUND,
            } });
        }
    }

    fn initEntropyPrng() std.Random.DefaultPrng {
        const seed = blk: {
            var seed: u64 = undefined;
            std.posix.getrandom(std.mem.asBytes(&seed)) catch break :blk @as(u64, @intCast(std.time.milliTimestamp()));
            break :blk seed;
        };
        return std.Random.DefaultPrng.init(seed);
    }

    fn shuffleWithEntropy(comptime T: type, slice: []T) void {
        if (slice.len <= 1) return;
        var prng = initEntropyPrng();
        prng.random().shuffle(T, slice);
    }

    fn pushGossip(self: *Self, peer: PeerId, control_ihave: *const rpc.ControlIHave) !void {
        var gossip_entry = try self.gossip.getOrPut(self.allocator, peer);
        if (!gossip_entry.found_existing) {
            gossip_entry.value_ptr.* = std.ArrayListUnmanaged(?rpc.ControlIHave).empty;
        }
        try gossip_entry.value_ptr.append(self.allocator, control_ihave.*);
    }

    fn enqueueGossip(self: *Self, peer: PeerId, topic: []const u8, message_ids: [][]const u8) !void {
        if (message_ids.len == 0) return;

        const limit = @min(message_ids.len, self.opts.max_ihave_len);
        if (limit == 0) return;

        // NOTE: gossip entries live in `self.gossip` until the next successful
        // flush. They can survive beyond the heartbeat arena (e.g. if sending
        // fails and we requeue), so we must allocate from the router allocator
        // instead of the per-heartbeat arena or `refTopic` pool.
        const topic_copy = try self.allocator.dupe(u8, topic);
        errdefer self.allocator.free(topic_copy);

        var ids = try self.allocator.alloc(?[]const u8, limit);
        errdefer self.allocator.free(ids);

        var copied: usize = 0;
        errdefer {
            var idx: usize = 0;
            while (idx < copied) : (idx += 1) {
                if (ids[idx]) |message_id| {
                    self.allocator.free(message_id);
                }
            }
        }

        const need_random_subset = message_ids.len > limit;
        var indices_buf: ?[]usize = null;
        defer if (indices_buf) |buf| self.allocator.free(buf);

        var prng_state = initEntropyPrng();
        const random = prng_state.random();

        if (need_random_subset) {
            var selection = try self.allocator.alloc(usize, limit);
            indices_buf = selection;

            for (selection, 0..) |*slot, idx| {
                slot.* = idx;
            }

            var i: usize = limit;
            while (i < message_ids.len) : (i += 1) {
                const j = random.uintLessThan(usize, i + 1);
                if (j < limit) {
                    selection[j] = i;
                }
            }

            for (selection, 0..) |idx_ptr, idx| {
                const mid = message_ids[idx_ptr];
                ids[idx] = try self.allocator.dupe(u8, mid);
                copied = idx + 1;
            }
        } else {
            for (message_ids[0..limit], 0..) |mid, idx| {
                ids[idx] = try self.allocator.dupe(u8, mid);
                copied = idx + 1;
            }
        }

        var control_msg = rpc.ControlIHave{
            .topic_i_d = topic_copy,
            .message_i_ds = ids,
        };
        try self.pushGossip(peer, &control_msg);
    }

    fn getRandomGossipPeers(self: *Self, topic: []const u8, count: usize, filter_ctx: ?*anyopaque, filter: *const fn (ctx: ?*anyopaque, peer: PeerId) bool) !std.ArrayListUnmanaged(PeerId) {
        const peers = self.topics.get(topic) orelse return std.ArrayListUnmanaged(PeerId).empty;
        if (peers.count() == 0) {
            return std.ArrayListUnmanaged(PeerId).empty;
        }

        var candidate_peers = try std.ArrayListUnmanaged(PeerId).initCapacity(self.allocator, peers.count());
        errdefer candidate_peers.deinit(self.allocator);

        var it = peers.keyIterator();
        while (it.next()) |peer_id| {
            const semi_duplex = self.peers.get(peer_id.*) orelse continue;
            if (semi_duplex.initiator == null) continue;

            const negotiated_protocol = semi_duplex.initiator.?.stream.negotiated_protocol orelse continue;

            if (self.supportsProtocol(negotiated_protocol) and !filter(filter_ctx, peer_id.*)) {
                try candidate_peers.append(self.allocator, peer_id.*);
            }
        }

        shuffleWithEntropy(PeerId, candidate_peers.items);

        if (candidate_peers.items.len > count) {
            candidate_peers.items = candidate_peers.items[0..count];
        }

        return candidate_peers;
    }

    fn supportsProtocol(self: *Self, protocol: []const u8) bool {
        for (self.protocols) |supported_protocol| {
            if (std.mem.eql(u8, protocol, supported_protocol)) {
                return true;
            }
        }
        return false;
    }

    fn hasUsableStream(self: *Self, peer: PeerId) bool {
        const semi_duplex = self.peers.get(peer) orelse return false;
        const initiator = semi_duplex.initiator orelse return false;
        const negotiated = initiator.stream.negotiated_protocol orelse return false;
        return self.supportsProtocol(negotiated);
    }

    fn collectTopicPeers(
        self: *Self,
        arena: Allocator,
        topic: []const u8,
        exclude_a: ?*std.AutoHashMapUnmanaged(PeerId, void),
        exclude_b: ?*std.AutoHashMapUnmanaged(PeerId, void),
    ) !std.ArrayListUnmanaged(PeerId) {
        var result = std.ArrayListUnmanaged(PeerId).empty;

        if (self.topics.getPtr(topic)) |topic_peers| {
            var it = topic_peers.keyIterator();
            while (it.next()) |peer_id_ptr| {
                const peer_id = peer_id_ptr.*;
                if (exclude_a) |map| {
                    if (map.contains(peer_id)) continue;
                }
                if (exclude_b) |map| {
                    if (map.contains(peer_id)) continue;
                }
                if (!self.hasUsableStream(peer_id)) continue;
                try result.append(arena, peer_id);
            }
        }

        return result;
    }

    fn sendGraft(self: *Self, arena: Allocator, to: *const PeerId, topic: []const u8) void {
        var graft_msg = arena.alloc(?rpc.ControlGraft, 1) catch |err| {
            std.log.warn("Failed to allocate GRAFT message for peer {}: {}", .{ to, err });
            return;
        };
        // Note: right now only implement gossipsub v1.0
        graft_msg[0] = .{
            .topic_i_d = topic,
        };

        const rpc_msg: rpc.RPC = .{ .control = .{ .graft = graft_msg } };
        _ = self.sendRPC(arena, to, &rpc_msg);
        // TODO: free rpc_msg
    }

    fn sendPrune(self: *Self, arena: Allocator, to: *const PeerId, topic: []const u8) void {
        var prune_msg = arena.alloc(?rpc.ControlPrune, 1) catch |err| {
            std.log.warn("Failed to allocate PRUNE message for peer {}: {}", .{ to, err });
            return;
        };
        // Note: right now only implement gossipsub v1.0
        prune_msg[0] = .{
            .topic_i_d = topic,
        };

        const rpc_msg: rpc.RPC = .{ .control = .{ .prune = prune_msg } };
        _ = self.sendRPC(arena, to, &rpc_msg);
        // TODO: free rpc_msg
    }

    const ControlKind = enum { graft, prune };

    fn controlKindLabel(kind: ControlKind) []const u8 {
        return switch (kind) {
            .graft => "GRAFT",
            .prune => "PRUNE",
        };
    }

    fn queueControlTopic(
        self: *Self,
        allocator: Allocator,
        map: *std.AutoHashMapUnmanaged(PeerId, std.ArrayListUnmanaged([]const u8)),
        peer: PeerId,
        topic: []const u8,
        kind: ControlKind,
    ) void {
        _ = self;
        const label = controlKindLabel(kind);
        const get_or_put = map.getOrPut(allocator, peer) catch |err| {
            std.log.warn("heartbeat: failed to track {s} topic {s} for peer {}: {}", .{ label, topic, peer, err });
            return;
        };

        if (!get_or_put.found_existing) {
            get_or_put.value_ptr.* = .empty;
        }

        get_or_put.value_ptr.append(allocator, topic) catch |err| {
            std.log.warn("heartbeat: failed to append {s} topic {s} for peer {}: {}", .{ label, topic, peer, err });
        };
    }

    fn queueGraftTopic(
        self: *Self,
        allocator: Allocator,
        map: *std.AutoHashMapUnmanaged(PeerId, std.ArrayListUnmanaged([]const u8)),
        peer: PeerId,
        topic: []const u8,
    ) void {
        self.queueControlTopic(allocator, map, peer, topic, .graft);
    }

    fn queuePruneTopic(
        self: *Self,
        allocator: Allocator,
        map: *std.AutoHashMapUnmanaged(PeerId, std.ArrayListUnmanaged([]const u8)),
        peer: PeerId,
        topic: []const u8,
    ) void {
        self.queueControlTopic(allocator, map, peer, topic, .prune);
    }

    fn ensureControlMessage(self: *Self, peer: PeerId) ?*rpc.ControlMessage {
        const entry = self.control.getOrPut(self.allocator, peer) catch |err| {
            std.log.warn("heartbeat: failed to queue control message for peer {}: {}", .{ peer, err });
            return null;
        };

        if (!entry.found_existing) {
            entry.value_ptr.* = null;
        }

        if (entry.value_ptr.* == null) {
            entry.value_ptr.* = rpc.ControlMessage{};
        }

        return &entry.value_ptr.*.?;
    }

    fn appendControlTopic(
        self: *Self,
        comptime T: type,
        list_ptr: *?[]const ?T,
        peer: PeerId,
        topic: []const u8,
        kind: ControlKind,
    ) void {
        const label = controlKindLabel(kind);
        if (topic.len == 0) return;

        if (list_ptr.*) |existing| {
            for (existing) |item_opt| {
                const item = item_opt orelse continue;
                const existing_topic = item.topic_i_d orelse continue;
                if (std.mem.eql(u8, existing_topic, topic)) {
                    return;
                }
            }

            const old_len = existing.len;
            var new_arr = self.allocator.alloc(?T, old_len + 1) catch |err| {
                std.log.warn("heartbeat: failed to expand {s} list for peer {} topic {s}: {}", .{ label, peer, topic, err });
                return;
            };
            for (existing, 0..) |item_opt, idx| {
                new_arr[idx] = item_opt;
            }

            const topic_copy = self.allocator.dupe(u8, topic) catch |err| {
                std.log.warn("heartbeat: failed to duplicate {s} topic {s} for peer {}: {}", .{ label, topic, peer, err });
                self.allocator.free(new_arr);
                return;
            };

            new_arr[old_len] = .{ .topic_i_d = topic_copy };
            self.allocator.free(@constCast(existing));
            list_ptr.* = new_arr;
        } else {
            const topic_copy = self.allocator.dupe(u8, topic) catch |err| {
                std.log.warn("heartbeat: failed to duplicate {s} topic {s} for peer {}: {}", .{ label, topic, peer, err });
                return;
            };

            var arr = self.allocator.alloc(?T, 1) catch |err| {
                std.log.warn("heartbeat: failed to allocate {s} list for peer {} topic {s}: {}", .{ label, peer, topic, err });
                self.allocator.free(topic_copy);
                return;
            };

            arr[0] = .{ .topic_i_d = topic_copy };
            list_ptr.* = arr;
        }
    }

    fn sendGraftPrune(
        self: *Self,
        to_graft: *std.AutoHashMapUnmanaged(PeerId, std.ArrayListUnmanaged([]const u8)),
        to_prune: *std.AutoHashMapUnmanaged(PeerId, std.ArrayListUnmanaged([]const u8)),
        no_px: *std.AutoHashMapUnmanaged(PeerId, bool),
    ) void {
        _ = no_px; // PX is not supported in v1.0 implementation yet

        var graft_iter = to_graft.iterator();
        while (graft_iter.next()) |entry| {
            const peer_id = entry.key_ptr.*;
            const topics = entry.value_ptr.*;
            if (topics.items.len == 0) continue;

            const ctrl = self.ensureControlMessage(peer_id) orelse continue;

            for (topics.items) |topic| {
                self.appendControlTopic(rpc.ControlGraft, &ctrl.graft, peer_id, topic, .graft);
            }

            if (to_prune.getPtr(peer_id)) |prune_topics_ptr| {
                const prune_topics = prune_topics_ptr.*;
                for (prune_topics.items) |topic| {
                    self.appendControlTopic(rpc.ControlPrune, &ctrl.prune, peer_id, topic, .prune);
                }
            }
        }

        var prune_iter = to_prune.iterator();
        while (prune_iter.next()) |entry| {
            const peer_id = entry.key_ptr.*;
            const topics = entry.value_ptr.*;
            if (topics.items.len == 0) continue;

            const ctrl = self.ensureControlMessage(peer_id) orelse continue;
            for (topics.items) |topic| {
                self.appendControlTopic(rpc.ControlPrune, &ctrl.prune, peer_id, topic, .prune);
            }
        }
    }

    fn sendExistingSubscriptionsToPeer(self: *Self, peer: PeerId) !void {
        if (self.subscriptions.count() == 0) return;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var topic_list = std.ArrayListUnmanaged([]const u8).empty;

        var it = self.subscriptions.keyIterator();
        while (it.next()) |topic_ptr| {
            try topic_list.append(arena_allocator, topic_ptr.*);
        }

        try self.sendSubscriptions(arena_allocator, &peer, topic_list.items, true);
    }

    fn sendSubscriptions(self: *Self, arena: Allocator, to: *const PeerId, topics: []const []const u8, subscribe_flag: bool) !void {
        var sub_opts: std.ArrayListUnmanaged(?rpc.RPC.SubOpts) = .empty;

        for (topics) |topic| {
            const sub: rpc.RPC.SubOpts = .{
                .topicid = topic,
                .subscribe = subscribe_flag,
            };
            try sub_opts.append(arena, sub);
        }

        const rpc_msg: rpc.RPC = .{ .subscriptions = sub_opts.items };
        _ = self.sendRPC(arena, to, &rpc_msg);
    }

    pub fn heartbeat(self: *Self) void {
        if (!self.swarm.transport.io_event_loop.inEventLoopThread()) {
            std.log.warn("heartbeat should be called from the IO loop thread", .{});
            return;
        }

        self.doHeartbeat();
    }

    fn doHeartbeat(self: *Self) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        const now_ms = std.time.milliTimestamp();
        self.heartbeat_ticks += 1;

        self.peer_have.deinit(self.allocator);
        self.peer_have = std.AutoHashMapUnmanaged(PeerId, usize).empty;
        self.iasked.deinit(self.allocator);
        self.iasked = std.AutoArrayHashMapUnmanaged(PeerId, usize).empty;

        const seed = heartbeatSeed(now_ms, self.heartbeat_ticks);
        var prng = std.Random.DefaultPrng.init(seed);
        const random = prng.random();

        var tograft = std.AutoHashMapUnmanaged(PeerId, std.ArrayListUnmanaged([]const u8)).empty;
        defer tograft.deinit(arena_allocator);
        var toprune = std.AutoHashMapUnmanaged(PeerId, std.ArrayListUnmanaged([]const u8)).empty;
        defer toprune.deinit(arena_allocator);
        var no_px = std.AutoHashMapUnmanaged(PeerId, bool).empty;
        defer no_px.deinit(arena_allocator);

        var mesh_iter = self.mesh.iterator();
        while (mesh_iter.next()) |entry| {
            const topic = entry.key_ptr.*;
            var peers = entry.value_ptr;

            var stale = std.ArrayListUnmanaged(PeerId).empty;
            defer stale.deinit(arena_allocator);
            var peer_it = peers.keyIterator();
            while (peer_it.next()) |peer_id_ptr| {
                if (!self.hasUsableStream(peer_id_ptr.*)) {
                    stale.append(arena_allocator, peer_id_ptr.*) catch {};
                }
            }

            for (stale.items) |peer_id| {
                _ = peers.remove(peer_id);
            }

            if (peers.count() < self.opts.D_lo) {
                const deficit = if (self.opts.D > peers.count()) self.opts.D - peers.count() else 0;
                if (deficit > 0) {
                    var filter_ctx: FilterCtx = .{ .mesh_peers = peers };
                    var new_peers = self.getRandomGossipPeers(topic, deficit, &filter_ctx, FilterCtx.filter) catch |err| {
                        std.log.warn("heartbeat: failed to select mesh peers for topic {s}: {}", .{ topic, err });
                        continue;
                    };
                    defer new_peers.deinit(self.allocator);

                    for (new_peers.items) |peer_id| {
                        if (peers.contains(peer_id)) continue;
                        peers.put(self.allocator, peer_id, {}) catch |err| {
                            std.log.warn("heartbeat: failed to add mesh peer {} for topic {s}: {}", .{ peer_id, topic, err });
                            continue;
                        };
                        self.queueGraftTopic(arena_allocator, &tograft, peer_id, topic);
                    }
                }
            }

            if (peers.count() > self.opts.D_hi) {
                var peer_list = std.ArrayListUnmanaged(PeerId).empty;
                defer peer_list.deinit(arena_allocator);
                var it = peers.keyIterator();
                while (it.next()) |peer_id_ptr| {
                    peer_list.append(arena_allocator, peer_id_ptr.*) catch {};
                }

                random.shuffle(PeerId, peer_list.items);
                const target = self.opts.D;
                var idx: usize = target;
                while (idx < peer_list.items.len) : (idx += 1) {
                    const peer_id = peer_list.items[idx];
                    if (peers.remove(peer_id)) {
                        self.queuePruneTopic(arena_allocator, &toprune, peer_id, topic);
                    }
                }
            }

            const fanout_ptr = self.fanout.getPtr(topic);
            var gossip_candidates = self.collectTopicPeers(arena_allocator, topic, peers, fanout_ptr) catch |err| {
                std.log.warn("heartbeat: failed to collect gossip peers for topic {s}: {}", .{ topic, err });
                continue;
            };
            defer gossip_candidates.deinit(arena_allocator);

            if (gossip_candidates.items.len == 0) continue;

            random.shuffle(PeerId, gossip_candidates.items);
            var target = self.opts.D_lazy;
            const candidate_float = @as(f32, @floatFromInt(gossip_candidates.items.len));
            const factor_float = @ceil(self.opts.gossip_factor * candidate_float);
            const factor = blk: {
                if (!std.math.isFinite(factor_float) or factor_float < 0) break :blk 0;
                const max_f32 = @as(f32, @floatFromInt(std.math.maxInt(usize)));
                if (factor_float > max_f32) break :blk gossip_candidates.items.len;
                break :blk @as(usize, @intFromFloat(factor_float));
            };
            if (factor > target) target = factor;
            if (target > gossip_candidates.items.len) target = gossip_candidates.items.len;
            if (target == 0) continue;

            const gossip_ids = self.mcache.getGossipIDs(topic) catch |err| {
                std.log.warn("heartbeat: failed to get gossip IDs for topic {s}: {}", .{ topic, err });
                continue;
            };
            defer self.allocator.free(gossip_ids);
            if (gossip_ids.len == 0) continue;

            for (gossip_candidates.items[0..target]) |peer_id| {
                self.enqueueGossip(peer_id, topic, gossip_ids) catch |err| {
                    std.log.warn("heartbeat: failed to enqueue gossip for peer {} topic {s}: {}", .{ peer_id, topic, err });
                };
            }
        }

        var expired = std.ArrayListUnmanaged([]const u8).empty;
        defer expired.deinit(arena_allocator);
        var fanout_last_iter = self.fanout_last_pub.iterator();
        while (fanout_last_iter.next()) |entry| {
            if (now_ms - entry.value_ptr.* >= self.opts.fanout_ttl_ms) {
                expired.append(arena_allocator, entry.key_ptr.*) catch {};
            }
        }

        for (expired.items) |topic| {
            if (self.fanout.fetchRemove(topic)) |removed| {
                var value = removed.value;
                value.deinit(self.allocator);
                self.unrefTopic(removed.key);
            }
            if (self.fanout_last_pub.fetchRemove(topic)) |removed| {
                self.unrefTopic(removed.key);
            }
        }

        var fanout_iter = self.fanout.iterator();
        while (fanout_iter.next()) |entry| {
            const topic = entry.key_ptr.*;
            var peers_map = entry.value_ptr;

            var removal = std.ArrayListUnmanaged(PeerId).empty;
            defer removal.deinit(arena_allocator);
            var it = peers_map.keyIterator();
            while (it.next()) |peer_id_ptr| {
                const peer_id = peer_id_ptr.*;
                const topic_peers = self.topics.getPtr(topic);
                const still_subscribed = topic_peers != null and topic_peers.?.contains(peer_id);
                if (!self.hasUsableStream(peer_id) or !still_subscribed) {
                    removal.append(arena_allocator, peer_id) catch {};
                }
            }
            for (removal.items) |peer_id| {
                _ = peers_map.remove(peer_id);
            }

            var fanout_candidates = self.collectTopicPeers(arena_allocator, topic, peers_map, null) catch |err| {
                std.log.warn("heartbeat: failed to collect fanout candidates for topic {s}: {}", .{ topic, err });
                continue;
            };
            defer fanout_candidates.deinit(arena_allocator);

            if (fanout_candidates.items.len > 0) {
                random.shuffle(PeerId, fanout_candidates.items);
            }

            var gossip_start_index: usize = 0;

            if (peers_map.count() < self.opts.D and fanout_candidates.items.len > 0) {
                var needed = self.opts.D - peers_map.count();
                if (needed > fanout_candidates.items.len) needed = fanout_candidates.items.len;
                if (needed > 0) {
                    var added_count: usize = 0;
                    var idx: usize = 0;
                    while (idx < fanout_candidates.items.len and added_count < needed) : (idx += 1) {
                        const peer_id = fanout_candidates.items[idx];
                        if (peers_map.contains(peer_id)) continue;
                        peers_map.put(self.allocator, peer_id, {}) catch |err| {
                            std.log.warn("heartbeat: failed to add fanout peer {} for topic {s}: {}", .{ peer_id, topic, err });
                            break;
                        };
                        if (idx != added_count) {
                            std.mem.swap(PeerId, &fanout_candidates.items[idx], &fanout_candidates.items[added_count]);
                        }
                        added_count += 1;
                    }
                    gossip_start_index = added_count;
                }
            }

            const fanout_gossip_candidates = fanout_candidates.items[gossip_start_index..];
            if (fanout_gossip_candidates.len > 0) {
                var target = self.opts.D_lazy;
                const candidate_float = @as(f32, @floatFromInt(fanout_gossip_candidates.len));
                const factor_float = @ceil(self.opts.gossip_factor * candidate_float);
                const factor = blk: {
                    if (!std.math.isFinite(factor_float) or factor_float < 0) break :blk 0;
                    const max_f32 = @as(f32, @floatFromInt(std.math.maxInt(usize)));
                    if (factor_float > max_f32) break :blk fanout_gossip_candidates.len;
                    break :blk @as(usize, @intFromFloat(factor_float));
                };
                if (factor > target) target = factor;
                if (target > fanout_gossip_candidates.len) target = fanout_gossip_candidates.len;
                if (target > 0) {
                    const gossip_ids = self.mcache.getGossipIDs(topic) catch |err| {
                        std.log.warn("heartbeat: failed to get fanout gossip IDs for topic {s}: {}", .{ topic, err });
                        continue;
                    };
                    defer self.allocator.free(gossip_ids);
                    if (gossip_ids.len > 0) {
                        for (fanout_gossip_candidates[0..target]) |peer_id| {
                            self.enqueueGossip(peer_id, topic, gossip_ids) catch |err| {
                                std.log.warn("heartbeat: failed to enqueue fanout gossip for peer {} topic {s}: {}", .{ peer_id, topic, err });
                            };
                        }
                    }
                }
            }

            if (self.fanout_last_pub.getPtr(topic)) |value_ptr| {
                value_ptr.* = now_ms;
            } else {
                self.fanout_last_pub.put(self.allocator, topic, now_ms) catch {};
            }
        }

        self.sendGraftPrune(&tograft, &toprune, &no_px);
        self.flush(arena_allocator);
        self.mcache.shift();

        self.event_emitter.emit(.{ .heartbeat = .{} });
    }
    fn flush(self: *Self, arena: Allocator) void {
        while (self.gossip.count() > 0) {
            var iter = self.gossip.iterator();
            const entry = iter.next() orelse break;
            const peer = entry.key_ptr.*;
            const removed = self.gossip.fetchRemove(peer) orelse continue;
            var ihave_list = removed.value;

            var rpc_msg = rpc.RPC{ .control = .{ .ihave = ihave_list.items } };
            if (!self.sendRPC(arena, &peer, &rpc_msg)) {
                if (self.gossip.put(self.allocator, peer, ihave_list)) |_| {
                    continue;
                } else |err| {
                    std.log.warn("heartbeat: failed to requeue gossip for peer {}: {}", .{ peer, err });
                    deinitControlIHaveList(self.allocator, &ihave_list);
                }
            } else {
                deinitControlIHaveList(self.allocator, &ihave_list);
            }
        }

        while (self.control.count() > 0) {
            var iter = self.control.iterator();
            const entry = iter.next() orelse break;
            const peer = entry.key_ptr.*;
            const removed = self.control.fetchRemove(peer) orelse continue;
            var ctrl_opt = removed.value;

            if (ctrl_opt) |*ctrl| {
                var rpc_msg = rpc.RPC{ .control = .{ .graft = ctrl.graft, .prune = ctrl.prune } };
                if (!self.sendRPC(arena, &peer, &rpc_msg)) {
                    if (self.control.put(self.allocator, peer, ctrl_opt)) |_| {
                        ctrl_opt = null;
                        continue;
                    } else |err| {
                        std.log.warn("heartbeat: failed to requeue control message for peer {}: {}", .{ peer, err });
                        pubsub.deinitControl(&ctrl_opt, self.allocator);
                        ctrl_opt = null;
                    }
                } else {
                    pubsub.deinitControl(&ctrl_opt, self.allocator);
                    ctrl_opt = null;
                }
            }
        }
    }

    fn join(self: *Self, arena: Allocator, topic: []const u8) !void {
        if (self.mesh.contains(topic)) {
            return;
        }

        var mesh_peers: std.AutoHashMapUnmanaged(PeerId, void) = .empty;
        errdefer mesh_peers.deinit(self.allocator);

        var cleanup_fanout = false;
        if (self.fanout.contains(topic)) {
            var fanout_peers = self.fanout.get(topic).?;
            var fanout_peers_iter = fanout_peers.keyIterator();
            while (fanout_peers_iter.next()) |peer_id| {
                try mesh_peers.put(self.allocator, peer_id.*, {});
            }
            if (mesh_peers.count() < self.opts.D) {
                var filter_ctx: FilterCtx = .{
                    .mesh_peers = &mesh_peers,
                };
                var more_peers = try self.getRandomGossipPeers(topic, self.opts.D - mesh_peers.count(), &filter_ctx, FilterCtx.filter);
                defer more_peers.deinit(self.allocator);
                for (more_peers.items) |p| {
                    // This should not fail as we are just adding new peers.
                    try mesh_peers.put(self.allocator, p, {});
                }
            }
            cleanup_fanout = true;
        } else {
            var new_peers = try self.getRandomGossipPeers(topic, self.opts.D, null, struct {
                fn filter(_: ?*anyopaque, _: PeerId) bool {
                    return false;
                }
            }.filter);
            defer new_peers.deinit(self.allocator);

            for (new_peers.items) |p| {
                try mesh_peers.put(self.allocator, p, {});
            }
        }

        const interned_topic = try self.refTopic(topic);

        self.mesh.put(self.allocator, interned_topic, mesh_peers) catch |err| {
            self.unrefTopic(interned_topic);
            return err;
        };

        if (cleanup_fanout) {
            var fanout_peers = self.fanout.fetchRemove(topic).?.value;
            self.unrefTopic(topic);
            fanout_peers.deinit(self.allocator);
            _ = self.fanout_last_pub.remove(topic);
            self.unrefTopic(topic);
        }

        var it = mesh_peers.keyIterator();
        while (it.next()) |peer_id| {
            // TODO: self.tracer.Graft(p, topic)
            self.sendGraft(arena, peer_id, interned_topic);
            // TODO: self.tagPeer(p, topic)
        }
    }

    fn leave(self: *Self, arena: Allocator, topic: []const u8) void {
        var removed_entry = self.mesh.fetchRemove(topic) orelse {
            return;
        };

        const interned_topic = removed_entry.key;
        defer removed_entry.value.deinit(self.allocator);
        defer self.unrefTopic(interned_topic);

        // TODO: self.tracer.Leave(topic)

        const peer_set = removed_entry.value;
        var it = peer_set.keyIterator();
        while (it.next()) |peer_id| {
            // TODO: self.tracer.Prune(p, topic)
            self.sendPrune(arena, peer_id, interned_topic);
            // TODO: self.tagPeer(p, topic)
        }
    }

    fn nextSeqno(self: *Self, arena: Allocator) !struct { u64, []u8 } {
        const counter = self.counter.fetchAdd(1, .monotonic);
        const buffer = try arena.alloc(u8, 8);
        std.mem.writeInt(u64, @as(*[8]u8, @ptrCast(buffer.ptr)), counter, .big);
        return .{ counter, buffer };
    }

    /// Gets or creates an interned version of the topic, incrementing its reference count.
    ///
    /// This function will copy the topic string into the interning pool if needed.
    /// The caller retains ownership of the input parameter.
    /// The returned slice points to memory in the interning pool, whose lifetime is managed by reference counting.
    ///
    /// Example:
    /// ```zig
    /// const my_topic = "my-topic"; // String literal or stack slice
    /// const interned = try self.internTopic(my_topic);
    /// // my_topic ownership remains with caller, can continue to use or free it
    /// ```
    ///
    /// Note: Must be called from the IO loop thread
    fn refTopic(self: *Self, topic: []const u8) ![]const u8 {
        const entry = try self.topic_pool.getOrPut(self.allocator, topic);
        if (!entry.found_existing) {
            // New topic, allocate memory and set reference count to 1
            entry.key_ptr.* = try self.allocator.dupe(u8, topic);
            entry.value_ptr.* = 1;
        } else {
            // Already exists, increment reference count
            entry.value_ptr.* += 1;
        }

        return entry.key_ptr.*;
    }

    /// Decrements the reference count for a topic.
    /// When the reference count reaches zero, the topic memory is automatically freed.
    ///
    /// Note: Must be called from the IO loop thread
    fn unrefTopic(self: *Self, topic: []const u8) void {
        if (self.topic_pool.getPtr(topic)) |count_ptr| {
            count_ptr.* -= 1;
            if (count_ptr.* == 0) {
                const removed = self.topic_pool.fetchRemove(topic).?;
                self.allocator.free(removed.key);
            }
        }
    }

    fn vtableHandleRPCFn(instance: *anyopaque, arena: Allocator, rpc_msg: *const pubsub.RPC) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.handleRPC(arena, rpc_msg);
    }

    fn vtableAddPeerFn(instance: *anyopaque, peer: Multiaddr, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.addPeer(peer, callback_ctx, callback);
    }

    fn vtableRemovePeerFn(instance: *anyopaque, peer: PeerId, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.removePeer(peer, callback_ctx, callback);
    }

    fn vtableSubscribeFn(instance: *anyopaque, topic: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.subscribe(topic, callback_ctx, callback);
    }

    fn vtablePublishFn(instance: *anyopaque, topic: []const u8, data: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.publish(topic, data, callback_ctx, callback);
    }

    fn vtablePublishOwnedFn(instance: *anyopaque, topic: []const u8, data: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror![]PeerId) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.publishOwnedInternal(topic, data, callback_ctx, callback);
    }

    fn vtableUnsubscribeFn(instance: *anyopaque, topic: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.unsubscribe(topic, callback_ctx, callback);
    }

    fn vtableStartHeartbeatFn(instance: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.startHeartbeatTimerOnLoop();
    }

    fn vtableStopHeartbeatFn(instance: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.stopHeartbeatTimerOnLoop();
    }

    fn vtableGetAllocatorFn(instance: *anyopaque) Allocator {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.getAllocator();
    }

    // --- Static VTable Instance ---
    const vtable_instance = pubsub.PubSubVTable{
        .handleRPCFn = vtableHandleRPCFn,
        .addPeerFn = vtableAddPeerFn,
        .removePeerFn = vtableRemovePeerFn,
        .subscribeFn = vtableSubscribeFn,
        .publishFn = vtablePublishFn,
        .publishOwnedFn = vtablePublishOwnedFn,
        .unsubscribeFn = vtableUnsubscribeFn,
        .startHeartbeatFn = vtableStartHeartbeatFn,
        .stopHeartbeatFn = vtableStopHeartbeatFn,
        .getAllocatorFn = vtableGetAllocatorFn,
    };

    // --- any() method ---
    pub fn any(self: *Self) pubsub.PubSub {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

const WaitError = error{Timeout};

fn waitForFlag(flag: *std.atomic.Value(bool), timeout_ns: u64) WaitError!void {
    const step = 10 * std.time.ns_per_ms;
    var waited: u64 = 0;

    while (!flag.load(.acquire)) {
        if (waited >= timeout_ns) return error.Timeout;
        std.time.sleep(step);
        waited += step;
    }
}

fn waitForMeshTopic(router: *Gossipsub, topic: []const u8, timeout_ns: u64) WaitError!void {
    const step = 10 * std.time.ns_per_ms;
    var waited: u64 = 0;

    while (true) {
        if (router.mesh.contains(topic)) return;

        if (waited >= timeout_ns) return error.Timeout;
        std.time.sleep(step);
        waited += step;
    }
}

fn waitForTopicPeer(router: *Gossipsub, topic: []const u8, peer: PeerId, timeout_ns: u64) WaitError!void {
    const step = 10 * std.time.ns_per_ms;
    var waited: u64 = 0;

    while (true) {
        if (router.topics.getPtr(topic)) |topic_peers| {
            if (topic_peers.contains(peer)) return;
        }

        if (waited >= timeout_ns) return error.Timeout;
        std.time.sleep(step);
        waited += step;
    }
}

fn waitForUsableStream(router: *Gossipsub, peer: PeerId, timeout_ns: u64) WaitError!void {
    const step = 10 * std.time.ns_per_ms;
    var waited: u64 = 0;

    while (true) {
        if (router.hasUsableStream(peer)) return;

        if (waited >= timeout_ns) return error.Timeout;
        std.time.sleep(step);
        waited += step;
    }
}

test "gossipsub heartbeat seed mixing" {
    const seed_zero = heartbeatSeed(0, 0);
    try std.testing.expectEqual(heartbeat_seed_salt, seed_zero);

    const sample_now: i64 = 42;
    const sample_ticks: u64 = 7;
    const expected_sample = blk: {
        const now_bits: u64 = @bitCast(sample_now);
        break :blk now_bits ^ (sample_ticks ^ heartbeat_seed_salt);
    };
    try std.testing.expectEqual(expected_sample, heartbeatSeed(sample_now, sample_ticks));

    const next_ticks = sample_ticks + 1;
    try std.testing.expect(heartbeatSeed(sample_now, sample_ticks) != heartbeatSeed(sample_now, next_ticks));

    const negative_now: i64 = -13579;
    const expected_negative = blk: {
        const now_bits: u64 = @bitCast(negative_now);
        break :blk now_bits ^ (sample_ticks ^ heartbeat_seed_salt);
    };
    try std.testing.expectEqual(expected_negative, heartbeatSeed(negative_now, sample_ticks));
}

test "gossipsub heartbeat prunes mesh peers without usable streams" {
    const allocator = std.testing.allocator;

    var node: TestGossipsubNode = undefined;
    try node.init(allocator, 10167, .{});
    defer node.deinit();

    const topic = "heartbeat-prune";
    const interned_topic = try node.router.refTopic(topic);

    var mesh_peers = std.AutoHashMapUnmanaged(PeerId, void).empty;
    var mesh_entry_installed = false;
    defer {
        if (mesh_entry_installed) {
            if (node.router.mesh.fetchRemove(interned_topic)) |removed_const| {
                var removed = removed_const;
                removed.value.deinit(node.router.allocator);
                node.router.unrefTopic(removed.key);
            }
        } else {
            mesh_peers.deinit(node.router.allocator);
            node.router.unrefTopic(interned_topic);
        }
    }

    try mesh_peers.put(node.router.allocator, node.router.peer_id, {});
    try node.router.mesh.put(node.router.allocator, interned_topic, mesh_peers);
    mesh_entry_installed = true;

    const mesh_before = node.router.mesh.getPtr(interned_topic).?;
    try std.testing.expectEqual(@as(usize, 1), mesh_before.count());

    const ticks_before = node.router.heartbeat_ticks;
    node.router.doHeartbeat();
    try std.testing.expectEqual(ticks_before + 1, node.router.heartbeat_ticks);

    const mesh_after = node.router.mesh.getPtr(interned_topic).?;
    try std.testing.expectEqual(@as(usize, 0), mesh_after.count());
}

test "gossipsub heartbeat timer increments ticks" {
    const allocator = std.testing.allocator;

    const heartbeat_interval_ms: u64 = 50;
    var node: TestGossipsubNode = undefined;
    try node.init(allocator, 10267, .{ .heartbeat_interval_ms = heartbeat_interval_ms });
    defer node.deinit();

    const initial_ticks = node.router.heartbeat_ticks;

    const wait_timeout_ns = 2 * std.time.ns_per_s;
    const deadline = std.time.nanoTimestamp() + wait_timeout_ns;
    while (node.router.heartbeat_ticks == initial_ticks) {
        if (std.time.nanoTimestamp() >= deadline) break;
        std.time.sleep(10 * std.time.us_per_ms);
    }

    try std.testing.expect(node.router.heartbeat_ticks > initial_ticks);
}

test "gossipsub heartbeat queues graft when mesh under target" {
    const allocator = std.testing.allocator;

    const opts: Gossipsub.Options = .{
        .D = 1,
        .D_lo = 1,
        .D_hi = 2,
        .D_lazy = 1,
    };

    var node_a: TestGossipsubNode = undefined;
    try node_a.init(allocator, 10367, opts);
    defer node_a.deinit();

    var node_b: TestGossipsubNode = undefined;
    try node_b.init(allocator, 10368, opts);
    defer node_b.deinit();

    try addPeerSync(&node_a.router, node_b.dial_addr);
    try addPeerSync(&node_b.router, node_a.dial_addr);

    const topic = "mesh-underflow";
    try subscribeSync(&node_a.router, topic);
    try subscribeSync(&node_b.router, topic);

    try waitForTopicPeer(&node_a.router, topic, node_b.transport.local_peer_id, 5 * std.time.ns_per_s);
    try waitForUsableStream(&node_a.router, node_b.transport.local_peer_id, 5 * std.time.ns_per_s);

    node_a.router.heartbeat_running = false;
    node_b.router.heartbeat_running = false;

    var mesh_ptr = node_a.router.mesh.getPtr(topic).?;
    _ = mesh_ptr.remove(node_b.transport.local_peer_id);
    try std.testing.expectEqual(@as(usize, 0), mesh_ptr.count());

    var graft_done = std.atomic.Value(bool).init(false);
    var graft_peer: ?PeerId = null;
    var graft_topic: ?[]const u8 = null;
    var graft_topic_buf: ?[]u8 = null;
    defer if (graft_topic_buf) |buf| std.testing.allocator.free(buf);
    var graft_direction: ?p2p_conn.Direction = null;

    const GraftListener = struct {
        done: *std.atomic.Value(bool),
        peer: *?PeerId,
        topic: *?[]const u8,
        direction: *?p2p_conn.Direction,
        allocator: Allocator,
        topic_buf: *?[]u8,

        const Self = @This();

        pub fn handle(self: *Self, e: Event) void {
            switch (e) {
                .gossipsub_graft => |ev| {
                    self.peer.* = ev.peer;
                    const maybe_topic = ev.topic;
                    if (maybe_topic.len > 0) {
                        if (self.topic_buf.*) |buf| {
                            self.allocator.free(buf);
                            self.topic_buf.* = null;
                        }
                        const copy = self.allocator.dupe(u8, maybe_topic) catch unreachable;
                        self.topic_buf.* = copy;
                        self.topic.* = copy;
                    } else {
                        self.topic.* = maybe_topic;
                    }
                    self.direction.* = ev.direction;
                    self.done.store(true, .release);
                },
                else => {},
            }
        }

        pub fn vtableHandleFn(instance: *anyopaque, e: Event) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.handle(e);
        }

        pub const vtable = event.EventListenerVTable(Event){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) event.AnyEventListener(Event) {
            return .{ .instance = @ptrCast(self), .vtable = &Self.vtable };
        }
    };

    var listener = GraftListener{
        .done = &graft_done,
        .peer = &graft_peer,
        .topic = &graft_topic,
        .direction = &graft_direction,
        .allocator = std.testing.allocator,
        .topic_buf = &graft_topic_buf,
    };
    try node_a.router.event_emitter.addListener(.gossipsub_graft, listener.any());
    defer _ = node_a.router.event_emitter.removeListener(.gossipsub_graft, listener.any());

    node_a.router.doHeartbeat();
    try waitForFlag(&graft_done, std.time.ns_per_s);

    try std.testing.expect(graft_peer != null);
    try std.testing.expect(graft_peer.?.eql(&node_b.transport.local_peer_id));
    try std.testing.expect(graft_topic != null);
    try std.testing.expect(std.mem.eql(u8, graft_topic.?, topic));
    try std.testing.expect(graft_direction != null);
    try std.testing.expectEqual(p2p_conn.Direction.OUTBOUND, graft_direction.?);

    try waitForTopicPeer(&node_a.router, topic, node_b.transport.local_peer_id, 5 * std.time.ns_per_s);
}

test "gossipsub replays existing subscriptions to new peer" {
    const allocator = std.testing.allocator;

    var node_a: TestGossipsubNode = undefined;
    try node_a.init(allocator, 10369, .{});
    defer node_a.deinit();

    var node_b: TestGossipsubNode = undefined;
    try node_b.init(allocator, 10370, .{});
    defer node_b.deinit();

    const topic = "existing-subscription";
    try subscribeSync(&node_a.router, topic);

    try addPeerSync(&node_a.router, node_b.dial_addr);
    try addPeerSync(&node_b.router, node_a.dial_addr);

    try waitForTopicPeer(&node_b.router, topic, node_a.transport.local_peer_id, 5 * std.time.ns_per_s);
}

fn addPeerSync(router: *Gossipsub, addr: Multiaddr) !void {
    var done = std.atomic.Value(bool).init(false);
    var err: ?anyerror = null;

    const State = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
    };

    const Callback = struct {
        fn call(ctx: ?*anyopaque, res: anyerror!void) void {
            const state: *State = @ptrCast(@alignCast(ctx.?));
            if (res) |_| {
                state.err.* = null;
            } else |e| {
                state.err.* = e;
            }
            state.done.store(true, .release);
        }
    };

    var state = State{ .done = &done, .err = &err };
    router.addPeer(addr, @ptrCast(@constCast(&state)), Callback.call);

    try waitForFlag(&done, 5 * std.time.ns_per_s);
    if (err) |e| return e;
}

fn subscribeSync(router: *Gossipsub, topic: []const u8) !void {
    var done = std.atomic.Value(bool).init(false);
    var err: ?anyerror = null;

    const State = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
    };

    const Callback = struct {
        fn call(ctx: ?*anyopaque, res: anyerror!void) void {
            const state: *State = @ptrCast(@alignCast(ctx.?));
            if (res) |_| {
                state.err.* = null;
            } else |e| {
                state.err.* = e;
            }
            state.done.store(true, .release);
        }
    };

    var state = State{ .done = &done, .err = &err };
    router.subscribe(topic, @ptrCast(@constCast(&state)), Callback.call);

    try waitForFlag(&done, 5 * std.time.ns_per_s);
    if (err) |e| return e;
}

fn publishSync(router: *Gossipsub, topic: []const u8, payload: []const u8, allocator: Allocator) ![]PeerId {
    var done = std.atomic.Value(bool).init(false);
    var err: ?anyerror = null;
    var recipients: ?[]PeerId = null;

    const State = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
        recipients: *?[]PeerId,
    };

    const Callback = struct {
        fn call(ctx: ?*anyopaque, res: anyerror![]PeerId) void {
            const state: *State = @ptrCast(@alignCast(ctx.?));
            const peers = res catch |e| {
                state.err.* = e;
                state.recipients.* = null;
                state.done.store(true, .release);
                return;
            };
            state.err.* = null;
            state.recipients.* = peers;
            state.done.store(true, .release);
        }
    };

    var state = State{ .done = &done, .err = &err, .recipients = &recipients };
    router.publish(topic, payload, @ptrCast(@constCast(&state)), Callback.call);

    try waitForFlag(&done, 5 * std.time.ns_per_s);
    if (err) |e| {
        if (recipients) |peers| allocator.free(peers);
        return e;
    }

    const peers = recipients orelse return error.AsyncResultMissing;
    return peers;
}

const TestGossipsubNode = struct {
    allocator: Allocator,
    listen_addr: Multiaddr,
    dial_addr: Multiaddr,
    loop: io_loop.ThreadEventLoop,
    transport: quic.QuicTransport,
    sw: swarm.Switch,
    handler: PubSubPeerProtocolHandler,
    ping_handler: ping.PingProtocolHandler,
    router: Gossipsub,
    host_key: identity.KeyPair,

    pub fn init(self: *TestGossipsubNode, allocator: Allocator, port: u16, options: Gossipsub.Options) !void {
        try self.initWithListenCallback(allocator, port, options, null, Gossipsub.onIncomingNewStream);
    }

    pub fn initWithListenCallback(
        self: *TestGossipsubNode,
        allocator: Allocator,
        port: u16,
        options: Gossipsub.Options,
        listen_ctx: ?*anyopaque,
        listen_callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        var listen_buf: [64]u8 = undefined;
        const listen_str = try std.fmt.bufPrint(&listen_buf, "/ip4/0.0.0.0/udp/{d}", .{port});

        var dial_buf: [64]u8 = undefined;
        const dial_str = try std.fmt.bufPrint(&dial_buf, "/ip4/127.0.0.1/udp/{d}", .{port});

        self.allocator = allocator;
        self.listen_addr = try Multiaddr.fromString(allocator, listen_str);
        errdefer self.listen_addr.deinit();

        self.dial_addr = try Multiaddr.fromString(allocator, dial_str);
        errdefer self.dial_addr.deinit();

        self.host_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
        errdefer self.host_key.deinit();

        try self.loop.init(allocator);
        errdefer self.loop.deinit();

        try self.transport.init(&self.loop, &self.host_key, keys.KeyType.ED25519, allocator);
        errdefer {
            self.transport.deinit();
            self.loop.deinit();
        }

        try self.dial_addr.push(.{ .P2P = self.transport.local_peer_id });

        self.sw.init(allocator, &self.transport);

        self.ping_handler = ping.PingProtocolHandler.init(allocator, &self.sw);
        self.handler = PubSubPeerProtocolHandler.init(allocator);
        errdefer {
            self.handler.deinit();
            self.ping_handler.deinit();
            self.sw.deinit();
            self.transport.deinit();
            self.loop.deinit();
        }
        try self.sw.addProtocolHandler(ping.protocol_id, self.ping_handler.any());
        try self.sw.addProtocolHandler(v1_id, self.handler.any());
        try self.sw.addProtocolHandler(v1_1_id, self.handler.any());

        try self.router.init(allocator, self.listen_addr, self.transport.local_peer_id, &self.host_key, &self.sw, options);
        errdefer self.router.deinit();

        const effective_ctx: ?*anyopaque = listen_ctx orelse @ptrCast(&self.router);
        try self.sw.listen(self.listen_addr, effective_ctx, listen_callback);
        std.time.sleep(300 * std.time.us_per_ms);
    }

    pub fn deinit(self: *TestGossipsubNode) void {
        self.sw.stop();
        self.sw.deinit();
        self.router.deinit();
        self.handler.deinit();
        self.ping_handler.deinit();
        self.dial_addr.deinit();
        self.listen_addr.deinit();
        self.loop.close();
        self.loop.deinit();
        self.host_key.deinit();
    }
};

test "gossipsub listen callback exposes negotiated protocol" {
    const allocator = std.testing.allocator;

    const ListenObserver = struct {
        event: std.Thread.ResetEvent = .{},
        observed_protocol: ?[]const u8 = null,
        router: ?*Gossipsub = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            defer self.event.set();

            const controller = res catch return;
            const stream = libp2p.protocols.getStream(controller) orelse return;
            self.observed_protocol = stream.negotiated_protocol;

            if (self.router) |router| {
                Gossipsub.onIncomingNewStream(@ptrCast(router), controller);
            }
        }
    };

    var observer = ListenObserver{};

    var server: TestGossipsubNode = undefined;
    try server.initWithListenCallback(allocator, 9450, .{}, &observer, ListenObserver.callback);
    observer.router = &server.router;
    defer server.deinit();

    var client: TestGossipsubNode = undefined;
    try client.init(allocator, 9451, .{});
    defer client.deinit();

    client.router.addPeer(server.dial_addr, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            _ = res catch {};
        }
    }.callback);

    observer.event.wait();

    try std.testing.expect(observer.observed_protocol != null);
    const proto = observer.observed_protocol.?;
    const is_supported = std.mem.eql(u8, proto, v1_id) or std.mem.eql(u8, proto, v1_1_id);
    try std.testing.expect(is_supported);

    std.time.sleep(200 * std.time.ns_per_ms);
}

test "switch handles ping and gossipsub simultaneously" {
    const allocator = std.testing.allocator;
    const server_port: u16 = 9472;
    const client_port: u16 = 9473;
    const topic = "interop/ping-gossipsub";
    const payload = "ping-gossipsub-payload";

    const ListenDispatch = struct {
        node: *TestGossipsubNode,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            const controller = res catch return;
            const stream = libp2p.protocols.getStream(controller) orelse return;
            const proto = stream.negotiated_protocol orelse return;

            if (std.mem.eql(u8, proto, v1_id) or std.mem.eql(u8, proto, v1_1_id)) {
                Gossipsub.onIncomingNewStream(@ptrCast(&self.node.router), controller);
                return;
            }

            if (std.mem.eql(u8, proto, ping.protocol_id)) {
                return;
            }

            stream.close(null, struct {
                fn onClosed(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
            }.onClosed);
        }
    };

    var server: TestGossipsubNode = undefined;
    var server_dispatch = ListenDispatch{ .node = &server };
    try server.initWithListenCallback(allocator, server_port, .{}, &server_dispatch, ListenDispatch.callback);
    defer server.deinit();

    var client: TestGossipsubNode = undefined;
    var client_dispatch = ListenDispatch{ .node = &client };
    try client.initWithListenCallback(allocator, client_port, .{}, &client_dispatch, ListenDispatch.callback);
    defer client.deinit();

    try addPeerSync(&client.router, server.dial_addr);
    try addPeerSync(&server.router, client.dial_addr);

    try subscribeSync(&server.router, topic);
    try subscribeSync(&client.router, topic);

    try waitForTopicPeer(&server.router, topic, client.transport.local_peer_id, 5 * std.time.ns_per_s);
    try waitForTopicPeer(&client.router, topic, server.transport.local_peer_id, 5 * std.time.ns_per_s);

    const MessageListener = struct {
        allocator: Allocator,
        done: *std.atomic.Value(bool),
        data: *?[]u8,
        source: *?PeerId,
        topic: []const u8,

        const Self = @This();

        fn handle(self: *Self, evt: Event) void {
            switch (evt) {
                .message => |msg| {
                    if (!std.mem.eql(u8, msg.message.topic, self.topic)) return;

                    if (self.data.*) |existing| {
                        self.allocator.free(existing);
                    }

                    const copy = self.allocator.dupe(u8, msg.message.data) catch {
                        self.source.* = null;
                        self.data.* = null;
                        self.done.store(true, .release);
                        return;
                    };

                    self.data.* = copy;
                    self.source.* = msg.propagation_source;
                    self.done.store(true, .release);
                },
                else => {},
            }
        }

        fn vtableHandleFn(instance: *anyopaque, evt: Event) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.handle(evt);
        }

        const vtable = event.EventListenerVTable(Event){
            .handleFn = vtableHandleFn,
        };

        fn any(self: *Self) event.AnyEventListener(Event) {
            return .{ .instance = @ptrCast(self), .vtable = &Self.vtable };
        }
    };

    var message_done = std.atomic.Value(bool).init(false);
    var message_data: ?[]u8 = null;
    errdefer if (message_data) |data_copy| allocator.free(data_copy);
    var message_source: ?PeerId = null;

    var listener = MessageListener{
        .allocator = allocator,
        .done = &message_done,
        .data = &message_data,
        .source = &message_source,
        .topic = topic,
    };
    try server.router.event_emitter.addListener(.message, listener.any());
    defer _ = server.router.event_emitter.removeListener(.message, listener.any());

    var ping_addr_buf: [64]u8 = undefined;
    const ping_addr_str = try std.fmt.bufPrint(&ping_addr_buf, "/ip4/127.0.0.1/udp/{d}", .{server_port});
    var ping_dial_addr = try Multiaddr.fromString(allocator, ping_addr_str);
    defer ping_dial_addr.deinit();
    try ping_dial_addr.push(.{ .P2P = server.transport.local_peer_id });

    const PingResultCtx = struct {
        event: std.Thread.ResetEvent = .{},
        result_ns: ?u64 = null,
        err: ?anyerror = null,
        sender: ?*ping.PingStream = null,

        const Self = @This();

        fn callback(ctx: ?*anyopaque, sender: ?*ping.PingStream, res: anyerror!u64) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            self.sender = sender;
            self.result_ns = res catch |err| {
                self.err = err;
                self.result_ns = null;
                self.event.set();
                return;
            };
            self.err = null;
            self.event.set();
        }
    };

    var ping_ctx = PingResultCtx{};
    var ping_service = ping.PingService.init(allocator, &client.sw);
    try ping_service.ping(ping_dial_addr, .{}, &ping_ctx, PingResultCtx.callback);
    ping_ctx.event.wait();
    try std.testing.expect(ping_ctx.err == null);
    try std.testing.expect(ping_ctx.result_ns != null);
    try std.testing.expect(ping_ctx.result_ns.? > 0);

    if (ping_ctx.sender) |sender| {
        const CloseCtx = struct {
            event: std.Thread.ResetEvent = .{},

            const Self = @This();

            fn callback(ctx: ?*anyopaque, _: anyerror!*quic.QuicStream) void {
                const self: *Self = @ptrCast(@alignCast(ctx.?));
                self.event.set();
            }
        };

        var close_ctx = CloseCtx{};
        sender.close(&close_ctx, CloseCtx.callback);
        close_ctx.event.wait();
    }

    const recipients = try publishSync(&client.router, topic, payload, allocator);
    defer allocator.free(recipients);
    try std.testing.expect(recipients.len >= 1);

    var saw_server = false;
    for (recipients) |peer| {
        if (peer.eql(&server.transport.local_peer_id)) {
            saw_server = true;
            break;
        }
    }
    try std.testing.expect(saw_server);

    try waitForFlag(&message_done, 5 * std.time.ns_per_s);
    try std.testing.expect(message_data != null);
    try std.testing.expect(std.mem.eql(u8, message_data.?, payload));
    try std.testing.expect(message_source != null);
    try std.testing.expect(message_source.?.eql(&client.transport.local_peer_id));

    if (message_data) |data_copy| {
        allocator.free(data_copy);
        message_data = null;
    }
}

test "pubsub add peer" {
    const allocator = std.testing.allocator;

    var node1: TestGossipsubNode = undefined;
    try node1.init(allocator, 9167, .{});
    defer node1.deinit();

    var node2: TestGossipsubNode = undefined;
    try node2.init(allocator, 9168, .{});
    defer node2.deinit();

    node1.router.addPeer(node2.dial_addr, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    node2.router.addPeer(node1.dial_addr, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, node1.router.peers.count());
    try std.testing.expectEqual(1, node2.router.peers.count());

    try std.testing.expect(node1.router.peers.get(node2.transport.local_peer_id).?.initiator != null);
    try std.testing.expect(node1.router.peers.get(node2.transport.local_peer_id).?.responder != null);

    try std.testing.expect(node2.router.peers.get(node1.transport.local_peer_id).?.initiator != null);
    try std.testing.expect(node2.router.peers.get(node1.transport.local_peer_id).?.responder != null);
}

test "pubsub add and remove peer" {
    const allocator = std.testing.allocator;

    var node1: TestGossipsubNode = undefined;
    try node1.init(allocator, 9767, .{});
    defer node1.deinit();

    var node2: TestGossipsubNode = undefined;
    try node2.init(allocator, 9768, .{});
    defer node2.deinit();

    node1.router.addPeer(node2.dial_addr, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    node2.router.addPeer(node1.dial_addr, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, node1.router.peers.count());
    try std.testing.expectEqual(1, node2.router.peers.count());

    try std.testing.expect(node1.router.peers.get(node2.transport.local_peer_id).?.initiator != null);
    try std.testing.expect(node1.router.peers.get(node2.transport.local_peer_id).?.responder != null);

    try std.testing.expect(node2.router.peers.get(node1.transport.local_peer_id).?.initiator != null);
    try std.testing.expect(node2.router.peers.get(node1.transport.local_peer_id).?.responder != null);

    node1.router.removePeer(node2.transport.local_peer_id, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to remove peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully removed peer\n", .{});
        }
    }.callback);

    node2.router.removePeer(node1.transport.local_peer_id, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to remove peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully removed peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(0, node1.router.peers.count());
    try std.testing.expectEqual(0, node2.router.peers.count());
}

test "pubsub add peer single direction" {
    const allocator = std.testing.allocator;

    var initiator: TestGossipsubNode = undefined;
    try initiator.init(allocator, 9267, .{});
    defer initiator.deinit();

    var responder: TestGossipsubNode = undefined;
    try responder.init(allocator, 9268, .{});
    defer responder.deinit();

    initiator.router.addPeer(responder.dial_addr, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, initiator.router.peers.count());
    try std.testing.expectEqual(1, responder.router.peers.count());

    try std.testing.expect(initiator.router.peers.get(responder.transport.local_peer_id).?.initiator != null);
    try std.testing.expect(initiator.router.peers.get(responder.transport.local_peer_id).?.responder == null);
}

test "pubsub add and remove peer single direction" {
    const allocator = std.testing.allocator;

    var initiator: TestGossipsubNode = undefined;
    try initiator.init(allocator, 9367, .{});
    defer initiator.deinit();

    var responder: TestGossipsubNode = undefined;
    try responder.init(allocator, 9368, .{});
    defer responder.deinit();

    initiator.router.addPeer(responder.dial_addr, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, initiator.router.peers.count());
    try std.testing.expectEqual(1, responder.router.peers.count());

    try std.testing.expect(initiator.router.peers.get(responder.transport.local_peer_id).?.initiator != null);
    try std.testing.expect(initiator.router.peers.get(responder.transport.local_peer_id).?.responder == null);

    initiator.router.removePeer(responder.transport.local_peer_id, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to remove peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully removed peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(0, initiator.router.peers.count());
    try std.testing.expectEqual(0, responder.router.peers.count());
}

test "pubsub add peer single direction with replace stream" {
    const allocator = std.testing.allocator;

    var initiator: TestGossipsubNode = undefined;
    try initiator.init(allocator, 9467, .{});
    defer initiator.deinit();

    var responder: TestGossipsubNode = undefined;
    try responder.init(allocator, 9468, .{});
    defer responder.deinit();

    initiator.router.addPeer(responder.dial_addr, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, initiator.router.peers.count());
    try std.testing.expectEqual(1, responder.router.peers.count());

    try std.testing.expect(initiator.router.peers.get(responder.transport.local_peer_id).?.initiator != null);
    try std.testing.expect(initiator.router.peers.get(responder.transport.local_peer_id).?.responder == null);

    try std.testing.expect(responder.router.peers.get(initiator.transport.local_peer_id).?.responder != null);
    std.debug.print("old responder {?*}\n", .{responder.router.peers.get(initiator.transport.local_peer_id).?.responder});
    initiator.sw.newStream(responder.dial_addr, initiator.router.protocols, null, struct {
        fn callback(_: ?*anyopaque, _: anyerror!?*anyopaque) void {}
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);
    try std.testing.expect(responder.router.peers.get(initiator.transport.local_peer_id).?.responder != null);
    std.debug.print("new responder {?*}\n", .{responder.router.peers.get(initiator.transport.local_peer_id).?.responder});

    // The old stream be closed by switch2, so that the pubsub1's outgoing stream is closed
    try std.testing.expectEqual(0, initiator.router.peers.count());
}

test "simulate onMessage" {
    const allocator = std.testing.allocator;

    const SubscriptionChangedListener = struct {
        allocator: std.mem.Allocator,

        const Self = @This();

        pub fn handle(_: *Self, e: Event) void {
            switch (e) {
                .subscription_changed => {
                    std.debug.print("Subscription changed event received\n", .{});
                },
                else => {
                    std.debug.print("Unknown event received\n", .{});
                },
            }
        }

        pub fn vtableHandleFn(instance: *anyopaque, e: Event) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            return self.handle(e);
        }

        pub const vtable = event.EventListenerVTable(Event){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) event.AnyEventListener(Event) {
            return event.AnyEventListener(Event){
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    var initiator: TestGossipsubNode = undefined;
    try initiator.init(allocator, 9567, .{});
    defer initiator.deinit();

    var responder: TestGossipsubNode = undefined;
    try responder.init(allocator, 9568, .{});
    defer responder.deinit();

    initiator.router.addPeer(responder.dial_addr, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, initiator.router.peers.count());
    try std.testing.expectEqual(1, responder.router.peers.count());

    try std.testing.expect(initiator.router.peers.get(responder.transport.local_peer_id).?.initiator != null);
    try std.testing.expect(initiator.router.peers.get(responder.transport.local_peer_id).?.responder == null);

    const rpc_message = rpc.RPC{
        .subscriptions = &[_]?rpc.RPC.SubOpts{ .{
            .topicid = "test_topic",
            .subscribe = true,
        }, rpc.RPC.SubOpts{
            .topicid = "test_topic1",
            .subscribe = true,
        } },
    };

    const rpc_message1 = rpc.RPC{
        .subscriptions = &[_]?rpc.RPC.SubOpts{ .{
            .topicid = "test_topic2",
            .subscribe = true,
        }, rpc.RPC.SubOpts{
            .topicid = "test_topic1",
            .subscribe = false,
        } },
    };

    const encoded_size = rpc_message.calcProtobufSize();
    var size_buffer: [200]u8 = undefined;
    const size_bytes = uvarint.encode(usize, encoded_size, &size_buffer);
    const encoded_message = try rpc_message.encode(std.testing.allocator);
    defer std.testing.allocator.free(encoded_message);

    const encoded_size1 = rpc_message1.calcProtobufSize();
    var size_buffer1: [200]u8 = undefined;
    const size_bytes1 = uvarint.encode(usize, encoded_size1, &size_buffer1);
    const encoded_message1 = try rpc_message1.encode(std.testing.allocator);
    defer std.testing.allocator.free(encoded_message1);

    const encoded_rpc_message = try std.mem.concat(std.testing.allocator, u8, &[_][]const u8{ size_bytes, encoded_message, size_bytes1, encoded_message1 });
    defer std.testing.allocator.free(encoded_rpc_message);

    const responder_stream = responder.router.peers.get(initiator.transport.local_peer_id).?.responder.?;
    var listener: SubscriptionChangedListener = .{
        .allocator = allocator,
    };
    try responder.router.event_emitter.addListener(.subscription_changed, listener.any());
    try responder_stream.onMessage(responder_stream.stream, encoded_rpc_message);

    try std.testing.expectEqual(3, responder.router.topics.count());
    try std.testing.expectEqual(0, responder.router.topics.get("test_topic1").?.count());
    try std.testing.expectEqual(1, responder.router.topics.get("test_topic").?.count());
    try std.testing.expectEqual(1, responder.router.topics.get("test_topic2").?.count());
}

test "pubsub heartbeat emits event" {
    const allocator = std.testing.allocator;

    var node: TestGossipsubNode = undefined;
    try node.init(allocator, 9655, .{ .heartbeat_interval_ms = 50 });
    defer node.deinit();

    const HeartbeatListener = struct {
        done: *std.atomic.Value(bool),

        const Self = @This();

        pub fn handle(self: *Self, evt: Event) void {
            switch (evt) {
                .heartbeat => self.done.store(true, .release),
                else => {},
            }
        }

        pub fn vtableHandleFn(instance: *anyopaque, evt: Event) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.handle(evt);
        }

        pub const vtable = event.EventListenerVTable(Event){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) event.AnyEventListener(Event) {
            return event.AnyEventListener(Event){
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    var heartbeat_done = std.atomic.Value(bool).init(false);
    var listener = HeartbeatListener{ .done = &heartbeat_done };
    try node.router.event_emitter.addListener(.heartbeat, listener.any());
    defer _ = node.router.event_emitter.removeListener(.heartbeat, listener.any());

    try waitForFlag(&heartbeat_done, 2 * std.time.ns_per_s);
}

test "pubsub subscribe and publish" {
    const allocator = std.testing.allocator;

    var node: TestGossipsubNode = undefined;
    try node.init(allocator, 9867, .{ .emit_self = true });
    defer node.deinit();

    const topic = "sanity-topic";

    const SubscribeState = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
    };

    const SubscribeCallback = struct {
        fn callback(ctx: ?*anyopaque, res: anyerror!void) void {
            const state: *SubscribeState = @ptrCast(@alignCast(ctx.?));
            if (res) |_| {
                state.err.* = null;
            } else |err| {
                state.err.* = err;
            }
            state.done.store(true, .release);
        }
    };

    var subscribe_done = std.atomic.Value(bool).init(false);
    var subscribe_err: ?anyerror = null;
    var subscribe_state = SubscribeState{
        .done = &subscribe_done,
        .err = &subscribe_err,
    };
    const subscribe_ctx: ?*anyopaque = @ptrCast(@constCast(&subscribe_state));
    node.router.subscribe(topic, subscribe_ctx, SubscribeCallback.callback);

    try waitForFlag(&subscribe_done, 5 * std.time.ns_per_s);
    try std.testing.expect(subscribe_err == null);

    const MessageListener = struct {
        allocator: std.mem.Allocator,
        done: *std.atomic.Value(bool),
        data: *?[]u8,
        source: *?PeerId,
        topic: []const u8,

        const Self = @This();

        pub fn handle(self: *Self, e: Event) void {
            switch (e) {
                .message => |msg| {
                    if (!std.mem.eql(u8, msg.message.topic, self.topic)) return;
                    if (self.data.*) |existing| {
                        self.allocator.free(existing);
                        self.data.* = null;
                    }
                    const copy = self.allocator.dupe(u8, msg.message.data) catch {
                        self.done.store(true, .release);
                        return;
                    };
                    self.data.* = copy;
                    self.source.* = msg.propagation_source;
                    self.done.store(true, .release);
                },
                else => {},
            }
        }

        pub fn vtableHandleFn(instance: *anyopaque, evt: Event) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.handle(evt);
        }

        pub const vtable = event.EventListenerVTable(Event){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) event.AnyEventListener(Event) {
            return .{
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    var message_done = std.atomic.Value(bool).init(false);
    var message_data: ?[]u8 = null;
    errdefer if (message_data) |data_copy| allocator.free(data_copy);
    var message_source: ?PeerId = null;
    var listener: MessageListener = .{
        .allocator = allocator,
        .done = &message_done,
        .data = &message_data,
        .source = &message_source,
        .topic = topic,
    };
    try node.router.event_emitter.addListener(.message, listener.any());
    errdefer _ = node.router.event_emitter.removeListener(.message, listener.any());

    const PublishState = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
        recipients: *?[]PeerId,
    };

    const PublishCallback = struct {
        fn callback(ctx: ?*anyopaque, res: anyerror![]PeerId) void {
            const state: *PublishState = @ptrCast(@alignCast(ctx.?));
            const peers = res catch |err| {
                std.log.err("publish callback error: {}", .{err});
                state.err.* = err;
                state.recipients.* = null;
                state.done.store(true, .release);
                return;
            };
            state.err.* = null;
            state.recipients.* = peers;
            state.done.store(true, .release);
        }
    };

    const payload = "hello from publish";

    var publish_done = std.atomic.Value(bool).init(false);
    var publish_err: ?anyerror = null;
    var publish_recipients: ?[]PeerId = null;
    errdefer if (publish_recipients) |recipients| allocator.free(recipients);
    var publish_state = PublishState{
        .done = &publish_done,
        .err = &publish_err,
        .recipients = &publish_recipients,
    };
    const publish_ctx: ?*anyopaque = @ptrCast(@constCast(&publish_state));

    node.router.publish(topic, payload, publish_ctx, PublishCallback.callback);

    try waitForFlag(&publish_done, 5 * std.time.ns_per_s);
    try std.testing.expect(publish_err == null);
    try std.testing.expect(publish_recipients != null);
    try std.testing.expectEqual(@as(usize, 1), publish_recipients.?.len);
    try std.testing.expect(publish_recipients.?[0].eql(&node.transport.local_peer_id));

    try waitForFlag(&message_done, 5 * std.time.ns_per_s);
    try std.testing.expect(message_source != null);
    try std.testing.expect(message_source.?.eql(&node.transport.local_peer_id));
    try std.testing.expect(message_data != null);
    try std.testing.expect(std.mem.eql(u8, message_data.?, payload));

    if (publish_recipients) |recipients| {
        allocator.free(recipients);
    }

    if (message_data) |data_copy| {
        allocator.free(data_copy);
    }

    const UnsubscribeState = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
    };

    const UnsubscribeCallback = struct {
        fn callback(ctx: ?*anyopaque, res: anyerror!void) void {
            const state: *UnsubscribeState = @ptrCast(@alignCast(ctx.?));
            if (res) |_| {
                state.err.* = null;
            } else |err| {
                state.err.* = err;
            }
            state.done.store(true, .release);
        }
    };

    var unsubscribe_done = std.atomic.Value(bool).init(false);
    var unsubscribe_err: ?anyerror = null;
    var unsubscribe_state = UnsubscribeState{
        .done = &unsubscribe_done,
        .err = &unsubscribe_err,
    };
    const unsubscribe_ctx: ?*anyopaque = @ptrCast(@constCast(&unsubscribe_state));
    node.router.unsubscribe(topic, unsubscribe_ctx, UnsubscribeCallback.callback);

    try waitForFlag(&unsubscribe_done, 5 * std.time.ns_per_s);
    try std.testing.expect(unsubscribe_err == null);

    _ = node.router.event_emitter.removeListener(.message, listener.any());
}

test "pubsub subscribe and publish between nodes" {
    const allocator = std.testing.allocator;

    // For now we only verify that two independent nodes can add each other successfully.
    // Once this is stable, we'll extend the test step-by-step to cover subscribe/publish flows again.
    const node_options: Gossipsub.Options = .{}; // defaults: StrictSign + signing

    var node_a: TestGossipsubNode = undefined;
    try node_a.init(allocator, 9967, node_options);
    defer node_a.deinit();

    var node_b: TestGossipsubNode = undefined;
    try node_b.init(allocator, 9968, node_options);
    defer node_b.deinit();

    const AddPeerState = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
    };

    const logAddPeer = struct {
        fn callback(ctx: ?*anyopaque, res: anyerror!void) void {
            const state: *AddPeerState = @ptrCast(@alignCast(ctx.?));
            if (res) |_| {
                std.debug.print("addPeer succeeded\n", .{});
                state.err.* = null;
            } else |err| {
                std.debug.print("addPeer failed: {any}\n", .{err});
                state.err.* = err;
            }
            state.done.store(true, .release);
        }
    };

    var add_peer_ab_done = std.atomic.Value(bool).init(false);
    var add_peer_ab_err: ?anyerror = null;
    var add_peer_ab_state = AddPeerState{ .done = &add_peer_ab_done, .err = &add_peer_ab_err };
    node_a.router.addPeer(node_b.dial_addr, @ptrCast(@constCast(&add_peer_ab_state)), logAddPeer.callback);

    var add_peer_ba_done = std.atomic.Value(bool).init(false);
    var add_peer_ba_err: ?anyerror = null;
    var add_peer_ba_state = AddPeerState{ .done = &add_peer_ba_done, .err = &add_peer_ba_err };
    node_b.router.addPeer(node_a.dial_addr, @ptrCast(@constCast(&add_peer_ba_state)), logAddPeer.callback);

    try waitForFlag(&add_peer_ab_done, 5 * std.time.ns_per_s);
    try waitForFlag(&add_peer_ba_done, 5 * std.time.ns_per_s);
    try std.testing.expect(add_peer_ab_err == null);
    try std.testing.expect(add_peer_ba_err == null);

    std.debug.print("peer counts: A = {d}, B = {d}\n", .{ node_a.router.peers.count(), node_b.router.peers.count() });
    try std.testing.expectEqual(@as(usize, 1), node_a.router.peers.count());
    try std.testing.expectEqual(@as(usize, 1), node_b.router.peers.count());

    try std.testing.expect(node_a.router.peers.get(node_b.transport.local_peer_id) != null);
    try std.testing.expect(node_b.router.peers.get(node_a.transport.local_peer_id) != null);

    const topic = "gossip-multi";

    const SubscribeState = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
    };

    const subscribeCallback = struct {
        fn callback(ctx: ?*anyopaque, res: anyerror!void) void {
            const state: *SubscribeState = @ptrCast(@alignCast(ctx.?));
            if (res) |_| {
                std.debug.print("subscribe succeeded\n", .{});
                state.err.* = null;
            } else |err| {
                std.debug.print("subscribe failed: {any}\n", .{err});
                state.err.* = err;
            }
            state.done.store(true, .release);
        }
    };

    var subscribe_a_done = std.atomic.Value(bool).init(false);
    var subscribe_a_err: ?anyerror = null;
    var subscribe_a_state = SubscribeState{ .done = &subscribe_a_done, .err = &subscribe_a_err };
    node_a.router.subscribe(topic, @ptrCast(@constCast(&subscribe_a_state)), subscribeCallback.callback);

    var subscribe_b_done = std.atomic.Value(bool).init(false);
    var subscribe_b_err: ?anyerror = null;
    var subscribe_b_state = SubscribeState{ .done = &subscribe_b_done, .err = &subscribe_b_err };
    node_b.router.subscribe(topic, @ptrCast(@constCast(&subscribe_b_state)), subscribeCallback.callback);

    try waitForFlag(&subscribe_a_done, 5 * std.time.ns_per_s);
    try waitForFlag(&subscribe_b_done, 5 * std.time.ns_per_s);
    try std.testing.expect(subscribe_a_err == null);
    try std.testing.expect(subscribe_b_err == null);

    std.time.sleep(200 * std.time.us_per_ms);
    try std.testing.expect(node_a.router.subscriptions.contains(topic));
    try std.testing.expect(node_b.router.subscriptions.contains(topic));

    std.debug.print("subscriptions: A = {d}, B = {d}\n", .{ node_a.router.subscriptions.count(), node_b.router.subscriptions.count() });

    const wait_ns = 5 * std.time.ns_per_s;
    try waitForTopicPeer(&node_a.router, topic, node_b.transport.local_peer_id, wait_ns);
    try waitForTopicPeer(&node_b.router, topic, node_a.transport.local_peer_id, wait_ns);
    try waitForMeshTopic(&node_a.router, topic, wait_ns);
    try waitForMeshTopic(&node_b.router, topic, wait_ns);
    try waitForUsableStream(&node_a.router, node_b.transport.local_peer_id, wait_ns);
    try waitForUsableStream(&node_b.router, node_a.transport.local_peer_id, wait_ns);

    const MessageListener = struct {
        allocator: std.mem.Allocator,
        done: *std.atomic.Value(bool),
        data: *?[]u8,
        source: *?PeerId,
        topic: []const u8,

        const Self = @This();

        pub fn handle(self: *Self, e: Event) void {
            switch (e) {
                .message => |msg| {
                    if (!std.mem.eql(u8, msg.message.topic, self.topic)) return;
                    if (self.data.*) |existing| {
                        self.allocator.free(existing);
                        self.data.* = null;
                    }
                    const copy = self.allocator.dupe(u8, msg.message.data) catch {
                        self.done.store(true, .release);
                        return;
                    };
                    self.data.* = copy;
                    self.source.* = msg.propagation_source;
                    self.done.store(true, .release);
                },
                else => {},
            }
        }

        pub fn vtableHandleFn(instance: *anyopaque, evt: Event) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.handle(evt);
        }

        pub const vtable = event.EventListenerVTable(Event){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) event.AnyEventListener(Event) {
            return .{
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    var message_b_done = std.atomic.Value(bool).init(false);
    var message_b_data: ?[]u8 = null;
    errdefer if (message_b_data) |data_copy| allocator.free(data_copy);
    var message_b_source: ?PeerId = null;
    var listener_b: MessageListener = .{
        .allocator = allocator,
        .done = &message_b_done,
        .data = &message_b_data,
        .source = &message_b_source,
        .topic = topic,
    };
    try node_b.router.event_emitter.addListener(.message, listener_b.any());
    defer _ = node_b.router.event_emitter.removeListener(.message, listener_b.any());

    var message_a_done = std.atomic.Value(bool).init(false);
    var message_a_data: ?[]u8 = null;
    errdefer if (message_a_data) |data_copy| allocator.free(data_copy);
    var message_a_source: ?PeerId = null;
    var listener_a: MessageListener = .{
        .allocator = allocator,
        .done = &message_a_done,
        .data = &message_a_data,
        .source = &message_a_source,
        .topic = topic,
    };
    try node_a.router.event_emitter.addListener(.message, listener_a.any());
    defer _ = node_a.router.event_emitter.removeListener(.message, listener_a.any());

    const PublishState = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
        recipients: *?[]PeerId,
    };

    const PublishCallback = struct {
        fn callback(ctx: ?*anyopaque, res: anyerror![]PeerId) void {
            const state: *PublishState = @ptrCast(@alignCast(ctx.?));
            const peers = res catch |err| {
                state.err.* = err;
                state.recipients.* = null;
                state.done.store(true, .release);
                return;
            };
            state.err.* = null;
            state.recipients.* = peers;
            state.done.store(true, .release);
        }
    };

    const payload = "hello-from-node-a";

    var publish_done = std.atomic.Value(bool).init(false);
    var publish_err: ?anyerror = null;
    var publish_recipients: ?[]PeerId = null;
    errdefer if (publish_recipients) |recipients| allocator.free(recipients);
    var publish_state = PublishState{
        .done = &publish_done,
        .err = &publish_err,
        .recipients = &publish_recipients,
    };
    const publish_ctx: ?*anyopaque = @ptrCast(@constCast(&publish_state));

    node_a.router.publish(topic, payload, publish_ctx, PublishCallback.callback);

    try waitForFlag(&publish_done, 5 * std.time.ns_per_s);
    std.debug.print("publish done on node A {?}\n", .{publish_err});
    try std.testing.expect(publish_err == null);
    try std.testing.expect(publish_recipients != null);
    try std.testing.expectEqual(@as(usize, 1), publish_recipients.?.len);
    try std.testing.expect(publish_recipients.?[0].eql(&node_b.transport.local_peer_id));

    try waitForFlag(&message_b_done, 5 * std.time.ns_per_s);
    try std.testing.expect(message_b_source != null);
    try std.testing.expect(message_b_source.?.eql(&node_a.transport.local_peer_id));
    try std.testing.expect(message_b_data != null);
    try std.testing.expect(std.mem.eql(u8, message_b_data.?, payload));

    if (publish_recipients) |recipients| {
        allocator.free(recipients);
        publish_recipients = null;
    }

    if (message_b_data) |data_copy| {
        allocator.free(data_copy);
        message_b_data = null;
    }

    const payload_back = "hello-from-node-b";

    var publish_back_done = std.atomic.Value(bool).init(false);
    var publish_back_err: ?anyerror = null;
    var publish_back_recipients: ?[]PeerId = null;
    errdefer if (publish_back_recipients) |recipients| allocator.free(recipients);
    var publish_back_state = PublishState{
        .done = &publish_back_done,
        .err = &publish_back_err,
        .recipients = &publish_back_recipients,
    };
    const publish_back_ctx: ?*anyopaque = @ptrCast(@constCast(&publish_back_state));

    node_b.router.publish(topic, payload_back, publish_back_ctx, PublishCallback.callback);

    try waitForFlag(&publish_back_done, 5 * std.time.ns_per_s);
    try std.testing.expect(publish_back_err == null);
    try std.testing.expect(publish_back_recipients != null);
    try std.testing.expectEqual(@as(usize, 1), publish_back_recipients.?.len);
    try std.testing.expect(publish_back_recipients.?[0].eql(&node_a.transport.local_peer_id));

    try waitForFlag(&message_a_done, 5 * std.time.ns_per_s);
    try std.testing.expect(message_a_source != null);
    try std.testing.expect(message_a_source.?.eql(&node_b.transport.local_peer_id));
    try std.testing.expect(message_a_data != null);
    try std.testing.expect(std.mem.eql(u8, message_a_data.?, payload_back));

    if (publish_back_recipients) |recipients| {
        allocator.free(recipients);
    }

    if (message_a_data) |data_copy| {
        allocator.free(data_copy);
        message_a_data = null;
    }

    const RemovePeerState = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
    };

    const removePeerCallback = struct {
        fn callback(ctx: ?*anyopaque, res: anyerror!void) void {
            const state: *RemovePeerState = @ptrCast(@alignCast(ctx.?));
            if (res) |_| {
                std.debug.print("removePeer succeeded\n", .{});
                state.err.* = null;
            } else |err| {
                std.debug.print("removePeer failed: {any}\n", .{err});
                state.err.* = err;
            }
            state.done.store(true, .release);
        }
    };

    var remove_peer_ab_done = std.atomic.Value(bool).init(false);
    var remove_peer_ab_err: ?anyerror = null;
    var remove_peer_ab_state = RemovePeerState{ .done = &remove_peer_ab_done, .err = &remove_peer_ab_err };
    node_a.router.removePeer(node_b.transport.local_peer_id, @ptrCast(@constCast(&remove_peer_ab_state)), removePeerCallback.callback);

    var remove_peer_ba_done = std.atomic.Value(bool).init(false);
    var remove_peer_ba_err: ?anyerror = null;
    var remove_peer_ba_state = RemovePeerState{ .done = &remove_peer_ba_done, .err = &remove_peer_ba_err };
    node_b.router.removePeer(node_a.transport.local_peer_id, @ptrCast(@constCast(&remove_peer_ba_state)), removePeerCallback.callback);

    try waitForFlag(&remove_peer_ab_done, 5 * std.time.ns_per_s);
    try waitForFlag(&remove_peer_ba_done, 5 * std.time.ns_per_s);
    try std.testing.expect(remove_peer_ab_err == null);
    try std.testing.expect(remove_peer_ba_err == null);

    std.time.sleep(200 * std.time.us_per_ms);

    try std.testing.expectEqual(@as(usize, 0), node_a.router.peers.count());
    try std.testing.expectEqual(@as(usize, 0), node_b.router.peers.count());
}

test "pubsub multi-topic mesh and publish" {
    const allocator = std.testing.allocator;

    const node_options: Gossipsub.Options = .{};

    var node_a: TestGossipsubNode = undefined;
    try node_a.init(allocator, 10067, node_options);
    defer node_a.deinit();

    var node_b: TestGossipsubNode = undefined;
    try node_b.init(allocator, 10068, node_options);
    defer node_b.deinit();

    const AddPeerState = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
    };

    const addPeerCallback = struct {
        fn callback(ctx: ?*anyopaque, res: anyerror!void) void {
            const state: *AddPeerState = @ptrCast(@alignCast(ctx.?));
            if (res) |_| {
                state.err.* = null;
            } else |err| {
                state.err.* = err;
            }
            state.done.store(true, .release);
        }
    };

    var add_peer_ab_done = std.atomic.Value(bool).init(false);
    var add_peer_ab_err: ?anyerror = null;
    var add_peer_ab_state = AddPeerState{ .done = &add_peer_ab_done, .err = &add_peer_ab_err };
    node_a.router.addPeer(node_b.dial_addr, @ptrCast(@constCast(&add_peer_ab_state)), addPeerCallback.callback);

    var add_peer_ba_done = std.atomic.Value(bool).init(false);
    var add_peer_ba_err: ?anyerror = null;
    var add_peer_ba_state = AddPeerState{ .done = &add_peer_ba_done, .err = &add_peer_ba_err };
    node_b.router.addPeer(node_a.dial_addr, @ptrCast(@constCast(&add_peer_ba_state)), addPeerCallback.callback);

    try waitForFlag(&add_peer_ab_done, 5 * std.time.ns_per_s);
    try waitForFlag(&add_peer_ba_done, 5 * std.time.ns_per_s);
    try std.testing.expect(add_peer_ab_err == null);
    try std.testing.expect(add_peer_ba_err == null);

    try std.testing.expectEqual(@as(usize, 1), node_a.router.peers.count());
    try std.testing.expectEqual(@as(usize, 1), node_b.router.peers.count());

    const topics = [_][]const u8{ "topic-alpha", "topic-beta" };

    const SubscribeState = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
    };

    const subscribeCallback = struct {
        fn callback(ctx: ?*anyopaque, res: anyerror!void) void {
            const state: *SubscribeState = @ptrCast(@alignCast(ctx.?));
            if (res) |_| {
                state.err.* = null;
            } else |err| {
                state.err.* = err;
            }
            state.done.store(true, .release);
        }
    };

    for (topics) |topic| {
        var subscribe_a_done = std.atomic.Value(bool).init(false);
        var subscribe_a_err: ?anyerror = null;
        var subscribe_a_state = SubscribeState{ .done = &subscribe_a_done, .err = &subscribe_a_err };
        node_a.router.subscribe(topic, @ptrCast(@constCast(&subscribe_a_state)), subscribeCallback.callback);

        var subscribe_b_done = std.atomic.Value(bool).init(false);
        var subscribe_b_err: ?anyerror = null;
        var subscribe_b_state = SubscribeState{ .done = &subscribe_b_done, .err = &subscribe_b_err };
        node_b.router.subscribe(topic, @ptrCast(@constCast(&subscribe_b_state)), subscribeCallback.callback);

        try waitForFlag(&subscribe_a_done, 5 * std.time.ns_per_s);
        try waitForFlag(&subscribe_b_done, 5 * std.time.ns_per_s);
        try std.testing.expect(subscribe_a_err == null);
        try std.testing.expect(subscribe_b_err == null);
    }

    std.time.sleep(400 * std.time.us_per_ms);

    for (topics) |topic| {
        try waitForTopicPeer(&node_a.router, topic, node_b.transport.local_peer_id, 5 * std.time.ns_per_s);
        try waitForTopicPeer(&node_b.router, topic, node_a.transport.local_peer_id, 5 * std.time.ns_per_s);

        try waitForMeshTopic(&node_a.router, topic, 5 * std.time.ns_per_s);
        try waitForMeshTopic(&node_b.router, topic, 5 * std.time.ns_per_s);

        try std.testing.expect(node_a.router.mesh.contains(topic));
        try std.testing.expect(node_b.router.mesh.contains(topic));
        try std.testing.expect(node_a.router.subscriptions.contains(topic));
        try std.testing.expect(node_b.router.subscriptions.contains(topic));
    }

    const MessageListener = struct {
        allocator: std.mem.Allocator,
        topic: []const u8,
        done: *std.atomic.Value(bool),
        data: *?[]u8,
        source: *?PeerId,

        const Self = @This();

        pub fn handle(self: *Self, e: Event) void {
            switch (e) {
                .message => |msg| {
                    if (!std.mem.eql(u8, msg.message.topic, self.topic)) return;
                    if (self.data.*) |existing| {
                        self.allocator.free(existing);
                        self.data.* = null;
                    }
                    const copy = self.allocator.dupe(u8, msg.message.data) catch {
                        self.done.store(true, .release);
                        return;
                    };
                    self.data.* = copy;
                    self.source.* = msg.propagation_source;
                    self.done.store(true, .release);
                },
                else => {},
            }
        }

        pub fn vtableHandleFn(instance: *anyopaque, evt: Event) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.handle(evt);
        }

        pub const vtable = event.EventListenerVTable(Event){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) event.AnyEventListener(Event) {
            return .{
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    var message_b_done_alpha = std.atomic.Value(bool).init(false);
    var message_b_data_alpha: ?[]u8 = null;
    errdefer if (message_b_data_alpha) |data_copy| allocator.free(data_copy);
    var message_b_source_alpha: ?PeerId = null;
    var listener_b_alpha: MessageListener = .{
        .allocator = allocator,
        .topic = topics[0],
        .done = &message_b_done_alpha,
        .data = &message_b_data_alpha,
        .source = &message_b_source_alpha,
    };
    try node_b.router.event_emitter.addListener(.message, listener_b_alpha.any());
    defer _ = node_b.router.event_emitter.removeListener(.message, listener_b_alpha.any());

    var message_b_done_beta = std.atomic.Value(bool).init(false);
    var message_b_data_beta: ?[]u8 = null;
    errdefer if (message_b_data_beta) |data_copy| allocator.free(data_copy);
    var message_b_source_beta: ?PeerId = null;
    var listener_b_beta: MessageListener = .{
        .allocator = allocator,
        .topic = topics[1],
        .done = &message_b_done_beta,
        .data = &message_b_data_beta,
        .source = &message_b_source_beta,
    };
    try node_b.router.event_emitter.addListener(.message, listener_b_beta.any());
    defer _ = node_b.router.event_emitter.removeListener(.message, listener_b_beta.any());

    var message_a_done_alpha = std.atomic.Value(bool).init(false);
    var message_a_data_alpha: ?[]u8 = null;
    errdefer if (message_a_data_alpha) |data_copy| allocator.free(data_copy);
    var message_a_source_alpha: ?PeerId = null;
    var listener_a_alpha: MessageListener = .{
        .allocator = allocator,
        .topic = topics[0],
        .done = &message_a_done_alpha,
        .data = &message_a_data_alpha,
        .source = &message_a_source_alpha,
    };
    try node_a.router.event_emitter.addListener(.message, listener_a_alpha.any());
    defer _ = node_a.router.event_emitter.removeListener(.message, listener_a_alpha.any());

    var message_a_done_beta = std.atomic.Value(bool).init(false);
    var message_a_data_beta: ?[]u8 = null;
    errdefer if (message_a_data_beta) |data_copy| allocator.free(data_copy);
    var message_a_source_beta: ?PeerId = null;
    var listener_a_beta: MessageListener = .{
        .allocator = allocator,
        .topic = topics[1],
        .done = &message_a_done_beta,
        .data = &message_a_data_beta,
        .source = &message_a_source_beta,
    };
    try node_a.router.event_emitter.addListener(.message, listener_a_beta.any());
    defer _ = node_a.router.event_emitter.removeListener(.message, listener_a_beta.any());

    const PublishState = struct {
        done: *std.atomic.Value(bool),
        err: *?anyerror,
        recipients: *?[]PeerId,
    };

    const publishCallback = struct {
        fn callback(ctx: ?*anyopaque, res: anyerror![]PeerId) void {
            const state: *PublishState = @ptrCast(@alignCast(ctx.?));
            const peers = res catch |err| {
                state.err.* = err;
                state.recipients.* = null;
                state.done.store(true, .release);
                return;
            };
            state.err.* = null;
            state.recipients.* = peers;
            state.done.store(true, .release);
        }
    };

    const payload_alpha = "msg-from-alpha";
    const payload_beta = "msg-from-beta";

    var publish_alpha_done = std.atomic.Value(bool).init(false);
    var publish_alpha_err: ?anyerror = null;
    var publish_alpha_recipients: ?[]PeerId = null;
    errdefer if (publish_alpha_recipients) |recipients| allocator.free(recipients);
    var publish_alpha_state = PublishState{
        .done = &publish_alpha_done,
        .err = &publish_alpha_err,
        .recipients = &publish_alpha_recipients,
    };
    const publish_alpha_ctx: ?*anyopaque = @ptrCast(@constCast(&publish_alpha_state));

    node_a.router.publish(topics[0], payload_alpha, publish_alpha_ctx, publishCallback.callback);

    try waitForFlag(&publish_alpha_done, 5 * std.time.ns_per_s);
    try std.testing.expect(publish_alpha_err == null);
    try std.testing.expect(publish_alpha_recipients != null);
    try std.testing.expectEqual(@as(usize, 1), publish_alpha_recipients.?.len);
    try std.testing.expect(publish_alpha_recipients.?[0].eql(&node_b.transport.local_peer_id));

    try waitForFlag(&message_b_done_alpha, 5 * std.time.ns_per_s);
    try std.testing.expect(message_b_source_alpha != null);
    try std.testing.expect(message_b_source_alpha.?.eql(&node_a.transport.local_peer_id));
    try std.testing.expect(message_b_data_alpha != null);
    try std.testing.expect(std.mem.eql(u8, message_b_data_alpha.?, payload_alpha));
    try std.testing.expect(message_b_data_beta == null);

    if (publish_alpha_recipients) |recipients| {
        allocator.free(recipients);
        publish_alpha_recipients = null;
    }

    if (message_b_data_alpha) |data_copy| {
        allocator.free(data_copy);
        message_b_data_alpha = null;
    }

    var publish_beta_done = std.atomic.Value(bool).init(false);
    var publish_beta_err: ?anyerror = null;
    var publish_beta_recipients: ?[]PeerId = null;
    errdefer if (publish_beta_recipients) |recipients| allocator.free(recipients);
    var publish_beta_state = PublishState{
        .done = &publish_beta_done,
        .err = &publish_beta_err,
        .recipients = &publish_beta_recipients,
    };
    const publish_beta_ctx: ?*anyopaque = @ptrCast(@constCast(&publish_beta_state));

    node_b.router.publish(topics[1], payload_beta, publish_beta_ctx, publishCallback.callback);

    try waitForFlag(&publish_beta_done, 5 * std.time.ns_per_s);
    try std.testing.expect(publish_beta_err == null);
    try std.testing.expect(publish_beta_recipients != null);
    try std.testing.expectEqual(@as(usize, 1), publish_beta_recipients.?.len);
    try std.testing.expect(publish_beta_recipients.?[0].eql(&node_a.transport.local_peer_id));

    try waitForFlag(&message_a_done_beta, 5 * std.time.ns_per_s);
    try std.testing.expect(message_a_source_beta != null);
    try std.testing.expect(message_a_source_beta.?.eql(&node_b.transport.local_peer_id));
    try std.testing.expect(message_a_data_beta != null);
    try std.testing.expect(std.mem.eql(u8, message_a_data_beta.?, payload_beta));
    try std.testing.expect(message_a_data_alpha == null);

    if (publish_beta_recipients) |recipients| {
        allocator.free(recipients);
    }

    if (message_a_data_beta) |data_copy| {
        allocator.free(data_copy);
        message_a_data_beta = null;
    }

    if (message_b_data_beta) |data_copy| {
        allocator.free(data_copy);
    }

    if (message_a_data_alpha) |data_copy| {
        allocator.free(data_copy);
        message_a_data_alpha = null;
    }
}

test "pubsub three-node propagation" {
    const allocator = std.testing.allocator;

    var node_a: TestGossipsubNode = undefined;
    try node_a.init(allocator, 12067, .{});
    defer node_a.deinit();

    var node_b: TestGossipsubNode = undefined;
    try node_b.init(allocator, 12068, .{});
    defer node_b.deinit();

    var node_c: TestGossipsubNode = undefined;
    try node_c.init(allocator, 12069, .{});
    defer node_c.deinit();

    try addPeerSync(&node_a.router, node_b.dial_addr);
    try addPeerSync(&node_b.router, node_a.dial_addr);
    try addPeerSync(&node_a.router, node_c.dial_addr);
    try addPeerSync(&node_c.router, node_a.dial_addr);
    try addPeerSync(&node_b.router, node_c.dial_addr);
    try addPeerSync(&node_c.router, node_b.dial_addr);

    const wait_ns = 5 * std.time.ns_per_s;
    try waitForUsableStream(&node_a.router, node_b.transport.local_peer_id, wait_ns);
    try waitForUsableStream(&node_a.router, node_c.transport.local_peer_id, wait_ns);
    try waitForUsableStream(&node_b.router, node_a.transport.local_peer_id, wait_ns);
    try waitForUsableStream(&node_b.router, node_c.transport.local_peer_id, wait_ns);
    try waitForUsableStream(&node_c.router, node_a.transport.local_peer_id, wait_ns);
    try waitForUsableStream(&node_c.router, node_b.transport.local_peer_id, wait_ns);

    const topic = "three-node";
    try subscribeSync(&node_a.router, topic);
    try subscribeSync(&node_b.router, topic);
    try subscribeSync(&node_c.router, topic);

    try waitForTopicPeer(&node_a.router, topic, node_b.transport.local_peer_id, wait_ns);
    try waitForTopicPeer(&node_a.router, topic, node_c.transport.local_peer_id, wait_ns);
    try waitForTopicPeer(&node_b.router, topic, node_a.transport.local_peer_id, wait_ns);
    try waitForTopicPeer(&node_b.router, topic, node_c.transport.local_peer_id, wait_ns);
    try waitForTopicPeer(&node_c.router, topic, node_a.transport.local_peer_id, wait_ns);
    try waitForTopicPeer(&node_c.router, topic, node_b.transport.local_peer_id, wait_ns);

    try waitForMeshTopic(&node_a.router, topic, wait_ns);
    try waitForMeshTopic(&node_b.router, topic, wait_ns);
    try waitForMeshTopic(&node_c.router, topic, wait_ns);

    const MessageListener = struct {
        allocator: Allocator,
        expected_topic: []const u8,
        done: *std.atomic.Value(bool),
        data: *?[]u8,
        source: *?PeerId,

        const Self = @This();

        pub fn handle(self: *Self, e: Event) void {
            switch (e) {
                .message => |msg| {
                    if (!std.mem.eql(u8, msg.message.topic, self.expected_topic)) return;
                    if (self.data.*) |existing| {
                        self.allocator.free(existing);
                        self.data.* = null;
                    }
                    const copy = self.allocator.dupe(u8, msg.message.data) catch {
                        self.done.store(true, .release);
                        return;
                    };
                    self.data.* = copy;
                    self.source.* = msg.propagation_source;
                    self.done.store(true, .release);
                },
                else => {},
            }
        }

        pub fn vtableHandleFn(instance: *anyopaque, evt: Event) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.handle(evt);
        }

        pub const vtable = event.EventListenerVTable(Event){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) event.AnyEventListener(Event) {
            return .{
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    var message_b_done = std.atomic.Value(bool).init(false);
    var message_b_data: ?[]u8 = null;
    defer if (message_b_data) |data_copy| allocator.free(data_copy);
    var message_b_source: ?PeerId = null;
    var listener_b: MessageListener = .{
        .allocator = allocator,
        .expected_topic = topic,
        .done = &message_b_done,
        .data = &message_b_data,
        .source = &message_b_source,
    };
    try node_b.router.event_emitter.addListener(.message, listener_b.any());
    defer _ = node_b.router.event_emitter.removeListener(.message, listener_b.any());

    var message_c_done = std.atomic.Value(bool).init(false);
    var message_c_data: ?[]u8 = null;
    defer if (message_c_data) |data_copy| allocator.free(data_copy);
    var message_c_source: ?PeerId = null;
    var listener_c: MessageListener = .{
        .allocator = allocator,
        .expected_topic = topic,
        .done = &message_c_done,
        .data = &message_c_data,
        .source = &message_c_source,
    };
    try node_c.router.event_emitter.addListener(.message, listener_c.any());
    defer _ = node_c.router.event_emitter.removeListener(.message, listener_c.any());

    const payload = "three-node-message";
    const recipients = try publishSync(&node_a.router, topic, payload, allocator);
    defer allocator.free(recipients);

    try std.testing.expectEqual(@as(usize, 2), recipients.len);
    var saw_b = false;
    var saw_c = false;
    for (recipients) |peer| {
        if (peer.eql(&node_b.transport.local_peer_id)) saw_b = true;
        if (peer.eql(&node_c.transport.local_peer_id)) saw_c = true;
    }
    try std.testing.expect(saw_b);
    try std.testing.expect(saw_c);

    try waitForFlag(&message_b_done, wait_ns);
    try std.testing.expect(message_b_source != null);
    try std.testing.expect(message_b_source.?.eql(&node_a.transport.local_peer_id));
    try std.testing.expect(message_b_data != null);
    try std.testing.expect(std.mem.eql(u8, message_b_data.?, payload));

    try waitForFlag(&message_c_done, wait_ns);
    try std.testing.expect(message_c_source != null);
    try std.testing.expect(message_c_source.?.eql(&node_a.transport.local_peer_id));
    try std.testing.expect(message_c_data != null);
    try std.testing.expect(std.mem.eql(u8, message_c_data.?, payload));
}
