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
const tls = libp2p.security.tls;
const keys = @import("peer_id").keys;
const ssl = @import("ssl");
const swarm = libp2p.swarm;
const rpc = libp2p.protobuf.rpc;
const uvarint = multiformats.uvarint;
const event = libp2p.event;
const pubsub = @import("../pubsub.zig");
const cache = @import("cache");

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
    inboundTransformFn: *const fn (instance: *anyopaque, topic: []const u8, data: []const u8) anyerror![]const u8,
    outboundTransformFn: *const fn (instance: *anyopaque, topic: []const u8, data: []const u8) anyerror![]const u8,
};

pub const AnyDataTransform = struct {
    instance: *anyopaque,
    vtable: *const DataTransformVTable,

    const Self = @This();

    pub fn inboundTransform(self: Self, topic: []const u8, data: []const u8) anyerror![]const u8 {
        return self.vtable.inboundTransformFn(self.instance, topic, data);
    }

    pub fn outboundTransform(self: Self, topic: []const u8, data: []const u8) anyerror![]const u8 {
        return self.vtable.outboundTransformFn(self.instance, topic, data);
    }
};

pub const Subscription = struct {
    topic: []const u8,
    subscribe: bool,
};

pub const Message = struct {
    from: ?PeerId,
    data: []const u8,
    seqno: ?u64,
    topic_hash: []const u8,
    signature: ?[]const u8,
    key: ?keys.PublicKey,
    validated: bool,
};

pub const MessageStatus = enum {
    Valid,
    Invalid,
    Duplicate,
};

pub const MessageValidationResult = union(enum) {
    valid: struct {
        message_id: []const u8,
        message: Message,
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
        }
    }
};

/// Gossipsub is a PubSub router implementation that follows the Gossipsub protocol.
/// It maintains a list of peers and topics, and handles incoming and outgoing messages.
/// It uses the Semiduplex struct to manage bidirectional communication channels.
/// It also provides methods to add and remove peers, and to handle incoming RPC messages.
///
pub const Gossipsub = struct {
    peers: std.AutoHashMap(PeerId, Semiduplex),

    swarm: *Switch,

    peer: Multiaddr,

    peer_id: PeerId,

    allocator: Allocator,

    topics: std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)),

    subscriptions: std.StringHashMapUnmanaged(void),

    mesh: std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)),

    peer_have: std.AutoHashMapUnmanaged(PeerId, usize),

    control: std.AutoHashMapUnmanaged(PeerId, rpc.ControlMessageReader),

    iasked: std.AutoArrayHashMapUnmanaged(PeerId, usize),

    event_emitter: event.EventEmitter(Event),

    seen_cache: cache.Cache(void),

    mcache: mcache.MessageCache,

    opts: Options,

    protocols: []const ProtocolId = &.{ v1_id, v1_1_id },

    const Options = struct {
        seen_ttl_s: u32 = 120,
        max_messages_per_rpc: ?usize = null,
        global_signature_policy: SignaturePolicy = .StrictSign,
        data_transform: ?AnyDataTransform = null,
        msg_id_fn: pubsub.MessageIdFn = pubsub.defaultMsgId,
        idontwant_min_size: usize = 512,
        idontwant_message_size_threshold: usize = 1024,
        max_ihave_messages: usize = 10,
        max_ihave_len: usize = 5000,
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

            if (self.semiduplex) |semi_duplex| {
                semi_duplex.initiator = stream_initiator;
            } else {
                const peer_id = stream_initiator.stream.conn.security_session.?.remote_id;

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

    pub fn init(self: *Self, allocator: Allocator, peer: Multiaddr, peer_id: PeerId, network_swarm: *Switch, opts: Options) !void {
        self.* = .{
            .allocator = allocator,
            .peer = peer,
            .peer_id = peer_id,
            .swarm = network_swarm,
            .peers = std.AutoHashMap(PeerId, Semiduplex).init(allocator),
            .topics = std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)).empty,
            .subscriptions = std.StringHashMapUnmanaged(void).empty,
            .mesh = std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)).empty,
            .peer_have = std.AutoHashMapUnmanaged(PeerId, usize).empty,
            .control = std.AutoHashMapUnmanaged(PeerId, rpc.ControlMessageReader).empty,
            .iasked = std.AutoArrayHashMapUnmanaged(PeerId, usize).empty,
            .event_emitter = event.EventEmitter(Event).init(allocator),
            .seen_cache = try cache.Cache(void).init(allocator, .{}),
            .mcache = try mcache.MessageCache.init(allocator, 128, 1024, pubsub.defaultMsgId),
            .opts = opts,
        };
    }

    pub fn deinit(self: *Self) void {
        self.peers.deinit();

        var topic_iter = self.topics.iterator();
        while (topic_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.topics.deinit(self.allocator);

        var my_topic_iter = self.subscriptions.iterator();
        while (my_topic_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.subscriptions.deinit(self.allocator);
        var mesh_iter = self.mesh.iterator();
        while (mesh_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.mesh.deinit(self.allocator);
        self.peer_have.deinit(self.allocator);
        var control_iter = self.control.iterator();
        while (control_iter.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.control.deinit(self.allocator);
        self.iasked.deinit(self.allocator);
        self.event_emitter.deinit();
        self.seen_cache.deinit();
        self.mcache.deinit();
    }

    pub fn acceptFrom(self: *Self, peer_id: PeerId) bool {
        // TODO: Implement peer acceptance logic
        _ = self;
        _ = peer_id;
        return true;
    }

    pub fn handleRPC(self: *Self, rpc_message: *const pubsub.RPC) !void {
        if (!self.acceptFrom(rpc_message.from)) {
            std.log.warn("Rejected RPC message from peer: {}", .{rpc_message.from});
            return;
        }

        const subs = try rpc_message.rpc_reader.getSubscriptions(self.allocator);
        defer self.allocator.free(subs);

        //TODO:Implement subscription filtering

        var subscriptions = std.ArrayListUnmanaged(Subscription).empty;
        defer subscriptions.deinit(self.allocator);
        var processed_subs: usize = 0;
        errdefer {
            for (subs[processed_subs..]) |*rem| rem.deinit();
        }
        for (subs) |sub| {
            defer {
                sub.deinit();
                processed_subs += 1;
            }

            const topic_id = sub.getTopicid();
            const subscribe = sub.getSubscribe();

            try self.handleSubscription(rpc_message.from, topic_id, subscribe);

            try subscriptions.append(self.allocator, .{ .topic = topic_id, .subscribe = subscribe });
        }

        self.event_emitter.emit(.{ .subscription_changed = .{
            .peer = rpc_message.from,
            .subscriptions = subscriptions.items,
        } });

        // Handle publish
        const msgs = try rpc_message.rpc_reader.getPublish(self.allocator);
        defer self.allocator.free(msgs);

        if (self.opts.max_messages_per_rpc) |max| if (msgs.len > max) {
            std.log.warn("Received {} messages, exceeding limit of {}", .{ msgs.len, max });
            return;
        };

        var processed_msgs: usize = 0;
        errdefer {
            for (msgs[processed_msgs..]) |*rem| rem.deinit();
        }
        for (msgs) |*msg| {
            defer {
                msg.deinit();
                processed_msgs += 1;
            }

            try self.handleMessage(rpc_message.from, msg);
        }

        const control = try rpc_message.rpc_reader.getControl(self.allocator);
        defer control.deinit();
        if (control.buf.bytes().len > 0) {
            try self.handleControl(&control, rpc_message.from);
        }
    }

    pub fn removePeer(self: *Self, peer: PeerId, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        if (self.swarm.transport.io_event_loop.inEventLoopThread()) {
            self.doRemovePeer(peer, callback_ctx, callback);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .pubsub_remove_peer = .{
                    .pubsub = self.any(),
                    .peer = peer,
                    .callback_ctx = callback_ctx,
                    .callback = callback,
                } },
            };
            self.swarm.transport.io_event_loop.queueMessage(message) catch unreachable;
        }
    }

    pub fn addPeer(self: *Self, peer: Multiaddr, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        if (self.swarm.transport.io_event_loop.inEventLoopThread()) {
            self.doAddPeer(peer, callback_ctx, callback);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .pubsub_add_peer = .{
                    .pubsub = self.any(),
                    .peer = peer,
                    .callback_ctx = callback_ctx,
                    .callback = callback,
                } },
            };

            self.swarm.transport.io_event_loop.queueMessage(message) catch unreachable;
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

    fn handleSubscription(self: *Self, from: PeerId, topic: []const u8, subscribe: bool) !void {
        if (subscribe) {
            if (!self.topics.contains(topic)) {
                const copied_topic_id = try self.allocator.dupe(u8, topic);
                errdefer self.allocator.free(copied_topic_id);
                const tmap = std.AutoHashMapUnmanaged(PeerId, void).empty;
                try self.topics.put(self.allocator, copied_topic_id, tmap);
            }

            const pgop = try self.topics.getPtr(topic).?.getOrPut(self.allocator, from);
            if (!pgop.found_existing) {
                pgop.value_ptr.* = {};
            }
        } else {
            if (!self.topics.contains(topic)) {
                return;
            } else {
                if (self.topics.getPtr(topic)) |peer_set| {
                    _ = peer_set.remove(from);
                }
            }
        }
    }

    fn validateReceivedMessage(self: *Self, msg: *const rpc.MessageReader, propagation_source: PeerId) !MessageValidationResult {
        var m: Message = undefined;
        validateMessage(self.allocator, msg, self.opts.global_signature_policy, &m) catch |err| {
            return MessageValidationResult{
                .invalid = .{
                    .reason = RejectReason.Error,
                    .err = err,
                },
            };
        };

        if (self.opts.data_transform) |dt| {
            const transformed_data = dt.inboundTransform(m.topic_hash, m.data) catch |err| {
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

        const msg_for_id = rpc.Message{
            .from = msg._from,
            .data = m.data,
            .seqno = msg._seqno,
            .topic = msg._topic,
            .signature = msg._signature,
            .key = msg._key,
        };
        // Generate message ID using the provided function and the copied message with transformed data
        const message_id = self.opts.msg_id_fn(self.allocator, &msg_for_id) catch |err| {
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
            try self.seen_cache.put(message_id, {}, .{
                .ttl = self.opts.seen_ttl_s,
            });
        }

        // Here use the origin message's data length to determine if we should send IDontWant
        if (msg._data) |data| {
            if (data.len >= self.opts.idontwant_message_size_threshold) {
                try self.sendIDontWants(m.topic_hash, propagation_source, message_id);
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
                // Here we would normally send the IDontWant message to the peer.
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

    fn handleMessage(self: *Self, from: PeerId, publish: *const rpc.MessageReader) !void {
        const validation_result = try self.validateReceivedMessage(publish, from);

        switch (validation_result) {
            .valid => |valid_msg| {
                // Here we would normally propagate the message to peers in the mesh
                // and deliver it to local subscribers. For brevity, this is omitted.
                _ = valid_msg;
            },
            .invalid => |invalid_msg| {
                _ = invalid_msg;
                // std.log.warn("Invalid message from peer {?}: {}", .{ publish._from, invalid_msg.err });
                // Optionally, we could take action based on invalid_msg.reason
            },
            .duplicate => |dup_msg| {
                _ = dup_msg;
                // std.log.debug("Duplicate message received from peer {}: ID {any}", .{ publish._from, dup_msg.message_id });
            },
        }
    }

    fn handleControl(self: *Self, control: *const rpc.ControlMessageReader, from: PeerId) !void {
        const ihave_messages = try control.getIhave(self.allocator);
        defer {
            // Free only if we allocated, len == 0 returns a static empty slice
            if (ihave_messages.len > 0) {
                self.allocator.free(ihave_messages);
            }
        }

        const iwant = if (ihave_messages.len > 0) blk: {
            const result = try self.handleIHave(from, ihave_messages);
            break :blk result;
        } else blk: {
            break :blk &.{};
        };
        std.log.debug("Sending IWANT with {d} message IDs to peer {}", .{ iwant.len, from });
    }

    // fn handleIWant(self: *Self, from: PeerId, iwant: []const rpc.ControlIWantReader) ![]rpc.Message {
    //     if (iwant.len == 0) {
    //         return &.{};
    //     }

    //     const ihave:std.StringHashMapUnmanaged(rpc.Message) = .empty;
    //     defer ihave.deinit(self.allocator);
    //     const iwant_by_topic: std.StringHashMapUnmanaged(usize) = .empty;
    //     defer iwant_by_topic.deinit(self.allocator);
    //     var iwant_dont_have: usize = 0;

    //     var processed_iwant: usize = 0;
    //     errdefer {
    //         for (iwant[processed_iwant..]) |*rem| rem.deinit();
    //     }
    //     for (iwant) |*iwant_msg| {
    //         defer {
    //             iwant_msg.deinit();
    //             processed_iwant += 1;
    //         }

    //         for (iwant_msg.getMessageIDs()) |msg_id| {
    //             // Here we would normally check if we have the message and send it to the requesting peer.
    //             // For brevity, this is omitted.
    //             _ = msg_id;
    //         }
    //     }
    // }

    fn handleIHave(self: *Self, from: PeerId, ihave: []const rpc.ControlIHaveReader) ![]const rpc.ControlIWant {
        if (ihave.len == 0) {
            return &.{};
        }
        const peer_have = (self.peer_have.get(from) orelse 0) + 1;
        try self.peer_have.put(self.allocator, from, peer_have);
        if (peer_have > self.opts.max_ihave_messages) {
            std.log.warn("Peer {} sent too many {d} IHAVE messages within this heartbeat, ignoring further IHAVE messages.", .{ from, peer_have });
            return &.{};
        }

        const iasked = self.iasked.get(from) orelse 0;
        if (iasked > self.opts.max_ihave_len) {
            std.log.warn("Peer {} sent too many {d} IHAVE message IDs within this heartbeat, ignoring further IHAVE messages.", .{ from, iasked });
            return &.{};
        }

        var iwant: std.StringHashMapUnmanaged(void) = .empty;
        defer iwant.deinit(self.allocator);
        var processed_ihave: usize = 0;
        errdefer {
            for (ihave[processed_ihave..]) |*rem| rem.deinit();
        }
        for (ihave) |*ihave_msg| {
            defer {
                ihave_msg.deinit();
                processed_ihave += 1;
            }
            if (ihave_msg.getTopicID().len == 0 or ihave_msg.getMessageIDs().len == 0 or !self.mesh.contains(ihave_msg.getTopicID())) {
                continue;
            }

            var idonthave: usize = 0;
            for (ihave_msg.getMessageIDs()) |msg_id| {
                if (!self.seen_cache.contains(msg_id)) {
                    try iwant.put(self.allocator, msg_id, {});
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

        std.log.debug("Asking for {d} out of {d} messages from peer {}", .{ iask, iwant.count(), from });

        var iwant_list = try std.ArrayListUnmanaged([]const u8).initCapacity(self.allocator, iwant.count());
        defer iwant_list.deinit(self.allocator);

        var it = iwant.keyIterator();
        while (it.next()) |id| {
            try iwant_list.append(self.allocator, id.*);
        }

        var prng = std.Random.DefaultPrng.init(blk: {
            var seed: u64 = undefined;
            std.posix.getrandom(std.mem.asBytes(&seed)) catch break :blk @intCast(std.time.milliTimestamp());
            break :blk seed;
        });
        const random = prng.random();
        random.shuffle([]const u8, iwant_list.items);
        try self.iasked.put(self.allocator, from, iask + iasked);

        const result = try self.allocator.alloc(rpc.ControlIWant, 1);
        errdefer self.allocator.free(result);

        const selected_ids_raw = iwant_list.items[0..iask];

        const dupe_slice = try self.allocator.dupe([]const u8, selected_ids_raw);
        errdefer self.allocator.free(dupe_slice);

        const selected_ids: []?[]const u8 = @ptrCast(dupe_slice);

        result[0] = .{
            .message_i_ds = selected_ids,
        };

        return result;
    }

    fn vtableHandleRPCFn(instance: *anyopaque, rpc_msg: *const pubsub.RPC) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.handleRPC(rpc_msg);
    }

    fn vtableAddPeerFn(instance: *anyopaque, peer: Multiaddr, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.addPeer(peer, callback_ctx, callback);
    }

    fn vtableRemovePeerFn(instance: *anyopaque, peer: PeerId, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.removePeer(peer, callback_ctx, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = pubsub.PubSubVTable{
        .handleRPCFn = vtableHandleRPCFn,
        .addPeerFn = vtableAddPeerFn,
        .removePeerFn = vtableRemovePeerFn,
    };

    // --- any() method ---
    pub fn any(self: *Self) pubsub.PubSub {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

pub fn validateMessage(allocator: Allocator, msg: *const rpc.MessageReader, policy: SignaturePolicy, m: *Message) ValidateError!void {
    switch (policy) {
        SignaturePolicy.StrictNoSign => {
            if (msg._signature != null) {
                return error.SignaturePresent;
            }
            if (msg._seqno != null) {
                return error.SeqnoPresent;
            }
            if (msg._from != null) {
                return error.FromPresent;
            }
            if (msg._key != null) {
                return error.KeyPresent;
            }

            m.* = .{
                .from = null,
                .data = msg.getData(),
                .seqno = null,
                .topic_hash = msg.getTopic(),
                .signature = null,
                .key = null,
                .validated = true,
            };

            return;
        },
        SignaturePolicy.StrictSign => {
            if (msg._seqno == null) {
                return error.InvalidSeqno;
            }
            if (msg._seqno.?.len != 8) {
                return error.InvalidSeqno;
            }
            if (msg._signature == null) {
                return error.InvalidSignature;
            }
            if (msg._from == null) {
                return error.InvalidPeerId;
            }
            const from_peer_id = PeerId.fromBytes(msg.getFrom()) catch {
                return error.InvalidPeerId;
            };

            var pubkey_bytes_buf: [128]u8 = undefined; // this is enough space for a PeerId
            const pubkey_bytes = if (msg._key) |k| k else (from_peer_id.toBytes(&pubkey_bytes_buf) catch {
                return error.InvalidPeerId;
            })[2..];
            const proto_pubkey_reader = keys.PublicKeyReader.init(allocator, pubkey_bytes) catch {
                return error.InvalidPeerId;
            };

            var proto_pubkey = keys.PublicKey{
                .type = proto_pubkey_reader.getType(),
                .data = try allocator.dupe(u8, proto_pubkey_reader.getData()),
            };
            errdefer allocator.free(proto_pubkey.data.?);

            const evp_key = tls.reconstructEvpKeyFromPublicKey(&proto_pubkey) catch {
                return error.InvalidPubkey;
            };
            defer ssl.EVP_PKEY_free(evp_key);

            if (msg._key != null) {
                const key_peer_id = PeerId.fromPublicKey(allocator, &proto_pubkey) catch {
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
            const encoded_data = data_to_sign.encode(allocator) catch {
                return error.InvalidSignature;
            };
            defer allocator.free(encoded_data);
            const prefixed_data = std.mem.concat(allocator, u8, &.{ sign_prefix, encoded_data }) catch {
                return error.InvalidSignature;
            };
            defer allocator.free(prefixed_data);
            const is_valid = tls.verifySignature(evp_key, prefixed_data, msg.getSignature()) catch {
                return error.InvalidSignature;
            };
            if (!is_valid) {
                return error.InvalidSignature;
            }

            m.* = .{
                .from = from_peer_id,
                .data = msg.getData(),
                .seqno = std.mem.readInt(u64, msg.getSeqno()[0..8], .big),
                .topic_hash = msg.getTopic(),
                .signature = msg.getSignature(),
                .key = proto_pubkey,
                .validated = true,
            };
            return;
        },
    }
}

test "pubsub add peer" {
    const allocator = std.testing.allocator;

    var switch1_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9167");
    defer switch1_listen_address.deinit();

    var loop1: io_loop.ThreadEventLoop = undefined;
    try loop1.init(allocator);
    defer loop1.deinit();

    const peer1_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer1_host_key);

    var transport1: quic.QuicTransport = undefined;
    try transport1.init(&loop1, peer1_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch1 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9167");
    try dial_ma_switch1.push(.{ .P2P = transport1.local_peer_id });
    defer dial_ma_switch1.deinit();

    var switch1: swarm.Switch = undefined;
    switch1.init(allocator, &transport1);

    var pubsub_peer_handler1 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler1.deinit();
    try switch1.addProtocolHandler(v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: Gossipsub = undefined;
    try pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1, .{});
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, Gossipsub.onIncomingNewStream);
    defer switch1.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    var switch2_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9168");
    defer switch2_listen_address.deinit();

    var loop2: io_loop.ThreadEventLoop = undefined;
    try loop2.init(allocator);
    defer loop2.deinit();

    const peer2_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer2_host_key);

    var transport2: quic.QuicTransport = undefined;
    try transport2.init(&loop2, peer2_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch2 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9168");
    try dial_ma_switch2.push(.{ .P2P = transport2.local_peer_id });
    defer dial_ma_switch2.deinit();

    var switch2: swarm.Switch = undefined;
    switch2.init(allocator, &transport2);

    var pubsub_peer_handler2 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler2.deinit();
    try switch2.addProtocolHandler(v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: Gossipsub = undefined;
    try pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2, .{});
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, Gossipsub.onIncomingNewStream);
    defer switch2.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    pubsub1.addPeer(dial_ma_switch2, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    pubsub2.addPeer(dial_ma_switch1, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, pubsub1.peers.count());
    try std.testing.expectEqual(1, pubsub2.peers.count());

    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.initiator != null);
    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.responder != null);

    try std.testing.expect(pubsub2.peers.get(transport1.local_peer_id).?.initiator != null);
    try std.testing.expect(pubsub2.peers.get(transport1.local_peer_id).?.responder != null);
}

test "pubsub add and remove peer" {
    const allocator = std.testing.allocator;

    var switch1_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9767");
    defer switch1_listen_address.deinit();

    var loop1: io_loop.ThreadEventLoop = undefined;
    try loop1.init(allocator);
    defer loop1.deinit();

    const peer1_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer1_host_key);

    var transport1: quic.QuicTransport = undefined;
    try transport1.init(&loop1, peer1_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch1 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9767");
    try dial_ma_switch1.push(.{ .P2P = transport1.local_peer_id });
    defer dial_ma_switch1.deinit();

    var switch1: swarm.Switch = undefined;
    switch1.init(allocator, &transport1);

    var pubsub_peer_handler1 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler1.deinit();
    try switch1.addProtocolHandler(v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: Gossipsub = undefined;
    try pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1, .{});
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, Gossipsub.onIncomingNewStream);
    defer switch1.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    var switch2_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9768");
    defer switch2_listen_address.deinit();

    var loop2: io_loop.ThreadEventLoop = undefined;
    try loop2.init(allocator);
    defer loop2.deinit();

    const peer2_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer2_host_key);

    var transport2: quic.QuicTransport = undefined;
    try transport2.init(&loop2, peer2_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch2 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9768");
    try dial_ma_switch2.push(.{ .P2P = transport2.local_peer_id });
    defer dial_ma_switch2.deinit();

    var switch2: swarm.Switch = undefined;
    switch2.init(allocator, &transport2);

    var pubsub_peer_handler2 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler2.deinit();
    try switch2.addProtocolHandler(v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: Gossipsub = undefined;
    try pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2, .{});
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, Gossipsub.onIncomingNewStream);
    defer switch2.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    pubsub1.addPeer(dial_ma_switch2, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    pubsub2.addPeer(dial_ma_switch1, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, pubsub1.peers.count());
    try std.testing.expectEqual(1, pubsub2.peers.count());

    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.initiator != null);
    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.responder != null);

    try std.testing.expect(pubsub2.peers.get(transport1.local_peer_id).?.initiator != null);
    try std.testing.expect(pubsub2.peers.get(transport1.local_peer_id).?.responder != null);

    pubsub1.removePeer(transport2.local_peer_id, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to remove peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully removed peer\n", .{});
        }
    }.callback);

    pubsub2.removePeer(transport1.local_peer_id, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to remove peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully removed peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(0, pubsub1.peers.count());
    try std.testing.expectEqual(0, pubsub2.peers.count());
}

test "pubsub add peer single direction" {
    const allocator = std.testing.allocator;

    var switch1_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9267");
    defer switch1_listen_address.deinit();

    var loop1: io_loop.ThreadEventLoop = undefined;
    try loop1.init(allocator);
    defer loop1.deinit();

    const peer1_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer1_host_key);

    var transport1: quic.QuicTransport = undefined;
    try transport1.init(&loop1, peer1_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch1 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9267");
    try dial_ma_switch1.push(.{ .P2P = transport1.local_peer_id });
    defer dial_ma_switch1.deinit();

    var switch1: swarm.Switch = undefined;
    switch1.init(allocator, &transport1);

    var pubsub_peer_handler1 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler1.deinit();
    try switch1.addProtocolHandler(v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: Gossipsub = undefined;
    try pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1, .{});
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, Gossipsub.onIncomingNewStream);
    defer switch1.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    var switch2_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9268");
    defer switch2_listen_address.deinit();

    var loop2: io_loop.ThreadEventLoop = undefined;
    try loop2.init(allocator);
    defer loop2.deinit();

    const peer2_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer2_host_key);

    var transport2: quic.QuicTransport = undefined;
    try transport2.init(&loop2, peer2_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch2 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9268");
    try dial_ma_switch2.push(.{ .P2P = transport2.local_peer_id });
    defer dial_ma_switch2.deinit();

    var switch2: swarm.Switch = undefined;
    switch2.init(allocator, &transport2);

    var pubsub_peer_handler2 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler2.deinit();
    try switch2.addProtocolHandler(v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: Gossipsub = undefined;
    try pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2, .{});
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, Gossipsub.onIncomingNewStream);
    defer switch2.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    pubsub1.addPeer(dial_ma_switch2, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, pubsub1.peers.count());
    try std.testing.expectEqual(1, pubsub2.peers.count());

    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.initiator != null);
    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.responder == null);
}

test "pubsub add and remove peer single direction" {
    const allocator = std.testing.allocator;

    var switch1_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9367");
    defer switch1_listen_address.deinit();

    var loop1: io_loop.ThreadEventLoop = undefined;
    try loop1.init(allocator);
    defer loop1.deinit();

    const peer1_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer1_host_key);

    var transport1: quic.QuicTransport = undefined;
    try transport1.init(&loop1, peer1_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch1 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9367");
    try dial_ma_switch1.push(.{ .P2P = transport1.local_peer_id });
    defer dial_ma_switch1.deinit();

    var switch1: swarm.Switch = undefined;
    switch1.init(allocator, &transport1);

    var pubsub_peer_handler1 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler1.deinit();
    try switch1.addProtocolHandler(v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: Gossipsub = undefined;
    try pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1, .{});
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, Gossipsub.onIncomingNewStream);
    defer switch1.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    var switch2_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9368");
    defer switch2_listen_address.deinit();

    var loop2: io_loop.ThreadEventLoop = undefined;
    try loop2.init(allocator);
    defer loop2.deinit();

    const peer2_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer2_host_key);

    var transport2: quic.QuicTransport = undefined;
    try transport2.init(&loop2, peer2_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch2 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9368");
    try dial_ma_switch2.push(.{ .P2P = transport2.local_peer_id });
    defer dial_ma_switch2.deinit();

    var switch2: swarm.Switch = undefined;
    switch2.init(allocator, &transport2);

    var pubsub_peer_handler2 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler2.deinit();
    try switch2.addProtocolHandler(v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: Gossipsub = undefined;
    try pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2, .{});
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, Gossipsub.onIncomingNewStream);
    defer switch2.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    pubsub1.addPeer(dial_ma_switch2, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, pubsub1.peers.count());
    try std.testing.expectEqual(1, pubsub2.peers.count());

    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.initiator != null);
    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.responder == null);

    pubsub1.removePeer(transport2.local_peer_id, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to remove peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully removed peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(0, pubsub1.peers.count());
    try std.testing.expectEqual(0, pubsub2.peers.count());
}

test "pubsub add peer single direction with replace stream" {
    const allocator = std.testing.allocator;

    var switch1_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9467");
    defer switch1_listen_address.deinit();

    var loop1: io_loop.ThreadEventLoop = undefined;
    try loop1.init(allocator);
    defer loop1.deinit();

    const peer1_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer1_host_key);

    var transport1: quic.QuicTransport = undefined;
    try transport1.init(&loop1, peer1_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch1 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9467");
    try dial_ma_switch1.push(.{ .P2P = transport1.local_peer_id });
    defer dial_ma_switch1.deinit();

    var switch1: swarm.Switch = undefined;
    switch1.init(allocator, &transport1);

    var pubsub_peer_handler1 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler1.deinit();
    try switch1.addProtocolHandler(v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: Gossipsub = undefined;
    try pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1, .{});
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, Gossipsub.onIncomingNewStream);
    defer switch1.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    var switch2_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9468");
    defer switch2_listen_address.deinit();

    var loop2: io_loop.ThreadEventLoop = undefined;
    try loop2.init(allocator);
    defer loop2.deinit();

    const peer2_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer2_host_key);

    var transport2: quic.QuicTransport = undefined;
    try transport2.init(&loop2, peer2_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch2 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9468");
    try dial_ma_switch2.push(.{ .P2P = transport2.local_peer_id });
    defer dial_ma_switch2.deinit();

    var switch2: swarm.Switch = undefined;
    switch2.init(allocator, &transport2);

    var pubsub_peer_handler2 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler2.deinit();
    try switch2.addProtocolHandler(v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: Gossipsub = undefined;
    try pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2, .{});
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, Gossipsub.onIncomingNewStream);
    defer switch2.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    pubsub1.addPeer(dial_ma_switch2, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, pubsub1.peers.count());
    try std.testing.expectEqual(1, pubsub2.peers.count());

    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.initiator != null);
    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.responder == null);

    try std.testing.expect(pubsub2.peers.get(transport1.local_peer_id).?.responder != null);
    std.debug.print("old responder {?*}\n", .{pubsub2.peers.get(transport1.local_peer_id).?.responder});
    switch1.newStream(dial_ma_switch2, pubsub1.protocols, null, struct {
        fn callback(_: ?*anyopaque, _: anyerror!?*anyopaque) void {}
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);
    try std.testing.expect(pubsub2.peers.get(transport1.local_peer_id).?.responder != null);
    std.debug.print("new responder {?*}\n", .{pubsub2.peers.get(transport1.local_peer_id).?.responder});

    // The old stream be closed by switch2, so that the pubsub1's outgoing stream is closed
    try std.testing.expectEqual(0, pubsub1.peers.count());
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

    var switch1_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9567");
    defer switch1_listen_address.deinit();

    var loop1: io_loop.ThreadEventLoop = undefined;
    try loop1.init(allocator);
    defer loop1.deinit();

    const peer1_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer1_host_key);

    var transport1: quic.QuicTransport = undefined;
    try transport1.init(&loop1, peer1_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch1 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9567");
    try dial_ma_switch1.push(.{ .P2P = transport1.local_peer_id });
    defer dial_ma_switch1.deinit();

    var switch1: swarm.Switch = undefined;
    switch1.init(allocator, &transport1);

    var pubsub_peer_handler1 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler1.deinit();
    try switch1.addProtocolHandler(v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: Gossipsub = undefined;
    try pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1, .{});
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, Gossipsub.onIncomingNewStream);
    defer switch1.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    var switch2_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9568");
    defer switch2_listen_address.deinit();

    var loop2: io_loop.ThreadEventLoop = undefined;
    try loop2.init(allocator);
    defer loop2.deinit();

    const peer2_host_key = try tls.generateKeyPair(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer2_host_key);

    var transport2: quic.QuicTransport = undefined;
    try transport2.init(&loop2, peer2_host_key, keys.KeyType.ED25519, allocator);

    var dial_ma_switch2 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9568");
    try dial_ma_switch2.push(.{ .P2P = transport2.local_peer_id });
    defer dial_ma_switch2.deinit();

    var switch2: swarm.Switch = undefined;
    switch2.init(allocator, &transport2);

    var pubsub_peer_handler2 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler2.deinit();
    try switch2.addProtocolHandler(v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: Gossipsub = undefined;
    try pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2, .{});
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, Gossipsub.onIncomingNewStream);
    defer switch2.deinit();
    std.time.sleep(300 * std.time.us_per_ms);

    pubsub1.addPeer(dial_ma_switch2, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(1 * std.time.ns_per_s);

    try std.testing.expectEqual(1, pubsub1.peers.count());
    try std.testing.expectEqual(1, pubsub2.peers.count());

    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.initiator != null);
    try std.testing.expect(pubsub1.peers.get(transport2.local_peer_id).?.responder == null);

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

    const responder = pubsub2.peers.get(transport1.local_peer_id).?.responder.?;
    var listener: SubscriptionChangedListener = .{
        .allocator = allocator,
    };
    try pubsub2.event_emitter.addListener(.subscription_changed, listener.any());
    try responder.onMessage(responder.stream, encoded_rpc_message);

    try std.testing.expectEqual(3, pubsub2.topics.count());
    try std.testing.expectEqual(0, pubsub2.topics.get("test_topic1").?.count());
    try std.testing.expectEqual(1, pubsub2.topics.get("test_topic").?.count());
    try std.testing.expectEqual(1, pubsub2.topics.get("test_topic2").?.count());
}
