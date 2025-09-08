const std = @import("std");
const libp2p = @import("../../root.zig");
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

pub const gossipsub = @import("routers/gossipsub.zig");
pub const semiduplex = @import("semiduplex.zig");
pub const Semiduplex = semiduplex.Semiduplex;
pub const PubSubPeerInitiator = semiduplex.PubSubPeerInitiator;
pub const PubSubPeerResponder = semiduplex.PubSubPeerResponder;
pub const PubSubPeerProtocolHandler = semiduplex.PubSubPeerProtocolHandler;
pub const gossipsub_v1_id: ProtocolId = gossipsub.v1_id;
pub const gossipsub_v1_1_id: ProtocolId = gossipsub.v1_1_id;

pub const Subscription = struct {
    topic: []const u8,
    subscribe: bool,
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

pub const RPC = struct {
    rpc_reader: rpc.RPCReader,
    from: PeerId,

    pub fn deinit(self: *RPC) void {
        self.rpc_reader.deinit();
    }
};
/// This is an implementation of the generic PubSub system which defined by the libp2p specification.
/// It manages the peers and their connections, and provides methods to add and remove peers.
/// It uses the Semiduplex struct to manage the bidirectional communication with each peer.
/// It also handles incoming and outgoing streams for the PubSub protocol.
/// It could use different PubSub routing algorithms, such as Gossipsub, Floodsub, etc.
pub const PubSub = struct {
    peers: std.AutoHashMap(PeerId, Semiduplex),

    swarm: *Switch,

    peer: Multiaddr,

    peer_id: PeerId,

    allocator: Allocator,

    topics: std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)),

    my_topics: std.StringHashMapUnmanaged(void),

    event_emitter: event.EventEmitter(Event),

    // TODO: Not hardcode protocol IDs
    protocols: []const ProtocolId = &.{ gossipsub_v1_id, gossipsub_v1_1_id },

    const AddPeerCtx = struct {
        pubsub: *PubSub,
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
                .callback = PubSub.onStreamClose,
            };

            self.callback(self.callback_ctx, {});
        }
    };

    const RemovePeerCtx = struct {
        pubsub: *PubSub,
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

    pub fn init(self: *Self, allocator: Allocator, peer: Multiaddr, peer_id: PeerId, network_swarm: *Switch) void {
        self.* = .{
            .allocator = allocator,
            .peer = peer,
            .peer_id = peer_id,
            .swarm = network_swarm,
            .peers = std.AutoHashMap(PeerId, Semiduplex).init(allocator),
            .topics = std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)).empty,
            .my_topics = std.StringHashMapUnmanaged(void).empty,
            .event_emitter = event.EventEmitter(Event).init(allocator),
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

        var my_topic_iter = self.my_topics.iterator();
        while (my_topic_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.my_topics.deinit(self.allocator);
        self.event_emitter.deinit();
    }

    pub fn handleIncomingRPC(self: *Self, rpc_message: *const RPC) !void {
        const subs = try rpc_message.rpc_reader.getSubscriptions(self.allocator);
        defer self.allocator.free(subs);
        //TODO:Implement subscription filtering

        var subscriptions = std.ArrayListUnmanaged(Subscription).empty;
        defer {
            for (subscriptions.items) |subscription| {
                self.allocator.free(subscription.topic);
            }
            subscriptions.deinit(self.allocator);
        }
        for (subs) |sub| {
            const topic_id = sub.getTopicid();
            if (sub.getSubscribe()) {
                if (!self.topics.contains(topic_id)) {
                    const copied_topic_id = try self.allocator.dupe(u8, topic_id);
                    errdefer self.allocator.free(copied_topic_id);
                    const tmap = std.AutoHashMapUnmanaged(PeerId, void).empty;
                    try self.topics.put(self.allocator, copied_topic_id, tmap);
                }

                const pgop = try self.topics.getPtr(topic_id).?.getOrPut(self.allocator, rpc_message.from);
                if (!pgop.found_existing) {
                    pgop.value_ptr.* = {};
                }
            } else {
                if (!self.topics.contains(topic_id)) {
                    continue;
                }

                if (self.topics.getPtr(topic_id).?.contains(rpc_message.from)) {
                    _ = self.topics.getPtr(topic_id).?.remove(rpc_message.from);
                }
            }

            const sub_copied = try self.allocator.dupe(u8, topic_id);
            errdefer self.allocator.free(sub_copied);
            try subscriptions.append(self.allocator, .{ .topic = sub_copied, .subscribe = sub.getSubscribe() });
        }

        if (self.event_emitter.getListenerCount(.subscription_changed) > 0) {
            self.event_emitter.emit(.{ .subscription_changed = .{
                .peer = rpc_message.from,
                .subscriptions = try subscriptions.toOwnedSlice(self.allocator),
            } });
        }
    }

    pub fn removePeer(self: *Self, peer: PeerId, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void) void {
        if (self.swarm.transport.io_event_loop.inEventLoopThread()) {
            self.doRemovePeer(peer, callback_ctx, callback);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .pubsub_remove_peer = .{
                    .pubsub = self,
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
                    .pubsub = self,
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
        responder.pubsub = self;
        const peer_id = responder.stream.conn.security_session.?.remote_id;

        const result = self.peers.getOrPut(peer_id) catch unreachable;

        if (result.found_existing) {
            if (result.value_ptr.responder != null) {
                std.debug.print("old responder11 {?*}\n", .{result.value_ptr.responder});
                std.debug.print("new responder111 {*}\n", .{responder});
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
};

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
    try switch1.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: PubSub = undefined;
    pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1);
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, PubSub.onIncomingNewStream);
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
    try switch2.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: PubSub = undefined;
    pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2);
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, PubSub.onIncomingNewStream);
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
    try switch1.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: PubSub = undefined;
    pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1);
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, PubSub.onIncomingNewStream);
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
    try switch2.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: PubSub = undefined;
    pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2);
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, PubSub.onIncomingNewStream);
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
    try switch1.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: PubSub = undefined;
    pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1);
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, PubSub.onIncomingNewStream);
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
    try switch2.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: PubSub = undefined;
    pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2);
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, PubSub.onIncomingNewStream);
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
    try switch1.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: PubSub = undefined;
    pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1);
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, PubSub.onIncomingNewStream);
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
    try switch2.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: PubSub = undefined;
    pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2);
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, PubSub.onIncomingNewStream);
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
    try switch1.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: PubSub = undefined;
    pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1);
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, PubSub.onIncomingNewStream);
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
    try switch2.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: PubSub = undefined;
    pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2);
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, PubSub.onIncomingNewStream);
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

        pub fn handle(self: *Self, e: Event) void {
            switch (e) {
                .subscription_changed => |sc| {
                    std.debug.print("Subscription changed event received\n", .{});
                    for (sc.subscriptions) |subscription| {
                        self.allocator.free(subscription.topic);
                    }
                    self.allocator.free(sc.subscriptions);
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
    try switch1.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: PubSub = undefined;
    pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1);
    defer pubsub1.deinit();
    try switch1.listen(switch1_listen_address, &pubsub1, PubSub.onIncomingNewStream);
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
    try switch2.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: PubSub = undefined;
    pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2);
    defer pubsub2.deinit();
    try switch2.listen(switch2_listen_address, &pubsub2, PubSub.onIncomingNewStream);
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
