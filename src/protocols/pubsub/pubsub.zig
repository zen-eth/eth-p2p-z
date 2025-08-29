const std = @import("std");
const libp2p = @import("../../root.zig");
const Multiaddr = @import("multiformats").multiaddr.Multiaddr;
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

pub const gossipsub = @import("algorithms/gossipsub.zig");
pub const semiduplex = @import("semiduplex.zig");
pub const Semiduplex = semiduplex.Semiduplex;
pub const PubSubPeerInitiator = semiduplex.PubSubPeerInitiator;
pub const PubSubPeerResponder = semiduplex.PubSubPeerResponder;
pub const PubSubPeerProtocolHandler = semiduplex.PubSubPeerProtocolHandler;
pub const gossipsub_v1_id: ProtocolId = gossipsub.v1_id;
pub const gossipsub_v1_1_id: ProtocolId = gossipsub.v1_1_id;

pub const PubSub = struct {
    peers: std.AutoHashMap(PeerId, Semiduplex),

    swarm: *Switch,

    peer: Multiaddr,

    peer_id: PeerId,

    allocator: Allocator,

    // TODO: Not hardcode protocol IDs
    protocols: []const ProtocolId = &.{ gossipsub_v1_id, gossipsub_v1_1_id },

    const AddPeerCtx = struct {
        pubsub: ?*PubSub,
        semiduplex: ?*Semiduplex,
        callback_ctx: ?*anyopaque,
        callback: *const fn (ctx: ?*anyopaque, res: anyerror!void) void,

        fn onOutgoingNewStream(callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void {
            const self: *AddPeerCtx = @ptrCast(@alignCast(callback_ctx.?));
            defer self.pubsub.?.allocator.destroy(self);
            const initiator = controller catch |err| {
                self.callback(self.callback_ctx, err);
                return;
            };
            const stream_initiator: *PubSubPeerInitiator = @ptrCast(@alignCast(initiator.?));

            if (self.pubsub) |pubsub| {
                pubsub.peers.putNoClobber(stream_initiator.stream.conn.security_session.?.remote_id, Semiduplex{
                    .initiator = stream_initiator,
                    .responder = null,
                    .allocator = pubsub.allocator,
                    .close_ctx = null,
                    .close_callback = null,
                }) catch |err| {
                    self.callback(self.callback_ctx, err);
                };
            } else if (self.semiduplex) |semi_duplex| {
                semi_duplex.initiator = stream_initiator;
            }

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
        };
    }

    pub fn deinit(self: *Self) void {
        self.peers.deinit();
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
                    .pubsub = null,
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

        const peer_id = responder.stream.conn.security_session.?.remote_id;

        const result = self.peers.getOrPut(peer_id) catch unreachable;

        if (result.found_existing) {
            result.value_ptr.responder = responder;
        } else {
            result.value_ptr.* = Semiduplex{
                .initiator = null,
                .responder = responder,
                .allocator = self.allocator,
                .close_ctx = null,
                .close_callback = null,
            };
        }

        responder.stream.close_ctx = .{
            .active_callback_ctx = null,
            .active_callback = null,
            .callback_ctx = self,
            .callback = Self.onStreamClose,
        };
    }

    fn onStreamClose(ctx: ?*anyopaque, stream: anyerror!*libp2p.QuicStream) void {
        const self: *Self = @ptrCast(@alignCast(ctx.?));
        const s = stream catch unreachable;

        if (!self.peers.contains(s.conn.security_session.?.remote_id)) {
            // This should not be reached
            std.log.warn("Stream closed for unknown peer: {}", .{s.conn.security_session.?.remote_id});
            return;
        }

        const semi_duplex = self.peers.getPtr(s.conn.security_session.?.remote_id).?;
        if (semi_duplex.initiator) |initiator| {
            if (initiator.stream == s) {
                semi_duplex.initiator = null;
            }
        } else if (semi_duplex.responder) |resp| {
            if (resp.stream == s) {
                semi_duplex.responder = null;
            }
        }

        const remove_peer_ctx = self.allocator.create(RemovePeerCtx) catch unreachable;
        remove_peer_ctx.* = RemovePeerCtx{
            .pubsub = self,
            .peer = s.conn.security_session.?.remote_id,
            .callback_ctx = null,
            .callback = struct {
                fn callback(_: ?*anyopaque, _: anyerror!void) void {}
            }.callback,
        };
        semi_duplex.close(self, RemovePeerCtx.onCloseSemiduplex);
    }
};

test "pubsub" {
    const allocator = std.testing.allocator;

    var switch1_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9767");
    defer switch1_listen_address.deinit();

    var loop1: io_loop.ThreadEventLoop = undefined;
    try loop1.init(allocator);
    defer loop1.deinit();

    const peer1_host_key = try tls.generateKeyPair1(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer1_host_key);

    var transport1: quic.QuicTransport = undefined;
    try transport1.init(&loop1, peer1_host_key, keys.KeyType.ED25519, allocator);

    var switch1: swarm.Switch = undefined;
    switch1.init(allocator, &transport1);

    var pubsub_peer_handler1 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler1.deinit();
    try switch1.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler1.any());
    try switch1.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler1.any());

    var pubsub1: PubSub = undefined;
    pubsub1.init(allocator, switch1_listen_address, transport1.local_peer_id, &switch1);

    try switch1.listen(switch1_listen_address, &pubsub1, PubSub.onIncomingNewStream);
    std.debug.print("Switch1 is listening on: {}\n", .{switch1_listen_address});

    std.time.sleep(300 * std.time.us_per_ms);

    var switch2_listen_address = try Multiaddr.fromString(allocator, "/ip4/0.0.0.0/udp/9768");
    defer switch2_listen_address.deinit();

    var loop2: io_loop.ThreadEventLoop = undefined;
    try loop2.init(allocator);
    defer loop2.deinit();

    const peer2_host_key = try tls.generateKeyPair1(keys.KeyType.ED25519);
    defer ssl.EVP_PKEY_free(peer2_host_key);

    var transport2: quic.QuicTransport = undefined;
    try transport2.init(&loop2, peer2_host_key, keys.KeyType.ED25519, allocator);

    var switch2: swarm.Switch = undefined;
    switch2.init(allocator, &transport2);

    var pubsub_peer_handler2 = PubSubPeerProtocolHandler.init(allocator);
    defer pubsub_peer_handler2.deinit();
    try switch2.addProtocolHandler(gossipsub_v1_id, pubsub_peer_handler2.any());
    try switch2.addProtocolHandler(gossipsub_v1_1_id, pubsub_peer_handler2.any());

    var pubsub2: PubSub = undefined;
    pubsub2.init(allocator, switch2_listen_address, transport2.local_peer_id, &switch2);

    try switch2.listen(switch2_listen_address, &pubsub2, PubSub.onIncomingNewStream);

    std.time.sleep(300 * std.time.us_per_ms);

    var dial_ma_switch2 = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/9768");
    try dial_ma_switch2.push(.{ .P2P = transport2.local_peer_id });

    pubsub1.addPeer(dial_ma_switch2, null, struct {
        fn callback(_: ?*anyopaque, res: anyerror!void) void {
            res catch |err| {
                std.debug.print("Failed to add peer: {}\n", .{err});
                return;
            };
            std.debug.print("Successfully added peer\n", .{});
        }
    }.callback);

    std.time.sleep(4 * std.time.ns_per_s);

    defer {
        pubsub1.peers.clearAndFree();
        pubsub1.deinit();
        switch1.deinit();

        pubsub2.peers.clearAndFree();
        pubsub2.deinit();
        switch2.deinit();

        dial_ma_switch2.deinit();
    }
}
