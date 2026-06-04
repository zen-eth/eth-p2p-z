//! The public gossipsub service and its Switch/QUIC binding. `Gossipsub`
//! constructs and owns a single-fiber `Router(SwitchTransport)`, registers it on
//! a `*Switch` (as the `/meshsub/1.1.0` inbound service and as the peer
//! connect/disconnect observer), and exposes the lifecycle.
//!
//! This file is the ONLY place that knows the router talks to a real Switch over
//! QUIC. The transport-agnostic router lives in router.zig; the concrete pieces
//! it needs from the real transport are here:
//!   - `SwitchTransport`: supplies the per-peer outbound `StreamSink` and the
//!     `*SwitchConnection` connection handle the `peer_connected` command carries.
//!   - `StreamSink` / `StreamSource`: the QUIC-stream sink/source satisfying the
//!     PeerWriter / PeerReader contracts.
//!   - the inbound service/handler registered on the Switch.
//!
//! The Switch's peer-event callbacks fire on whichever fiber called
//! dial/accept/deinit, OUTSIDE the Switch's connection lock. They must only post
//! to the router inbox and return — which is exactly what the callbacks here do,
//! keeping all router-state mutation on the single router fiber.
//!
//! This is wired on the root import path (root.zig), NOT through protocols.zig:
//! the router imports peer_io/pubsub and this file imports switch.zig, which
//! imports protocols.zig, so routing gossipsub through the protocols chain would
//! risk an import cycle.

const std = @import("std");
const protocols = @import("../../protocols.zig");
const quic = @import("../../quic.zig");
const swarm = @import("../../switch.zig");
const pubsub = @import("pubsub.zig");
const peer_io = @import("peer_io.zig");
const router_mod = @import("router.zig");
const PeerId = @import("peer_id").PeerId;

const Switch = swarm.Switch;
const SwitchConnection = swarm.SwitchConnection;
const ProtocolId = protocols.ProtocolId;
const Stream = quic.Stream;

/// Drains a peer's OutboundQueue onto its `/meshsub/1.1.0` stream. The sink owns
/// the current QUIC stream and re-opens it lazily; it satisfies the PeerWriter
/// Sink contract (open / writeFrame / close, with close idempotent).
pub const StreamSink = struct {
    conn: *SwitchConnection,
    proto: ProtocolId,
    /// The currently-open outbound stream, or null when none is open.
    current: ?*Stream = null,

    /// (Re)establish the outbound stream by negotiating the protocol on a new
    /// QUIC stream. Leaves `current` set on success; on failure `current` is
    /// untouched (it was already null — the writer only opens when it has none).
    pub fn open(self: *StreamSink, io: std.Io) !void {
        _ = io;
        self.current = try self.conn.openProtocolStream(self.proto, .{});
    }

    /// Write one framed RPC to the current stream. Errors propagate so the
    /// writer can tear the stream down and re-open.
    pub fn writeFrame(self: *StreamSink, io: std.Io, bytes: []const u8) !void {
        try self.current.?.writeAll(io, bytes, .{});
    }

    /// Tear down the current stream. Idempotent: safe when none is open. The
    /// stream close is fire-and-forget; deinit drops the handle.
    pub fn close(self: *StreamSink, io: std.Io) void {
        if (self.current) |s| {
            s.close(io) catch {};
            s.deinit();
            self.current = null;
        }
    }
};

/// Reads parsed RPCs off a peer's inbound `/meshsub/1.1.0` stream. Satisfies the
/// PeerReader Source contract; surfaces EOF / shutdown / connection-closed as
/// errors so the reader loop exits.
pub const StreamSource = struct {
    stream: *Stream,

    pub fn read(self: *StreamSource, allocator: std.mem.Allocator, io: std.Io) !pubsub.OwnedRpc {
        return pubsub.readRpc(allocator, io, self.stream);
    }
};

/// The real transport binding for the gossipsub Router: it produces a per-peer
/// `StreamSink` over a `*SwitchConnection`. The router stays generic over this
/// (see router.zig's Transport contract); the only thing the router asks of it
/// is `makeSink`, plus the `Sink`/`ConnHandle` types.
///
/// Empty (stateless): held by value in the router. `makeSink` allocates the sink
/// but does no I/O — the writer fiber opens the stream lazily on its first frame.
pub const SwitchTransport = struct {
    pub const ConnHandle = *SwitchConnection;
    pub const Sink = StreamSink;

    pub fn makeSink(self: *SwitchTransport, allocator: std.mem.Allocator, peer: PeerId, conn: ConnHandle) !*StreamSink {
        _ = self;
        _ = peer;
        const sink = try allocator.create(StreamSink);
        sink.* = .{ .conn = conn, .proto = pubsub.protocol_id };
        return sink;
    }
};

const Router = router_mod.Router(SwitchTransport);

/// Per-stream inbound handler. The Switch's dispatcher creates one of these per
/// inbound `/meshsub/1.1.0` stream (capturing the sender's peer id) and runs it
/// on a handler fiber the Switch owns and cancels on connection teardown. The
/// handler reads RPCs and posts them to the router inbox until the stream ends.
const InboundHandler = struct {
    router: *Router,
    peer: PeerId,

    /// Fiber body run by the Switch. Reads + posts until the stream breaks.
    fn run(self: *InboundHandler, io: std.Io, stream: *Stream) anyerror!void {
        var source = StreamSource{ .stream = stream };
        var poster = Router.InboxPoster{ .router = self.router };
        var reader = peer_io.PeerReader(StreamSource, Router.InboxPoster){
            .source = &source,
            .poster = &poster,
            .peer = self.peer,
            .allocator = self.router.allocator,
        };
        reader.run(io);
    }
};

/// The inbound service object registered on the Switch for `/meshsub/1.1.0`.
/// Its `openInbound` captures the negotiated peer id into a fresh, heap-owned
/// `InboundHandler` so the read loop knows who sent the RPCs. The handler is
/// freed by the Switch via the AnyProtocolStreamHandler deinit after `run`
/// returns.
const InboundService = struct {
    router: *Router,

    fn openInbound(
        self: *InboundService,
        allocator: std.mem.Allocator,
        ctx: protocols.InboundProtocolContext,
    ) anyerror!protocols.AnyProtocolStreamHandler {
        const handler = try allocator.create(InboundHandler);
        handler.* = .{ .router = self.router, .peer = ctx.peer_id };
        return protocols.ownedProtocolStreamHandler(InboundHandler, InboundHandler.run, handler);
    }
};

/// The gossipsub service handle. Owns the router; borrows the Switch (which must
/// outlive this). Construct one per Switch before any dial/accept so its
/// peer-event callback is registered when connections come up.
pub const Gossipsub = struct {
    allocator: std.mem.Allocator,
    /// Borrowed; must outlive the router (the router's per-peer state borrows
    /// the Switch's connections, and the Switch holds the peer-event callback
    /// and inbound service that reference the router). Cleared via this on
    /// deinit before the router is freed.
    sw: *Switch,
    router: *Router,

    /// Construct the router, start its fiber, register the inbound service and
    /// the peer-event callback on the Switch. On any failure the router is torn
    /// down so nothing leaks.
    pub fn init(allocator: std.mem.Allocator, io: std.Io, sw: *Switch) !*Gossipsub {
        const router = try Router.create(allocator, io, .{});
        errdefer router.destroy();

        try router.start();

        const service = try inboundService(router);
        var service_owned = true;
        // If addProtocolService fails it does NOT take ownership, so free the
        // service object ourselves on that path.
        errdefer if (service_owned) service.deinit();
        try sw.addProtocolService(pubsub.protocol_id, service);
        service_owned = false;

        sw.setPeerEventCallback(.{
            .ctx = router,
            .on_connected = onConnected,
            .on_disconnected = onDisconnected,
        });

        const self = try allocator.create(Gossipsub);
        self.* = .{ .allocator = allocator, .sw = sw, .router = router };
        return self;
    }

    /// Build the inbound service object that reads inbound RPCs and posts them to
    /// the inbox. The returned AnyProtocolService borrows `router` (its instance
    /// is a heap-owned InboundService destroyed via the service deinit).
    fn inboundService(router: *Router) !protocols.AnyProtocolService {
        const svc = try router.allocator.create(InboundService);
        svc.* = .{ .router = router };
        return protocols.ownedProtocolService(
            InboundService,
            InboundService.openInbound,
            destroyInboundService,
            svc,
        );
    }

    fn destroyInboundService(svc: *InboundService) void {
        svc.router.allocator.destroy(svc);
    }

    /// Shut the router down (tears down every peer, joins its fiber) and free.
    /// Call before deiniting the Switch: the Switch cancels the inbound handler
    /// fibers on connection teardown, and the router's peer state borrows the
    /// Switch connections, so the router must stop while the Switch is still live.
    ///
    /// PRECONDITION (closes the router↔Switch lifetime hole): before this runs,
    /// every connection on this Switch that uses gossipsub must already be torn
    /// down (or be torn down with no inbound `/meshsub` RPC in flight), OR this
    /// Gossipsub must be deinited before any such connection outlives it.
    ///
    /// The Switch holds three references that point at the router we are about to
    /// free: (a) the peer-event callback's `ctx`, (b) the registered `/meshsub`
    /// inbound-service instance's `router` field, and (c) any live Switch-owned
    /// inbound handler fiber holding `*Router`. We close all three holes here:
    ///   - First `clearPeerEventCallback`: after this, no connect/disconnect can
    ///     post into the router, so freeing it cannot be raced by a peer event.
    ///     (Per its contract this assumes no concurrent connect/disconnect is in
    ///     flight — the precondition above.)
    ///   - Then `router.destroy`: it tears down every peer (closing each peer's
    ///     sink/stream while the connections are still alive) and frees.
    /// The inbound-handler hole (b)/(c) is closed by the Switch's own teardown
    /// property: inbound handler fibers are Switch-owned and the Switch cancels
    /// them on connection teardown, so once a connection is closed no handler can
    /// post to the router. Given the precondition (connections torn down first),
    /// no handler survives to touch the freed router; the dangling service-object
    /// `router` field is likewise only reached via `openInbound` on a new inbound
    /// stream, which cannot arrive once the connections are gone.
    pub fn deinit(self: *Gossipsub) void {
        self.sw.clearPeerEventCallback();
        self.router.destroy();
        self.allocator.destroy(self);
    }

    /// Number of peers the router currently tracks. Used by tests to observe the
    /// per-peer I/O lifecycle.
    pub fn peerCount(self: *const Gossipsub) usize {
        return self.router.peerCount();
    }

    // The Switch peer-event callbacks: ctx is the *Router. Each posts one Command
    // to the router inbox and returns. `putOne` blocks only if the inbox is full
    // (it never drops): the inbox is sized large enough that a full inbox is not
    // expected in practice, so the post is effectively non-blocking, but the
    // blocking variant is the safe choice — dropping a `peer_disconnected` would
    // leak a peer. The callbacks must stay cheap (just this post); on a closed
    // inbox the post fails and is swallowed (the router is shutting down anyway).

    fn onConnected(ctx: *anyopaque, peer: PeerId, conn: *SwitchConnection, remote_addr: std.Io.net.IpAddress) void {
        const router: *Router = @ptrCast(@alignCast(ctx));
        router.inbox.putOne(router.io, .{ .peer_connected = .{
            .peer = peer,
            .conn = conn,
            .remote_addr = remote_addr,
        } }) catch {};
    }

    fn onDisconnected(ctx: *anyopaque, peer: PeerId) void {
        const router: *Router = @ptrCast(@alignCast(ctx));
        router.inbox.putOne(router.io, .{ .peer_disconnected = .{ .peer = peer } }) catch {};
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const identity = @import("../../identity.zig");
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;
const io_time = @import("../../quic/io/time.zig");

test "two gossipsub nodes wire up per-peer I/O on connect and tear down on disconnect" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var server_key = try identity.KeyPair.generate(.ED25519);
    defer server_key.deinit();
    var client_key = try identity.KeyPair.generate(.ED25519);
    defer client_key.deinit();

    const server_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &server_key, .{});
    defer server_endpoint.deinit();
    const client_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &client_key, .{});
    defer client_endpoint.deinit();

    const server = try Switch.init(allocator, io, server_endpoint);
    defer server.deinit();
    const client = try Switch.init(allocator, io, client_endpoint);
    defer client.deinit();

    // Construct a gossipsub on each switch BEFORE dialing so each one's peer-event
    // callback is registered when the connection comes up.
    const server_gs = try Gossipsub.init(allocator, io, server);
    var server_gs_live = true;
    defer if (server_gs_live) server_gs.deinit();
    const client_gs = try Gossipsub.init(allocator, io, client);
    var client_gs_live = true;
    defer if (client_gs_live) client_gs.deinit();

    var listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer listen_addr.deinit(allocator);
    try server.listen(listen_addr);
    var client_listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer client_listen_addr.deinit(allocator);
    try client.listen(client_listen_addr);

    var addrs = try server.listenMultiaddrs(allocator);
    defer {
        for (addrs.items) |addr| allocator.free(addr);
        addrs.deinit(allocator);
    }
    var dial_addr = try Multiaddr.fromString(allocator, addrs.items[0]);
    defer dial_addr.deinit(allocator);

    const client_conn = try client.dial(dial_addr, .{});
    var client_conn_live = true;
    defer if (client_conn_live) client_conn.deinit();

    const server_conn = try server.accept();
    var server_conn_live = true;
    defer if (server_conn_live) server_conn.deinit();

    // Both routers must observe the peer and bring its per-peer I/O up. peer_count
    // reaching 1 on BOTH sides proves: peer_connected was processed, the writer
    // opened (or is keeping) its outbound /meshsub stream (an open-exhaustion give-up
    // would post peer_disconnected and drop the count back to 0), and the inbound
    // handler is running. Poll with a bounded timeout for the cross-fiber connect.
    var waited_ms: u64 = 0;
    while (waited_ms < 2000) : (waited_ms += 10) {
        if (server_gs.peerCount() == 1 and client_gs.peerCount() == 1) break;
        io_time.ms(10).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), server_gs.peerCount());
    try std.testing.expectEqual(@as(usize, 1), client_gs.peerCount());

    // Tear the client connection down: the client router sees peer_disconnected
    // (and the server router observes its own peer drop once its connection ends).
    client_conn.deinit();
    client_conn_live = false;
    server_conn.deinit();
    server_conn_live = false;

    // After both connections are gone, both routers should drop back to 0 peers.
    waited_ms = 0;
    while (waited_ms < 2000) : (waited_ms += 10) {
        if (server_gs.peerCount() == 0 and client_gs.peerCount() == 0) break;
        io_time.ms(10).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 0), client_gs.peerCount());
    try std.testing.expectEqual(@as(usize, 0), server_gs.peerCount());

    // Shut the routers down before the switches/endpoints (the deinit order the
    // gossipsub service requires). The remaining `defer`s free switches/endpoints.
    //
    // Order matters and satisfies Gossipsub.deinit's precondition: both
    // connections were already torn down above (peerCount dropped to 0 on both
    // sides), so by the time we deinit each gossipsub no inbound `/meshsub`
    // handler fiber is alive and no connect/disconnect can fire. Gossipsub.deinit
    // then clears the Switch peer-event callback before freeing the router, so the
    // Switch holds no live reference to the freed router.
    server_gs.deinit();
    server_gs_live = false;
    client_gs.deinit();
    client_gs_live = false;
}
