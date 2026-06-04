//! The gossipsub Router actor: a single fiber that owns ALL per-peer state
//! lock-free (the go-libp2p processLoop model). Every event — a peer connecting
//! or disconnecting, an inbound RPC arriving, a shutdown request — reaches the
//! router as a `Command` on its one inbox queue. Producers (the Switch's
//! peer-event callback, the per-stream inbound handlers) only POST commands;
//! they never touch router state, so the single fiber serialises all mutation
//! with no locks.
//!
//! This layer is lifecycle + per-peer I/O wiring only: on connect it opens a
//! per-peer outbound `/meshsub/1.1.0` stream (lazily, via a writer fiber draining
//! an OutboundQueue) and starts reading the peer's inbound stream; on disconnect
//! it tears that down cleanly. There is no subscribe/publish/forwarding/mesh
//! logic here — inbound RPCs are currently freed on arrival. Message handling is
//! a later layer that will dispatch on the command in `inbound_rpc`.

const std = @import("std");
const protocols = @import("../../protocols.zig");
const quic = @import("../../quic.zig");
const swarm = @import("../../switch.zig");
const pubsub = @import("pubsub.zig");
const peer_io = @import("peer_io.zig");
const io_time = @import("../../quic/io/time.zig");
const PeerId = @import("peer_id").PeerId;

const Switch = swarm.Switch;
const SwitchConnection = swarm.SwitchConnection;
const ProtocolId = protocols.ProtocolId;
const Stream = quic.Stream;

/// The router inbox holds at most this many un-processed commands. The single
/// fiber drains it continuously, so this only needs to absorb bursts of
/// connect/disconnect/inbound-rpc events between drains.
const inbox_capacity = 256;

/// A peer key usable as an AutoHashMap key. A PeerId is `[64]u8` plus a length;
/// the bytes past `len` are undefined, so the struct is not directly hashable.
/// Zero-padding the unused tail makes a fixed-size, content-defined key (two
/// peer ids with the same meaningful prefix and length hash and compare equal).
const PeerKey = [64]u8;

fn peerKey(peer: *const PeerId) PeerKey {
    var key: PeerKey = [_]u8{0} ** 64;
    @memcpy(key[0..peer.len], peer.bytes[0..peer.len]);
    return key;
}

/// Drains a peer's OutboundQueue onto its `/meshsub/1.1.0` stream. The sink owns
/// the current QUIC stream and re-opens it lazily; it satisfies the PeerWriter
/// Sink contract (open / writeFrame / close, with close idempotent).
const StreamSink = struct {
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
const StreamSource = struct {
    stream: *Stream,

    pub fn read(self: *StreamSource, allocator: std.mem.Allocator, io: std.Io) !pubsub.OwnedRpc {
        return pubsub.readRpc(allocator, io, self.stream);
    }
};

/// Posts inbound RPCs to the router's single Command inbox by wrapping each in
/// the `inbound_rpc` Command variant. This is the bridge the generalised
/// PeerReader posts through, so inbound RPCs share one ordered queue with every
/// other router event.
const InboxPoster = struct {
    router: *Router,

    pub fn post(self: *InboxPoster, io: std.Io, rpc: peer_io.InboundRpc) anyerror!void {
        return self.router.inbox.putOne(io, .{ .inbound_rpc = rpc });
    }
};

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
        var poster = InboxPoster{ .router = self.router };
        var reader = peer_io.PeerReader(StreamSource, InboxPoster){
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

/// A command posted to the router's single inbox. Producers post; the router
/// fiber processes. (subscribe/publish/heartbeat variants arrive in later
/// layers.)
const Command = union(enum) {
    /// A peer's connection is up (handshake done, peer id known). The
    /// `*SwitchConnection` stays valid until the matching `peer_disconnected`.
    peer_connected: struct {
        peer: PeerId,
        conn: *SwitchConnection,
        remote_addr: std.Io.net.IpAddress,
    },
    /// A peer's connection is gone. Tear the peer down.
    peer_disconnected: struct { peer: PeerId },
    /// An RPC arrived on a peer's inbound stream. The router owns it and must
    /// free it (no forwarding yet).
    inbound_rpc: peer_io.InboundRpc,
    /// Stop the router: tear down every peer, then exit the main fiber.
    shutdown,
};

/// All state for one connected peer, owned by the router fiber. The writer fiber
/// drains `queue` through `sink`; the writer's lifetime is the `writer_future`.
const PeerState = struct {
    peer: PeerId,
    /// Borrowed; valid until peer_disconnected (the Switch owns it).
    conn: *SwitchConnection,
    queue: peer_io.OutboundQueue,
    /// Heap-owned so its address is stable while the writer fiber holds it. The
    /// sink (and its stream) MUST outlive the writer fiber — torn down only after
    /// the writer is awaited.
    sink: *StreamSink,
    writer: *Writer,
    writer_future: std.Io.Future(void),
    /// Back-pointer for the writer's on_disconnect callback, which receives this
    /// PeerState as its context and needs the router to post peer_disconnected.
    /// Set in the initializer below so it is valid before the writer fiber spawns.
    router_for_disconnect: *Router,
};

const Writer = peer_io.PeerWriter(StreamSink);

/// The single-fiber gossipsub router. Owns the peer map and inbox; the main
/// fiber serialises every mutation.
pub const Router = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    /// Borrowed; must outlive the router.
    sw: *Switch,
    inbox_storage: []Command,
    inbox: std.Io.Queue(Command),
    /// Keyed by zero-padded peer bytes (see PeerKey). Values are heap-owned.
    peers: std.AutoHashMap(PeerKey, *PeerState),
    main_future: ?std.Io.Future(void) = null,
    /// Set once when teardown begins so the main loop stops after the inbox
    /// drains/closes. Atomic because it is read on the main fiber but set on the
    /// caller's fiber (deinit path).
    stopping: std.atomic.Value(bool) = .init(false),
    /// Number of live peers, published for observers (e.g. the integration test).
    peer_count: std.atomic.Value(usize) = .init(0),

    pub fn create(allocator: std.mem.Allocator, io: std.Io, sw: *Switch) !*Router {
        const inbox_storage = try allocator.alloc(Command, inbox_capacity);
        errdefer allocator.free(inbox_storage);

        const router = try allocator.create(Router);
        router.* = .{
            .allocator = allocator,
            .io = io,
            .sw = sw,
            .inbox_storage = inbox_storage,
            .inbox = std.Io.Queue(Command).init(inbox_storage),
            .peers = std.AutoHashMap(PeerKey, *PeerState).init(allocator),
        };
        return router;
    }

    /// Spawn the main fiber. Call once after `create`.
    pub fn start(router: *Router) std.Io.ConcurrentError!void {
        router.main_future = try std.Io.concurrent(router.io, mainLoop, .{router});
    }

    /// Build the inbound service object that reads inbound RPCs and posts them to
    /// the inbox. The returned AnyProtocolService borrows `router` (its instance
    /// is a heap-owned InboundService destroyed via the service deinit).
    pub fn inboundService(router: *Router) !protocols.AnyProtocolService {
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

    /// Stop the router and free all of its resources. Sets the persistent
    /// stopping flag, closes the inbox to wake the main loop, posts a `shutdown`
    /// as a backstop, cancels + awaits the main fiber, then frees the inbox
    /// storage and the router. The main fiber tears down every peer on its way
    /// out, so this is safe to call from any other fiber.
    pub fn destroy(router: *Router) void {
        router.stopping.store(true, .release);

        // Post shutdown first (so a main loop parked in getOne wakes and runs the
        // peer teardown on its own fiber), then close the inbox. close() alone
        // would make getOne return Closed before processing shutdown, but the
        // main loop also tears peers down on Closed via `teardownAllPeers`, so
        // either path is safe. Use the uncancelable post to avoid losing it.
        router.inbox.putOneUncancelable(router.io, .shutdown) catch {};

        if (router.main_future) |*future| {
            // The main loop exits on shutdown / closed inbox; cancel is a backstop
            // in case it is parked in a non-inbox cancellation point. Every
            // blocking point in the loop is a cancel point, so this cannot
            // re-park. cancel before await so a parked loop unparks.
            future.cancel(router.io);
            future.await(router.io);
            router.main_future = null;
        } else {
            // Never spawned: tear down directly.
            router.teardownAllPeers();
        }

        router.peers.deinit();
        router.allocator.free(router.inbox_storage);
        router.allocator.destroy(router);
    }

    pub fn peerCount(router: *const Router) usize {
        return router.peer_count.load(.acquire);
    }

    // ----- main fiber ------------------------------------------------------

    fn mainLoop(router: *Router) void {
        // On any exit (shutdown, closed inbox, cancellation) tear down every peer
        // and drain the inbox so nothing leaks.
        defer {
            router.teardownAllPeers();
            router.drainInbox();
        }

        while (true) {
            const command = router.inbox.getOne(router.io) catch return; // Closed/Canceled
            switch (command) {
                .peer_connected => |c| router.onPeerConnected(c.peer, c.conn, c.remote_addr),
                .peer_disconnected => |c| router.onPeerDisconnected(c.peer),
                .inbound_rpc => |rpc| router.onInboundRpc(rpc),
                .shutdown => return,
            }
        }
    }

    /// Handle a peer connecting: dedup, then create per-peer state and spawn its
    /// writer fiber. The writer opens the outbound stream lazily on its first
    /// frame; on open-exhaustion it posts `peer_disconnected` so the peer is torn
    /// down through the normal path.
    fn onPeerConnected(router: *Router, peer: PeerId, conn: *SwitchConnection, remote_addr: std.Io.net.IpAddress) void {
        _ = remote_addr;
        const key = peerKey(&peer);
        // A second connection to a peer we already track: keep the first, ignore
        // the new one's gossipsub setup. One logical peer entry.
        if (router.peers.contains(key)) return;

        const state = router.allocator.create(PeerState) catch return;
        var state_live = false;
        defer if (!state_live) router.allocator.destroy(state);

        const sink = router.allocator.create(StreamSink) catch return;
        var sink_live = false;
        defer if (!sink_live) router.allocator.destroy(sink);

        const writer = router.allocator.create(Writer) catch return;
        var writer_live = false;
        defer if (!writer_live) router.allocator.destroy(writer);

        sink.* = .{ .conn = conn, .proto = pubsub.protocol_id };
        state.* = .{
            .peer = peer,
            .conn = conn,
            .queue = peer_io.OutboundQueue.init(router.allocator, .{}),
            .sink = sink,
            .writer = writer,
            .writer_future = undefined,
            // Set up-front (not on a later line) so the back-pointer is valid
            // before the writer fiber spawns and can fire on_disconnect.
            .router_for_disconnect = router,
        };
        // Past this point `state.queue` is initialised; deinit it on any failure
        // before the writer fiber takes ownership of draining it.
        var queue_live = false;
        defer if (!queue_live) state.queue.deinit(router.io);

        writer.* = .{
            .queue = &state.queue,
            .sink = sink,
            .allocator = router.allocator,
            .on_disconnect = onWriterDisconnect,
            .disconnect_ctx = state,
        };
        // The writer reaches the router via `state.router_for_disconnect` (set in
        // the PeerState initializer above) and tags peer_disconnected with
        // `state.peer`, recovering both from `disconnect_ctx`.

        const future = std.Io.concurrent(router.io, Writer.run, .{ writer, router.io }) catch return;

        // All fallible steps done: the entry is live. Disarm the cleanups so the
        // PeerState (and the writer fiber draining its queue) survives.
        state.writer_future = future;
        router.peers.put(key, state) catch {
            // The map insert failed after the writer fiber started. Tear the
            // writer down cleanly (close queue, cancel+await fiber) before
            // freeing, so we don't leak the fiber or use-after-free the sink.
            // Cancel before await so a writer parked in its reopen backoff does
            // not stall this fiber; the backoff sleep is a cancellation point.
            state.queue.close(router.io);
            var f = future;
            f.cancel(router.io);
            f.await(router.io);
            state.sink.close(router.io);
            return; // defers free writer/sink/state; queue deinit fires too
        };
        state_live = true;
        sink_live = true;
        writer_live = true;
        queue_live = true;
        _ = router.peer_count.fetchAdd(1, .release);
    }

    /// Handle a peer disconnecting: look up, tear down its writer + state. Absent
    /// peer (already removed, or a dedup'd second connection) is a no-op.
    fn onPeerDisconnected(router: *Router, peer: PeerId) void {
        const key = peerKey(&peer);
        const entry = router.peers.fetchRemove(key) orelse return;
        router.teardownPeer(entry.value);
        _ = router.peer_count.fetchSub(1, .release);
    }

    /// Handle an inbound RPC: no forwarding yet, so just free it. Freed whether
    /// or not the peer is still tracked (proves the reader runs + posts without
    /// leaking).
    fn onInboundRpc(router: *Router, rpc: peer_io.InboundRpc) void {
        var owned = rpc;
        owned.rpc.deinit(router.allocator);
    }

    /// Tear down one peer's state. Order matters: close the queue (the writer
    /// drains remaining frames and exits), then CANCEL+AWAIT the writer fiber
    /// BEFORE freeing the sink (the writer's trailing `sink.close` must not race a
    /// freed sink), then close the sink, deinit the queue, and free the heap
    /// allocations.
    ///
    /// Cancel before await so a writer parked in `ensureStream`'s reopen backoff
    /// does not stall this single router fiber for up to
    /// max_open_retries × reopen_backoff_ms (which would serialize every peer's
    /// teardown). The backoff `sleep` is a cancellation point, so cancel collapses
    /// the wait; await then joins. cancel+await on the same Future is safe here:
    /// cancel is idempotent and clears the future, so the following await returns
    /// the cached result without double-consuming. Under cancellation the writer
    /// unwinds cleanly — popBlocking returns Closed (queue already closed) or
    /// Canceled, or a Cancelable surfaces from open/writeFrame/sleep — runs its
    /// `defer sink.close`, and returns; the router's own `sink.close` below is
    /// idempotent.
    fn teardownPeer(router: *Router, state: *PeerState) void {
        state.queue.close(router.io);
        state.writer_future.cancel(router.io);
        state.writer_future.await(router.io);
        state.sink.close(router.io);
        state.queue.deinit(router.io);
        router.allocator.destroy(state.writer);
        router.allocator.destroy(state.sink);
        router.allocator.destroy(state);
    }

    fn teardownAllPeers(router: *Router) void {
        var it = router.peers.iterator();
        while (it.next()) |entry| {
            router.teardownPeer(entry.value_ptr.*);
            _ = router.peer_count.fetchSub(1, .release);
        }
        router.peers.clearRetainingCapacity();
    }

    /// Drain and free any commands still buffered in the inbox after the loop
    /// exits, so an inbound RPC posted concurrently with teardown is not leaked.
    fn drainInbox(router: *Router) void {
        router.inbox.close(router.io);
        var buf: [16]Command = undefined;
        while (true) {
            const n = router.inbox.getUncancelable(router.io, &buf, 0) catch return;
            if (n == 0) return;
            for (buf[0..n]) |command| switch (command) {
                .inbound_rpc => |rpc| {
                    var owned = rpc;
                    owned.rpc.deinit(router.allocator);
                },
                else => {},
            };
        }
    }
};

/// PeerWriter on_disconnect callback. The writer exhausted its open retries, so
/// hand the peer back to the router by posting `peer_disconnected`. Runs on the
/// writer fiber, so it must only post — never free the state/sink (the writer's
/// trailing `sink.close` still fires after this returns).
fn onWriterDisconnect(ctx: ?*anyopaque) void {
    const state: *PeerState = @ptrCast(@alignCast(ctx.?));
    const router = state.router_for_disconnect;
    router.inbox.putOneUncancelable(router.io, .{ .peer_disconnected = .{ .peer = state.peer } }) catch {};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const identity = @import("../../identity.zig");
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;
const gossipsub = @import("gossipsub.zig");

test "peerKey distinguishes peers and matches equal ids" {
    const a = try PeerId.random();
    var b_bytes = a.bytes;
    b_bytes[0] +%= 1;
    const b = PeerId{ .bytes = b_bytes, .len = a.len };

    const ka = peerKey(&a);
    const ka2 = peerKey(&a);
    const kb = peerKey(&b);
    try std.testing.expectEqual(ka, ka2);
    try std.testing.expect(!std.mem.eql(u8, &ka, &kb));
}

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
    const server_gs = try gossipsub.Gossipsub.init(allocator, io, server);
    var server_gs_live = true;
    defer if (server_gs_live) server_gs.deinit();
    const client_gs = try gossipsub.Gossipsub.init(allocator, io, client);
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
