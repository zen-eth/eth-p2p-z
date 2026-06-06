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
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;

const Switch = swarm.Switch;
const SwitchConnection = swarm.SwitchConnection;
const ProtocolId = protocols.ProtocolId;
const Stream = quic.Stream;

/// Drains a peer's OutboundQueue onto its `/meshsub` stream. The sink owns the
/// current QUIC stream and re-opens it lazily; it satisfies the PeerWriter Sink
/// contract (open / writeFrame / close, with close idempotent).
///
/// On open it proposes the full `/meshsub` version list (1.2.0, 1.1.0, 1.0.0)
/// and uses whichever the peer accepts, recording the negotiated protocol id in
/// `selected` so the router can learn our outbound version for the peer.
pub const StreamSink = struct {
    conn: *SwitchConnection,
    /// The protocol id the peer accepted on the most recent open, or null before
    /// the first successful open. A pointer into `pubsub.supported_protocols`
    /// (multistream returns the matched candidate, not a copy), so it is stable.
    selected: ?ProtocolId = null,
    /// The currently-open outbound stream, or null when none is open.
    current: ?*Stream = null,

    /// (Re)establish the outbound stream by negotiating the best common /meshsub
    /// version on a new QUIC stream. Leaves `current` and `selected` set on
    /// success; on failure both are untouched (the writer only opens when it has
    /// none).
    pub fn open(self: *StreamSink, io: std.Io) !void {
        _ = io;
        const result = try self.conn.openProtocolStreamMulti(&pubsub.supported_protocols, .{});
        self.current = result.stream;
        self.selected = result.selected;
        std.log.info("gossipsub: outbound stream negotiated {s}", .{result.selected});
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
/// `StreamSink` over a `*SwitchConnection`, and dials peers on the router's
/// behalf. The router stays generic over this (see router.zig's Transport
/// contract); it asks for `makeSink` and `dial`, plus the `Sink`/`ConnHandle`
/// types.
///
/// Held by value in the router but NOT stateless: it borrows the `*Switch` (to
/// dial + to start inbound dispatch on a dialed connection) and a `*std.Io.Group`
/// that OWNS the detached dial fibers. The group lives in the `Gossipsub` handle
/// so its `deinit` can await/cancel every dial fiber — the router never joins
/// them. `makeSink` allocates the sink but does no I/O (the writer fiber opens
/// the stream lazily on its first frame).
pub const SwitchTransport = struct {
    /// Borrowed; must outlive the router. Used by `dial` to establish a
    /// connection and start its inbound dispatcher.
    sw: *Switch,
    /// Borrowed; owned by the `Gossipsub` handle. Every detached dial fiber is
    /// spawned into this group; `Gossipsub.deinit` cancels + awaits it, so no
    /// dial fiber is leaked or left unjoined.
    dial_group: *std.Io.Group,
    /// Allocator for the dial fiber's transient work (parsing the multiaddr).
    allocator: std.mem.Allocator,

    pub const ConnHandle = *SwitchConnection;
    pub const Sink = StreamSink;

    pub fn makeSink(self: *SwitchTransport, allocator: std.mem.Allocator, peer: PeerId, conn: ConnHandle) !*StreamSink {
        _ = self;
        _ = peer;
        const sink = try allocator.create(StreamSink);
        sink.* = .{ .conn = conn };
        return sink;
    }

    /// Fire-and-forget dial of the multiaddr STRING `addr`. Spawns a detached
    /// fiber (tracked in `dial_group`) that parses the address, dials it through
    /// the Switch, and on success starts the connection's inbound dispatcher so
    /// inbound /meshsub streams are read — mirroring the manual dial in the 2-node
    /// tests. Success surfaces as the Switch's peer-event callback firing
    /// `onConnected` → the router's `onPeerConnected` (the normal connect path
    /// that opens the outbound stream). On any failure the fiber logs and returns;
    /// the router re-dials a still-disconnected direct peer on its next tick. The
    /// dialed connection is owned by the Switch (registered in its connection
    /// list, torn down by `Switch.deinit`), so the fiber does not retain it.
    ///
    /// If the spawn itself fails (executor exhaustion) the dial is dropped with a
    /// log — the next direct-connect tick retries — so a transient spawn failure
    /// never crashes the router fiber that called this.
    pub fn dial(self: *SwitchTransport, io: std.Io, addr: []const u8) void {
        // Copy the address: the borrowed `addr` (a router-owned direct-peer
        // string) outlives this call, but the detached fiber runs past it, so it
        // gets its own copy and frees it. On OOM, drop the dial (retried next tick).
        const addr_copy = self.allocator.dupe(u8, addr) catch {
            std.log.warn("gossipsub: dial dropped (out of memory copying address)", .{});
            return;
        };
        // The dial fiber dials via `sw.io` (the Switch's own Io); it needs no
        // separate Io, so only `sw`, the allocator, and the owned address are
        // passed. `io` here is just the spawn context for `concurrent`.
        self.dial_group.concurrent(io, dialFiber, .{ self.sw, self.allocator, addr_copy }) catch {
            std.log.warn("gossipsub: dial dropped (could not spawn dial fiber)", .{});
            self.allocator.free(addr_copy);
        };
    }
};

/// Detached dial-fiber body. Parses `addr` (owned, freed here), dials it through
/// `sw`, and on success starts the connection's inbound dispatcher. All failures
/// are logged and dropped — a direct peer is re-dialed by the router on its next
/// direct-connect tick. The connection is owned by the Switch once dialed (it is
/// registered there and torn down by `Switch.deinit`), so this fiber does not
/// hold or free it.
fn dialFiber(sw: *Switch, allocator: std.mem.Allocator, addr: []u8) void {
    defer allocator.free(addr);

    var ma = Multiaddr.fromString(allocator, addr) catch |err| {
        std.log.warn("gossipsub: dial failed to parse {s}: {s}", .{ addr, @errorName(err) });
        return;
    };
    defer ma.deinit(allocator);

    const conn = sw.dial(ma, .{}) catch |err| {
        std.log.info("gossipsub: dial to {s} failed: {s}", .{ addr, @errorName(err) });
        return;
    };
    // The peer-event callback already fired `onConnected` synchronously inside
    // `sw.dial` (before it returned), so the router is wiring the peer up. Start
    // the inbound dispatcher so inbound /meshsub streams dispatch; a failure here
    // leaves the connection up (the peer is still connected) but without inbound
    // reads — logged, and the connection lives on under the Switch.
    conn.startInboundDispatcher(.{}) catch |err| {
        std.log.warn("gossipsub: dial to {s} connected but inbound dispatch failed: {s}", .{ addr, @errorName(err) });
    };
}

const Router = router_mod.Router(SwitchTransport);

/// The gossipsub heartbeat period (go-libp2p default: one second). The router
/// runs a heartbeat fiber on this interval to age out backoffs (and, in later
/// layers, to maintain the mesh).
const heartbeat_interval_ms: u64 = 1000;

/// Re-export so callers construct a handler without importing router.zig.
pub const MessageHandler = router_mod.MessageHandler;

/// Re-export the optional peer-scoring config type (and a ready-made baseline)
/// so callers can opt into scoring without importing router.zig / score.zig.
/// Pass `null` to `Gossipsub.init` to leave scoring disabled (the default — no
/// events, no gates, behaviour unchanged), or `Gossipsub.default_score_config`
/// to enable it with the documented baseline params/thresholds.
pub const ScoreConfig = router_mod.ScoreConfig;

/// Re-export the router behaviour config (flood-publish, signature policy, etc.)
/// so callers can tune it without importing router.zig. Pass `.{}` to
/// `Gossipsub.init` for the go-libp2p defaults (flood-publish ON, signature
/// policy inferred from the host key).
pub const RouterConfig = router_mod.RouterConfig;

/// Re-export the direct-peer descriptor (peer id + multiaddr string to dial) so
/// a caller can populate `RouterConfig.direct_peers` without importing
/// router.zig. The router keeps each configured direct peer connected itself
/// (dials at start, re-dials disconnected ones on a tick).
pub const DirectPeer = router_mod.DirectPeer;

/// Re-export the signature-policy enum (and the message-id override types) so a
/// caller can select `anonymous` (StrictNoSign) — or supply a custom message-id
/// function — via `RouterConfig` without importing router.zig.
pub const SignaturePolicy = router_mod.SignaturePolicy;
pub const MessageIdFn = router_mod.MessageIdFn;
pub const MessageIdConfig = router_mod.MessageIdConfig;

const score_mod = @import("score.zig");
pub const default_score_config: ScoreConfig = .{
    .params = score_mod.default_params,
    .thresholds = score_mod.default_thresholds,
};

/// The gossipsub stream protocol id (`/meshsub/1.1.0`). Re-exported so callers
/// holding only this module (e.g. an identify responder advertising which
/// protocols this node speaks) can name it without importing pubsub.zig.
pub const protocol_id = pubsub.protocol_id;

/// Every `/meshsub` version this node speaks, newest-first (1.2.0, 1.1.0,
/// 1.0.0). Re-exported so an identify responder can advertise the full set (a
/// peer then negotiates the best common version) without importing pubsub.zig.
pub const supported_protocols = pubsub.supported_protocols;

/// Per-stream inbound handler. The Switch's dispatcher creates one of these per
/// inbound `/meshsub/1.1.0` stream (capturing the sender's peer id) and runs it
/// on a handler fiber the Switch owns and cancels on connection teardown. The
/// handler reads RPCs and posts them to the router inbox until the stream ends.
const InboundHandler = struct {
    router: *Router,
    peer: PeerId,
    /// The /meshsub version the peer negotiated on THIS inbound stream (parsed
    /// from the protocol id it selected). The peer proposes its best version and
    /// we accept it, so this is the highest version that peer supports.
    version: pubsub.Version,

    /// Fiber body run by the Switch. Reads + posts until the stream breaks.
    fn run(self: *InboundHandler, io: std.Io, stream: *Stream) anyerror!void {
        std.log.info("gossipsub: inbound stream negotiated {s}", .{@tagName(self.version)});
        // Tell the router which /meshsub version this peer speaks before reading,
        // so the per-peer version is recorded as soon as the peer reaches us
        // (even if its RPCs are slow to follow). Best-effort post.
        self.router.postPeerProtocol(io, self.peer, self.version);

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
        handler.* = .{
            .router = self.router,
            .peer = ctx.peer_id,
            // ctx.protocol_id is the /meshsub version this stream negotiated.
            .version = pubsub.Version.fromProtocolId(ctx.protocol_id),
        };
        return protocols.ownedProtocolStreamHandler(InboundHandler, InboundHandler.run, handler);
    }
};

/// The gossipsub service handle. Owns the router; borrows the Switch (which must
/// outlive this). Construct one per Switch before any dial/accept so its
/// peer-event callback is registered when connections come up.
pub const Gossipsub = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    /// Borrowed; must outlive the router (the router's per-peer state borrows
    /// the Switch's connections, and the Switch holds the peer-event callback
    /// and inbound service that reference the router). Cleared via this on
    /// deinit before the router is freed.
    sw: *Switch,
    router: *Router,
    /// Owns the detached dial fibers the `SwitchTransport.dial` spawns (direct
    /// peer auto-connect). Heap-owned so its address is stable for the router's
    /// by-value `SwitchTransport`, which holds a `*std.Io.Group` into it. Cancel
    /// + awaited in `deinit` (before the router is freed) so no dial fiber leaks
    /// or outlives the Gossipsub.
    dial_group: *std.Io.Group,

    /// Construct the router, start its fiber, register the inbound service and
    /// the peer-event callback on the Switch. On any failure the router is torn
    /// down so nothing leaks.
    ///
    /// `local_peer` is this node's own peer id (used as Message.from on publish).
    /// The Switch/QuicEndpoint here does not expose a local-peer-id accessor (the
    /// endpoint does not even retain the host KeyPair), so the caller — which owns
    /// the KeyPair — passes it in (e.g. `try key.peerId(allocator)`).
    ///
    /// `host_key` (together with `config.signature_policy`) selects the signature
    /// policy. By default (`config.signature_policy == null`) the key infers it:
    /// pass the node's host KeyPair (the same one used for the endpoint identity)
    /// to enable StrictSign — outbound messages are signed and inbound messages
    /// are verified + rejected if their signature is invalid or absent, required
    /// for interop with go-libp2p (whose default is StrictSign) — or pass null for
    /// the none policy (from+seqno, no signature, no verification). When a key is
    /// given the router derives `local_peer` from it. To run anonymously
    /// (StrictNoSign — published messages carry only topic+data, no peer-id,
    /// content-based message-ids) pass `null` for `host_key` AND set
    /// `config.signature_policy = .anonymous`; a key + anonymous is rejected. The
    /// key is BORROWED and must outlive this Gossipsub.
    ///
    /// `message_handler` (optional) receives messages delivered on topics this
    /// node subscribes to; see router's MessageHandler for the call contract.
    ///
    /// `score_config` (optional) enables gossipsub peer scoring: pass null (the
    /// default behaviour, and what the interop binary uses) to leave scoring off
    /// — no scoring events, no score-based gates — or pass a config (e.g.
    /// `Gossipsub.default_score_config`) to have the router score peers and gate
    /// its mesh/gossip/graylist decisions on those scores.
    ///
    /// `config` tunes router behaviour (e.g. flood-publish); pass `.{}` for the
    /// go-libp2p defaults (flood-publish ON, matching go's out-of-the-box node).
    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        sw: *Switch,
        local_peer: PeerId,
        host_key: ?*const identity.KeyPair,
        message_handler: ?MessageHandler,
        score_config: ?ScoreConfig,
        config: RouterConfig,
    ) !*Gossipsub {
        // The dial-fiber group must outlive the router's by-value SwitchTransport
        // (which holds a pointer into it) and be awaited in deinit, so heap-own it
        // up front with a stable address. This `destroy` errdefer is declared
        // first so it runs LAST on the error path — after the cancel errdefer
        // below joins every dial fiber, since a group with running fibers must not
        // be freed.
        const dial_group = try allocator.create(std.Io.Group);
        errdefer allocator.destroy(dial_group);
        dial_group.* = .init;

        const transport = SwitchTransport{
            .sw = sw,
            .dial_group = dial_group,
            .allocator = allocator,
        };
        const router = try Router.create(allocator, io, transport, local_peer, message_handler, heartbeat_interval_ms, host_key, score_config, config);
        errdefer router.destroy();

        // Cancel + await any spawned dial fibers BEFORE the router is destroyed on
        // the error path. Declared after `router.destroy`'s errdefer so it runs
        // FIRST (LIFO): a dial fiber mid-`sw.dial` can fire `onConnected` into the
        // router, so it must be joined before the router is freed. Idempotent —
        // deinit's own cancel on the success path is a second (no-op) call, and a
        // group with no spawned fibers cancels for free.
        errdefer dial_group.cancel(io);

        // Register the inbound service for EVERY /meshsub version we speak so we
        // accept inbound streams from 1.0/1.1/1.2 peers alike. Each id gets its
        // own heap-owned InboundService instance (all pointing at the same
        // router) so the Switch can free each independently — registering one
        // shared instance under several keys would double-free it on teardown.
        // The negotiated version reaches the handler via ctx.protocol_id.
        for (pubsub.supported_protocols) |proto| {
            const service = try inboundService(router);
            var service_owned = true;
            // If addProtocolService fails it does NOT take ownership, so free the
            // service object ourselves on that path.
            errdefer if (service_owned) service.deinit();
            try sw.addProtocolService(proto, service);
            service_owned = false;
        }

        // Register the peer-event callback BEFORE starting the router: start()
        // dials the configured direct peers, and a dial that connects fires this
        // callback (onConnected) synchronously. Registering first guarantees the
        // connect is observed by the router rather than dropped.
        sw.setPeerEventCallback(.{
            .ctx = router,
            .on_connected = onConnected,
            .on_disconnected = onDisconnected,
        });
        errdefer sw.clearPeerEventCallback();

        // Start the router last: spawns its fibers and dials the direct peers (via
        // the transport's dial_group, awaited in deinit).
        try router.start();

        const self = try allocator.create(Gossipsub);
        self.* = .{ .allocator = allocator, .io = io, .sw = sw, .router = router, .dial_group = dial_group };
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
    ///
    /// FIRST cancel + await the dial-fiber group: a detached direct-peer dial
    /// fiber borrows the Switch (to dial / start dispatch) and would fire
    /// `onConnected` into the router. Joining it before clearing the callback and
    /// freeing the router ensures no dial fiber is left running (no leak) and none
    /// touches the router or Switch after this returns. `cancel` collapses any
    /// in-flight dial (the dial's blocking points are cancellation points) and
    /// joins; it is idempotent, so an init-path `errdefer` having already called
    /// it is harmless.
    pub fn deinit(self: *Gossipsub) void {
        self.dial_group.cancel(self.io);
        self.sw.clearPeerEventCallback();
        self.router.destroy();
        self.allocator.destroy(self.dial_group);
        self.allocator.destroy(self);
    }

    /// Number of peers the router currently tracks. Used by tests to observe the
    /// per-peer I/O lifecycle.
    pub fn peerCount(self: *const Gossipsub) usize {
        return self.router.peerCount();
    }

    /// Subscribe the local node to `topic`. Dups the topic into an owned payload
    /// and posts a `subscribe` command; the router announces the subscription to
    /// every peer and begins delivering matching messages to the handler. On a
    /// closed inbox (shutting down) the dup is freed and the call is a no-op.
    pub fn subscribe(self: *Gossipsub, topic: []const u8) !void {
        const owned = try self.allocator.dupe(u8, topic);
        errdefer self.allocator.free(owned);
        self.router.inbox.putOne(self.router.io, .{ .subscribe = .{ .topic = owned } }) catch |err| switch (err) {
            error.Closed => self.allocator.free(owned),
            else => return err,
        };
    }

    /// Unsubscribe the local node from `topic`. Dups the topic and posts an
    /// `unsubscribe` command; the router announces the withdrawal to every peer.
    pub fn unsubscribe(self: *Gossipsub, topic: []const u8) !void {
        const owned = try self.allocator.dupe(u8, topic);
        errdefer self.allocator.free(owned);
        self.router.inbox.putOne(self.router.io, .{ .unsubscribe = .{ .topic = owned } }) catch |err| switch (err) {
            error.Closed => self.allocator.free(owned),
            else => return err,
        };
    }

    /// Publish `data` on `topic` from the local node. Dups both into owned
    /// payloads and posts a `publish` command; the router forwards the message to
    /// every subscribed peer (and delivers locally if we subscribe to `topic`).
    pub fn publish(self: *Gossipsub, topic: []const u8, data: []const u8) !void {
        const owned_topic = try self.allocator.dupe(u8, topic);
        errdefer self.allocator.free(owned_topic);
        const owned_data = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(owned_data);
        self.router.inbox.putOne(self.router.io, .{ .publish = .{
            .topic = owned_topic,
            .data = owned_data,
        } }) catch |err| switch (err) {
            error.Closed => {
                self.allocator.free(owned_topic);
                self.allocator.free(owned_data);
            },
            else => return err,
        };
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

    const server_peer = try server_key.peerId(allocator);
    const client_peer = try client_key.peerId(allocator);

    // Construct a gossipsub on each switch BEFORE dialing so each one's peer-event
    // callback is registered when the connection comes up.
    const server_gs = try Gossipsub.init(allocator, io, server, server_peer, null, null, null, .{});
    var server_gs_live = true;
    defer if (server_gs_live) server_gs.deinit();
    const client_gs = try Gossipsub.init(allocator, io, client, client_peer, null, null, null, .{});
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

/// A thread-safe recording handler for the end-to-end delivery test: the
/// router fiber (on a worker thread) calls `onMessage`; the test fiber polls
/// `received`. The captured slices are copied under a mutex; freed on deinit.
const DeliveryRecorder = struct {
    allocator: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    io: std.Io,
    received: std.atomic.Value(bool) = .init(false),
    topic: ?[]u8 = null,
    from: ?[]u8 = null,
    data: ?[]u8 = null,

    fn deinit(self: *DeliveryRecorder) void {
        if (self.topic) |t| self.allocator.free(t);
        if (self.from) |f| self.allocator.free(f);
        if (self.data) |d| self.allocator.free(d);
    }

    /// Clear the captured delivery so the recorder can accept a fresh one (used
    /// by the reconnect test to assert a SECOND publish is delivered after the
    /// connection was dropped and re-established). Runs on the test fiber while
    /// no router fiber is delivering (the test waits for the prior receipt and
    /// drops the connection before resetting).
    fn reset(self: *DeliveryRecorder) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.topic) |t| self.allocator.free(t);
        if (self.from) |f| self.allocator.free(f);
        if (self.data) |d| self.allocator.free(d);
        self.topic = null;
        self.from = null;
        self.data = null;
        self.received.store(false, .release);
    }

    fn handler(self: *DeliveryRecorder) MessageHandler {
        return .{ .ctx = self, .on_message = onMessage };
    }

    fn onMessage(ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) void {
        const self: *DeliveryRecorder = @ptrCast(@alignCast(ctx));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        // First delivery wins; ignore any later duplicate so the captured slices
        // stay stable for the test's assertions.
        if (self.received.load(.acquire)) return;
        self.topic = self.allocator.dupe(u8, topic) catch null;
        self.from = self.allocator.dupe(u8, from) catch null;
        self.data = self.allocator.dupe(u8, data) catch null;
        self.received.store(true, .release);
    }
};

test "two gossipsub nodes: subscribe propagates and a publish is delivered end to end" {
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

    const server_peer = try server_key.peerId(allocator);
    const client_peer = try client_key.peerId(allocator);

    // The client (A) is the publisher; the server (B) subscribes and records.
    // Construct both gossipsubs BEFORE dialing so the peer-event callbacks are
    // registered when the connection comes up.
    var rec = DeliveryRecorder{ .allocator = allocator, .io = io };
    defer rec.deinit();

    // StrictSign on BOTH sides (pass each node's host key): the publisher signs
    // and the subscriber verifies, exercising the full sign->verify path over
    // real QUIC. The subscriber must accept the publisher's signed message.
    const server_gs = try Gossipsub.init(allocator, io, server, server_peer, &server_key, rec.handler(), null, .{});
    var server_gs_live = true;
    defer if (server_gs_live) server_gs.deinit();
    const client_gs = try Gossipsub.init(allocator, io, client, client_peer, &client_key, null, null, .{});
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

    // Start inbound stream dispatch on BOTH connections so each side's
    // `/meshsub` inbound handler runs (the Switch does not auto-dispatch). Without
    // this the peers wire up but never exchange RPCs, so subscriptions/publishes
    // never reach the other router.
    try client_conn.startInboundDispatcher(.{});
    try server_conn.startInboundDispatcher(.{});

    // Wait for both sides to wire up the peer.
    var waited_ms: u64 = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (server_gs.peerCount() == 1 and client_gs.peerCount() == 1) break;
        io_time.ms(10).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), server_gs.peerCount());
    try std.testing.expectEqual(@as(usize, 1), client_gs.peerCount());

    // B subscribes to "t"; the subscription must propagate to A so A learns B is
    // interested (A's router tracks B under its peer_topics). Poll for that.
    try server_gs.subscribe("t");

    waited_ms = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (client_gs.router.peers.get(peerKeyOf(server_peer))) |state| {
            if (state.topics.contains("t")) break;
        }
        io_time.ms(10).sleep(io) catch {};
    }
    {
        const tracked = if (client_gs.router.peers.get(peerKeyOf(server_peer))) |state|
            state.topics.contains("t")
        else
            false;
        try std.testing.expect(tracked);
    }

    // A publishes on "t"; B's handler must receive ("t", A's id, "hello").
    try client_gs.publish("t", "hello");

    waited_ms = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (rec.received.load(.acquire)) break;
        io_time.ms(10).sleep(io) catch {};
    }
    try std.testing.expect(rec.received.load(.acquire));
    try std.testing.expectEqualSlices(u8, "t", rec.topic.?);
    try std.testing.expectEqualSlices(u8, "hello", rec.data.?);
    try std.testing.expectEqualSlices(u8, client_peer.bytes[0..client_peer.len], rec.from.?);

    // Both sides advertise [1.2, 1.1, 1.0] and accept the first proposed, so each
    // peer's inbound /meshsub stream negotiates 1.2.0 → each router records the
    // other as a 1.2 peer. A side only learns the version once the OTHER side
    // opens its outbound stream (lazy, on its first frame): the server opened on
    // its subscription announce and the client opened to publish above, so by now
    // both inbound streams exist. The version post races with peer_connected, so
    // poll for it to settle.
    waited_ms = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        const c = if (client_gs.router.peers.get(peerKeyOf(server_peer))) |st| st.protocol_version == .v1_2 else false;
        const s = if (server_gs.router.peers.get(peerKeyOf(client_peer))) |st| st.protocol_version == .v1_2 else false;
        if (c and s) break;
        io_time.ms(10).sleep(io) catch {};
    }
    {
        const client_state = client_gs.router.peers.get(peerKeyOf(server_peer)) orelse return error.MissingPeer;
        const server_state = server_gs.router.peers.get(peerKeyOf(client_peer)) orelse return error.MissingPeer;
        try std.testing.expectEqual(pubsub.Version.v1_2, client_state.protocol_version);
        try std.testing.expectEqual(pubsub.Version.v1_2, server_state.protocol_version);
    }

    // Tear down in the gossipsub-required order: close connections first (so no
    // inbound /meshsub handler survives and no peer event can fire), then deinit
    // each gossipsub (clears the Switch callback + frees the router) before the
    // switches/endpoints are freed by the remaining defers.
    client_conn.deinit();
    client_conn_live = false;
    server_conn.deinit();
    server_conn_live = false;

    waited_ms = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (server_gs.peerCount() == 0 and client_gs.peerCount() == 0) break;
        io_time.ms(10).sleep(io) catch {};
    }

    server_gs.deinit();
    server_gs_live = false;
    client_gs.deinit();
    client_gs_live = false;
}

test "two gossipsub nodes: pub/sub survives a drop+reconnect of the connection" {
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

    const server_peer = try server_key.peerId(allocator);
    const client_peer = try client_key.peerId(allocator);

    // Client (A) publishes; server (B) subscribes to "t" and records deliveries.
    var rec = DeliveryRecorder{ .allocator = allocator, .io = io };
    defer rec.deinit();

    const server_gs = try Gossipsub.init(allocator, io, server, server_peer, &server_key, rec.handler(), null, .{});
    var server_gs_live = true;
    defer if (server_gs_live) server_gs.deinit();
    const client_gs = try Gossipsub.init(allocator, io, client, client_peer, &client_key, null, null, .{});
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

    // B subscribes ONCE, up front. `my_topics` persists across a disconnect, so
    // on every (re)connect the router re-announces "t" to the freshly-wired peer
    // automatically — no re-subscribe is needed after the reconnect.
    try server_gs.subscribe("t");

    // Run the connect → propagate-subscription → publish → receive cycle twice,
    // dropping and re-dialing the connection between the two rounds. A second
    // delivery after the reconnect proves pub/sub resumes on the new connection.
    var round: usize = 0;
    while (round < 2) : (round += 1) {
        const want = if (round == 0) "hello" else "again";

        const client_conn = try client.dial(dial_addr, .{});
        var client_conn_live = true;
        defer if (client_conn_live) client_conn.deinit();
        const server_conn = try server.accept();
        var server_conn_live = true;
        defer if (server_conn_live) server_conn.deinit();

        try client_conn.startInboundDispatcher(.{});
        try server_conn.startInboundDispatcher(.{});

        // Both routers wire the peer up.
        var waited_ms: u64 = 0;
        while (waited_ms < 3000) : (waited_ms += 10) {
            if (server_gs.peerCount() == 1 and client_gs.peerCount() == 1) break;
            io_time.ms(10).sleep(io) catch {};
        }
        try std.testing.expectEqual(@as(usize, 1), server_gs.peerCount());
        try std.testing.expectEqual(@as(usize, 1), client_gs.peerCount());

        // A must learn B subscribes to "t" (re-announced on this connection)
        // before A's publish, or the publish has no subscriber to flood to.
        waited_ms = 0;
        while (waited_ms < 3000) : (waited_ms += 10) {
            if (client_gs.router.peers.get(peerKeyOf(server_peer))) |state| {
                if (state.topics.contains("t")) break;
            }
            io_time.ms(10).sleep(io) catch {};
        }
        {
            const tracked = if (client_gs.router.peers.get(peerKeyOf(server_peer))) |state|
                state.topics.contains("t")
            else
                false;
            try std.testing.expect(tracked);
        }

        // A publishes; B's handler must receive it. Republish on a short interval
        // until it lands: right after a reconnect the mesh/flood topology can take
        // a moment to settle, so a single publish may race the wire-up — repeating
        // is how the other 2-node tests handle this timing without a fixed sleep.
        waited_ms = 0;
        while (waited_ms < 3000) : (waited_ms += 50) {
            try client_gs.publish("t", want);
            if (rec.received.load(.acquire)) break;
            io_time.ms(50).sleep(io) catch {};
        }
        try std.testing.expect(rec.received.load(.acquire));
        try std.testing.expectEqualSlices(u8, "t", rec.topic.?);
        try std.testing.expectEqualSlices(u8, want, rec.data.?);
        try std.testing.expectEqualSlices(u8, client_peer.bytes[0..client_peer.len], rec.from.?);

        // Drop the connection: both routers must observe peer_disconnected and
        // fall back to 0 peers. This fires the disconnect/teardown path under test
        // (mesh/fanout drop; backoff — none here — would persist).
        client_conn.deinit();
        client_conn_live = false;
        server_conn.deinit();
        server_conn_live = false;
        waited_ms = 0;
        while (waited_ms < 3000) : (waited_ms += 10) {
            if (server_gs.peerCount() == 0 and client_gs.peerCount() == 0) break;
            io_time.ms(10).sleep(io) catch {};
        }
        try std.testing.expectEqual(@as(usize, 0), server_gs.peerCount());
        try std.testing.expectEqual(@as(usize, 0), client_gs.peerCount());

        // Clear the recorder so the next round's delivery is observed fresh. Both
        // connections are down and both routers are at 0 peers, so no router fiber
        // is delivering while we reset.
        rec.reset();
    }

    // Shut the routers down before the switches/endpoints. Both connections are
    // already torn down (peerCount 0 on both), satisfying Gossipsub.deinit's
    // precondition.
    server_gs.deinit();
    server_gs_live = false;
    client_gs.deinit();
    client_gs_live = false;
}

/// The Router's PeerKey for a PeerId (zero-padded bytes). Mirrors router.zig's
/// private `peerKey`, reproduced here so this test can index the router's peer
/// map directly to assert the subscription propagated.
fn peerKeyOf(peer: PeerId) [64]u8 {
    var key: [64]u8 = [_]u8{0} ** 64;
    @memcpy(key[0..peer.len], peer.bytes[0..peer.len]);
    return key;
}
