//! The public gossipsub service and its Switch/QUIC binding. `Gossipsub` owns a
//! single-fiber `Router(SwitchTransport)` and registers it on a `*Switch` (as the
//! `/meshsub/1.1.0` inbound service and the peer connect/disconnect observer).
//!
//! The ONLY place that knows the transport-agnostic router (router.zig) talks to a
//! real Switch over QUIC. The concrete pieces it needs live here:
//!   - `SwitchTransport`: the per-peer outbound `StreamSink` + the
//!     `*SwitchConnection` the `peer_connected` command carries.
//!   - `StreamSink` / `StreamSource`: QUIC-stream sink/source for the
//!     PeerWriter / PeerReader contracts.
//!   - the inbound service/handler registered on the Switch.
//!
//! The Switch's peer-event callbacks fire on whichever fiber called
//! dial/accept/deinit, OUTSIDE the Switch's connection lock, so they only post to
//! the router inbox — keeping all router-state mutation on the single router fiber.
//!
//! Wired on the root import path (root.zig), NOT protocols.zig: this file imports
//! switch.zig → protocols.zig, so routing through that chain would risk a cycle.

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

/// Per-write deadline on the `/meshsub` stream; matches the switch's multistream
/// negotiation timeout.
const write_timeout: std.Io.Timeout = .{
    .duration = .{ .raw = .fromNanoseconds(10 * std.time.ns_per_s), .clock = .awake },
};

/// Drains a peer's OutboundQueue onto its `/meshsub` stream, satisfying the
/// PeerWriter Sink contract (open / writeFrame / close, close idempotent). Owns
/// the current QUIC stream and re-opens it lazily.
pub const StreamSink = struct {
    conn: *SwitchConnection,
    /// The protocol id the peer accepted on the most recent open, or null before
    /// the first successful open. A pointer into `pubsub.supported_protocols`
    /// (multistream returns the matched candidate, not a copy), so it is stable.
    selected: ?ProtocolId = null,
    /// The currently-open outbound stream, or null when none is open.
    current: ?*Stream = null,

    /// (Re)establish the outbound stream by negotiating the best common /meshsub
    /// version on a new QUIC stream. On failure `current`/`selected` are untouched
    /// (the writer only opens when it has none).
    pub fn open(self: *StreamSink, io: std.Io) !void {
        _ = io;
        const result = try self.conn.openProtocolStreamMulti(&pubsub.supported_protocols, .{});
        self.current = result.stream;
        self.selected = result.selected;
        std.log.info("gossipsub: outbound stream negotiated {s}", .{result.selected});
    }

    /// Write one framed RPC. Errors propagate so the writer can re-open. Bounded
    /// per write so a stalled-but-alive peer (flow-control frozen) can't park the
    /// writer forever; repeated timeouts become a disconnect via the give-up counter.
    pub fn writeFrame(self: *StreamSink, io: std.Io, bytes: []const u8) !void {
        try self.current.?.writeAll(io, bytes, .{ .timeout = write_timeout });
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
    /// Initialized lazily on the first read (when an `io` is in hand). Must not
    /// move after that: the reader's vtable resolves through its own address,
    /// and it lives as a stack local in the inbound handler fiber.
    reader: ?Stream.Reader = null,
    buffer: [read_buffer_len]u8 = undefined,

    const read_buffer_len = 4096;

    pub fn read(self: *StreamSource, allocator: std.mem.Allocator, io: std.Io) !pubsub.OwnedRpc {
        if (self.reader == null) self.reader = self.stream.reader(io, &self.buffer);
        return pubsub.readRpcBuffered(allocator, &self.reader.?.interface);
    }
};

/// The real transport binding for the gossipsub Router (router.zig's Transport
/// contract: `makeSink`, `dial`, `Sink`/`ConnHandle`): produces a per-peer
/// `StreamSink` over a `*SwitchConnection` and dials peers on the router's behalf.
///
/// Held by value in the router but NOT stateless: borrows the `*Switch` and a
/// `*std.Io.Group` that OWNS the detached dial fibers. The group lives in the
/// `Gossipsub` handle so its `deinit` can cancel/await every dial fiber — the
/// router never joins them. `makeSink` does no I/O (the writer opens lazily).
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

    /// Fire-and-forget dial of the multiaddr STRING `addr`: spawns the detached
    /// `dialFiber` into `dial_group`. Success surfaces via the Switch's
    /// `onConnected` callback (the normal connect path that opens the outbound
    /// stream); a spawn failure is dropped with a log (the next direct-connect tick
    /// retries) so it never crashes the calling router fiber.
    pub fn dial(self: *SwitchTransport, io: std.Io, addr: []const u8) void {
        // Copy the address: the borrowed `addr` (a router-owned direct-peer
        // string) outlives this call, but the detached fiber runs past it, so it
        // gets its own copy and frees it. On OOM, drop the dial (retried next tick).
        const addr_copy = self.allocator.dupe(u8, addr) catch {
            std.log.warn("gossipsub: dial dropped (out of memory copying address)", .{});
            return;
        };
        // The dial fiber dials via `sw.io`; `io` here is just the spawn context.
        self.dial_group.concurrent(io, dialFiber, .{ self.sw, self.allocator, addr_copy }) catch {
            std.log.warn("gossipsub: dial dropped (could not spawn dial fiber)", .{});
            self.allocator.free(addr_copy);
        };
    }
};

/// Detached dial-fiber body. Parses `addr` (owned, freed here), dials it through
/// `sw`, and on success starts the connection's inbound dispatcher. All failures
/// are logged and dropped (the router re-dials on its next tick). The dialed
/// connection is owned by the Switch, so this fiber does not retain it.
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
    // `sw.dial` already fired `onConnected` synchronously, so the peer is wiring
    // up; start the inbound dispatcher so its /meshsub streams read. A failure
    // here leaves the connection up but without inbound reads — logged only.
    conn.startInboundDispatcher(.{}) catch |err| {
        std.log.warn("gossipsub: dial to {s} connected but inbound dispatch failed: {s}", .{ addr, @errorName(err) });
    };
}

const Router = router_mod.Router(SwitchTransport);

/// The gossipsub heartbeat period (go-libp2p default: one second). The router
/// runs a heartbeat fiber on this interval to maintain the mesh and age backoffs.
const heartbeat_interval_ms: u64 = 1000;

/// Receiver for messages delivered on subscribed topics.
pub const MessageHandler = router_mod.MessageHandler;

/// Application topic-message validator (and its verdict enum): gate propagation
/// accept/reject/ignore via `RouterConfig.validator`, matching go-libp2p-pubsub
/// `RegisterTopicValidator` / rust-libp2p `MessageAcceptance`. Null accepts every
/// message.
pub const MessageValidator = router_mod.MessageValidator;
pub const ValidationResult = router_mod.ValidationResult;

/// Optional peer-scoring config. Pass `null` to `Gossipsub.init` to leave scoring
/// disabled (the default — no events, no gates), or `Gossipsub.default_score_config`
/// to enable it with the baseline params/thresholds.
pub const ScoreConfig = router_mod.ScoreConfig;

/// Router behaviour config (flood-publish, signature policy, etc.). Pass `.{}` to
/// `Gossipsub.init` for the go-libp2p defaults (flood-publish ON, signature policy
/// inferred from the host key).
pub const RouterConfig = router_mod.RouterConfig;

/// Direct-peer descriptor (peer id + multiaddr string to dial) for
/// `RouterConfig.direct_peers`. The router keeps each configured direct peer
/// connected itself (dials at start, re-dials disconnected ones on a tick).
pub const DirectPeer = router_mod.DirectPeer;

/// Mesh-degree override (`d`/`d_low`/`d_high`) for `RouterConfig.mesh_degree`,
/// e.g. to drive a small topology over-degree so the heartbeat prunes it. Null
/// (the default) keeps the go-libp2p baseline.
pub const MeshDegree = router_mod.MeshDegree;

/// Signature-policy enum + message-id override types: select `anonymous`
/// (StrictNoSign) or supply a custom message-id function via `RouterConfig`.
pub const SignaturePolicy = router_mod.SignaturePolicy;
pub const MessageIdFn = router_mod.MessageIdFn;
pub const MessageIdConfig = router_mod.MessageIdConfig;
/// The id bytes a `MessageIdFn` returns (owned, freed by the router).
pub const MessageId = pubsub.rpc.MessageId;

const score_mod = @import("score.zig");
pub const default_score_config: ScoreConfig = .{
    .params = score_mod.default_params,
    .thresholds = score_mod.default_thresholds,
};

/// The gossipsub stream protocol id (`/meshsub/1.1.0`), e.g. for an identify
/// responder advertising which protocols this node speaks.
pub const protocol_id = pubsub.protocol_id;

/// Every `/meshsub` version this node speaks, newest-first (1.2.0, 1.1.0, 1.0.0):
/// an identify responder advertises the full set; a peer then negotiates the best
/// common version.
pub const supported_protocols = pubsub.supported_protocols;

/// The other `/meshsub` version ids (`protocol_id` above is 1.1.0), to
/// register/advertise a specific version.
pub const protocol_id_v1_2 = pubsub.protocol_id_v1_2;
pub const protocol_id_v1_0 = pubsub.protocol_id_v1_0;

/// Frame a pubsub RPC into its length-prefixed wire bytes
/// (uvarint(len) || RPC.encode()). General gossipsub wire primitive; caller owns
/// the returned slice.
pub const frameRpc = pubsub.frameRpc;

/// Per-stream inbound handler. The Switch's dispatcher creates one of these per
/// inbound `/meshsub/1.1.0` stream (capturing the sender's peer id) and runs it
/// on a handler fiber the Switch owns and cancels on connection teardown. The
/// handler reads RPCs and posts them to the router inbox until the stream ends.
const InboundHandler = struct {
    router: *Router,
    peer: PeerId,
    /// The /meshsub version negotiated on THIS inbound stream. The peer proposes
    /// its best version and we accept it, so this is its highest supported version.
    version: pubsub.Version,

    /// Fiber body run by the Switch. Reads + posts until the stream breaks.
    fn run(self: *InboundHandler, io: std.Io, stream: *Stream) anyerror!void {
        std.log.info("gossipsub: inbound stream negotiated {s}", .{@tagName(self.version)});
        // Record the peer's /meshsub version before reading, so it's known as soon
        // as the peer reaches us even if its RPCs are slow. Best-effort post.
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
    /// A copy of the router's allocator so `destroyInboundService` can free this
    /// WITHOUT dereferencing `router`: the Switch frees its service objects in its
    /// own `deinit`, which runs AFTER `Gossipsub.deinit` has destroyed the router,
    /// so reaching the allocator through `router.allocator` would be a UAF.
    allocator: std.mem.Allocator,

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
    /// Borrowed; must outlive the router (its per-peer state borrows the Switch's
    /// connections; the Switch holds the peer-event callback + inbound service that
    /// reference the router). The callback is cleared on deinit before the router is freed.
    sw: *Switch,
    router: *Router,
    /// Owns the detached dial fibers `SwitchTransport.dial` spawns. Heap-owned for
    /// a stable address (the by-value `SwitchTransport` holds a `*std.Io.Group`
    /// into it); cancelled + awaited in `deinit` before the router is freed.
    dial_group: *std.Io.Group,

    /// Construct the router, start its fiber, register the inbound service and
    /// the peer-event callback on the Switch. On any failure the router is torn
    /// down so nothing leaks.
    ///
    /// `local_peer` is this node's own peer id (Message.from on publish). The
    /// caller passes it in because the endpoint exposes no local-peer accessor and
    /// does not retain the host KeyPair (e.g. `try key.peerId(allocator)`).
    ///
    /// `host_key` (with `config.signature_policy`) selects the signature policy;
    /// the key is BORROWED and must outlive this Gossipsub. By default
    /// (`signature_policy == null`) the key infers it: (a) pass the host KeyPair to
    /// enable StrictSign (sign outbound, verify+reject inbound — go-libp2p's default,
    /// required for interop), deriving `local_peer` from the key; (b) pass null for
    /// the none policy (from+seqno, no signature). To run anonymously (StrictNoSign:
    /// topic+data only, content-based ids) pass null AND
    /// `config.signature_policy = .anonymous`; a key + anonymous is rejected.
    ///
    /// `message_handler` (optional) receives messages on subscribed topics; see
    /// router's MessageHandler for the call contract.
    ///
    /// `score_config` (optional) enables peer scoring: null leaves it off (no
    /// events, no gates); a config (e.g. `Gossipsub.default_score_config`) gates
    /// mesh/gossip/graylist decisions on scores.
    ///
    /// `config` tunes router behaviour; pass `.{}` for the defaults (see
    /// `RouterConfig`).
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
        // Heap-own the dial-fiber group for a stable address (the by-value
        // SwitchTransport points into it). This `destroy` errdefer is declared
        // first so it runs LAST — after the cancel errdefer below joins every dial
        // fiber, since a group with running fibers must not be freed.
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

        // Declared after `router.destroy`'s errdefer so it runs FIRST (LIFO): a
        // dial fiber mid-`sw.dial` can fire `onConnected` into the router, so it
        // must be joined before the router is freed. Idempotent (deinit cancels
        // again on the success path; an empty group cancels for free).
        errdefer dial_group.cancel(io);

        // Register the inbound service for EVERY /meshsub version so we accept
        // 1.0/1.1/1.2 peers alike. Each id gets its OWN heap-owned InboundService
        // (all pointing at the same router) so the Switch can free each
        // independently — one shared instance under several keys would double-free.
        for (pubsub.supported_protocols) |proto| {
            const service = try inboundService(router);
            var service_owned = true;
            // If addProtocolService fails it does NOT take ownership, so free the
            // service object ourselves on that path.
            errdefer if (service_owned) service.deinit();
            try sw.addProtocolService(proto, service);
            service_owned = false;
        }

        // Register the callback BEFORE start(): start() dials direct peers and a
        // connect fires `onConnected` synchronously, so registering first
        // guarantees the connect is observed rather than dropped.
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
        svc.* = .{ .router = router, .allocator = router.allocator };
        return protocols.ownedProtocolService(
            InboundService,
            InboundService.openInbound,
            destroyInboundService,
            svc,
        );
    }

    fn destroyInboundService(svc: *InboundService) void {
        // The directly-held allocator, NOT `svc.router.allocator` (already freed —
        // see `InboundService.allocator`): dereferencing `svc.router` would be a UAF.
        svc.allocator.destroy(svc);
    }

    /// Shut the router down (tears down every peer, joins its fiber) and free.
    /// Call before deiniting the Switch: the router's peer state borrows the Switch
    /// connections, so the router must stop while the Switch is still live.
    ///
    /// PRECONDITION (closes the router↔Switch lifetime hole): before this runs,
    /// every gossipsub connection on this Switch must already be torn down (with no
    /// inbound `/meshsub` RPC in flight), OR this Gossipsub must be deinited before
    /// any such connection outlives it.
    ///
    /// The Switch holds three references at the router we free: (a) the peer-event
    /// callback's `ctx`, (b) the inbound-service instance's `router` field, (c) any
    /// live Switch-owned inbound handler fiber holding `*Router`. Closed here in
    /// order: FIRST cancel+await the dial-fiber group (a detached dial borrows the
    /// Switch and would fire `onConnected` into the router; cancel is idempotent so
    /// init's errdefer double-call is harmless); then `clearPeerEventCallback` (no
    /// connect/disconnect can post after); then `router.destroy` (tears every peer
    /// down while connections are still alive). (b)/(c) hold via the precondition:
    /// the Switch cancels inbound handler fibers on connection teardown, and
    /// `openInbound` cannot fire once the connections are gone.
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

    /// Consistent snapshot of the router's counters. Blocks for one
    /// control-inbox round trip; meant for periodic scrapes.
    pub fn stats(self: *Gossipsub) Router.Stats {
        return self.router.stats();
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

    /// Hand a peer's signed peer record (its `signedPeerRecord` from a completed
    /// identify exchange) to the router's certified-record store, so peer-exchange
    /// can vouch for `peer`'s certified addresses. The caller passes the ORIGINAL
    /// envelope wire bytes (already verified + bound to `peer`); this dups them and
    /// posts a `peer_record` command (the router re-verifies). On OOM or a closed
    /// inbox the record is dropped — PX simply has nothing to offer, safe.
    pub fn consumeIdentifyRecord(self: *Gossipsub, peer: PeerId, envelope_bytes: []const u8) void {
        const owned = self.allocator.dupe(u8, envelope_bytes) catch {
            std.log.warn("gossipsub: dropped identify peer record (out of memory)", .{});
            return;
        };
        self.router.postPeerRecord(self.router.io, peer, owned);
    }

    // The Switch peer-event callbacks: ctx is the *Router. Each posts one Command
    // to the router's CONTROL inbox (drained with priority over data, so a
    // connect/disconnect is never backpressured behind an inbound-RPC flood) and
    // returns. The blocking `putOne` is the safe choice: dropping a
    // `peer_disconnected` would leak a peer. A closed inbox swallows the post (the
    // router is shutting down anyway).

    fn onConnected(ctx: *anyopaque, peer: PeerId, conn: *SwitchConnection, remote_addr: std.Io.net.IpAddress) void {
        const router: *Router = @ptrCast(@alignCast(ctx));
        router.control_inbox.putOne(router.io, .{ .peer_connected = .{
            .peer = peer,
            .conn = conn,
            .remote_addr = remote_addr,
        } }) catch return;
        router.notifyControl();
    }

    fn onDisconnected(ctx: *anyopaque, peer: PeerId, conn: *SwitchConnection) void {
        const router: *Router = @ptrCast(@alignCast(ctx));
        // `conn` rides along as an identity token: the router tears a peer down
        // only when the connection its PeerState is bound to dies, so the close
        // of a dedup'd duplicate connection (simultaneous dial) cannot destroy
        // the live peer's state.
        router.control_inbox.putOne(router.io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } }) catch return;
        router.notifyControl();
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

    // peerCount==1 on BOTH sides proves the full wire-up: peer_connected processed,
    // the writer opened its outbound /meshsub stream (an open-exhaustion give-up
    // would drop the count back to 0), and the inbound handler is running.
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

    // Routers before switches/endpoints (Gossipsub.deinit's required order). Both
    // connections are already torn down (peerCount 0 on both), satisfying its
    // precondition: no inbound handler is alive and no peer event can fire.
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

    /// Clear the captured delivery so the recorder can accept a fresh one (the
    /// reconnect test asserts a SECOND publish lands). Runs on the test fiber while
    /// no router fiber is delivering.
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
        if (client_gs.router.probeForTest(server_peer, "t").subscribed) break;
        io_time.ms(10).sleep(io) catch {};
    }
    {
        try std.testing.expect(client_gs.router.probeForTest(server_peer, "t").subscribed);
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
    // inbound stream negotiates 1.2.0. A side learns the version only once the
    // OTHER opens its outbound stream (lazy, on first frame) — both have by now
    // (server's announce, client's publish). The post races peer_connected, so poll.
    waited_ms = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        const c = client_gs.router.probeForTest(server_peer, "t");
        const s = server_gs.router.probeForTest(client_peer, "t");
        if (c.tracked and s.tracked and c.version == .v1_2 and s.version == .v1_2) break;
        io_time.ms(10).sleep(io) catch {};
    }
    {
        const client_probe = client_gs.router.probeForTest(server_peer, "t");
        const server_probe = server_gs.router.probeForTest(client_peer, "t");
        if (!client_probe.tracked or !server_probe.tracked) return error.MissingPeer;
        try std.testing.expectEqual(pubsub.Version.v1_2, client_probe.version);
        try std.testing.expectEqual(pubsub.Version.v1_2, server_probe.version);
    }

    // Gossipsub-required teardown order: close connections first (so no inbound
    // handler survives and no peer event can fire), then deinit each gossipsub.
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

test "two gossipsub nodes: keep-alive holds an idle connection past max_idle_timeout" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var server_key = try identity.KeyPair.generate(.ED25519);
    defer server_key.deinit();
    var client_key = try identity.KeyPair.generate(.ED25519);
    defer client_key.deinit();

    // Idle 1s + keep-alive 200ms. Idling 1.6s (> max_idle_timeout) with no app
    // traffic would idle-close the connection WITHOUT keep-alive; the periodic
    // ack-eliciting PING resets both ends' idle timers so it survives. Regression
    // guard: a subscriber that waits between bursts must not silently lose its mesh.
    const opts: quic.Options = .{ .transport = .{ .max_idle_timeout_ms = 1000, .keep_alive_period_ms = 200 } };

    const server_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &server_key, opts);
    defer server_endpoint.deinit();
    const client_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &client_key, opts);
    defer client_endpoint.deinit();

    const server = try Switch.init(allocator, io, server_endpoint);
    defer server.deinit();
    const client = try Switch.init(allocator, io, client_endpoint);
    defer client.deinit();

    const server_peer = try server_key.peerId(allocator);
    const client_peer = try client_key.peerId(allocator);

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

    try client_conn.startInboundDispatcher(.{});
    try server_conn.startInboundDispatcher(.{});

    var waited_ms: u64 = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (server_gs.peerCount() == 1 and client_gs.peerCount() == 1) break;
        io_time.ms(10).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), server_gs.peerCount());
    try std.testing.expectEqual(@as(usize, 1), client_gs.peerCount());

    // B subscribes; wait for the announce to reach A (the last app traffic before idle).
    try server_gs.subscribe("t");
    waited_ms = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (client_gs.router.probeForTest(server_peer, "t").subscribed) break;
        io_time.ms(10).sleep(io) catch {};
    }

    // Idle 1.6s (> max_idle_timeout) with no app traffic, keeping the runtime
    // turning for keep-alive PINGs. The heartbeat is silent here (empty mcache, no
    // mesh to GRAFT), so ONLY keep-alive can hold the connection.
    var idled_ms: u64 = 0;
    while (idled_ms < 1600) : (idled_ms += 50) {
        io_time.ms(50).sleep(io) catch {};
    }

    // Connection must still be up — keep-alive held it past the idle timeout.
    try std.testing.expectEqual(@as(usize, 1), client_gs.peerCount());
    try std.testing.expectEqual(@as(usize, 1), server_gs.peerCount());

    // And a publish after the long idle must still deliver.
    try client_gs.publish("t", "after-idle");
    waited_ms = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (rec.received.load(.acquire)) break;
        io_time.ms(10).sleep(io) catch {};
    }
    try std.testing.expect(rec.received.load(.acquire));
    try std.testing.expectEqualSlices(u8, "after-idle", rec.data.?);

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

test "two gossipsub nodes: keep-alive clamps to a peer's shorter negotiated idle timeout" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var server_key = try identity.KeyPair.generate(.ED25519);
    defer server_key.deinit();
    var client_key = try identity.KeyPair.generate(.ED25519);
    defer client_key.deinit();

    // Asymmetric idle timeouts: server advertises 600ms idle and runs NO keep-alive;
    // the client's 1500ms keep-alive is LONGER than the negotiated idle
    // (min(2000,600)=600ms). Without the peer-idle clamp the client PINGs too late
    // and the connection idle-closes at 600ms; the clamp drops its period to
    // peer_idle/2 (300ms) so it survives — go-libp2p's min(KeepAlivePeriod, idleTimeout/2).
    const server_opts: quic.Options = .{ .transport = .{ .max_idle_timeout_ms = 600, .keep_alive_period_ms = 0 } };
    const client_opts: quic.Options = .{ .transport = .{ .max_idle_timeout_ms = 2000, .keep_alive_period_ms = 1500 } };

    const server_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &server_key, server_opts);
    defer server_endpoint.deinit();
    const client_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &client_key, client_opts);
    defer client_endpoint.deinit();

    const server = try Switch.init(allocator, io, server_endpoint);
    defer server.deinit();
    const client = try Switch.init(allocator, io, client_endpoint);
    defer client.deinit();

    const server_peer = try server_key.peerId(allocator);
    const client_peer = try client_key.peerId(allocator);

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

    try client_conn.startInboundDispatcher(.{});
    try server_conn.startInboundDispatcher(.{});

    var waited_ms: u64 = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (server_gs.peerCount() == 1 and client_gs.peerCount() == 1) break;
        io_time.ms(10).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), server_gs.peerCount());

    try server_gs.subscribe("t");
    waited_ms = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (client_gs.router.probeForTest(server_peer, "t").subscribed) break;
        io_time.ms(10).sleep(io) catch {};
    }

    // Idle 1s — longer than the 600ms negotiated idle, and longer than the client's
    // UNclamped 1500ms keep-alive (so without the clamp no PING fires in time).
    var idled_ms: u64 = 0;
    while (idled_ms < 1000) : (idled_ms += 50) {
        io_time.ms(50).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), client_gs.peerCount());
    try std.testing.expectEqual(@as(usize, 1), server_gs.peerCount());

    try client_gs.publish("t", "after-idle");
    waited_ms = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (rec.received.load(.acquire)) break;
        io_time.ms(10).sleep(io) catch {};
    }
    try std.testing.expect(rec.received.load(.acquire));
    try std.testing.expectEqualSlices(u8, "after-idle", rec.data.?);

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

test "two gossipsub nodes: a publish larger than the stream outbound queue is delivered" {
    // A message bigger than the per-stream outbound queue forces the writer to
    // block and resume as the connection actor drains it — the multi-chunk path a
    // small publish never exercises (the interop blob scenario sends ~96 KiB).
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

    const client_conn = try client.dial(dial_addr, .{});
    var client_conn_live = true;
    defer if (client_conn_live) client_conn.deinit();

    const server_conn = try server.accept();
    var server_conn_live = true;
    defer if (server_conn_live) server_conn.deinit();

    try client_conn.startInboundDispatcher(.{});
    try server_conn.startInboundDispatcher(.{});

    var waited_ms: u64 = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (server_gs.peerCount() == 1 and client_gs.peerCount() == 1) break;
        io_time.ms(10).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), server_gs.peerCount());
    try std.testing.expectEqual(@as(usize, 1), client_gs.peerCount());

    try server_gs.subscribe("t");

    waited_ms = 0;
    while (waited_ms < 3000) : (waited_ms += 10) {
        if (client_gs.router.probeForTest(server_peer, "t").subscribed) break;
        io_time.ms(10).sleep(io) catch {};
    }
    {
        try std.testing.expect(client_gs.router.probeForTest(server_peer, "t").subscribed);
    }

    // A payload twice the default per-stream outbound queue (48 KiB), so the
    // writer cannot fit it in one go and must drain across multiple chunks.
    const big_len: usize = 96 * 1024;
    const big = try allocator.alloc(u8, big_len);
    defer allocator.free(big);
    for (big, 0..) |*b, i| b.* = @truncate(i);

    try client_gs.publish("t", big);

    waited_ms = 0;
    while (waited_ms < 5000) : (waited_ms += 10) {
        if (rec.received.load(.acquire)) break;
        io_time.ms(10).sleep(io) catch {};
    }
    try std.testing.expect(rec.received.load(.acquire));
    try std.testing.expectEqualSlices(u8, "t", rec.topic.?);
    try std.testing.expectEqualSlices(u8, big, rec.data.?);

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
            if (client_gs.router.probeForTest(server_peer, "t").subscribed) break;
            io_time.ms(10).sleep(io) catch {};
        }
        {
            try std.testing.expect(client_gs.router.probeForTest(server_peer, "t").subscribed);
        }

        // A publishes; B must receive. Republish until it lands: right after a
        // reconnect the topology can take a moment to settle, so a single publish
        // may race the wire-up.
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

fn routerHasPeer(router: *Router, peer: PeerId) bool {
    return router.probeForTest(peer, "").tracked;
}

fn routerHasRecord(router: *Router, peer: PeerId) bool {
    return router.probeForTest(peer, "").has_record;
}

/// Probes with the node's own id (never a peer of itself), so only `mesh_size`
/// of the returned probe is meaningful.
fn routerMeshSize(router: *Router, topic: []const u8) usize {
    return router.probeForTest(router.local_peer, topic).mesh_size;
}

/// A live gossipsub node for the peer-exchange end-to-end test: real QUIC
/// endpoint + Switch + Gossipsub, an identify responder advertising a signed peer
/// record (so a peer can certify this node's address and offer it via PX), and a
/// fiber accepting EVERY inbound connection (both star-centre dials and out-of-band
/// PX dials). Accepted connections are stashed so teardown can stop their inbound
/// handler fibers before the gossipsub is freed (Switch-borrows-router rule).
const PxNode = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    key: identity.KeyPair,
    peer: PeerId,
    endpoint: *quic.QuicEndpoint,
    sw: *Switch,
    gs: *Gossipsub,
    id_handler: protocols.identify.IdentifyHandler,
    /// The node's own listen multiaddrs. Owned here and kept alive for the whole
    /// run: the identify handler BORROWS these slices and re-encodes them on
    /// every inbound identify stream, so they must outlive the handler.
    listen_addrs: std.ArrayList([]u8) = .empty,
    /// Inbound connections this node accepted, owned here for teardown.
    accepted: std.ArrayList(*SwitchConnection) = .empty,
    accept_future: ?std.Io.Future(void) = null,

    /// Bring the node up: generate a key, bind a loopback listener (port 0 →
    /// OS-assigned, so the bound addr is a dialable `127.0.0.1:<port>`),
    /// register identify with a signed record over the real listen addrs, start
    /// the gossipsub, and spawn the accept fiber. `mesh_degree` lets the centre
    /// run a tiny mesh so two leaves push it over `d_high`.
    fn up(
        node: *PxNode,
        allocator: std.mem.Allocator,
        io: std.Io,
        mesh_degree: ?MeshDegree,
    ) !void {
        node.* = .{
            .allocator = allocator,
            .io = io,
            .key = try identity.KeyPair.generate(.ED25519),
            .peer = undefined,
            .endpoint = undefined,
            .sw = undefined,
            .gs = undefined,
            .id_handler = undefined,
        };
        errdefer node.key.deinit();
        node.peer = try node.key.peerId(allocator);

        node.endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &node.key, .{});
        errdefer node.endpoint.deinit();
        node.sw = try Switch.init(allocator, io, node.endpoint);
        errdefer node.sw.deinit();

        // PX enabled on every node; the centre also runs the tiny mesh degree.
        node.gs = try Gossipsub.init(allocator, io, node.sw, node.peer, &node.key, null, null, .{
            .peer_exchange_enabled = true,
            .mesh_degree = mesh_degree,
        });
        errdefer node.gs.deinit();

        var listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
        defer listen_addr.deinit(allocator);
        try node.sw.listen(listen_addr);

        // Register identify with a signed record over the REAL bound listen addrs,
        // so a peer running identify against us certifies a dialable address and can
        // vouch for us in its own PX. The slices are node-owned (freed in
        // `deinitRest`) because the handler borrows and re-reads them per stream.
        node.listen_addrs = try node.sw.listenMultiaddrs(allocator);
        errdefer {
            for (node.listen_addrs.items) |item| allocator.free(item);
            node.listen_addrs.deinit(allocator);
        }
        node.id_handler = try protocols.identify.IdentifyHandler.initWithSignedRecord(
            allocator,
            .{
                .agent_version = "eth-p2p-z-gossipsub-px-test/0.1.0",
                .protocols = &(.{protocols.identify.protocol_id} ++ supported_protocols),
                .listen_addrs = node.listen_addrs.items,
            },
            &node.key,
            1,
        );
        errdefer node.id_handler.deinit();
        try node.sw.addProtocolService(
            protocols.identify.protocol_id,
            protocols.streamHandlerService(protocols.identify.IdentifyHandler, protocols.identify.IdentifyHandler.run, &node.id_handler),
        );

        node.accept_future = try std.Io.concurrent(io, acceptLoop, .{node});
    }

    /// Accept inbound connections in a loop, starting each one's inbound
    /// `/meshsub` dispatcher and stashing it for teardown. Returns cleanly when
    /// the listener is closed (graceful stop) or on cancel.
    fn acceptLoop(node: *PxNode) void {
        while (true) {
            const conn = node.sw.accept() catch return;
            conn.startInboundDispatcher(.{}) catch {
                conn.deinit();
                continue;
            };
            node.accepted.append(node.allocator, conn) catch {
                conn.deinit();
                continue;
            };
        }
    }

    /// This node's full dialable `/ip4/127.0.0.1/udp/<port>/quic-v1/p2p/<id>`.
    fn dialAddr(node: *PxNode, allocator: std.mem.Allocator) ![]u8 {
        var addrs = try node.sw.listenMultiaddrs(allocator);
        defer {
            for (addrs.items) |a| allocator.free(a);
            addrs.deinit(allocator);
        }
        var local = try Multiaddr.fromString(allocator, addrs.items[0]);
        defer local.deinit(allocator);
        const parsed = try local.parseIpUdp(allocator);
        const peer_text = try node.peer.toString(allocator);
        defer allocator.free(peer_text);
        return std.fmt.allocPrint(
            allocator,
            "/ip4/127.0.0.1/udp/{d}/quic-v1/p2p/{s}",
            .{ parsed.address.getPort(), peer_text },
        );
    }

    /// Stop accepting, join the accept fiber, and deinit every accepted connection
    /// — stopping their inbound handler fibers so the gossipsub can later be freed
    /// with no Switch-owned handler holding the router. Leaves the rest to
    /// `deinitRest`.
    fn quiesce(node: *PxNode) void {
        node.sw.closeListener(node.io);
        if (node.accept_future) |*future| {
            future.cancel(node.io);
            future.await(node.io);
            node.accept_future = null;
        }
        for (node.accepted.items) |conn| conn.deinit();
        node.accepted.deinit(node.allocator);
        node.accepted = .empty;
    }

    /// Tear down the rest (gossipsub before switch — the router borrows the Switch's
    /// connections) and free the key. Call after every node is `quiesce`d, so no
    /// accept fiber or inbound handler survives to post into a freed router.
    fn deinitRest(node: *PxNode) void {
        node.gs.deinit();
        // The identify handler borrows `listen_addrs`; deinit it (frees only its
        // owned sealed record) before freeing the borrowed slices.
        node.id_handler.deinit();
        for (node.listen_addrs.items) |item| node.allocator.free(item);
        node.listen_addrs.deinit(node.allocator);
        node.sw.deinit();
        node.endpoint.deinit();
        node.key.deinit();
    }
};

/// Dial `target` from `from`, start its inbound dispatcher, run identify as the
/// client, and hand the peer's verified signed record to `from`'s gossipsub —
/// exercising the real identify → `consumeIdentifyRecord` → cert-store chain that
/// populates the PX offer store. The returned connection is caller-owned.
fn dialAndIdentify(allocator: std.mem.Allocator, io: std.Io, from: *PxNode, target: *PxNode) !*SwitchConnection {
    const addr_text = try target.dialAddr(allocator);
    defer allocator.free(addr_text);
    var addr = try Multiaddr.fromString(allocator, addr_text);
    defer addr.deinit(allocator);

    const conn = try from.sw.dial(addr, .{});
    errdefer conn.deinit();
    try conn.startInboundDispatcher(.{});

    const stream = try conn.openProtocolStream(protocols.identify.protocol_id, .{});
    var owned = try protocols.identify.readIdentify(allocator, io, stream);
    defer owned.deinit(allocator);
    stream.close(io) catch {};
    stream.deinit();

    const peer = conn.peerId();
    if (try protocols.identify.consumeSignedPeerRecord(allocator, &owned.reader, peer)) |*rec| {
        var r = rec.*;
        r.deinit(allocator);
        from.gs.consumeIdentifyRecord(peer, owned.reader.getSignedPeerRecord());
    }
    return conn;
}

test "live peer-exchange: an over-degree prune offers a signed record and the pruned peer dials it" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Three-node star: centre A meshes with leaves B and C. A's tiny degree
    // (target 1, prune above 1) makes the heartbeat prune one leaf once both are in
    // A's mesh. With peer-exchange on, that over-degree prune is a doPX path: A
    // offers the surviving leaf's signed record to the pruned leaf, which dials it
    // (accept-PX gate skipped, scoring off) — yielding a NEW leaf↔leaf QUIC link
    // that did not exist before (each leaf had dialed only the centre).
    const tiny_degree: MeshDegree = .{ .d = 1, .d_low = 0, .d_high = 1 };

    var a: PxNode = undefined;
    try a.up(allocator, io, tiny_degree);
    var a_quiesced = false;
    var a_torn = false;
    defer if (!a_torn) {
        if (!a_quiesced) a.quiesce();
        a.deinitRest();
    };

    var b: PxNode = undefined;
    try b.up(allocator, io, null);
    var b_quiesced = false;
    var b_torn = false;
    defer if (!b_torn) {
        if (!b_quiesced) b.quiesce();
        b.deinitRest();
    };

    var c: PxNode = undefined;
    try c.up(allocator, io, null);
    var c_quiesced = false;
    var c_torn = false;
    defer if (!c_torn) {
        if (!c_quiesced) c.quiesce();
        c.deinitRest();
    };

    // A dials both leaves and runs identify as the client against each, so A's
    // certified-record store holds B's and C's signed records (the addresses a PX
    // offer for them carries). A holds the two dial connections for teardown.
    const conn_ab = try dialAndIdentify(allocator, io, &a, &b);
    var conn_ab_live = true;
    defer if (conn_ab_live) conn_ab.deinit();
    const conn_ac = try dialAndIdentify(allocator, io, &a, &c);
    var conn_ac_live = true;
    defer if (conn_ac_live) conn_ac.deinit();

    // Confirm A learned both records before relying on PX to carry them.
    {
        var waited: u64 = 0;
        while (waited < 3000) : (waited += 10) {
            if (routerHasRecord(a.gs.router, b.peer) and routerHasRecord(a.gs.router, c.peer)) break;
            io_time.ms(10).sleep(io) catch {};
        }
        try std.testing.expect(routerHasRecord(a.gs.router, b.peer));
        try std.testing.expect(routerHasRecord(a.gs.router, c.peer));
    }

    // Everyone subscribes so a mesh can form on the topic. The leaves graft toward
    // A; A accepts both (inbound GRAFTs have no upper-size cap — the heartbeat is
    // what shrinks an over-full mesh).
    try a.gs.subscribe("t");
    try b.gs.subscribe("t");
    try c.gs.subscribe("t");

    // Wait for A's mesh to reach both leaves (over-degree), so the next heartbeat
    // has an excess to prune with PX.
    {
        var waited: u64 = 0;
        while (waited < 5000) : (waited += 10) {
            if (routerMeshSize(a.gs.router, "t") >= 2) break;
            io_time.ms(10).sleep(io) catch {};
        }
        try std.testing.expect(routerMeshSize(a.gs.router, "t") >= 2);
    }

    // Before the prune, the leaves know only the centre — they are NOT peers of
    // each other. This is the baseline the PX dial must change.
    try std.testing.expect(!routerHasPeer(b.gs.router, c.peer));
    try std.testing.expect(!routerHasPeer(c.gs.router, b.peer));

    // The heartbeat prunes A's mesh from 2 to 1 via doPX. Which leaf is pruned is
    // not deterministic (scoring off → arbitrary victim), so assert the SYMMETRIC
    // outcome: the leaves become peers of each other over the freshly-dialed QUIC
    // link. Poll generously — dial + handshake + connect-event span fibers.
    var connected = false;
    {
        var waited: u64 = 0;
        while (waited < 15000) : (waited += 20) {
            if (routerHasPeer(b.gs.router, c.peer) and routerHasPeer(c.gs.router, b.peer)) {
                connected = true;
                break;
            }
            io_time.ms(20).sleep(io) catch {};
        }
    }
    try std.testing.expect(connected);

    // Teardown discipline: no Switch-owned inbound handler may survive into a freed
    // router. Quiesce EVERY node (joins each node's accepted-connection handlers),
    // then deinit A's dial connections. The only remaining live handler is the
    // PX-dialed leaf↔leaf link, which EOFs as its peer's connections close. Only
    // then free each node (gossipsub before switch), across all nodes so the order
    // holds regardless of which leaf was pruned.
    a.quiesce();
    a_quiesced = true;
    b.quiesce();
    b_quiesced = true;
    c.quiesce();
    c_quiesced = true;

    conn_ab.deinit();
    conn_ab_live = false;
    conn_ac.deinit();
    conn_ac_live = false;

    // Let the per-peer disconnects settle so the routers drop the torn-down peers
    // (and the PX-dialed link's inbound handler EOFs) before any router is freed.
    {
        var waited: u64 = 0;
        while (waited < 5000) : (waited += 20) {
            if (a.gs.peerCount() == 0 and b.gs.peerCount() == 0 and c.gs.peerCount() == 0) break;
            io_time.ms(20).sleep(io) catch {};
        }
    }

    a.deinitRest();
    a_torn = true;
    b.deinitRest();
    b_torn = true;
    c.deinitRest();
    c_torn = true;
}
