//! The gossipsub Router actor: a single fiber that owns ALL per-peer state
//! lock-free (the go-libp2p processLoop model). Every event — a peer connecting
//! or disconnecting, an inbound RPC arriving, a shutdown request — reaches the
//! router as a `Command` on its one inbox queue. Producers (the Switch's
//! peer-event callback, the per-stream inbound handlers) only POST commands;
//! they never touch router state, so the single fiber serialises all mutation
//! with no locks.
//!
//! This layer handles per-peer I/O lifecycle plus gossipsub mesh pub/sub: on
//! connect it opens a per-peer outbound stream (lazily, via a writer fiber
//! draining an OutboundQueue) and starts reading the peer's inbound stream; on
//! disconnect it tears that down cleanly. On top of that it implements the
//! gossipsub mesh — the local node subscribes to topics and publishes messages;
//! inbound subscriptions are tracked per peer; subscribing to a topic JOINs its
//! mesh (graft peers) and unsubscribing LEAVEs it (prune them); the heartbeat
//! keeps each mesh sized around the target degree D (grafting in below D_low,
//! pruning out above D_high). A received message is forwarded only to the
//! topic's mesh members (minus the sender); a message published to a topic we do
//! NOT subscribe to goes to a transient `fanout` peer set instead. GRAFT/PRUNE
//! drive mesh membership; IHAVE/IWANT/IDONTWANT (gossip + scoring) are later
//! layers and stay parsed-but-ignored.
//!
//! The Router is generic over a comptime `Transport` so its lifecycle logic can
//! be unit-tested against an in-memory fake (no real QUIC), exactly how
//! go-libp2p-pubsub and rust-libp2p test their gossipsub cores. The real
//! Switch/QUIC binding (`SwitchTransport`, the concrete stream sink/source, the
//! inbound service) lives in gossipsub.zig; this file knows nothing about
//! `*Switch`/`*SwitchConnection`/`*quic.Stream`.

const std = @import("std");
const pubsub = @import("pubsub.zig");
const peer_io = @import("peer_io.zig");
const mcache = @import("mcache.zig");
const rpc = @import("rpc.zig");
const rpc_pb = @import("../../protobuf.zig").rpc;
const signing = @import("signing.zig");
const identity = @import("../../identity.zig");
const io_time = @import("../../quic/io/time.zig");
const score_mod = @import("score.zig");
const PeerId = @import("peer_id").PeerId;

/// Optional peer-scoring configuration handed to `Router.create` /
/// `Gossipsub.init`. When provided, the router builds a `PeerScore` engine,
/// fires scoring events on every relevant transition, and gates its
/// mesh/gossip/graylist decisions on the score. When null, scoring is entirely
/// off: no engine, no events, no gates — the router behaves exactly as it did
/// before scoring existed (matching go-libp2p, where scoring is app-configured
/// and disabled by default). `score_mod.default_params` /
/// `score_mod.default_thresholds` are a ready-made baseline a caller can pass.
pub const ScoreConfig = struct {
    params: score_mod.ScoreParams,
    thresholds: score_mod.PeerScoreThresholds,
};

/// Behaviour knobs handed to `Router.create` / `Gossipsub.init`. Defaults match
/// go-libp2p so an empty `.{}` reproduces go's out-of-the-box gossipsub.
pub const RouterConfig = struct {
    /// Flood-publish (go-libp2p default ON): a message the local node ORIGINATES
    /// is sent to EVERY peer subscribed to its topic (above the publish
    /// threshold when scoring is on), not just the topic's mesh/fanout — which
    /// maximises propagation of locally-originated messages. RELAYED (received)
    /// messages are never flooded; they go only to the mesh. With this off, an
    /// originated message uses the mesh (if we subscribe) or a fanout set (if we
    /// do not), exactly as a relay would.
    flood_publish: bool = true,
};

/// Upper bound on remembered message-ids in the seen-cache (loop/duplicate
/// suppression). A bounded FIFO of owned id copies: once full, inserting a new id
/// evicts the oldest. This is a deliberately minimal dedup window — a proper
/// time-bounded seen-cache (with per-entry expiry, as go-libp2p-pubsub keeps)
/// arrives with the message cache in a later phase. The bound guarantees no
/// unbounded growth.
const seen_cache_capacity = 1024;

/// Invoked on the router fiber for each delivered message on a topic WE
/// subscribe to. The `topic`/`from`/`data` slices are only valid for the
/// duration of the call; a handler that needs to retain them must copy. Keep it
/// cheap: it runs inline on the single router fiber and stalls every other event
/// while it executes.
pub const MessageHandler = struct {
    ctx: *anyopaque,
    on_message: *const fn (ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) void,
};

/// A bounded FIFO set of owned message-id byte copies, used to suppress
/// duplicate/looping messages. Membership is a hash set; eviction order is a ring
/// of the same owned id slices. Inserting past `capacity` evicts (and frees) the
/// oldest id. Owns every id copy; `deinit` frees them all.
const SeenCache = struct {
    allocator: std.mem.Allocator,
    /// Set of currently-remembered ids. Keys are owned copies (the same slices
    /// stored in `ring`), so freeing happens exactly once, on eviction/deinit.
    set: std.StringHashMapUnmanaged(void) = .empty,
    /// Insertion-ordered ring of the owned id slices, for oldest-first eviction.
    ring: []?[]u8,
    head: usize = 0,
    count: usize = 0,

    fn init(allocator: std.mem.Allocator) std.mem.Allocator.Error!SeenCache {
        const ring = try allocator.alloc(?[]u8, seen_cache_capacity);
        @memset(ring, null);
        return .{ .allocator = allocator, .ring = ring };
    }

    fn deinit(self: *SeenCache) void {
        var it = self.set.keyIterator();
        while (it.next()) |key| self.allocator.free(key.*);
        self.set.deinit(self.allocator);
        self.allocator.free(self.ring);
        self.* = undefined;
    }

    /// Whether `id` was seen recently (still in the bounded window).
    fn contains(self: *const SeenCache, id: []const u8) bool {
        return self.set.contains(id);
    }

    /// Remember `id` (copying it). No-op if already present. On capacity the
    /// oldest id is evicted and freed first. On OOM the id is simply not
    /// remembered (dedup degrades to forwarding a possible duplicate — safe, and
    /// the only alternative is to drop the message, which is worse).
    fn add(self: *SeenCache, id: []const u8) void {
        if (self.set.contains(id)) return;
        const owned = self.allocator.dupe(u8, id) catch return;
        self.set.put(self.allocator, owned, {}) catch {
            self.allocator.free(owned);
            return;
        };
        // Evict the slot we are about to overwrite (the oldest) before storing.
        if (self.ring[self.head]) |old| {
            std.debug.assert(self.count == self.ring.len);
            _ = self.set.remove(old);
            self.allocator.free(old);
            self.count -= 1;
        }
        self.ring[self.head] = owned;
        self.head = (self.head + 1) % self.ring.len;
        self.count += 1;
    }
};

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

/// The mesh sizing parameters (go-libp2p defaults). The mesh for a topic is the
/// set of peers we exchange full messages with directly; the heartbeat keeps its
/// size between `d_low` and `d_high` around the target degree `d`. (Scoring-aware
/// degrees and the gossip-only fanout degree arrive with later layers.)
const MeshParams = struct {
    /// Target mesh degree per topic.
    d: usize = 6,
    /// Lower bound: below this the heartbeat grafts more peers in.
    d_low: usize = 5,
    /// Upper bound: at or above this we reject inbound GRAFTs and the heartbeat
    /// prunes peers out.
    d_high: usize = 12,
    /// Minimum outbound peers kept in a topic mesh (used by later maintenance).
    d_out: usize = 2,
    /// How long (in heartbeat ticks; one tick is one second) a PRUNE keeps us
    /// from re-grafting the pruned peer for that topic.
    prune_backoff_ticks: u64 = 60,
    /// How long (in heartbeat ticks) a fanout topic survives without a publish.
    /// Once `heartbeat_tick - fanout_last_pub[topic]` exceeds this the fanout
    /// peer set is dropped (we stopped publishing to a topic we don't subscribe).
    fanout_ttl_ticks: u64 = 60,
    /// Number of gossip (IHAVE) targets per topic per heartbeat: peers subscribed
    /// to the topic but NOT in its mesh/fanout, picked up to this many. (go-libp2p
    /// `D_lazy`; go also uses `max(D_lazy, GossipFactor*eligible)` — the
    /// GossipFactor scaling is a future refinement.)
    d_lazy: usize = 6,
    /// Upper bound on message-ids advertised in a single IHAVE; a longer id list
    /// is truncated. (go-libp2p `MaxIHaveLength`.)
    max_ihave_length: usize = 5000,
    /// Upper bound on message-ids we will request (in one IWANT) in response to a
    /// single inbound IHAVE. A simple per-IHAVE cap; finer rate-limiting and
    /// IWANT-promise tracking (for scoring) are a future refinement.
    max_iwant_request_ids: usize = 5000,
    /// Upper bound on messages we will serve from the cache in response to a
    /// single inbound IWANT. Bounds the work one peer can ask of us per request;
    /// finer rate-limiting is a future refinement.
    max_iwant_to_serve: usize = 5000,
};

const mesh_params: MeshParams = .{};

/// A set of peers (mesh membership), keyed by the zero-padded peer bytes.
const PeerSet = std.AutoHashMapUnmanaged(PeerKey, void);

/// A set of backed-off peers for one topic: peer → the heartbeat tick at which
/// the backoff expires (the peer becomes graftable again once the tick passes).
const BackoffSet = std.AutoHashMapUnmanaged(PeerKey, u64);

/// The single-fiber gossipsub router, generic over a comptime `Transport` that
/// supplies the per-peer outbound sink (and the opaque connection handle the
/// `peer_connected` command carries). The router owns the peer map and inbox;
/// the main fiber serialises every mutation.
///
/// The `Transport` type must provide:
///
///   pub const ConnHandle = ...;
///       An opaque per-peer connection handle, carried in the `peer_connected`
///       Command and handed back to `makeSink`. The router treats it as a value
///       it stores and forwards; it never dereferences it. (For the real
///       transport this is `*SwitchConnection`; for tests it is a fake handle.)
///
///   pub const Sink = ...;
///       The per-peer outbound sink type. Must satisfy the PeerWriter Sink
///       contract:
///         fn open(self: *Sink, io: std.Io) anyerror!void
///             (re)establish the current outbound stream. No I/O happens until
///             the writer fiber calls this lazily on its first frame.
///         fn writeFrame(self: *Sink, io: std.Io, bytes: []const u8) anyerror!void
///             write framed bytes to the current stream.
///         fn close(self: *Sink, io: std.Io) void
///             tear down the current stream; idempotent (safe when none open).
///
///   pub fn makeSink(self: *Transport, allocator: std.mem.Allocator,
///                   peer: PeerId, conn: ConnHandle) !*Sink;
///       Allocate + initialise (NO I/O — the writer calls `open` lazily) an
///       outbound sink for the peer. The Router OWNS the returned sink: on
///       teardown it calls `sink.close(io)` then `allocator.destroy(sink)`.
pub fn Router(comptime Transport: type) type {
    return struct {
        const Self = @This();

        const Sink = Transport.Sink;
        const ConnHandle = Transport.ConnHandle;
        const Writer = peer_io.PeerWriter(Sink);

        /// A command posted to the router's single inbox. Producers post; the
        /// router fiber processes. Generic in the transport's connection handle.
        pub const Command = union(enum) {
            /// A peer's connection is up (handshake done, peer id known). The
            /// `conn` handle stays valid until the matching `peer_disconnected`.
            peer_connected: struct {
                peer: PeerId,
                conn: ConnHandle,
                remote_addr: std.Io.net.IpAddress,
            },
            /// A peer's connection is gone. Tear the peer down.
            peer_disconnected: struct { peer: PeerId },
            /// An RPC arrived on a peer's inbound stream. The router owns it and
            /// must free it after parsing + forward-frame construction.
            inbound_rpc: peer_io.InboundRpc,
            /// Subscribe the local node to a topic: announce it to every peer and
            /// start delivering matching messages to the message handler. Owns
            /// `topic`; the router frees it once processed.
            subscribe: struct { topic: []u8 },
            /// Unsubscribe the local node from a topic: announce the withdrawal to
            /// every peer. Owns `topic`; freed once processed.
            unsubscribe: struct { topic: []u8 },
            /// Publish application data on a topic from the local node: forward it
            /// to every subscribed peer (and deliver locally if we subscribe).
            /// Owns `topic` and `data`; both freed once processed.
            publish: struct { topic: []u8, data: []u8 },
            /// One heartbeat tick: advance the tick counter and expire stale
            /// backoff entries. Posted by the heartbeat fiber on its interval (or
            /// directly by tests when the interval is disabled).
            heartbeat,
            /// Stop the router: tear down every peer, then exit the main fiber.
            shutdown,
            /// Test-only: enqueue a frame reference onto a tracked peer's outbound
            /// queue, on the router fiber (so the peer-map lookup is race-free).
            /// Used by router unit tests to make a peer's writer actually attempt
            /// to open its stream (the writer opens lazily on its first frame).
            /// The command carries one reference; if the push fails or the peer is
            /// not tracked the reference is released. The reply event is set once
            /// the push (or release) is done.
            enqueue_for_test: struct {
                peer: PeerId,
                frame: *peer_io.OutboundFrame,
                reply: *std.Io.Event,
            },
        };

        /// All state for one connected peer, owned by the router fiber. The
        /// writer fiber drains `queue` through `sink`; the writer's lifetime is
        /// the `writer_future`.
        const PeerState = struct {
            peer: PeerId,
            queue: peer_io.OutboundQueue,
            /// Topics this peer announced it subscribes to (its SUBSCRIBE
            /// SubOpts). Keys are owned copies; freed on remove and on teardown.
            /// Used to pick graft/fanout candidates: a peer is eligible for a
            /// topic's mesh or fanout only if its set contains that topic.
            topics: std.StringHashMapUnmanaged(void) = .empty,
            /// Heap-owned (Transport-allocated) so its address is stable while
            /// the writer fiber holds it. The sink (and its stream) MUST outlive
            /// the writer fiber — torn down only after the writer is awaited.
            sink: *Sink,
            writer: *Writer,
            writer_future: std.Io.Future(void),
            /// Back-pointer for the writer's on_disconnect callback, which
            /// receives this PeerState as its context and needs the router to
            /// post peer_disconnected. Set in the initializer below so it is
            /// valid before the writer fiber spawns.
            router_for_disconnect: *Self,
        };

        /// Posts inbound RPCs to the router's single Command inbox by wrapping
        /// each in the `inbound_rpc` Command variant. This is the bridge the
        /// generalised PeerReader posts through, so inbound RPCs share one
        /// ordered queue with every other router event. Transport-agnostic
        /// (references only the router + Command), so it lives here.
        pub const InboxPoster = struct {
            router: *Self,

            pub fn post(self: *InboxPoster, io: std.Io, in: peer_io.InboundRpc) anyerror!void {
                return self.router.inbox.putOne(io, .{ .inbound_rpc = in });
            }
        };

        allocator: std.mem.Allocator,
        io: std.Io,
        /// The per-peer sink factory. Held by value: the real `SwitchTransport`
        /// is empty, so by-value avoids an extra borrowed pointer.
        transport: Transport,
        inbox_storage: []Command,
        inbox: std.Io.Queue(Command),
        /// Keyed by zero-padded peer bytes (see PeerKey). Values are heap-owned.
        peers: std.AutoHashMap(PeerKey, *PeerState),
        /// Topics the local node subscribes to. Keys are owned copies; freed on
        /// unsubscribe and on teardown. We announce these to every peer (on our
        /// own subscribe, and to each newly-connected peer) and deliver matching
        /// inbound/published messages to `message_handler`.
        my_topics: std.StringHashMapUnmanaged(void) = .empty,
        /// Per-topic mesh membership: topic → set of peers we exchange full
        /// messages with for that topic. Topic keys are owned copies (freed on
        /// teardown); the nested PeerSet holds zero-padded peer keys.
        mesh: std.StringHashMapUnmanaged(PeerSet) = .empty,
        /// Per-topic graft backoff: topic → (peer → expiry tick). A peer is in
        /// backoff for a topic while its expiry tick is still in the future
        /// (`> heartbeat_tick`); we will not re-graft it until then. Topic keys
        /// are owned copies (freed on teardown).
        backoff: std.StringHashMapUnmanaged(BackoffSet) = .empty,
        /// Per-topic fanout membership: topic → set of peers we forward to when we
        /// publish to a topic we do NOT subscribe to (so we have no mesh for it).
        /// Topic keys are owned copies (freed on expiry and on teardown); the
        /// nested PeerSet holds zero-padded peer keys.
        fanout: std.StringHashMapUnmanaged(PeerSet) = .empty,
        /// Per-topic last-publish tick for fanout topics: topic → the
        /// `heartbeat_tick` at which we last published. A fanout topic whose last
        /// publish is older than `fanout_ttl_ticks` is dropped by the heartbeat.
        /// Topic keys are owned copies (freed alongside the matching `fanout`
        /// entry, which always exists when this does).
        fanout_last_pub: std.StringHashMapUnmanaged(u64) = .empty,
        /// Monotonic heartbeat counter (one tick per heartbeat). Backoff expiry is
        /// measured against this.
        heartbeat_tick: u64 = 0,
        /// Heartbeat period in milliseconds. Zero disables the heartbeat fiber
        /// (tests then drive ticks by posting `Command{.heartbeat}` directly).
        heartbeat_interval_ms: u64,
        /// The heartbeat fiber's future, when one was spawned. Cancelled + awaited
        /// on destroy (mirrors the per-peer writer teardown).
        heartbeat_future: ?std.Io.Future(void) = null,
        /// Bounded dedup window over recently-seen message ids.
        seen: SeenCache,
        /// Windowed cache of recently-forwarded/published messages (keyed by
        /// message id), holding one reference on each message's shared frame.
        /// Serves later gossip needs — IWANT (the full message via `get`) and
        /// IHAVE (recent ids per topic via `getGossipIDs`). Distinct from `seen`
        /// (which only remembers ids for dedup). The window slides once per
        /// heartbeat (`shift`).
        message_cache: mcache.MessageCache,
        /// Monotonic sequence number for messages WE originate; encoded big-endian
        /// into Message.seqno so (from, seqno) is a unique message id.
        seqno: u64 = 0,
        /// Our own peer id, used as Message.from on publish.
        local_peer: PeerId,
        /// Signature policy. When set (StrictSign), every published message is
        /// signed and every inbound published message is verified — invalid or
        /// unsigned inbound messages are rejected (dropped, never delivered or
        /// forwarded). When null (none), messages carry from+seqno with no
        /// signature and no inbound verification (the de-risk / fake-transport
        /// behaviour). Owns its cached pubkey bytes; freed on destroy.
        signer: ?signing.Signer,
        /// Optional sink for messages delivered on topics we subscribe to.
        message_handler: ?MessageHandler,
        /// When true (go-libp2p default), a message the local node ORIGINATES is
        /// flooded to every topic subscriber above the publish threshold rather
        /// than only its mesh/fanout. Relayed messages are unaffected (always
        /// mesh-only). See `RouterConfig.flood_publish`.
        flood_publish: bool,
        /// Optional peer-scoring engine (the gossipsub v1.1 P1-P7 accountant).
        /// Null disables scoring entirely: no events are fired and no gate is
        /// applied, so the router's behaviour is unchanged. When non-null it is
        /// heap-owned (built in `create`, freed in `destroy`) and driven only on
        /// the single router fiber, so it needs no locking.
        score: ?*score_mod.PeerScore,
        /// Per-heartbeat snapshot of every tracked peer's score, recomputed once
        /// at the top of each heartbeat (after `decay`) so the gates run on
        /// fresh scores without recomputing `score()` in the hot mesh/gossip
        /// loops (go-libp2p caches scores per heartbeat the same way). Empty when
        /// scoring is disabled; cleared + rebuilt each heartbeat. Freed on
        /// destroy. Keys mirror the engine's `PeerKey` (zero-padded peer bytes).
        score_snapshot: std.AutoHashMapUnmanaged(score_mod.PeerKey, f64) = .empty,
        main_future: ?std.Io.Future(void) = null,
        /// Set once when teardown begins so the main loop stops after the inbox
        /// drains/closes. Atomic because it is read on the main fiber but set on
        /// the caller's fiber (deinit path).
        stopping: std.atomic.Value(bool) = .init(false),
        /// Number of live peers, published for observers (e.g. tests).
        peer_count: std.atomic.Value(usize) = .init(0),

        /// `host_key` selects the signature policy: non-null enables StrictSign
        /// (sign outbound, verify inbound) and the router derives + uses the
        /// key's peer-id as `local_peer` (ignoring the passed `local_peer`, which
        /// must match — they are the same node); null keeps the none policy and
        /// uses `local_peer` as given. The KeyPair is borrowed and must outlive
        /// the router.
        pub fn create(
            allocator: std.mem.Allocator,
            io: std.Io,
            transport: Transport,
            local_peer: PeerId,
            message_handler: ?MessageHandler,
            heartbeat_interval_ms: u64,
            host_key: ?*const identity.KeyPair,
            score_config: ?ScoreConfig,
            config: RouterConfig,
        ) !*Self {
            const inbox_storage = try allocator.alloc(Command, inbox_capacity);
            errdefer allocator.free(inbox_storage);

            var seen = try SeenCache.init(allocator);
            errdefer seen.deinit();

            // StrictSign when a host key is supplied: the Signer caches the
            // marshaled pubkey + peer-id once so per-publish signing is cheap.
            // The signer's peer-id is authoritative for `from` on publish.
            var signer: ?signing.Signer = if (host_key) |k| try signing.Signer.init(allocator, k) else null;
            errdefer if (signer) |*s| s.deinit();
            const effective_peer = if (signer) |*s| s.from_peer else local_peer;

            // Build the scoring engine only when a config is supplied; null leaves
            // `score` null and every gate a no-op (current behaviour).
            const score_engine: ?*score_mod.PeerScore = if (score_config) |cfg| blk: {
                const ps = try allocator.create(score_mod.PeerScore);
                ps.* = score_mod.PeerScore.init(allocator, cfg.params, cfg.thresholds);
                break :blk ps;
            } else null;
            errdefer if (score_engine) |ps| {
                ps.deinit();
                allocator.destroy(ps);
            };

            const router = try allocator.create(Self);
            router.* = .{
                .allocator = allocator,
                .io = io,
                .transport = transport,
                .inbox_storage = inbox_storage,
                .inbox = std.Io.Queue(Command).init(inbox_storage),
                .peers = std.AutoHashMap(PeerKey, *PeerState).init(allocator),
                .seen = seen,
                .message_cache = mcache.MessageCache.init(allocator),
                .local_peer = effective_peer,
                .signer = signer,
                .message_handler = message_handler,
                .flood_publish = config.flood_publish,
                .score = score_engine,
                .heartbeat_interval_ms = heartbeat_interval_ms,
            };
            return router;
        }

        /// Spawn the main fiber (and, when the heartbeat interval is non-zero, the
        /// heartbeat fiber). Call once after `create`.
        pub fn start(router: *Self) std.Io.ConcurrentError!void {
            router.main_future = try std.Io.concurrent(router.io, mainLoop, .{router});
            if (router.heartbeat_interval_ms > 0) {
                router.heartbeat_future = try std.Io.concurrent(router.io, heartbeatLoop, .{router});
            }
        }

        /// Heartbeat fiber body: post a `heartbeat` command every interval until
        /// the router is stopping (or the inbox closes). The interval `sleep` is a
        /// cancellation point, so `destroy`'s cancel collapses the wait. The post
        /// is uncancelable + best-effort: a closed inbox (shutdown) just ends the
        /// loop. Only spawned when `heartbeat_interval_ms > 0`.
        fn heartbeatLoop(router: *Self) void {
            while (!router.stopping.load(.acquire)) {
                io_time.ms(router.heartbeat_interval_ms).sleep(router.io) catch break;
                router.inbox.putOneUncancelable(router.io, .heartbeat) catch break;
            }
        }

        /// Stop the router and free all of its resources. Sets the persistent
        /// stopping flag, closes the inbox to wake the main loop, posts a
        /// `shutdown` as a backstop, cancels + awaits the main fiber, then frees
        /// the inbox storage and the router. The main fiber tears down every
        /// peer on its way out, so this is safe to call from any other fiber.
        pub fn destroy(router: *Self) void {
            router.stopping.store(true, .release);

            // Stop the heartbeat fiber before draining the main loop: it posts to
            // the inbox, so it must be joined before the inbox storage is freed.
            // Cancel collapses any in-flight interval `sleep` (a cancellation
            // point); await then joins. cancel+await on one Future is safe (cancel
            // is idempotent and clears the future, so await returns the cached
            // result). Mirrors the per-peer writer teardown.
            if (router.heartbeat_future) |*future| {
                future.cancel(router.io);
                future.await(router.io);
                router.heartbeat_future = null;
            }

            // Post shutdown first (so a main loop parked in getOne wakes and runs
            // the peer teardown on its own fiber), then close the inbox. close()
            // alone would make getOne return Closed before processing shutdown,
            // but the main loop also tears peers down on Closed via
            // `teardownAllPeers`, so either path is safe. Use the uncancelable
            // post to avoid losing it.
            router.inbox.putOneUncancelable(router.io, .shutdown) catch {};

            if (router.main_future) |*future| {
                // The main loop exits on shutdown / closed inbox; cancel is a
                // backstop in case it is parked in a non-inbox cancellation
                // point. Every blocking point in the loop is a cancel point, so
                // this cannot re-park. cancel before await so a parked loop
                // unparks.
                future.cancel(router.io);
                future.await(router.io);
                router.main_future = null;
            } else {
                // Never spawned: tear down directly.
                router.teardownAllPeers();
            }

            router.peers.deinit();
            router.freeMyTopics();
            router.freeMesh();
            router.freeBackoff();
            router.freeFanout();
            router.seen.deinit();
            router.message_cache.deinit();
            if (router.signer) |*s| s.deinit();
            if (router.score) |ps| {
                ps.deinit();
                router.allocator.destroy(ps);
            }
            router.score_snapshot.deinit(router.allocator);
            router.allocator.free(router.inbox_storage);
            router.allocator.destroy(router);
        }

        /// Free every key in `my_topics` and the map itself. The main fiber is no
        /// longer running by the time this is called (destroy joined it), so the
        /// map is quiescent.
        fn freeMyTopics(router: *Self) void {
            var it = router.my_topics.keyIterator();
            while (it.next()) |key| router.allocator.free(key.*);
            router.my_topics.deinit(router.allocator);
        }

        /// Free every topic key + nested PeerSet in the mesh map and the map
        /// itself. The main fiber is joined by the time this runs, so the map is
        /// quiescent.
        fn freeMesh(router: *Self) void {
            var it = router.mesh.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit(router.allocator);
                router.allocator.free(entry.key_ptr.*);
            }
            router.mesh.deinit(router.allocator);
        }

        /// Free every topic key + nested BackoffSet in the backoff map and the map
        /// itself.
        fn freeBackoff(router: *Self) void {
            var it = router.backoff.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit(router.allocator);
                router.allocator.free(entry.key_ptr.*);
            }
            router.backoff.deinit(router.allocator);
        }

        /// Free every topic key + nested PeerSet in the fanout map, the
        /// fanout_last_pub map (its keys are the SAME owned topic copies as the
        /// fanout map's, freed once above), and both maps. The main fiber is joined
        /// by the time this runs, so the maps are quiescent.
        fn freeFanout(router: *Self) void {
            var it = router.fanout.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit(router.allocator);
                router.allocator.free(entry.key_ptr.*);
            }
            router.fanout.deinit(router.allocator);
            // fanout_last_pub's keys alias fanout's (freed above), so only the map.
            router.fanout_last_pub.deinit(router.allocator);
        }

        // ----- mesh + backoff helpers -------------------------------------

        /// Whether `topic`'s mesh contains `peer`.
        fn meshContains(router: *Self, topic: []const u8, peer: PeerId) bool {
            const set = router.mesh.getPtr(topic) orelse return false;
            return set.contains(peerKey(&peer));
        }

        /// Add `peer` to `topic`'s mesh, creating the topic's PeerSet (with an
        /// owned topic-key copy) on first use. Best-effort: an allocation failure
        /// silently leaves the peer out of the mesh. Fires a scoring GRAFT event
        /// (when scoring is enabled) so the engine starts accruing P1 mesh time
        /// and treats the peer as a mesh member for P3 delivery credit.
        fn meshAdd(router: *Self, topic: []const u8, peer: PeerId) void {
            const gop = router.mesh.getOrPut(router.allocator, topic) catch return;
            if (!gop.found_existing) {
                const owned = router.allocator.dupe(u8, topic) catch {
                    router.mesh.removeByPtr(gop.key_ptr);
                    return;
                };
                gop.key_ptr.* = owned;
                gop.value_ptr.* = .empty;
            }
            gop.value_ptr.put(router.allocator, peerKey(&peer), {}) catch {};
            if (router.score) |sc| sc.graft(peer, topic);
        }

        /// Remove `peer` from `topic`'s mesh (no-op if absent). The empty PeerSet
        /// is kept (cheap; reused on the next graft). Fires a scoring PRUNE event
        /// (when scoring is enabled) so the engine captures any P3b mesh-failure
        /// penalty and stops counting the peer as a mesh member.
        fn meshRemove(router: *Self, topic: []const u8, peer: PeerId) void {
            const set = router.mesh.getPtr(topic) orelse return;
            _ = set.remove(peerKey(&peer));
            if (router.score) |sc| sc.prune(peer, topic);
        }

        /// Number of peers in `topic`'s mesh (zero if the topic has no mesh yet).
        fn meshSize(router: *Self, topic: []const u8) usize {
            const set = router.mesh.getPtr(topic) orelse return 0;
            return set.count();
        }

        /// Whether `peer` is currently backed off for `topic` (its expiry tick is
        /// still in the future).
        fn inBackoff(router: *Self, topic: []const u8, peer: PeerId) bool {
            const set = router.backoff.getPtr(topic) orelse return false;
            const expiry = set.get(peerKey(&peer)) orelse return false;
            return expiry > router.heartbeat_tick;
        }

        /// Back `peer` off for `topic` for `ticks` heartbeats from now (the expiry
        /// tick is `heartbeat_tick +| ticks`, saturating). Creates the topic's
        /// BackoffSet (with an owned topic-key copy) on first use. A later/larger
        /// expiry already stored is kept (never shortened). Best-effort on
        /// allocation failure. `ticks` can be an attacker-controlled wire value
        /// (PRUNE backoff seconds): the saturating add can never overflow, so a
        /// peer sending a huge backoff only locks itself out (its own loss) rather
        /// than crashing us (Debug/safe builds) or wrapping to a near-zero expiry
        /// that would silently disable the backoff (release builds).
        fn setBackoff(router: *Self, topic: []const u8, peer: PeerId, ticks: u64) void {
            const expiry = router.heartbeat_tick +| ticks;
            const gop = router.backoff.getOrPut(router.allocator, topic) catch return;
            if (!gop.found_existing) {
                const owned = router.allocator.dupe(u8, topic) catch {
                    router.backoff.removeByPtr(gop.key_ptr);
                    return;
                };
                gop.key_ptr.* = owned;
                gop.value_ptr.* = .empty;
            }
            const peer_gop = gop.value_ptr.getOrPut(router.allocator, peerKey(&peer)) catch return;
            if (!peer_gop.found_existing or peer_gop.value_ptr.* < expiry) {
                peer_gop.value_ptr.* = expiry;
            }
        }

        /// Drop `peer` from every topic's mesh, every topic's fanout set, and
        /// every topic's backoff set. Called when a peer disconnects so no stale
        /// membership survives it.
        fn dropPeerFromMeshAndBackoff(router: *Self, peer: PeerId) void {
            const key = peerKey(&peer);
            var mesh_it = router.mesh.valueIterator();
            while (mesh_it.next()) |set| _ = set.remove(key);
            var fanout_it = router.fanout.valueIterator();
            while (fanout_it.next()) |set| _ = set.remove(key);
            var backoff_it = router.backoff.valueIterator();
            while (backoff_it.next()) |set| _ = set.remove(key);
        }

        // ----- scoring helpers --------------------------------------------

        /// The peer's current score for gate decisions: read from the
        /// per-heartbeat snapshot if present (the common case during mesh/gossip
        /// maintenance, where the snapshot was just refreshed), else computed
        /// live from the engine. Returns 0 when scoring is disabled (no engine),
        /// so an "is this peer below zero?" gate is naturally inert. An untracked
        /// peer also scores 0.
        fn peerScore(router: *Self, peer: PeerId) f64 {
            const sc = router.score orelse return 0;
            if (router.score_snapshot.get(score_mod.peerKey(&peer))) |s| return s;
            return sc.score(peer);
        }

        /// Advance the scoring engine one heartbeat tick (decaying counters,
        /// accruing mesh time, purging long-gone peers) and rebuild the
        /// per-heartbeat score snapshot so the gates run on fresh scores without
        /// recomputing `score()` in the hot loops. No-op when scoring is
        /// disabled. The snapshot is cleared and repopulated for exactly the
        /// currently-tracked peers each heartbeat. Best-effort: a snapshot insert
        /// that fails to allocate just falls back to a live `score()` in
        /// `peerScore` for that peer.
        fn refreshScoreSnapshot(router: *Self) void {
            const sc = router.score orelse return;
            sc.decay(router.heartbeat_tick);
            router.score_snapshot.clearRetainingCapacity();
            var it = router.peers.iterator();
            while (it.next()) |entry| {
                const peer = entry.value_ptr.*.peer;
                router.score_snapshot.put(router.allocator, score_mod.peerKey(&peer), sc.score(peer)) catch {};
            }
        }

        /// Whether `peer` is eligible to receive a flood-published (originated)
        /// message: with scoring disabled every peer qualifies; with scoring
        /// enabled the peer must be at or above the publish threshold (go-libp2p
        /// floods originated messages only to peers above `PublishThreshold`).
        fn abovePublishThreshold(router: *Self, peer: PeerId) bool {
            const sc = router.score orelse return true;
            return sc.abovePublishThreshold(peer);
        }

        // ----- graft / prune peer selection -------------------------------

        /// Select up to `n` peers to GRAFT into `topic`'s mesh: peers that
        /// announced they subscribe to `topic`, are not already in its mesh, and
        /// are not in backoff for it. Appends the chosen peers to `out` (capped at
        /// `n`) and returns the count appended. Iteration order of the peer map is
        /// the selection order; randomised shuffle for fairness/anti-eclipse is a
        /// future refinement.
        fn selectGraftCandidates(router: *Self, topic: []const u8, n: usize, out: *std.ArrayListUnmanaged(PeerId)) usize {
            if (n == 0) return 0;
            var added: usize = 0;
            var it = router.peers.iterator();
            while (it.next()) |entry| {
                if (added >= n) break;
                const peer = entry.value_ptr.*.peer;
                if (!entry.value_ptr.*.topics.contains(topic)) continue;
                if (router.meshContains(topic, peer)) continue;
                if (router.inBackoff(topic, peer)) continue;
                // Never graft a negative-scoring peer into the mesh (go-libp2p
                // only grafts peers at or above zero during maintenance). Inert
                // when scoring is disabled (peerScore is 0).
                if (router.peerScore(peer) < 0) continue;
                out.append(router.allocator, peer) catch break;
                added += 1;
            }
            return added;
        }

        /// Select up to `n` current members of `topic`'s mesh to PRUNE out (used to
        /// shrink an over-full mesh back toward D). Appends the chosen peers to
        /// `out` and returns the count appended.
        ///
        /// With scoring enabled, the victims are the LOWEST-scoring excess: the
        /// mesh members are sorted by score (descending) and the bottom `n` are
        /// dropped, so the heartbeat retains the highest-scoring ~D peers (a
        /// simplified form of go-libp2p's score-aware prune; the finer D_out
        /// outbound-quota retention is a future refinement). With scoring
        /// disabled the victims are arbitrary excess in mesh-iteration order
        /// (unchanged from before scoring existed).
        fn selectPruneVictims(router: *Self, topic: []const u8, n: usize, out: *std.ArrayListUnmanaged(PeerKey)) usize {
            if (n == 0) return 0;
            const set = router.mesh.getPtr(topic) orelse return 0;

            if (router.score != null) {
                // Gather (key, score) for every mesh member, sort ascending by
                // score, and take the lowest-scoring `n` as victims (keeping the
                // highest-scoring peers in the mesh).
                const Scored = struct { key: PeerKey, score: f64 };
                var scored: std.ArrayListUnmanaged(Scored) = .empty;
                defer scored.deinit(router.allocator);
                var it = set.keyIterator();
                while (it.next()) |key_ptr| {
                    const sc = if (router.score_snapshot.get(key_ptr.*)) |s| s else 0;
                    scored.append(router.allocator, .{ .key = key_ptr.*, .score = sc }) catch break;
                }
                std.mem.sort(Scored, scored.items, {}, struct {
                    fn lessThan(_: void, a: Scored, b: Scored) bool {
                        return a.score < b.score;
                    }
                }.lessThan);
                var added: usize = 0;
                for (scored.items) |s| {
                    if (added >= n) break;
                    out.append(router.allocator, s.key) catch break;
                    added += 1;
                }
                return added;
            }

            var added: usize = 0;
            var it = set.keyIterator();
            while (it.next()) |key_ptr| {
                if (added >= n) break;
                out.append(router.allocator, key_ptr.*) catch break;
                added += 1;
            }
            return added;
        }

        // ----- fanout helpers ---------------------------------------------

        /// Ensure a `fanout` PeerSet exists for `topic` and return it. Creates the
        /// entry (with an owned topic-key copy, shared with `fanout_last_pub`) on
        /// first use. Returns null on allocation failure.
        fn fanoutGetOrCreate(router: *Self, topic: []const u8) ?*PeerSet {
            const gop = router.fanout.getOrPut(router.allocator, topic) catch return null;
            if (!gop.found_existing) {
                const owned = router.allocator.dupe(u8, topic) catch {
                    router.fanout.removeByPtr(gop.key_ptr);
                    return null;
                };
                gop.key_ptr.* = owned;
                gop.value_ptr.* = .empty;
                // Mirror the same owned key into fanout_last_pub so the two maps
                // stay in lockstep (and share the one key allocation). On failure
                // unwind the fanout entry so we never leave a half-created topic.
                router.fanout_last_pub.putNoClobber(router.allocator, owned, router.heartbeat_tick) catch {
                    gop.value_ptr.deinit(router.allocator);
                    router.allocator.free(owned);
                    router.fanout.removeByPtr(gop.key_ptr);
                    return null;
                };
            }
            return gop.value_ptr;
        }

        /// Drop a fanout topic entirely: free its PeerSet and remove both the
        /// `fanout` and `fanout_last_pub` entries (the one owned key, freed once).
        fn fanoutDrop(router: *Self, topic: []const u8) void {
            if (router.fanout.fetchRemove(topic)) |kv| {
                var set = kv.value;
                set.deinit(router.allocator);
                _ = router.fanout_last_pub.remove(topic);
                router.allocator.free(kv.key);
            }
        }

        /// Top `set` up to D peers with peers subscribed to `topic` that are not
        /// already in it (used both to seed a fresh fanout and to replenish a
        /// shrunken one). Peer-map iteration order is the selection order.
        fn fanoutReplenish(router: *Self, topic: []const u8, set: *PeerSet) void {
            if (set.count() >= mesh_params.d) return;
            var it = router.peers.iterator();
            while (it.next()) |entry| {
                if (set.count() >= mesh_params.d) break;
                const peer = entry.value_ptr.*.peer;
                if (!entry.value_ptr.*.topics.contains(topic)) continue;
                set.put(router.allocator, peerKey(&peer), {}) catch break;
            }
        }

        /// Build a transient PeerSet of every tracked peer subscribed to `topic`
        /// that is eligible to receive an originated (flood-published) message —
        /// i.e. above the publish threshold when scoring is on, all of them when
        /// scoring is off. The caller OWNS the returned set and must `deinit` it;
        /// it is used only to TARGET the flood (the frame's references are held by
        /// the per-peer queues, not the set), so freeing it does not touch any
        /// frame. Returns an empty set on allocation failure (the publish still
        /// caches; it simply reaches no peers).
        fn floodTargets(router: *Self, topic: []const u8) PeerSet {
            var set: PeerSet = .empty;
            var it = router.peers.iterator();
            while (it.next()) |entry| {
                const peer = entry.value_ptr.*.peer;
                if (!entry.value_ptr.*.topics.contains(topic)) continue;
                if (!router.abovePublishThreshold(peer)) continue;
                set.put(router.allocator, peerKey(&peer), {}) catch break;
            }
            return set;
        }

        pub fn peerCount(router: *const Self) usize {
            return router.peer_count.load(.acquire);
        }

        /// Test-only: enqueue a data frame reference onto `peer`'s outbound queue
        /// and block until the router fiber has processed the push (or released
        /// the reference, if the peer is gone). Doing the lookup + push on the
        /// router fiber keeps the peer map single-threaded. Used to make a peer's
        /// writer attempt to open its stream, exercising the writer give-up path.
        pub fn enqueueDataForTest(router: *Self, peer: PeerId, frame: *peer_io.OutboundFrame) !void {
            var reply: std.Io.Event = .unset;
            try router.inbox.putOne(router.io, .{ .enqueue_for_test = .{
                .peer = peer,
                .frame = frame,
                .reply = &reply,
            } });
            reply.waitUncancelable(router.io);
        }

        // ----- main fiber --------------------------------------------------

        fn mainLoop(router: *Self) void {
            // On any exit (shutdown, closed inbox, cancellation) tear down every
            // peer and drain the inbox so nothing leaks.
            defer {
                router.teardownAllPeers();
                router.drainInbox();
            }

            while (true) {
                const command = router.inbox.getOne(router.io) catch return; // Closed/Canceled
                switch (command) {
                    .peer_connected => |c| router.onPeerConnected(c.peer, c.conn, c.remote_addr),
                    .peer_disconnected => |c| router.onPeerDisconnected(c.peer),
                    .inbound_rpc => |in| router.onInboundRpc(in),
                    .subscribe => |s| router.onSubscribe(s.topic),
                    .unsubscribe => |u| router.onUnsubscribe(u.topic),
                    .publish => |p| router.onPublish(p.topic, p.data),
                    .enqueue_for_test => |e| router.onEnqueueForTest(e.peer, e.frame, e.reply),
                    .heartbeat => router.onHeartbeat(),
                    .shutdown => return,
                }
            }
        }

        /// Handle a peer connecting: dedup, then create per-peer state and spawn
        /// its writer fiber. The writer opens the outbound stream lazily on its
        /// first frame; on open-exhaustion it posts `peer_disconnected` so the
        /// peer is torn down through the normal path.
        fn onPeerConnected(router: *Self, peer: PeerId, conn: ConnHandle, remote_addr: std.Io.net.IpAddress) void {
            const key = peerKey(&peer);
            // A second connection to a peer we already track: keep the first,
            // ignore the new one's gossipsub setup. One logical peer entry.
            if (router.peers.contains(key)) return;

            const state = router.allocator.create(PeerState) catch return;
            var state_live = false;
            defer if (!state_live) router.allocator.destroy(state);

            // The transport allocates + initialises the sink (no I/O). The router
            // owns it from here: it is closed + destroyed in teardownPeer.
            const sink = router.transport.makeSink(router.allocator, peer, conn) catch return;
            var sink_live = false;
            defer if (!sink_live) router.allocator.destroy(sink);

            const writer = router.allocator.create(Writer) catch return;
            var writer_live = false;
            defer if (!writer_live) router.allocator.destroy(writer);

            state.* = .{
                .peer = peer,
                .queue = peer_io.OutboundQueue.init(router.allocator, .{}),
                .sink = sink,
                .writer = writer,
                .writer_future = undefined,
                // Set up-front (not on a later line) so the back-pointer is valid
                // before the writer fiber spawns and can fire on_disconnect.
                .router_for_disconnect = router,
            };
            // Past this point `state.queue` is initialised; deinit it on any
            // failure before the writer fiber takes ownership of draining it.
            var queue_live = false;
            defer if (!queue_live) state.queue.deinit(router.io);

            writer.* = .{
                .queue = &state.queue,
                .sink = sink,
                .on_disconnect = onWriterDisconnect,
                .disconnect_ctx = state,
            };
            // The writer reaches the router via `state.router_for_disconnect`
            // (set in the PeerState initializer above) and tags
            // peer_disconnected with `state.peer`, recovering both from
            // `disconnect_ctx`.

            const future = std.Io.concurrent(router.io, Writer.run, .{ writer, router.io }) catch return;

            // All fallible steps done: the entry is live. Disarm the cleanups so
            // the PeerState (and the writer fiber draining its queue) survives.
            state.writer_future = future;
            router.peers.put(key, state) catch {
                // The map insert failed after the writer fiber started. Tear the
                // writer down cleanly (close queue, cancel+await fiber) before
                // freeing, so we don't leak the fiber or use-after-free the sink.
                // Cancel before await so a writer parked in its reopen backoff
                // does not stall this fiber; the backoff sleep is a cancellation
                // point.
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

            // Begin scoring the peer (a brand-new entry, or a re-activation of a
            // recently-disconnected one retained for its score) and record its
            // remote IP for the P6 colocation term. The IP is the peer's address
            // with the port stripped (the engine keys colocation per address).
            if (router.score) |sc| {
                sc.addPeer(peer);
                sc.addIP(peer, remote_addr);
            }

            // Tell the new peer which topics we already subscribe to, so it can
            // start forwarding matching messages to us right away (mirrors how
            // go-libp2p-pubsub sends the full current subscription set on a new
            // connection). Best-effort: a framing/push failure just means the peer
            // learns our subscriptions on our next subscribe/unsubscribe.
            router.sendCurrentSubscriptions(state);
        }

        /// Which peers a `fanOut` hands a shared frame to. The frame is built once
        /// and one reference is pushed per resolved target.
        const Targets = union(enum) {
            /// Every tracked peer. Used for subscription announces.
            all,
            /// Every still-tracked peer whose key is in `set` (a mesh or fanout
            /// PeerSet), except `exclude` (the relay source) when set. The set is
            /// borrowed and read on this fiber, so it cannot mutate underneath us.
            peer_set: struct { set: *const PeerSet, exclude: ?PeerId = null },
            /// A single peer; a no-op if it is not tracked.
            one: PeerId,
        };

        /// Frame `rpc` ONCE into a refcounted shared `OutboundFrame` and hand one
        /// reference to each resolved target's `lane` queue — the single home of
        /// the builder-reference protocol (frame once, hold the builder reference,
        /// `retain` before every push and `release` on a rejected push, then drop
        /// the builder reference at the end so the frame frees itself iff no queue
        /// kept a copy). No per-peer copy of the (up-to-1 MiB) wire bytes.
        ///
        /// Takes ownership of `ids` (the message ids carried in the frame for a
        /// later IDONTWANT purge; pass an empty owned slice for non-data frames):
        /// on success the frame owns them; on a framing/allocation failure they are
        /// freed here. Best-effort throughout (a void return): a framing failure or
        /// a per-peer push failure logs nothing and simply drops that copy.
        fn fanOut(router: *Self, lane: peer_io.Lane, rpc_frame: rpc_pb.RPC, ids: [][]u8, targets: Targets) void {
            const framed = pubsub.frameRpc(router.allocator, rpc_frame) catch {
                for (ids) |id| router.allocator.free(id);
                router.allocator.free(ids);
                return;
            };
            const frame = peer_io.OutboundFrame.create(router.allocator, framed, ids, 1) catch {
                router.allocator.free(framed);
                for (ids) |id| router.allocator.free(id);
                router.allocator.free(ids);
                return;
            };
            // Builder reference dropped at the end; queues hold the rest. If no
            // queue accepted a push this release frees the whole frame.
            defer frame.release();
            router.fanOutFrame(lane, frame, targets);
        }

        /// Fan an already-built shared frame out to `targets` on `lane`, handing
        /// one reference to each resolved peer's queue (`pushTo` retains before
        /// the push and releases on a rejected push). The CALLER owns its builder
        /// reference and must release it afterward — this does NOT consume one.
        /// Lets a caller that built a frame once (e.g. the message forward path,
        /// which also stores the frame in the message cache) reuse it across both
        /// the cache and the fan-out without re-framing. The set, when given, is
        /// borrowed and read on this fiber, so it cannot mutate underneath us.
        fn fanOutFrame(router: *Self, lane: peer_io.Lane, frame: *peer_io.OutboundFrame, targets: Targets) void {
            switch (targets) {
                .one => |peer| {
                    const state = router.peers.get(peerKey(&peer)) orelse return;
                    router.pushTo(state, lane, frame);
                },
                .all => {
                    var it = router.peers.iterator();
                    while (it.next()) |entry| router.pushTo(entry.value_ptr.*, lane, frame);
                },
                .peer_set => |t| {
                    var it = t.set.keyIterator();
                    while (it.next()) |key_ptr| {
                        const state = router.peers.get(key_ptr.*) orelse continue;
                        if (t.exclude) |ex| if (state.peer.eql(&ex)) continue;
                        router.pushTo(state, lane, frame);
                    }
                },
            }
        }

        /// Hand one shared-frame reference to a peer's lane queue: retain before
        /// the push, release on a rejected push (the queue did not take it).
        fn pushTo(router: *Self, state: *PeerState, lane: peer_io.Lane, frame: *peer_io.OutboundFrame) void {
            frame.retain();
            state.queue.push(router.io, lane, frame) catch frame.release();
        }

        /// Allocate an empty owned id slice for a frame that carries no message
        /// ids (every lane except data forwards). Returns null on OOM so the
        /// caller can bail before framing.
        fn emptyIds(router: *Self) ?[][]u8 {
            return router.allocator.alloc([]u8, 0) catch null;
        }

        /// Send the local node's full current subscription set to one peer's
        /// `.subscribe` lane as a single subscription RPC. No-op when we have no
        /// subscriptions. Best-effort: drops the frame on a push failure.
        ///
        /// The transient SubOpts array (one entry per local topic) is the only
        /// genuinely multi-allocation scratch on this path, so it goes in a
        /// per-command arena that is freed in one shot — no per-entry bookkeeping.
        fn sendCurrentSubscriptions(router: *Self, state: *PeerState) void {
            if (router.my_topics.count() == 0) return;

            var arena = std.heap.ArenaAllocator.init(router.allocator);
            defer arena.deinit();
            const scratch = arena.allocator();

            var subs: std.ArrayListUnmanaged(?rpc_pb.RPC.SubOpts) = .empty;
            var it = router.my_topics.keyIterator();
            while (it.next()) |key| {
                subs.append(scratch, rpc.buildSubscription(key.*, true)) catch return;
            }

            const ids = router.emptyIds() orelse return;
            router.fanOut(.subscribe, (rpc.RpcOut{ .subscriptions = subs.items }).toRpc(), ids, .{ .one = state.peer });
        }

        /// Handle a peer disconnecting: look up, tear down its writer + state.
        /// Absent peer (already removed, or a dedup'd second connection) is a
        /// no-op.
        fn onPeerDisconnected(router: *Self, peer: PeerId) void {
            const key = peerKey(&peer);
            const entry = router.peers.fetchRemove(key) orelse return;
            router.dropPeerFromMeshAndBackoff(peer);
            router.teardownPeer(entry.value);
            _ = router.peer_count.fetchSub(1, .release);
        }

        /// Handle one heartbeat tick: advance the tick counter, expire stale
        /// backoffs, then run mesh maintenance (graft below D_low / prune above
        /// D_high for each subscribed topic) and fanout maintenance (expire stale
        /// fanout topics, replenish short ones). Gossip emission (IHAVE),
        /// opportunistic graft, and score-based selection are later layers.
        fn onHeartbeat(router: *Self) void {
            router.heartbeat_tick += 1;
            const tick = router.heartbeat_tick;
            var topic_it = router.backoff.valueIterator();
            while (topic_it.next()) |set| {
                // Restart the scan after each removal: removing during iteration
                // can move the unscanned tail in an open-addressing map, so a
                // single pass could skip an entry. Backoff sets are small (peers
                // pruned for one topic), so the restart is cheap.
                var changed = true;
                while (changed) {
                    changed = false;
                    var entry_it = set.iterator();
                    while (entry_it.next()) |entry| {
                        if (entry.value_ptr.* <= tick) {
                            _ = set.remove(entry.key_ptr.*);
                            changed = true;
                            break;
                        }
                    }
                }
            }

            // Advance the scoring engine one tick (decays the fading counters,
            // accrues mesh time, activates P3, purges long-gone peers) and
            // refresh the per-heartbeat score snapshot, BEFORE mesh/gossip
            // maintenance so every gate below runs on the freshly-decayed scores
            // (go-libp2p caches scores per heartbeat the same way).
            router.refreshScoreSnapshot();

            router.maintainMeshes();
            router.maintainFanout();

            // Advertise recently-cached message ids (IHAVE) to gossip-eligible
            // non-mesh peers BEFORE sliding the window: emission reads the
            // gossipable windows, and the shift below evicts the oldest, so
            // emitting first advertises everything still in the window.
            router.emitGossip();

            // Slide the message-cache window once per heartbeat so it retains the
            // last history_length heartbeats' worth of messages (the oldest is
            // evicted, a fresh newest window opens).
            router.message_cache.shift();
        }

        /// Emit IHAVE gossip for every topic we participate in. For each topic
        /// (subscribed topics, plus active fanout topics) advertise the message ids
        /// in the cache's gossipable windows to up to `d_lazy` peers that subscribe
        /// to the topic but are NOT in its mesh (and, for a fanout topic, not in its
        /// fanout set) — the "lazy" peers that get gossip rather than full messages.
        ///
        /// Selection is deterministic (peer-map iteration order); a random shuffle
        /// for anti-eclipse fairness, and `GossipFactor`-scaled fan-out, are future
        /// refinements. Score-gated emission arrives with peer scoring.
        fn emitGossip(router: *Self) void {
            var topic_it = router.my_topics.keyIterator();
            while (topic_it.next()) |key| router.emitGossipForTopic(key.*);

            var fanout_it = router.fanout.iterator();
            while (fanout_it.next()) |entry| {
                // A subscribed topic also living in fanout cannot happen (subscribe
                // drops the fanout entry), so this never double-emits.
                router.emitGossipForTopic(entry.key_ptr.*);
            }
        }

        /// Emit IHAVE for a single topic: gather the cache's gossipable ids for it
        /// (truncated at `max_ihave_length`), then send an IHAVE carrying them to up
        /// to `d_lazy` gossip-eligible peers. No-op if the cache has nothing for the
        /// topic. The borrowed ids stay valid through the sends (no `shift` happens
        /// before this returns) and `frameRpc` copies them into each frame.
        fn emitGossipForTopic(router: *Self, topic: []const u8) void {
            const ids = router.message_cache.getGossipIDs(router.allocator, topic) catch return;
            defer router.allocator.free(ids);
            if (ids.len == 0) return;

            // Cap the advertised id count. The borrowed `[]const u8` ids are passed
            // straight to the builder (frameRpc copies them), so a sub-slice is fine.
            const capped = ids[0..@min(ids.len, mesh_params.max_ihave_length)];

            var targets: std.ArrayListUnmanaged(PeerId) = .empty;
            defer targets.deinit(router.allocator);
            router.selectGossipTargets(topic, &targets);

            for (targets.items) |peer| router.sendIHave(peer, topic, capped);
        }

        /// Append up to `d_lazy` gossip targets for `topic` to `out`: peers that
        /// announced they subscribe to `topic` but are NOT in its mesh and NOT in
        /// its fanout set (those already get full messages). Peer-map iteration
        /// order is the selection order (deterministic; shuffle is a refinement).
        fn selectGossipTargets(router: *Self, topic: []const u8, out: *std.ArrayListUnmanaged(PeerId)) void {
            const mesh_set = router.mesh.getPtr(topic);
            const fanout_set = router.fanout.getPtr(topic);
            var it = router.peers.iterator();
            while (it.next()) |entry| {
                if (out.items.len >= mesh_params.d_lazy) break;
                const peer = entry.value_ptr.*.peer;
                if (!entry.value_ptr.*.topics.contains(topic)) continue;
                const key = peerKey(&peer);
                if (mesh_set) |s| if (s.contains(key)) continue;
                if (fanout_set) |s| if (s.contains(key)) continue;
                // Gossip gate: only advertise (IHAVE) to peers whose score clears
                // the gossip threshold; a peer below it is denied gossip. Reads
                // the per-heartbeat snapshot (gossip emission runs after the
                // snapshot refresh) rather than recomputing the score. No effect
                // when scoring is disabled (the `if` is skipped).
                if (router.score) |sc| {
                    if (router.peerScore(peer) < sc.thresholds.gossip_threshold) continue;
                }
                out.append(router.allocator, peer) catch break;
            }
        }

        /// Send an IHAVE(topic, ids) to `peer` on its control lane. The borrowed
        /// `ids` are copied by `frameRpc`, so they need only outlive this call. The
        /// IHAVE builder takes `[]const ?[]const u8`, so the ids are wrapped through
        /// a small scratch array of optionals (freed once the frame is built).
        fn sendIHave(router: *Self, peer: PeerId, topic: []const u8, ids: []const []const u8) void {
            const opt_ids = router.allocator.alloc(?[]const u8, ids.len) catch return;
            defer router.allocator.free(opt_ids);
            for (ids, 0..) |id, i| opt_ids[i] = id;

            const empty_ids = router.emptyIds() orelse return;
            const ihave = rpc.buildIHave(topic, opt_ids);
            const ctrl = rpc_pb.ControlMessage{ .ihave = &.{ihave} };
            router.fanOut(.control, (rpc.RpcOut{ .control = ctrl }).toRpc(), empty_ids, .{ .one = peer });
        }

        /// Mesh maintenance for every subscribed topic: first (when scoring is
        /// enabled) prune any current mesh peer that has gone negative, then graft
        /// new peers in when the mesh is below D_low (up to the target D), and
        /// prune excess peers out when it is above D_high (down to D). The
        /// negative-peer prune runs every heartbeat, independent of the D_high
        /// overflow, so a misbehaving peer leaves the mesh promptly. Iterating
        /// `my_topics` while grafting/pruning only mutates the `mesh`/`backoff`
        /// maps (never `my_topics`), so the iterator stays valid.
        fn maintainMeshes(router: *Self) void {
            var topic_it = router.my_topics.keyIterator();
            while (topic_it.next()) |key| {
                const topic = key.*;
                router.pruneNegativeMeshPeers(topic);
                const size = router.meshSize(topic);
                if (size < mesh_params.d_low) {
                    router.graftToTarget(topic, mesh_params.d - size);
                } else if (size > mesh_params.d_high) {
                    router.pruneToTarget(topic, size - mesh_params.d);
                }
            }
        }

        /// Prune every current member of `topic`'s mesh whose score is negative
        /// (each backed off + sent a PRUNE, the same as an overflow prune). No-op
        /// when scoring is disabled. Victims are collected first (we cannot remove
        /// from the mesh set while iterating it), then removed.
        fn pruneNegativeMeshPeers(router: *Self, topic: []const u8) void {
            if (router.score == null) return;
            const set = router.mesh.getPtr(topic) orelse return;

            var victims: std.ArrayListUnmanaged(PeerKey) = .empty;
            defer victims.deinit(router.allocator);
            var it = set.keyIterator();
            while (it.next()) |key_ptr| {
                const sc = if (router.score_snapshot.get(key_ptr.*)) |s| s else 0;
                if (sc < 0) victims.append(router.allocator, key_ptr.*) catch break;
            }

            for (victims.items) |k| {
                const state = router.peers.get(k) orelse {
                    _ = set.remove(k);
                    continue;
                };
                router.meshRemove(topic, state.peer);
                router.setBackoff(topic, state.peer, mesh_params.prune_backoff_ticks);
                router.sendPrune(state.peer, topic);
            }
        }

        /// Graft up to `want` fresh candidate peers into `topic`'s mesh, sending
        /// each a GRAFT on its control lane. The candidate set is gathered into a
        /// transient list first (so we are not mutating the mesh while a selection
        /// helper reads it), then each is added + grafted.
        fn graftToTarget(router: *Self, topic: []const u8, want: usize) void {
            var picks: std.ArrayListUnmanaged(PeerId) = .empty;
            defer picks.deinit(router.allocator);
            _ = router.selectGraftCandidates(topic, want, &picks);
            for (picks.items) |peer| {
                router.meshAdd(topic, peer);
                router.sendGraft(peer, topic);
            }
        }

        /// Prune `excess` peers out of `topic`'s mesh, backing each off and sending
        /// it a PRUNE on its control lane. Victims are gathered as peer KEYS into a
        /// transient list first (so we are not removing from the mesh set while
        /// iterating it), then each is removed + pruned.
        fn pruneToTarget(router: *Self, topic: []const u8, excess: usize) void {
            var victims: std.ArrayListUnmanaged(PeerKey) = .empty;
            defer victims.deinit(router.allocator);
            _ = router.selectPruneVictims(topic, excess, &victims);
            for (victims.items) |key| {
                const peer = router.peers.get(key) orelse {
                    // The mesh holds the key even if the peer just disconnected;
                    // drop it from the mesh and move on (no frame to send).
                    if (router.mesh.getPtr(topic)) |set| _ = set.remove(key);
                    continue;
                };
                router.meshRemove(topic, peer.peer);
                router.setBackoff(topic, peer.peer, mesh_params.prune_backoff_ticks);
                router.sendPrune(peer.peer, topic);
            }
        }

        /// Fanout maintenance: drop any fanout topic whose last publish is older
        /// than the TTL, and replenish (up to D) any surviving topic whose set has
        /// thinned below D. Collect the to-drop topics first (we cannot mutate the
        /// fanout map while iterating it), then drop them.
        fn maintainFanout(router: *Self) void {
            var to_drop: std.ArrayListUnmanaged([]const u8) = .empty;
            defer to_drop.deinit(router.allocator);

            var it = router.fanout.iterator();
            while (it.next()) |entry| {
                const topic = entry.key_ptr.*;
                const last = router.fanout_last_pub.get(topic) orelse router.heartbeat_tick;
                if (router.heartbeat_tick -| last > mesh_params.fanout_ttl_ticks) {
                    to_drop.append(router.allocator, topic) catch {};
                    continue;
                }
                router.fanoutReplenish(topic, entry.value_ptr);
            }
            for (to_drop.items) |topic| router.fanoutDrop(topic);
        }

        /// Handle an inbound RPC from a peer: apply its subscription changes to
        /// the SOURCE peer's announced-topics set, then forward each published
        /// message over the topic's MESH (every mesh member except the source) and
        /// deliver it locally if we subscribe. GRAFT/PRUNE drive mesh membership;
        /// IHAVE/IWANT/IDONTWANT are parsed-but-ignored (later layers). The
        /// OwnedRpc is freed only after all parsing AND forward-frame construction,
        /// since its bytes back the readers and are copied by frameRpc.
        fn onInboundRpc(router: *Self, in: peer_io.InboundRpc) void {
            var owned = in;
            defer owned.rpc.deinit(router.allocator);
            const source = owned.peer;
            var reader = owned.rpc.reader;

            // Graylist gate: a peer whose score has sunk at or below the graylist
            // threshold is ignored entirely — drop the whole RPC without parsing
            // its subscriptions, publishes, or control. The OwnedRpc is freed by
            // the defer above. No-op when scoring is disabled. (Uses a live score
            // rather than the per-heartbeat snapshot: an RPC can arrive between
            // heartbeats, and a peer that just crossed the graylist line should be
            // shut out immediately, matching go-libp2p's per-RPC check.)
            if (router.score) |sc| {
                if (sc.belowGraylist(source)) return;
            }

            // Subscription changes update the source peer's announced topics.
            while (reader.subscriptionsNext()) |sub| {
                router.applyPeerSubscription(source, sub.getTopicid(), sub.getSubscribe());
            }

            // Published messages: dedup, deliver locally, forward over the mesh.
            while (reader.publishNext()) |msg| {
                router.handleIncomingMessage(
                    source,
                    msg.getFrom(),
                    msg.getSeqno(),
                    msg.getTopic(),
                    msg.getData(),
                    msg.getSignature(),
                    msg.getKey(),
                );
            }

            // Mesh + gossip control. GRAFT/PRUNE move the source peer in/out of a
            // topic's mesh. IHAVE makes us reply with an IWANT for the ids we have
            // not seen; IWANT makes us serve the requested messages from the cache.
            // IDONTWANT belongs to a later layer and stays ignored. Any control
            // replies (a PRUNE rejecting a GRAFT, an IWANT, a served message) are
            // built + framed inside the handlers, which copy the bytes, so freeing
            // the OwnedRpc after this returns is safe.
            if (reader.getControl()) |ctrl_reader| {
                var control = ctrl_reader;
                while (control.graftNext()) |graft| {
                    router.handleGraft(source, graft.getTopicID());
                }
                while (control.pruneNext()) |prune| {
                    router.handlePrune(source, prune.getTopicID(), prune.getBackoff());
                }
                while (control.ihaveNext()) |ihave| {
                    var ih = ihave;
                    router.handleIHave(source, &ih);
                }
                while (control.iwantNext()) |iwant| {
                    var iw = iwant;
                    router.handleIWant(source, &iw);
                }
            } else |_| {}
        }

        /// Handle an inbound IHAVE: the source peer announces it holds the listed
        /// message ids for a topic. Request (in ONE IWANT, on the control lane) the
        /// ids we have NOT already seen — capped at `max_iwant_request_ids` so one
        /// peer's IHAVE cannot make us request an unbounded set. The unseen ids are
        /// copied into a transient owned list (the wire-borrowed ids back the reader
        /// bytes, which `frameRpc` copies, but a local list keeps the call simple
        /// and lets us dedup within the IHAVE). We do NOT mark these ids seen — that
        /// happens only on actual receipt of the message (via the normal inbound
        /// path), so a dropped IWANT/serve does not permanently suppress the id.
        ///
        /// Finer rate-limiting and IWANT-promise tracking (so a peer that fails to
        /// deliver what it advertised is penalised) arrive with peer scoring.
        fn handleIHave(router: *Self, source: PeerId, ihave: *rpc_pb.ControlIHaveReader) void {
            if (!router.peers.contains(peerKey(&source))) return;

            var wanted: std.ArrayListUnmanaged(?[]const u8) = .empty;
            defer wanted.deinit(router.allocator);
            while (ihave.messageIDsNext()) |id| {
                if (wanted.items.len >= mesh_params.max_iwant_request_ids) break;
                if (router.seen.contains(id)) continue;
                wanted.append(router.allocator, id) catch break;
            }
            if (wanted.items.len == 0) return;

            const ids = router.emptyIds() orelse return;
            const iwant = rpc.buildIWant(wanted.items);
            const ctrl = rpc_pb.ControlMessage{ .iwant = &.{iwant} };
            router.fanOut(.control, (rpc.RpcOut{ .control = ctrl }).toRpc(), ids, .{ .one = source });
        }

        /// Handle an inbound IWANT: the source peer requests the listed message
        /// ids. For each id still in the cache, retain its frame and push it onto
        /// the source's `.data` lane — the cached frame is already a complete
        /// single-message publish RPC, so the served message re-enters the peer's
        /// normal inbound path on the other side (dedup, deliver, cache, forward).
        /// Ids we do not have are ignored. Capped at `max_iwant_to_serve` messages
        /// per request so one peer cannot make us serve an unbounded set; finer
        /// rate-limiting is a future refinement.
        fn handleIWant(router: *Self, source: PeerId, iwant: *rpc_pb.ControlIWantReader) void {
            const state = router.peers.get(peerKey(&source)) orelse return;
            var served: usize = 0;
            while (iwant.messageIDsNext()) |id| {
                if (served >= mesh_params.max_iwant_to_serve) break;
                const frame = router.message_cache.get(id) orelse continue;
                // Retain before pushing (the queue holds the reference; the cache
                // keeps its own). `pushTo` releases on a rejected push, so a full
                // queue does not leak the retained reference.
                router.pushTo(state, .data, frame);
                served += 1;
            }
        }

        /// Handle an inbound GRAFT(topic) from `source`: add the peer to the
        /// topic's mesh iff we subscribe to the topic and the peer is not in
        /// backoff for it. A GRAFT from a peer that is already a mesh member is an
        /// idempotent accept (no self-eviction). Crucially there is NO upper-size
        /// reject here (go-libp2p / rust-libp2p behaviour): a GRAFT can push the
        /// mesh past D_high and the HEARTBEAT prunes it back to D. Growth is still
        /// bounded — a GRAFT is only accepted for a topic we subscribe to, and the
        /// next heartbeat shrinks any over-full mesh. Rejecting at D_high would
        /// make heartbeat-prune unreachable and diverge from interop. On reject,
        /// reply with a PRUNE (default backoff, no PX yet) on the peer's control
        /// lane and back the peer off so we do not immediately re-accept. Untracked
        /// source is ignored.
        fn handleGraft(router: *Self, source: PeerId, topic: []const u8) void {
            if (!router.peers.contains(peerKey(&source))) return;

            // A negative-scoring peer is not admitted to the mesh (PRUNE back +
            // backoff), even if it would otherwise be accepted. An existing
            // member that has gone negative is also pushed out here. No effect
            // when scoring is disabled (peerScore is 0, so the `< 0` test fails).
            const negative = router.peerScore(source) < 0;

            const in_backoff = router.inBackoff(topic, source);

            const accept = !negative and
                (router.meshContains(topic, source) or
                    (router.my_topics.contains(topic) and !in_backoff));

            if (accept) {
                router.meshAdd(topic, source);
            } else {
                // A GRAFT that arrives while the peer is still backed off is a
                // flood signal (it is grafting faster than our backoff allows):
                // charge a behaviour penalty (P7) so a peer that repeatedly does
                // this earns the squared penalty. No effect when scoring is off.
                if (in_backoff) {
                    if (router.score) |sc| sc.addPenalty(source, 1.0);
                }
                router.setBackoff(topic, source, mesh_params.prune_backoff_ticks);
                router.sendPrune(source, topic);
            }
        }

        /// Handle an inbound PRUNE(topic) from `source`: drop the peer from the
        /// topic's mesh and back it off. The wire `backoff` is in seconds (≈ ticks
        /// at one tick per second); use the larger of it and the default so a peer
        /// cannot shorten our backoff below the floor. Untracked source is ignored.
        /// (PX peers are ignored for now.)
        fn handlePrune(router: *Self, source: PeerId, topic: []const u8, backoff_secs: u64) void {
            if (!router.peers.contains(peerKey(&source))) return;
            router.meshRemove(topic, source);
            const ticks = @max(backoff_secs, mesh_params.prune_backoff_ticks);
            router.setBackoff(topic, source, ticks);
        }

        /// Send a PRUNE(topic) to `peer` on its control lane, carrying the default
        /// backoff (in seconds, ≈ ticks) and no PX peers. Framed once via `fanOut`
        /// to the single target.
        fn sendPrune(router: *Self, peer: PeerId, topic: []const u8) void {
            const ids = router.emptyIds() orelse return;
            const prune = rpc.buildPrune(topic, &.{}, mesh_params.prune_backoff_ticks);
            const ctrl = rpc_pb.ControlMessage{ .prune = &.{prune} };
            router.fanOut(.control, (rpc.RpcOut{ .control = ctrl }).toRpc(), ids, .{ .one = peer });
        }

        /// Send a GRAFT(topic) to `peer` on its control lane (telling it we have
        /// added it to our mesh for the topic). Framed once via `fanOut` to the
        /// single target.
        fn sendGraft(router: *Self, peer: PeerId, topic: []const u8) void {
            const ids = router.emptyIds() orelse return;
            const graft = rpc.buildGraft(topic);
            const ctrl = rpc_pb.ControlMessage{ .graft = &.{graft} };
            router.fanOut(.control, (rpc.RpcOut{ .control = ctrl }).toRpc(), ids, .{ .one = peer });
        }

        /// Apply one inbound SUBSCRIBE/UNSUBSCRIBE from `source` to that peer's
        /// announced-topics set. Untracked source → ignored. Subscribe inserts an
        /// owned key copy (no-op if already present); unsubscribe removes + frees
        /// the stored key.
        fn applyPeerSubscription(router: *Self, source: PeerId, topic: []const u8, subscribe: bool) void {
            const state = router.peers.get(peerKey(&source)) orelse return;
            if (subscribe) {
                if (state.topics.contains(topic)) return;
                const key = router.allocator.dupe(u8, topic) catch return;
                state.topics.put(router.allocator, key, {}) catch {
                    router.allocator.free(key);
                    return;
                };
            } else if (state.topics.fetchRemove(topic)) |kv| {
                router.allocator.free(kv.key);
            }
        }

        /// Process one incoming (relayed) published message: dedup on its id,
        /// deliver to the local handler if we subscribe, and forward it over the
        /// topic's MESH (every mesh member except the source — NOT all subscribers;
        /// floodsub is gone). `exclude` is the source peer (no echo back to it).
        fn handleIncomingMessage(
            router: *Self,
            exclude: PeerId,
            from: []const u8,
            seqno: []const u8,
            topic: []const u8,
            data: []const u8,
            signature: []const u8,
            key: []const u8,
        ) void {
            // StrictSign: reject (drop — no deliver, no cache, no forward) any
            // message whose signature does not verify against the key carried on
            // the wire AND whose `from` is not that key's peer-id. An unsigned
            // message (empty signature/key) also fails verification, so it is
            // dropped. Under the none policy we accept everything (current
            // behaviour) and the message's signature/key — if any — pass through
            // unchanged on forward.
            if (router.signer != null) {
                if (!signing.verifyMessage(router.allocator, from, seqno, topic, data, signature, key)) {
                    // P4: the SENDING peer (the inbound stream's peer, not the
                    // message's `from`) relayed an invalid message. Charge it the
                    // squared invalid-delivery penalty before dropping the message.
                    if (router.score) |sc| sc.rejectMessage(exclude, topic);
                    return;
                }
            }

            var id = rpc.messageId(router.allocator, from, seqno) catch return;
            defer id.deinit(router.allocator);
            if (router.seen.contains(id.bytes)) {
                // A duplicate of an already-seen message. If the relaying peer is
                // a current mesh member for this topic, credit it the P3
                // mesh-delivery counter (it did relay the message to us, just not
                // first). The engine itself no-ops for a non-mesh peer.
                if (router.score) |sc| {
                    if (router.meshContains(topic, exclude)) sc.duplicateMessage(exclude, topic);
                }
                return;
            }
            router.seen.add(id.bytes);

            // P2/P3: the relaying peer delivered a NEW, accepted message — credit
            // its first-delivery (and, if it is a mesh member, mesh-delivery)
            // counters. Keyed on the SENDING peer, not the message's `from`.
            if (router.score) |sc| sc.deliverMessage(exclude, topic);

            router.deliverLocal(topic, from, data);

            // Cache EVERY accepted message (so a later IWANT can be served and a
            // heartbeat IHAVE can advertise it) and forward it over the topic's
            // mesh, minus the sender. Caching is independent of forwarding: a
            // message accepted on a topic with no mesh members is still cached.
            // Forward the ORIGINAL signature/key so relayed copies keep the
            // publisher's signature (empty slices map to null = field absent).
            router.cacheAndForward(
                router.mesh.getPtr(topic),
                exclude,
                from,
                seqno,
                topic,
                data,
                if (signature.len > 0) signature else null,
                if (key.len > 0) key else null,
                id.bytes,
            );
        }

        /// Invoke the message handler if the local node subscribes to `topic`.
        /// The slices are valid only for the call (the handler copies to retain).
        fn deliverLocal(router: *Self, topic: []const u8, from: []const u8, data: []const u8) void {
            if (!router.my_topics.contains(topic)) return;
            if (router.message_handler) |h| h.on_message(h.ctx, topic, from, data);
        }

        /// Frame a single message ONCE, store it in the message cache (so a later
        /// IWANT can be served and a heartbeat IHAVE can advertise it), and — when
        /// `set` is non-null — hand one reference to every still-tracked peer in it
        /// (a mesh or fanout PeerSet), optionally excluding one peer (the relay
        /// source). The shared frame carries the message id for a later IDONTWANT
        /// purge. Fanned out on the `.data` lane.
        ///
        /// Caching is unconditional and independent of forwarding: a message
        /// accepted on a topic we have no mesh for (a null `set`) is still cached,
        /// matching go-libp2p (every accepted message enters the cache). The
        /// fan-out target set may also be empty.
        ///
        /// Builds the frame here (ref = 1, the builder reference) rather than via
        /// `fanOut` so the single allocation can be shared with the cache: `put`
        /// retains it (the cache then holds a reference for ~history_length
        /// heartbeats), `fanOutFrame` retains it per target, and the trailing
        /// `release` drops the builder reference — leaving exactly the cache's and
        /// the accepting queues' references live.
        fn cacheAndForward(
            router: *Self,
            set: ?*const PeerSet,
            exclude: ?PeerId,
            from: []const u8,
            seqno: []const u8,
            topic: []const u8,
            data: []const u8,
            signature: ?[]const u8,
            key: ?[]const u8,
            id: []const u8,
        ) void {
            // The data frame carries one message id (owned by the frame, for a
            // later IDONTWANT purge); free it on any pre-frame failure.
            const ids = router.allocator.alloc([]u8, 1) catch return;
            ids[0] = router.allocator.dupe(u8, id) catch {
                router.allocator.free(ids);
                return;
            };
            // The cached/forwarded frame carries whatever signature/key the
            // message was published or relayed with (null under the none policy),
            // so IWANT-served and mesh-forwarded copies stay byte-identical and
            // keep the original publisher's signature.
            const msg = rpc_pb.Message{ .from = from, .seqno = seqno, .topic = topic, .data = data, .signature = signature, .key = key };
            const framed = pubsub.frameRpc(router.allocator, (rpc.RpcOut{ .publish = &.{msg} }).toRpc()) catch {
                router.allocator.free(ids[0]);
                router.allocator.free(ids);
                return;
            };
            const frame = peer_io.OutboundFrame.create(router.allocator, framed, ids, 1) catch {
                router.allocator.free(framed);
                router.allocator.free(ids[0]);
                router.allocator.free(ids);
                return;
            };
            // Builder reference dropped at the end; the cache and any accepting
            // queues hold the rest. If none keeps a reference this frees the frame.
            defer frame.release();

            // Cache the message (retains the frame) so an IWANT can later be
            // served by retaining + pushing it; `put` dedups so the same id is
            // never stored or retained twice. Done for every accepted message,
            // whether or not there are mesh peers to forward to.
            router.message_cache.put(id, topic, frame) catch {};

            // Forward over the mesh/fanout set when there is one (it may be empty).
            if (set) |s| {
                router.fanOutFrame(.data, frame, .{ .peer_set = .{ .set = s, .exclude = exclude } });
            }
        }

        /// Local subscribe: record the topic, announce it to every peer, then JOIN
        /// the topic's mesh — eagerly graft up to D candidate peers (sending each a
        /// GRAFT). If a fanout set already exists for the topic (from prior
        /// publishing), seed the mesh from it and drop the fanout entry, matching
        /// go-libp2p. Owns `topic` (frees it); the stored key is a separate copy.
        fn onSubscribe(router: *Self, topic: []u8) void {
            defer router.allocator.free(topic);
            if (router.my_topics.contains(topic)) return;

            const key = router.allocator.dupe(u8, topic) catch return;
            router.my_topics.put(router.allocator, key, {}) catch {
                router.allocator.free(key);
                return;
            };
            router.announceSubscription(topic, true);

            // Seed the mesh from any existing fanout peers (we were publishing to
            // this topic without subscribing), grafting each, then drop the fanout.
            if (router.fanout.getPtr(topic)) |fanout_set| {
                var it = fanout_set.keyIterator();
                while (it.next()) |key_ptr| {
                    if (router.meshSize(topic) >= mesh_params.d) break;
                    const state = router.peers.get(key_ptr.*) orelse continue;
                    const peer = state.peer;
                    if (router.meshContains(topic, peer) or router.inBackoff(topic, peer)) continue;
                    router.meshAdd(topic, peer);
                    router.sendGraft(peer, topic);
                }
                router.fanoutDrop(topic);
            }

            // Top the mesh up to D with fresh candidates.
            const have = router.meshSize(topic);
            if (have < mesh_params.d) router.graftToTarget(topic, mesh_params.d - have);
        }

        /// Local unsubscribe: drop the topic, announce the withdrawal to every
        /// peer, then LEAVE the topic's mesh — send a PRUNE to every current mesh
        /// member (backing each off, matching go-libp2p), then clear and free the
        /// topic's mesh set. Owns `topic` (frees it). No-op if we were not
        /// subscribed.
        fn onUnsubscribe(router: *Self, topic: []u8) void {
            defer router.allocator.free(topic);
            const removed = router.my_topics.fetchRemove(topic) orelse return;
            router.allocator.free(removed.key);
            router.announceSubscription(topic, false);

            router.leaveMesh(topic);
        }

        /// Send a PRUNE to every member of `topic`'s mesh, back each off, then free
        /// the topic's mesh set + key. A no-op if the topic has no mesh.
        fn leaveMesh(router: *Self, topic: []const u8) void {
            const kv = router.mesh.fetchRemove(topic) orelse return;
            var set = kv.value;
            var it = set.keyIterator();
            while (it.next()) |key_ptr| {
                if (router.peers.get(key_ptr.*)) |state| {
                    // The mesh entry is being torn out wholesale here (not via
                    // meshRemove), so fire the scoring PRUNE explicitly for each
                    // departing member so the engine stops counting it in-mesh.
                    if (router.score) |sc| sc.prune(state.peer, topic);
                    router.setBackoff(topic, state.peer, mesh_params.prune_backoff_ticks);
                    router.sendPrune(state.peer, topic);
                }
            }
            set.deinit(router.allocator);
            router.allocator.free(kv.key);
        }

        /// Announce a single (un)subscription to every peer's `.subscribe` lane:
        /// framed once and fanned out (see `fanOut`).
        fn announceSubscription(router: *Self, topic: []const u8, subscribe: bool) void {
            const ids = router.emptyIds() orelse return;
            const sub = rpc.buildSubscription(topic, subscribe);
            router.fanOut(.subscribe, (rpc.RpcOut{ .subscriptions = &.{sub} }).toRpc(), ids, .all);
        }

        /// Local publish: build a Message from us, dedup it, deliver locally if we
        /// subscribe, then forward it.
        ///
        /// With flood-publish on (the go-libp2p default), the message floods to a
        /// transient set of EVERY topic subscriber above the publish threshold —
        /// not just the mesh/fanout — to maximise propagation of a message we
        /// originated. With it off, we fall back to the relay topology: forward
        /// over the topic's MESH if we subscribe, otherwise over a transient
        /// FANOUT set (created / replenished to D from peers subscribed to the
        /// topic) whose last-publish tick the heartbeat uses to expire it after
        /// the TTL. Flooding does NOT touch the fanout state — it is a one-shot
        /// target set, distinct from the relay path. (Relayed messages never
        /// flood; only this originator path does.)
        ///
        /// Owns `topic` and `data` (frees both AFTER framing/handler, since
        /// frameRpc copies them and the handler reads them).
        fn onPublish(router: *Self, topic: []u8, data: []u8) void {
            defer router.allocator.free(topic);
            defer router.allocator.free(data);

            const from = router.local_peer.bytes[0..router.local_peer.len];

            var seqno_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &seqno_buf, router.seqno, .big);
            router.seqno += 1;
            const seqno = seqno_buf[0..];

            // Under StrictSign, sign the message and attach the signature + the
            // marshaled libp2p public key. A signing failure drops the publish (we
            // must not emit an unsigned message a StrictSign peer would reject).
            // Under the none policy both stay null. `sig` is owned here and freed
            // after framing (cacheAndForward copies the bytes into the frame).
            var sig: ?[]u8 = null;
            defer if (sig) |s| router.allocator.free(s);
            var key: ?[]const u8 = null;
            if (router.signer) |*s| {
                sig = s.sign(from, seqno, topic, data) catch |err| {
                    std.log.warn("gossipsub: signing publish failed: {any}", .{err});
                    return;
                };
                key = s.keyBytes();
            }

            var id = rpc.messageId(router.allocator, from, seqno) catch return;
            defer id.deinit(router.allocator);
            router.seen.add(id.bytes);

            router.deliverLocal(topic, from, data);

            // A published message is ALWAYS cached (so a later IWANT can serve it
            // and a heartbeat IHAVE can advertise it), whether or not we have any
            // mesh/fanout peers to forward it to right now. The frame is built and
            // cached once, then fanned out over whichever set applies.

            // Flood-publish: an ORIGINATED message goes to every topic subscriber
            // above the publish threshold (a transient set), bypassing the
            // mesh/fanout topology entirely. The set targets the flood only; the
            // frame's references live on the per-peer queues, so freeing the set
            // afterward touches no frame. Caching still happens (in cacheAndForward)
            // exactly once. No source to exclude — we are the origin.
            if (router.flood_publish) {
                var flood_set = router.floodTargets(topic);
                defer flood_set.deinit(router.allocator);
                router.cacheAndForward(&flood_set, null, from, seqno, topic, data, sig, key, id.bytes);
                return;
            }

            if (router.my_topics.contains(topic)) {
                // Subscribed: cache + forward over the topic's mesh (may be empty
                // or absent — no source to exclude).
                router.cacheAndForward(router.mesh.getPtr(topic), null, from, seqno, topic, data, sig, key, id.bytes);
            } else if (router.fanoutGetOrCreate(topic)) |set| {
                // Not subscribed: cache + forward over the fanout set, topping it
                // up to D first, and refresh the last-publish tick (the entry
                // exists — fanoutGetOrCreate inserted it — so this only updates the
                // value, never stores a new key) so the heartbeat times out the TTL
                // from the most recent publish.
                router.fanoutReplenish(topic, set);
                if (router.fanout_last_pub.getPtr(topic)) |last| last.* = router.heartbeat_tick;
                router.cacheAndForward(set, null, from, seqno, topic, data, sig, key, id.bytes);
            } else {
                // Not subscribed and the fanout set could not be created (OOM):
                // still cache the message so it is gossipable/servable.
                router.cacheAndForward(null, null, from, seqno, topic, data, sig, key, id.bytes);
            }
        }

        /// Test-only: push a frame reference onto a tracked peer's outbound queue,
        /// running on the router fiber so the peer-map lookup never races the
        /// router's own mutations. Releases the reference if the peer is gone or
        /// the push fails. Always sets the reply event.
        fn onEnqueueForTest(router: *Self, peer: PeerId, frame: *peer_io.OutboundFrame, reply: *std.Io.Event) void {
            if (router.peers.get(peerKey(&peer))) |state| {
                state.queue.push(router.io, .data, frame) catch frame.release();
            } else {
                frame.release();
            }
            reply.set(router.io);
        }

        /// Tear down one peer's state. Order matters: close the queue (the writer
        /// drains remaining frames and exits), then CANCEL+AWAIT the writer fiber
        /// BEFORE freeing the sink (the writer's trailing `sink.close` must not
        /// race a freed sink), then close the sink, deinit the queue, and free
        /// the heap allocations.
        ///
        /// Cancel before await so a writer parked in `ensureStream`'s reopen
        /// backoff does not stall this single router fiber for up to
        /// max_open_retries × reopen_backoff_ms (which would serialize every
        /// peer's teardown). The backoff `sleep` is a cancellation point, so
        /// cancel collapses the wait; await then joins. cancel+await on the same
        /// Future is safe here: cancel is idempotent and clears the future, so
        /// the following await returns the cached result without double-
        /// consuming. Under cancellation the writer unwinds cleanly —
        /// popBlocking returns Closed (queue already closed) or Canceled, or a
        /// Cancelable surfaces from open/writeFrame/sleep — runs its
        /// `defer sink.close`, and returns; the router's own `sink.close` below
        /// is idempotent.
        fn teardownPeer(router: *Self, state: *PeerState) void {
            // Mark the peer disconnected in the scoring engine: its stats (and
            // any negative score) are retained for a while so a quick reconnect
            // keeps them, but its IP colocation slots are released immediately.
            // Covers both the disconnect path and shutdown's teardownAllPeers.
            if (router.score) |sc| sc.removePeer(state.peer);
            state.queue.close(router.io);
            state.writer_future.cancel(router.io);
            state.writer_future.await(router.io);
            state.sink.close(router.io);
            state.queue.deinit(router.io);
            router.freePeerTopics(state);
            router.allocator.destroy(state.writer);
            router.allocator.destroy(state.sink);
            router.allocator.destroy(state);
        }

        /// Free every key in a peer's announced-topics map and the map itself.
        fn freePeerTopics(router: *Self, state: *PeerState) void {
            var it = state.topics.keyIterator();
            while (it.next()) |key| router.allocator.free(key.*);
            state.topics.deinit(router.allocator);
        }

        fn teardownAllPeers(router: *Self) void {
            var it = router.peers.iterator();
            while (it.next()) |entry| {
                router.teardownPeer(entry.value_ptr.*);
                _ = router.peer_count.fetchSub(1, .release);
            }
            router.peers.clearRetainingCapacity();
        }

        /// Drain and free any commands still buffered in the inbox after the loop
        /// exits, so an inbound RPC posted concurrently with teardown is not
        /// leaked.
        fn drainInbox(router: *Self) void {
            router.inbox.close(router.io);
            var buf: [16]Command = undefined;
            while (true) {
                const n = router.inbox.getUncancelable(router.io, &buf, 0) catch return;
                if (n == 0) return;
                for (buf[0..n]) |command| switch (command) {
                    .inbound_rpc => |in| {
                        var owned = in;
                        owned.rpc.deinit(router.allocator);
                    },
                    .subscribe => |s| router.allocator.free(s.topic),
                    .unsubscribe => |u| router.allocator.free(u.topic),
                    .publish => |p| {
                        router.allocator.free(p.topic);
                        router.allocator.free(p.data);
                    },
                    .enqueue_for_test => |e| {
                        // The peer map is already torn down; release the frame
                        // reference and wake any waiter so a test post in flight at
                        // shutdown neither leaks nor hangs.
                        e.frame.release();
                        e.reply.set(router.io);
                    },
                    else => {},
                };
            }
        }

        /// PeerWriter on_disconnect callback. The writer exhausted its open
        /// retries, so hand the peer back to the router by posting
        /// `peer_disconnected`. Runs on the writer fiber, so it must only post —
        /// never free the state/sink (the writer's trailing `sink.close` still
        /// fires after this returns).
        fn onWriterDisconnect(ctx: ?*anyopaque) void {
            const state: *PeerState = @ptrCast(@alignCast(ctx.?));
            const router = state.router_for_disconnect;
            router.inbox.putOneUncancelable(router.io, .{ .peer_disconnected = .{ .peer = state.peer } }) catch {};
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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

// --- FakeTransport: an in-memory transport for router-level unit tests -----

/// A fake outbound sink that records every byte written to each "stream" it
/// opens, into transport-owned storage so the recording survives the sink being
/// closed + destroyed during teardown. Supports a configurable open-failure mode
/// to exercise the writer's give-up path. Satisfies the PeerWriter Sink
/// contract.
///
/// The recording (`record`) is borrowed from the FakeTransport; the sink only
/// appends to it, so a sink destroyed in teardownPeer does not lose the frames a
/// test wants to assert on.
const FakeSink = struct {
    allocator: std.mem.Allocator,
    record: *FakeRecord,
    /// Number of leading open() calls that should fail. `maxInt` = every open
    /// fails, driving the writer to exhaust its retries and give up.
    fail_open_count: usize,

    pub fn open(self: *FakeSink, io: std.Io) anyerror!void {
        _ = io;
        self.record.open_calls += 1;
        if (self.record.open_calls <= self.fail_open_count) return error.OpenFailed;
        self.record.streams_opened += 1;
    }

    pub fn writeFrame(self: *FakeSink, io: std.Io, bytes: []const u8) anyerror!void {
        _ = io;
        try self.record.written.appendSlice(self.allocator, bytes);
    }

    pub fn close(self: *FakeSink, io: std.Io) void {
        _ = self;
        _ = io;
    }
};

/// Transport-owned, per-peer recording of what a peer's sink did. Outlives the
/// sink so tests can inspect it after teardown frees the sink.
const FakeRecord = struct {
    allocator: std.mem.Allocator,
    open_calls: usize = 0,
    streams_opened: usize = 0,
    written: std.ArrayList(u8) = .empty,

    fn deinit(self: *FakeRecord) void {
        self.written.deinit(self.allocator);
    }
};

/// An in-memory transport for router unit tests. `ConnHandle` is a tiny fake
/// connection that carries the per-peer recording; `Sink` is a FakeSink that
/// records into that connection's recording. `makeSink` allocates a FakeSink
/// and points it at the connection's transport-owned record.
const FakeTransport = struct {
    /// When non-zero every sink made by this transport fails its first N opens
    /// (use `maxInt` for "always fail"), exercising the writer give-up path.
    fail_open_count: usize = 0,

    pub const ConnHandle = *FakeConn;
    pub const Sink = FakeSink;

    /// A fake per-peer connection. Owns its recording; the test owns the conn.
    const FakeConn = struct {
        record: FakeRecord,
    };

    pub fn makeSink(self: *FakeTransport, allocator: std.mem.Allocator, peer: PeerId, conn: ConnHandle) !*FakeSink {
        _ = peer;
        const sink = try allocator.create(FakeSink);
        sink.* = .{
            .allocator = allocator,
            .record = &conn.record,
            .fail_open_count = self.fail_open_count,
        };
        return sink;
    }
};

/// Build a FakeConn on the heap with a fresh recording. The test owns it and
/// frees it with `destroyFakeConn` once the router has fully torn the peer down
/// (so the sink can no longer touch the record).
fn makeFakeConn(allocator: std.mem.Allocator) !*FakeTransport.FakeConn {
    const conn = try allocator.create(FakeTransport.FakeConn);
    conn.* = .{ .record = .{ .allocator = allocator } };
    return conn;
}

fn destroyFakeConn(allocator: std.mem.Allocator, conn: *FakeTransport.FakeConn) void {
    conn.record.deinit();
    allocator.destroy(conn);
}

const FakeRouter = Router(FakeTransport);

/// Build a deterministic, distinct test PeerId. `PeerId.random()` is seeded with
/// a fixed constant (so it returns the same id every call); `testPeer(seed)`
/// stamps `seed` into the digest so each value is unique, which the mesh
/// forwarding tests need to track several peers at once.
fn testPeer(seed: u8) PeerId {
    var id = PeerId.random() catch unreachable;
    id.bytes[2] = seed;
    return id;
}

/// A local peer id distinct from every `testPeer` seed used below, so a
/// forwarded message's `from` (our id) never collides with a peer's id.
const local_test_peer = blk: {
    var id = PeerId{ .bytes = [_]u8{0} ** 64, .len = 34 };
    id.bytes[0] = 0x00;
    id.bytes[1] = 32;
    id.bytes[2] = 0xff;
    break :blk id;
};

/// Whether `state.topics` for the peer tracked under `peer` contains `topic`.
/// Reads private router/peer state directly (the tests are in-file).
fn peerTracksTopic(router: *FakeRouter, peer: PeerId, topic: []const u8) bool {
    const state = router.peers.get(peerKey(&peer)) orelse return false;
    return state.topics.contains(topic);
}

/// Spin until `pred()` holds or the bounded wait elapses, yielding the fiber
/// between checks (so the router's own fiber can make progress). Mirrors the
/// poll loop the real 2-node test uses. Returns whether the predicate held.
fn waitFor(io: std.Io, comptime pred: fn (*FakeRouter) bool, router: *FakeRouter) bool {
    var waited_ms: u64 = 0;
    while (waited_ms < 2000) : (waited_ms += 5) {
        if (pred(router)) return true;
        io_time.ms(5).sleep(io) catch {};
    }
    return pred(router);
}

fn peerCountIsOne(router: *FakeRouter) bool {
    return router.peerCount() == 1;
}

fn peerCountIsZero(router: *FakeRouter) bool {
    return router.peerCount() == 0;
}

// The router ignores remote_addr; a loopback placeholder keeps the command
// well-formed.
const dummy_addr = std.Io.net.IpAddress{ .ip4 = .loopback(0) };

test "router peer lifecycle: connect makes a sink + writer, disconnect tears down" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));

    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));
    // The sink was freed in teardown; the conn's record still lives (it is the
    // test's, freed by destroyFakeConn). std.testing.allocator confirms the
    // sink/writer/state allocations were all freed — no leak.
}

test "router dedups a second peer_connected for the same peer id" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    const peer = try PeerId.random();
    const conn_a = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_b);

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn_a, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));

    // Second connect for the same peer id: the first entry is kept, the second
    // makes no sink (no clobber, no second count). Post it and then post a probe
    // disconnect for a DIFFERENT peer to flush the queue, so we can assert the
    // dedup'd connect did not raise the count.
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn_b, .remote_addr = dummy_addr } });

    // conn_b's sink was never made (dedup returned before makeSink), so its
    // record stays at zero opens; conn_a's is the live peer.
    try std.testing.expect(waitFor(io, peerCountIsOne, router));
    try std.testing.expectEqual(@as(usize, 1), router.peerCount());
    try std.testing.expectEqual(@as(usize, 0), conn_b.record.open_calls);

    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));
}

test "router tears the peer down when the writer exhausts open retries" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Every open fails → the writer's ensureStream exhausts retries → it fires
    // on_disconnect → the router posts peer_disconnected → the peer is torn down
    // and peerCount drops back to 0. Tiny retry/backoff so the give-up is fast.
    const router = try FakeRouter.create(allocator, io, .{ .fail_open_count = std.math.maxInt(usize) }, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));

    // Push a frame so the writer actually tries to open (the open is lazy on the
    // first frame). It cannot open, so it gives up and the router tears the peer
    // down on its own.
    try router.enqueueDataForTest(peer, try testDataFrame(allocator));

    try std.testing.expect(waitFor(io, peerCountIsZero, router));
    // The give-up freed the popped frame and the router freed the sink/state; no
    // leak. The writer never opened a stream.
    try std.testing.expectEqual(@as(usize, 0), conn.record.streams_opened);
}

test "router frees an inbound RPC (peer tracked and peer absent)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // Peer-absent case: no peer_connected was posted, so the inbound RPC arrives
    // for an untracked peer. The router must still free it.
    const stranger = try PeerId.random();
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = stranger, .rpc = try buildInboundRpc(allocator, "t-absent") } });

    // Peer-tracked case: connect first, then deliver an inbound RPC for it.
    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundRpc(allocator, "t-present") } });

    // Disconnect to flush + tear down; on test exit destroy() drains any leftover
    // inbox commands. std.testing.allocator confirms both RPCs were freed.
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));
}

test "router clean shutdown tears down registered peers" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    // Tear the router down on every exit (not just the happy path): an early
    // assertion failure must NOT leave the main + writer fibers orphaned, or
    // threaded.deinit() would hang joining them.
    defer router.destroy();
    try router.start();

    // Register two peers, then destroy() without disconnecting them: the main
    // fiber's teardownAllPeers must close + free every peer. No leak.
    //
    // The two ids must be DISTINCT or onPeerConnected dedups the second (one
    // logical peer entry per id). `PeerId.random()` is not collision-free here,
    // so derive a second id by flipping a byte of the first — same approach the
    // peerKey test uses to get two non-equal ids.
    const peer_a = try PeerId.random();
    var peer_b = peer_a;
    peer_b.bytes[2] +%= 1;
    const conn_a = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_b);

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer_a, .conn = conn_a, .remote_addr = dummy_addr } });
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer_b, .conn = conn_b, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, struct {
        fn pred(r: *FakeRouter) bool {
            return r.peerCount() == 2;
        }
    }.pred, router));

    // destroy() runs teardownAllPeers on the main fiber on its way out (via the
    // defer above, so an assertion failure still tears the fibers down).
}

// --- test helpers for the router-level tests -------------------------------

/// Build a one-byte shared data frame (one reference) for the writer-give-up
/// test (the writer pops it, fails to open, releases the reference).
fn testDataFrame(allocator: std.mem.Allocator) !*peer_io.OutboundFrame {
    const bytes = try allocator.alloc(u8, 1);
    errdefer allocator.free(bytes);
    bytes[0] = 0x7f;
    const ids = try allocator.alloc([]u8, 0);
    errdefer allocator.free(ids);
    return peer_io.OutboundFrame.create(allocator, bytes, ids, 1);
}

/// Build a real OwnedRpc carrying a single subscription, mirroring what the
/// inbound reader produces, so onInboundRpc has genuine heap bytes to free.
fn buildInboundRpc(allocator: std.mem.Allocator, topic: []const u8) !pubsub.OwnedRpc {
    return buildInboundSub(allocator, topic, true);
}

/// Build an OwnedRpc carrying a single SUBSCRIBE/UNSUBSCRIBE SubOpts for `topic`.
fn buildInboundSub(allocator: std.mem.Allocator, topic: []const u8, subscribe: bool) !pubsub.OwnedRpc {
    const sub = rpc_pb.RPC{
        .subscriptions = &[_]?rpc_pb.RPC.SubOpts{.{ .subscribe = subscribe, .topicid = topic }},
    };
    return ownedFromRpc(allocator, sub);
}

/// Build an OwnedRpc carrying a single published Message, mirroring what an
/// inbound reader yields for a peer relaying/originating a message.
fn buildInboundPublish(
    allocator: std.mem.Allocator,
    from: []const u8,
    seqno: []const u8,
    topic: []const u8,
    data: []const u8,
) !pubsub.OwnedRpc {
    const msg = rpc_pb.Message{ .from = from, .seqno = seqno, .topic = topic, .data = data };
    const frame = rpc_pb.RPC{ .publish = &[_]?rpc_pb.Message{msg} };
    return ownedFromRpc(allocator, frame);
}

/// Build an OwnedRpc carrying a single control GRAFT for `topic`.
fn buildInboundGraft(allocator: std.mem.Allocator, topic: []const u8) !pubsub.OwnedRpc {
    const ctrl = rpc_pb.ControlMessage{ .graft = &[_]?rpc_pb.ControlGraft{rpc.buildGraft(topic)} };
    return ownedFromRpc(allocator, rpc_pb.RPC{ .control = ctrl });
}

/// Build an OwnedRpc carrying a single control PRUNE for `topic` with `backoff`
/// (seconds on the wire) and no PX peers.
fn buildInboundPrune(allocator: std.mem.Allocator, topic: []const u8, backoff: u64) !pubsub.OwnedRpc {
    const ctrl = rpc_pb.ControlMessage{ .prune = &[_]?rpc_pb.ControlPrune{rpc.buildPrune(topic, &.{}, backoff)} };
    return ownedFromRpc(allocator, rpc_pb.RPC{ .control = ctrl });
}

/// Build an OwnedRpc carrying a single control IHAVE(`topic`, `ids`).
fn buildInboundIHave(allocator: std.mem.Allocator, topic: []const u8, ids: []const ?[]const u8) !pubsub.OwnedRpc {
    const ctrl = rpc_pb.ControlMessage{ .ihave = &[_]?rpc_pb.ControlIHave{rpc.buildIHave(topic, ids)} };
    return ownedFromRpc(allocator, rpc_pb.RPC{ .control = ctrl });
}

/// Build an OwnedRpc carrying a single control IWANT(`ids`).
fn buildInboundIWant(allocator: std.mem.Allocator, ids: []const ?[]const u8) !pubsub.OwnedRpc {
    const ctrl = rpc_pb.ControlMessage{ .iwant = &[_]?rpc_pb.ControlIWant{rpc.buildIWant(ids)} };
    return ownedFromRpc(allocator, rpc_pb.RPC{ .control = ctrl });
}

/// Encode `frame` into a heap buffer the RPCReader can borrow, wrapped as an
/// OwnedRpc (matching what readRpc produces). The bytes are the raw protobuf
/// payload — NOT length-prefixed — which is what RPCReader.init expects.
fn ownedFromRpc(allocator: std.mem.Allocator, frame: rpc_pb.RPC) !pubsub.OwnedRpc {
    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);
    const payload = try allocator.dupe(u8, encoded);
    errdefer allocator.free(payload);
    return .{ .bytes = payload, .reader = try rpc_pb.RPCReader.init(payload) };
}

/// Decode a length-prefixed RPC frame at the front of `written` (the byte stream
/// a FakeSink records). Returns the parsed RPCReader plus the slice consumed, so
/// a multi-frame stream can be walked. Frames are `uvarint(len) || payload`.
const DecodedFrame = struct { reader: rpc_pb.RPCReader, total_len: usize };

fn decodeFrame(written: []const u8) ?DecodedFrame {
    var len: usize = 0;
    var shift: u6 = 0;
    var i: usize = 0;
    while (i < written.len) : (i += 1) {
        len |= @as(usize, written[i] & 0x7f) << shift;
        if ((written[i] & 0x80) == 0) {
            i += 1;
            break;
        }
        shift += 7;
    } else return null;
    if (i + len > written.len) return null;
    const payload = written[i .. i + len];
    const reader = rpc_pb.RPCReader.init(payload) catch return null;
    return .{ .reader = reader, .total_len = i + len };
}

/// Whether `written` contains a frame whose first subscription matches
/// (`topic`, `subscribe`). Walks every recorded frame.
fn recordHasSubscription(written: []const u8, topic: []const u8, subscribe: bool) bool {
    var rest = written;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        while (reader.subscriptionsNext()) |sub| {
            if (sub.getSubscribe() == subscribe and std.mem.eql(u8, sub.getTopicid(), topic)) return true;
        }
        rest = rest[decoded.total_len..];
    }
    return false;
}

/// Count recorded frames carrying a published Message on `topic` with the given
/// `data`. Used to assert exactly-once forwarding (dedup).
fn recordCountPublishes(written: []const u8, topic: []const u8, data: []const u8) usize {
    var count: usize = 0;
    var rest = written;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        while (reader.publishNext()) |msg| {
            if (std.mem.eql(u8, msg.getTopic(), topic) and std.mem.eql(u8, msg.getData(), data)) count += 1;
        }
        rest = rest[decoded.total_len..];
    }
    return count;
}

/// Count recorded frames carrying a control PRUNE for `topic`. Walks every
/// recorded frame and every prune in each frame's control message.
fn recordCountPrunes(written: []const u8, topic: []const u8) usize {
    var count: usize = 0;
    var rest = written;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        if (reader.getControl()) |ctrl_reader| {
            var control = ctrl_reader;
            while (control.pruneNext()) |prune| {
                if (std.mem.eql(u8, prune.getTopicID(), topic)) count += 1;
            }
        } else |_| {}
        rest = rest[decoded.total_len..];
    }
    return count;
}

/// Count recorded frames carrying a control GRAFT for `topic`. Walks every
/// recorded frame and every graft in each frame's control message.
fn recordCountGrafts(written: []const u8, topic: []const u8) usize {
    var count: usize = 0;
    var rest = written;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        if (reader.getControl()) |ctrl_reader| {
            var control = ctrl_reader;
            while (control.graftNext()) |graft| {
                if (std.mem.eql(u8, graft.getTopicID(), topic)) count += 1;
            }
        } else |_| {}
        rest = rest[decoded.total_len..];
    }
    return count;
}

/// Whether `written` contains a control IHAVE for `topic` that advertises `id`.
/// Walks every recorded frame and every IHAVE/message-id within each control msg.
fn recordHasIHave(written: []const u8, topic: []const u8, id: []const u8) bool {
    var rest = written;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        if (reader.getControl()) |ctrl_reader| {
            var control = ctrl_reader;
            while (control.ihaveNext()) |ihave| {
                var ih = ihave;
                if (!std.mem.eql(u8, ih.getTopicID(), topic)) continue;
                while (ih.messageIDsNext()) |mid| {
                    if (std.mem.eql(u8, mid, id)) return true;
                }
            }
        } else |_| {}
        rest = rest[decoded.total_len..];
    }
    return false;
}

/// Count control IWANT messages in `written` that request `id`. Walks every
/// recorded frame and every IWANT/message-id within each control message.
fn recordCountIWants(written: []const u8, id: []const u8) usize {
    var count: usize = 0;
    var rest = written;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        if (reader.getControl()) |ctrl_reader| {
            var control = ctrl_reader;
            while (control.iwantNext()) |iwant| {
                var iw = iwant;
                while (iw.messageIDsNext()) |mid| {
                    if (std.mem.eql(u8, mid, id)) count += 1;
                }
            }
        } else |_| {}
        rest = rest[decoded.total_len..];
    }
    return count;
}

/// A recording message handler: captures the last (topic, from, data) delivered
/// to the local node, into testing-allocator-owned buffers it frees on deinit.
const RecordingHandler = struct {
    allocator: std.mem.Allocator,
    calls: usize = 0,
    topic: ?[]u8 = null,
    from: ?[]u8 = null,
    data: ?[]u8 = null,

    fn deinit(self: *RecordingHandler) void {
        if (self.topic) |t| self.allocator.free(t);
        if (self.from) |f| self.allocator.free(f);
        if (self.data) |d| self.allocator.free(d);
    }

    fn handler(self: *RecordingHandler) MessageHandler {
        return .{ .ctx = self, .on_message = onMessage };
    }

    fn onMessage(ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) void {
        const self: *RecordingHandler = @ptrCast(@alignCast(ctx));
        // Replace any prior capture (the tests deliver once); copy the borrowed
        // slices, which are only valid for this call.
        if (self.topic) |t| self.allocator.free(t);
        if (self.from) |f| self.allocator.free(f);
        if (self.data) |d| self.allocator.free(d);
        self.topic = self.allocator.dupe(u8, topic) catch null;
        self.from = self.allocator.dupe(u8, from) catch null;
        self.data = self.allocator.dupe(u8, data) catch null;
        self.calls += 1;
    }
};

// --- mesh pub/sub fake tests -----------------------------------------------

/// Connect a fake peer to a running router and wait until it is tracked. Returns
/// the conn (owned by the caller; free with destroyFakeConn after teardown).
fn connectFakePeer(io: std.Io, allocator: std.mem.Allocator, router: *FakeRouter, peer: PeerId) !*FakeTransport.FakeConn {
    const conn = try makeFakeConn(allocator);
    errdefer destroyFakeConn(allocator, conn);
    const before = router.peerCount();
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    // Spin until the count rises (peer fully wired) with a bounded wait.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.peerCount() > before) break;
        io_time.ms(5).sleep(io) catch {};
    }
    return conn;
}

test "subscribe announces the subscription to a connected peer" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // Subscribe locally; the router announces it to every peer on the subscribe
    // lane. The writer fiber opens the fake stream and records the framed RPC.
    const topic = try allocator.dupe(u8, "t");
    try router.inbox.putOne(io, .{ .subscribe = .{ .topic = topic } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordHasSubscription(conn.record.written.items, "t", true)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(recordHasSubscription(conn.record.written.items, "t", true));
}

test "inbound subscription is tracked on the source peer" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // Peer X announces it subscribes to "t"; the router records it on X's state.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", true) } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer, "t"));

    // And an unsubscribe removes it.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", false) } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (!peerTracksTopic(router, peer, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(!peerTracksTopic(router, peer, "t"));
}

test "received message forwards over the mesh, not to all subscribers" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // We subscribe to "t". Peer A is grafted into our mesh; peer B is subscribed
    // to "t" but NOT in the mesh. A received publish must reach A (mesh) and NOT
    // B (mesh-only forwarding — floodsub-to-all-subscribers is gone). A separate
    // source peer S relays the publish.
    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const peer_b = testPeer(2);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try connectFakePeer(io, allocator, router, peer_b);
    defer destroyFakeConn(allocator, conn_b);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);

    // B announces it subscribes to "t" (so floodsub WOULD have forwarded to it),
    // but we never graft B, so it is not a mesh member.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_b, .rpc = try buildInboundSub(allocator, "t", true) } });

    // A GRAFTs into the mesh.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expect(router.meshContains("t", peer_a));

    // S relays a publish on "t": only mesh member A receives it.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x01", "t", "hello"),
    } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_a.record.written.items, "t", "hello") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give a wrong forward to B a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_a.record.written.items, "t", "hello"));
    // B is a subscriber but not in the mesh, so it gets nothing.
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(conn_b.record.written.items, "t", "hello"));
}

test "mesh forwarding dedups a repeated publish (same from+seqno) to forward once" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // Subscribe to "t" and graft Y into the mesh so a forward has a destination.
    try subscribeAndWait(io, allocator, router, "t");

    const peer_x = testPeer(1);
    const peer_y = testPeer(2);
    const conn_x = try connectFakePeer(io, allocator, router, peer_x);
    defer destroyFakeConn(allocator, conn_x);
    const conn_y = try connectFakePeer(io, allocator, router, peer_y);
    defer destroyFakeConn(allocator, conn_y);

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_y, peer_y, "t"));

    // Post the identical publish (same from + seqno) twice from X (a non-mesh
    // relay). The seen-cache must suppress the second so Y receives exactly one.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_x,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x07", "t", "dup"),
    } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_x,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x07", "t", "dup"),
    } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_y.record.written.items, "t", "dup") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give the (suppressed) second a chance to wrongly land before asserting.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_y.record.written.items, "t", "dup"));
}

test "publish to a subscribed topic forwards over the mesh and delivers locally" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // Subscribe locally so the publish is delivered to our own handler too.
    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);

    // A announces it subscribes to "t" (so it is both a topic subscriber and,
    // once grafted, a mesh member). With flood-publish on (the default) an
    // originated message reaches A as a subscriber; with it off it reaches A as a
    // mesh member — either way A receives exactly one copy.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_a, .rpc = try buildInboundSub(allocator, "t", true) } });
    var sub_waited: u64 = 0;
    while (sub_waited < 2000) : (sub_waited += 5) {
        if (peerTracksTopic(router, peer_a, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer_a, "t"));

    // Graft A into the mesh so the local publish has a mesh destination.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expect(router.meshContains("t", peer_a));

    // Publish locally: A gets the forwarded frame and our handler fires.
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "hello"),
    } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (rec.calls > 0 and recordCountPublishes(conn_a.record.written.items, "t", "hello") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_a.record.written.items, "t", "hello"));
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
    try std.testing.expectEqualSlices(u8, "t", rec.topic.?);
    try std.testing.expectEqualSlices(u8, "hello", rec.data.?);
    // The delivered `from` is our own peer id (the publish origin).
    try std.testing.expectEqualSlices(u8, local_test_peer.bytes[0..local_test_peer.len], rec.from.?);
}

test "flood-publish floods an originated message to ALL topic subscribers, not just the mesh" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Default config → flood-publish ON.
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // We subscribe to "t". A is grafted into our mesh; B subscribes to "t" but is
    // NOT in the mesh. A flood-published (originated) message must reach BOTH —
    // unlike a relayed message, which reaches only the mesh member A.
    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const peer_b = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try connectFakePeer(io, allocator, router, peer_b);
    defer destroyFakeConn(allocator, conn_b);

    // Both announce they subscribe to "t".
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_a, .rpc = try buildInboundSub(allocator, "t", true) } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_b, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer_a, "t") and peerTracksTopic(router, peer_b, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer_a, "t") and peerTracksTopic(router, peer_b, "t"));

    // Only A is grafted into the mesh; B is a subscriber but not a mesh member.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expect(router.meshContains("t", peer_a));
    try std.testing.expect(!router.meshContains("t", peer_b));

    // Publish (originated): with flood-publish on, BOTH A (mesh) and B (non-mesh
    // subscriber) receive it.
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "flood"),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_a.record.written.items, "t", "flood") >= 1 and
            recordCountPublishes(conn_b.record.written.items, "t", "flood") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_a.record.written.items, "t", "flood"));
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_b.record.written.items, "t", "flood"));

    // Contrast: a RELAYED message on "t" (from a third source) reaches only the
    // mesh member A, NOT the non-mesh subscriber B — relays stay mesh-only.
    const source = testPeer(3);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x01", "t", "relay"),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_a.record.written.items, "t", "relay") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give a wrong forward to B a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_a.record.written.items, "t", "relay"));
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(conn_b.record.written.items, "t", "relay"));
}

test "flood-publish gates on the publish threshold: a below-threshold subscriber is excluded" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Scoring ON with StrictSign (so invalid inbound publishes drive a peer's
    // score down) and a publish threshold of -1: a clean peer (score 0) is above
    // it; one invalid message (score -1) is NOT above it (the gate is `>=`).
    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();
    var cfg = scoringConfig();
    cfg.thresholds.publish_threshold = -1.0;
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, &host_key, cfg, .{});
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const peer_b = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try connectFakePeer(io, allocator, router, peer_b);
    defer destroyFakeConn(allocator, conn_b);

    // Both subscribe to "t".
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_a, .rpc = try buildInboundSub(allocator, "t", true) } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_b, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer_a, "t") and peerTracksTopic(router, peer_b, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer_a, "t") and peerTracksTopic(router, peer_b, "t"));

    // Drive B below the publish threshold (2 invalid → -4 < -1); A stays at 0.
    try driveInvalid(io, allocator, router, peer_b, "t", 2);
    try std.testing.expect(liveScore(router, peer_b) < -1.0);
    try std.testing.expect(liveScore(router, peer_a) >= -1.0);

    // Publish (originated): the flood reaches A (above threshold) but NOT B
    // (below the publish threshold → excluded from the flood target set).
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "gated"),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_a.record.written.items, "t", "gated") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give a wrong forward to B a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_a.record.written.items, "t", "gated"));
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(conn_b.record.written.items, "t", "gated"));
}

test "flood-publish OFF: an originated message reaches mesh members only, not other subscribers" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Flood-publish OFF → originated messages use the mesh (we subscribe to "t").
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{ .flood_publish = false });
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const peer_b = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try connectFakePeer(io, allocator, router, peer_b);
    defer destroyFakeConn(allocator, conn_b);

    // Both subscribe to "t", but only A is grafted into the mesh.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_a, .rpc = try buildInboundSub(allocator, "t", true) } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_b, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer_a, "t") and peerTracksTopic(router, peer_b, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer_a, "t") and peerTracksTopic(router, peer_b, "t"));

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expect(router.meshContains("t", peer_a));
    try std.testing.expect(!router.meshContains("t", peer_b));

    // Publish (originated) with flood off: reaches mesh member A only; the
    // subscribed-but-non-mesh peer B gets nothing (current relay-topology
    // behaviour).
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "meshonly"),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_a.record.written.items, "t", "meshonly") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give a wrong forward to B a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_a.record.written.items, "t", "meshonly"));
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(conn_b.record.written.items, "t", "meshonly"));
}

test "a forwarded message lands in the message cache and is evicted after history_length heartbeats" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // Subscribe to "t" and graft a mesh member so a received publish is forwarded
    // (the forward path is what populates the message cache).
    try subscribeAndWait(io, allocator, router, "t");
    const peer_a = testPeer(1);
    const source = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    // Source relays a publish; once mesh member A has recorded the forward the
    // router fiber is parked, so reading its message_cache is race-free (the same
    // pattern the mesh-state assertions use).
    const from = "origin";
    const seqno = "\x00\x00\x00\x05";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "cached"),
    } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_a.record.written.items, "t", "cached") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_a.record.written.items, "t", "cached"));

    // The message id is from ++ seqno (the default libp2p id).
    var id = try rpc.messageId(allocator, from, seqno);
    defer id.deinit(allocator);

    // It is in the cache: get() returns the frame and getGossipIDs("t") lists it.
    try std.testing.expect(router.message_cache.get(id.bytes) != null);
    const ids = try router.message_cache.getGossipIDs(allocator, "t");
    defer allocator.free(ids);
    var listed = false;
    for (ids) |gid| if (std.mem.eql(u8, gid, id.bytes)) {
        listed = true;
    };
    try std.testing.expect(listed);

    // After history_length heartbeat shifts the message is evicted. Use the
    // mcache's own history_length so the test tracks the source of truth.
    const history_length = @import("mcache.zig").historyLengthForTest();
    try beatHeartbeats(io, router, history_length);
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.message_cache.get(id.bytes) == null) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(router.message_cache.get(id.bytes) == null);
}

// --- mesh: GRAFT / PRUNE / backoff / heartbeat fake tests ------------------

/// Subscribe the local node to `topic` and spin (bounded) until the router has
/// recorded it in `my_topics`. The router processes the subscribe on its fiber,
/// so the GRAFT handler's `my_topics.contains` check is only meaningful once this
/// has landed.
fn subscribeAndWait(io: std.Io, allocator: std.mem.Allocator, router: *FakeRouter, topic: []const u8) !void {
    try router.inbox.putOne(io, .{ .subscribe = .{ .topic = try allocator.dupe(u8, topic) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.my_topics.contains(topic)) return;
        io_time.ms(5).sleep(io) catch {};
    }
}

/// Spin (bounded) until P's recorded outbound bytes contain at least one PRUNE
/// for `topic`. Returns whether it held.
fn waitPruneSent(io: std.Io, conn: *FakeTransport.FakeConn, topic: []const u8) bool {
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPrunes(conn.record.written.items, topic) >= 1) return true;
        io_time.ms(5).sleep(io) catch {};
    }
    return recordCountPrunes(conn.record.written.items, topic) >= 1;
}

/// Spin (bounded) until P's recorded outbound bytes contain at least one GRAFT
/// for `topic`. Returns whether it held.
fn waitGraftSent(io: std.Io, conn: *FakeTransport.FakeConn, topic: []const u8) bool {
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountGrafts(conn.record.written.items, topic) >= 1) return true;
        io_time.ms(5).sleep(io) catch {};
    }
    return recordCountGrafts(conn.record.written.items, topic) >= 1;
}

/// Post a GRAFT(topic) inbound from `peer` and wait until the router has applied
/// its effect: either the mesh now contains `peer` (accepted) or a PRUNE was sent
/// back (rejected). Returns the observed outcome.
const GraftOutcome = enum { accepted, rejected };
fn graftAndWait(
    io: std.Io,
    allocator: std.mem.Allocator,
    router: *FakeRouter,
    conn: *FakeTransport.FakeConn,
    peer: PeerId,
    topic: []const u8,
) !GraftOutcome {
    const prunes_before = recordCountPrunes(conn.record.written.items, topic);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundGraft(allocator, topic) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.meshContains(topic, peer)) return .accepted;
        if (recordCountPrunes(conn.record.written.items, topic) > prunes_before) return .rejected;
        io_time.ms(5).sleep(io) catch {};
    }
    return if (router.meshContains(topic, peer)) .accepted else .rejected;
}

/// Drive `n` heartbeat ticks and wait until the router's tick counter has
/// advanced by at least `n` (each posted heartbeat advances it by one on the
/// router fiber). Used to age out backoffs deterministically with the fiber
/// disabled (interval 0).
fn beatHeartbeats(io: std.Io, router: *FakeRouter, n: u64) !void {
    const target = router.heartbeat_tick + n;
    var i: u64 = 0;
    while (i < n) : (i += 1) try router.inbox.putOne(io, .heartbeat);
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.heartbeat_tick >= target) return;
        io_time.ms(5).sleep(io) catch {};
    }
}

test "GRAFT accepted: subscribed topic, peer joins the mesh, no PRUNE back" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    const outcome = try graftAndWait(io, allocator, router, conn, peer, "t");
    try std.testing.expectEqual(GraftOutcome.accepted, outcome);
    try std.testing.expect(router.meshContains("t", peer));
    // Accept sends nothing back: no PRUNE on the peer's recorded stream.
    try std.testing.expectEqual(@as(usize, 0), recordCountPrunes(conn.record.written.items, "t"));
}

test "GRAFT rejected when we do not subscribe: PRUNE sent, peer not in mesh" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // We do NOT subscribe to "t".
    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    const outcome = try graftAndWait(io, allocator, router, conn, peer, "t");
    try std.testing.expectEqual(GraftOutcome.rejected, outcome);
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(waitPruneSent(io, conn, "t"));
    try std.testing.expectEqual(@as(usize, 1), recordCountPrunes(conn.record.written.items, "t"));
}

test "GRAFT can push the mesh past D_high; the heartbeat prunes it back to D" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // Drive the mesh ABOVE D_high with distinct peers, each via an accepted GRAFT
    // (the D_high cap is gone — go/rust-faithful, the heartbeat does the pruning).
    const n = mesh_params.d_high + 1; // 13: strictly above D_high
    var conns: [mesh_params.d_high + 1]*FakeTransport.FakeConn = undefined;
    var peers: [mesh_params.d_high + 1]PeerId = undefined;
    var filled: usize = 0;
    defer for (conns[0..filled]) |c| destroyFakeConn(allocator, c);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const p = testPeer(@intCast(10 + i));
        const c = try connectFakePeer(io, allocator, router, p);
        conns[i] = c;
        peers[i] = p;
        filled += 1;
        try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, c, p, "t"));
    }
    try std.testing.expectEqual(n, router.meshSize("t"));

    // One heartbeat: size (13) > D_high (12), so the mesh is pruned back to D (6).
    try beatHeartbeats(io, router, 1);
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.meshSize("t") == mesh_params.d) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(mesh_params.d, router.meshSize("t"));

    // Wait for the PRUNE control frames to actually land on the pruned peers'
    // streams before counting (the writer fibers drain asynchronously). Once the
    // total reaches n - D the writers are quiescent, which also keeps the FakeSink
    // records from being mutated while the test tears down. The router-state
    // checks (meshContains / inBackoff) settle synchronously when the heartbeat
    // is processed, so they are read directly.
    const want_pruned = n - mesh_params.d;
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        var total: usize = 0;
        for (conns[0..n]) |c| total += recordCountPrunes(c.record.written.items, "t");
        if (total >= want_pruned) break;
        io_time.ms(5).sleep(io) catch {};
    }

    // Exactly (n - D) peers were pruned: each pruned peer got a PRUNE on its
    // stream and is now in backoff (it is no longer a mesh member).
    var pruned: usize = 0;
    var backed_off: usize = 0;
    i = 0;
    while (i < n) : (i += 1) {
        if (!router.meshContains("t", peers[i])) {
            if (recordCountPrunes(conns[i].record.written.items, "t") >= 1) pruned += 1;
            if (router.inBackoff("t", peers[i])) backed_off += 1;
        }
    }
    try std.testing.expectEqual(want_pruned, pruned);
    try std.testing.expectEqual(want_pruned, backed_off);
}

test "heartbeat grafts up to D candidate peers when the mesh is below D_low" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // Subscribe to "t" BEFORE any peer is known so the eager subscribe-join finds
    // no candidates; the heartbeat must then do the grafting.
    try subscribeAndWait(io, allocator, router, "t");

    // Connect 3 peers (< D_low), each announcing it subscribes to "t".
    const n = 3;
    var conns: [n]*FakeTransport.FakeConn = undefined;
    var peers: [n]PeerId = undefined;
    var i: usize = 0;
    defer for (conns[0..]) |c| destroyFakeConn(allocator, c);
    while (i < n) : (i += 1) {
        const p = testPeer(@intCast(20 + i));
        peers[i] = p;
        conns[i] = try connectFakePeer(io, allocator, router, p);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, "t", true) } });
    }
    // Wait until all three are tracked as subscribers (the GRAFT selection reads
    // each peer's announced topics).
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peers[0], "t") and
            peerTracksTopic(router, peers[1], "t") and
            peerTracksTopic(router, peers[2], "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }

    // Mesh starts empty; one heartbeat grafts all three (3 < D_low).
    try std.testing.expectEqual(@as(usize, 0), router.meshSize("t"));
    try beatHeartbeats(io, router, 1);
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.meshSize("t") == n) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, n), router.meshSize("t"));
    // Each grafted peer is in the mesh and got a GRAFT on its stream.
    i = 0;
    while (i < n) : (i += 1) {
        try std.testing.expect(router.meshContains("t", peers[i]));
        try std.testing.expect(waitGraftSent(io, conns[i], "t"));
    }
}

test "heartbeat grafts only up to D, not beyond, when many candidates exist" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // Connect 10 peers (all subscribed to "t") BEFORE subscribing locally, so the
    // eager subscribe-join grafts up to D immediately; a later heartbeat must add
    // no more (the mesh is already at D >= D_low). Net effect: mesh == D, exactly
    // D GRAFTs sent across all peers.
    const n = 10;
    var conns: [n]*FakeTransport.FakeConn = undefined;
    var peers: [n]PeerId = undefined;
    var i: usize = 0;
    defer for (conns[0..]) |c| destroyFakeConn(allocator, c);
    while (i < n) : (i += 1) {
        const p = testPeer(@intCast(30 + i));
        peers[i] = p;
        conns[i] = try connectFakePeer(io, allocator, router, p);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, "t", true) } });
    }
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        var all = true;
        for (peers) |p| {
            if (!peerTracksTopic(router, p, "t")) all = false;
        }
        if (all) break;
        io_time.ms(5).sleep(io) catch {};
    }

    try subscribeAndWait(io, allocator, router, "t");
    // The eager subscribe-join grafts up to D; wait for the mesh to reach D.
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.meshSize("t") == mesh_params.d) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // A heartbeat must not push it past D (D >= D_low → no further graft).
    try beatHeartbeats(io, router, 1);
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(mesh_params.d, router.meshSize("t"));

    // Exactly D GRAFTs were sent in total (one per grafted peer); the other
    // n - D candidates got none.
    var grafts: usize = 0;
    for (conns) |c| grafts += recordCountGrafts(c.record.written.items, "t");
    try std.testing.expectEqual(mesh_params.d, grafts);
}

test "PRUNE removes peer from mesh and backs it off (a later GRAFT is rejected)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // P joins the mesh, then PRUNEs us (with a small backoff < default floor).
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundPrune(allocator, "t", 5) } });

    // Wait for the prune to take effect (peer removed from mesh).
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (!router.meshContains("t", peer)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(!router.meshContains("t", peer));

    // A subsequent GRAFT from P is rejected because P is in backoff → PRUNE back.
    const outcome = try graftAndWait(io, allocator, router, conn, peer, "t");
    try std.testing.expectEqual(GraftOutcome.rejected, outcome);
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(waitPruneSent(io, conn, "t"));
}

test "backoff expires after prune_backoff_ticks heartbeats, then GRAFT is accepted" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // PRUNE from P (default-floor backoff) backs P off for prune_backoff_ticks.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundPrune(allocator, "t", 0) } });
    // A GRAFT now is rejected (still in backoff).
    try std.testing.expectEqual(GraftOutcome.rejected, try graftAndWait(io, allocator, router, conn, peer, "t"));

    // Age the backoff out with exactly prune_backoff_ticks heartbeats.
    try beatHeartbeats(io, router, mesh_params.prune_backoff_ticks);

    // A GRAFT is now accepted: P joins the mesh.
    const outcome = try graftAndWait(io, allocator, router, conn, peer, "t");
    try std.testing.expectEqual(GraftOutcome.accepted, outcome);
    try std.testing.expect(router.meshContains("t", peer));
}

test "peer disconnect cleans the peer out of mesh and backoff" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // We subscribe to "t" (so a GRAFT(t) is accepted into the mesh) but NOT to
    // "t2" (so a GRAFT(t2) is rejected, which backs P off for "t2"). After this P
    // has an entry in BOTH mesh["t"] and backoff["t2"].
    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try std.testing.expectEqual(GraftOutcome.rejected, try graftAndWait(io, allocator, router, conn, peer, "t2"));
    try std.testing.expect(router.meshContains("t", peer));
    try std.testing.expect(router.inBackoff("t2", peer));

    // Disconnect P: it must be removed from every mesh and every backoff set.
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));

    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(!router.inBackoff("t2", peer));
    // The testing allocator's end-of-test leak check confirms destroy() freed the
    // (now peer-less) mesh/backoff topic entries with no leak.
}

test "PRUNE with a max-u64 backoff does not crash and actually backs the peer off" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // P joins the mesh, then PRUNEs us with an attacker-controlled max-u64 backoff.
    // Computing the expiry as heartbeat_tick + backoff would overflow: a panic in
    // this Debug build, or (in release) a wrap to a near-zero expiry that silently
    // disables the backoff. The saturating add must avoid both.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try router.inbox.putOne(io, .{
        .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundPrune(allocator, "t", std.math.maxInt(u64)) },
    });

    // Wait for the prune to take effect (peer removed from mesh) — proves we did
    // not crash processing it.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (!router.meshContains("t", peer)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(!router.meshContains("t", peer));

    // The backoff must be set (not wrapped to immediately-expired): a subsequent
    // GRAFT from P is rejected → PRUNE back.
    const outcome = try graftAndWait(io, allocator, router, conn, peer, "t");
    try std.testing.expectEqual(GraftOutcome.rejected, outcome);
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(waitPruneSent(io, conn, "t"));
}

test "re-GRAFT from an existing mesh member at D_high is an idempotent accept (no self-eviction)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // Fill the mesh to D_high with distinct peers, each via an accepted GRAFT.
    const d_high = mesh_params.d_high;
    var conns: [mesh_params.d_high]*FakeTransport.FakeConn = undefined;
    var peers: [mesh_params.d_high]PeerId = undefined;
    var filled: usize = 0;
    defer for (conns[0..filled]) |c| destroyFakeConn(allocator, c);
    var i: usize = 0;
    while (i < d_high) : (i += 1) {
        const p = testPeer(@intCast(10 + i));
        const c = try connectFakePeer(io, allocator, router, p);
        conns[i] = c;
        peers[i] = p;
        filled += 1;
        try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, c, p, "t"));
    }
    try std.testing.expectEqual(d_high, router.meshSize("t"));

    // An existing member re-GRAFTs while the mesh sits at exactly D_high. This
    // must be an idempotent accept: the member stays in the mesh and gets no
    // PRUNE back. The heartbeat prunes only when size is strictly ABOVE D_high, so
    // a mesh at exactly D_high is left untouched (no prune to D here).
    // (graftAndWait would short-circuit to .accepted just because the member is
    // already present, so we instead post the GRAFT directly and then drive one
    // heartbeat: the inbox is a single-fiber FIFO, so once the later heartbeat is
    // processed the GRAFT is fully handled and any spurious PRUNE would be visible.)
    const member = peers[0];
    const member_conn = conns[0];
    const prunes_before = recordCountPrunes(member_conn.record.written.items, "t");
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = member, .rpc = try buildInboundGraft(allocator, "t") } });
    try beatHeartbeats(io, router, 1);
    try std.testing.expect(router.meshContains("t", member));
    try std.testing.expectEqual(d_high, router.meshSize("t"));
    try std.testing.expectEqual(prunes_before, recordCountPrunes(member_conn.record.written.items, "t"));
}

// --- fanout + subscribe-join / unsubscribe-leave fake tests ----------------

test "publish to an UNSUBSCRIBED topic forms a fanout, forwards, then expires" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Flood-publish OFF: this test exercises the relay topology (fanout for a
    // topic we publish to but do not subscribe). With flood-publish on, an
    // originated message bypasses the fanout entirely (it floods straight to all
    // subscribers), so no fanout entry would form — a separate flood test covers
    // that path.
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{ .flood_publish = false });
    defer router.destroy();
    try router.start();

    // We do NOT subscribe to "t". Connect two peers that DO subscribe to "t".
    const peer_a = testPeer(1);
    const peer_b = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try connectFakePeer(io, allocator, router, peer_b);
    defer destroyFakeConn(allocator, conn_b);

    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_a, .rpc = try buildInboundSub(allocator, "t", true) } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_b, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer_a, "t") and peerTracksTopic(router, peer_b, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer_a, "t") and peerTracksTopic(router, peer_b, "t"));

    // Publish on the unsubscribed topic: a fanout["t"] forms (up to D) from the
    // subscribed peers and the message is forwarded to them.
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "fan"),
    } });

    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_a.record.written.items, "t", "fan") >= 1 and
            recordCountPublishes(conn_b.record.written.items, "t", "fan") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_a.record.written.items, "t", "fan"));
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_b.record.written.items, "t", "fan"));
    // The fanout set exists and last-publish was stamped (we never subscribed, so
    // there is no mesh for "t").
    try std.testing.expect(router.fanout.contains("t"));
    try std.testing.expect(router.fanout_last_pub.contains("t"));
    try std.testing.expect(!router.mesh.contains("t"));

    // Drive more than FanoutTTL heartbeats with no further publish: the fanout
    // topic must be dropped (freed). One extra beat past the TTL guarantees the
    // strict `> ttl` comparison fires.
    try beatHeartbeats(io, router, mesh_params.fanout_ttl_ticks + 1);
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (!router.fanout.contains("t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(!router.fanout.contains("t"));
    try std.testing.expect(!router.fanout_last_pub.contains("t"));
}

test "subscribe JOINs the mesh (eager GRAFTs); unsubscribe LEAVEs it (PRUNEs)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // Connect three candidate peers, each subscribed to "t", BEFORE we subscribe,
    // so the eager subscribe-join finds them.
    const n = 3;
    var conns: [n]*FakeTransport.FakeConn = undefined;
    var peers: [n]PeerId = undefined;
    var i: usize = 0;
    defer for (conns[0..]) |c| destroyFakeConn(allocator, c);
    while (i < n) : (i += 1) {
        const p = testPeer(@intCast(40 + i));
        peers[i] = p;
        conns[i] = try connectFakePeer(io, allocator, router, p);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, "t", true) } });
    }
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        var all = true;
        for (peers) |p| {
            if (!peerTracksTopic(router, p, "t")) all = false;
        }
        if (all) break;
        io_time.ms(5).sleep(io) catch {};
    }

    // Subscribe → eager JOIN: all three (< D) are grafted into the mesh and each
    // gets a GRAFT.
    try subscribeAndWait(io, allocator, router, "t");
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.meshSize("t") == n) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, n), router.meshSize("t"));
    for (peers, 0..) |p, idx| {
        try std.testing.expect(router.meshContains("t", p));
        try std.testing.expect(waitGraftSent(io, conns[idx], "t"));
    }

    // Unsubscribe → LEAVE: a PRUNE is sent to every mesh member and mesh["t"] is
    // cleared (the topic key freed).
    try router.inbox.putOne(io, .{ .unsubscribe = .{ .topic = try allocator.dupe(u8, "t") } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (!router.my_topics.contains("t") and !router.mesh.contains("t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(!router.my_topics.contains("t"));
    try std.testing.expect(!router.mesh.contains("t"));
    for (conns) |c| try std.testing.expect(waitPruneSent(io, c, "t"));
}

// --- gossip: cache-on-accept + IHAVE / IWANT fake tests --------------------

test "cache-on-accept: an accepted message with an EMPTY mesh is still cached" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // Subscribe to "t" but connect NO peers and graft nobody: mesh["t"] is empty
    // (or absent). A relay source S delivers a publish on "t". Before this fix the
    // message was cached only on the forward path, so with no mesh it would never
    // enter the cache. It must now be cached regardless. (S is connected only so
    // the inbound_rpc has a tracked source; it is not in the mesh.)
    try subscribeAndWait(io, allocator, router, "t");
    const source = testPeer(1);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);

    const from = "origin";
    const seqno = "\x00\x00\x00\x09";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "lonely"),
    } });

    var id = try rpc.messageId(allocator, from, seqno);
    defer id.deinit(allocator);

    // Poll until the router fiber has processed the inbound RPC and cached the id.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.message_cache.get(id.bytes) != null) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(router.message_cache.get(id.bytes) != null);
    // No mesh member existed, so nothing was forwarded to S either.
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(conn_s.record.written.items, "t", "lonely"));
}

test "IHAVE emission: heartbeat advertises cached ids to a non-mesh subscriber" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // We subscribe to "t". Peer P subscribes to "t" but is NOT grafted into the
    // mesh — a gossip-eligible lazy peer. A relay source S delivers a publish on
    // "t" (caching it). The next heartbeat must send P an IHAVE listing the id.
    try subscribeAndWait(io, allocator, router, "t");
    const peer_p = testPeer(1);
    const source = testPeer(2);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);

    // P announces it subscribes to "t" (so it is a gossip target) but never GRAFTs.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_p, .rpc = try buildInboundSub(allocator, "t", true) } });
    // P also PRUNEs us for "t" so the heartbeat's mesh maintenance will NOT graft
    // it into the mesh (a backed-off peer is not a graft candidate). It stays a
    // gossip target, since gossip-target selection ignores backoff. This keeps P a
    // lazy/non-mesh subscriber across the heartbeat the test then drives.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_p, .rpc = try buildInboundPrune(allocator, "t", 0) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer_p, "t") and router.inBackoff("t", peer_p)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer_p, "t"));
    // P must not be a mesh member.
    try std.testing.expect(!router.meshContains("t", peer_p));

    const from = "origin";
    const seqno = "\x00\x00\x00\x11";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "gossiped"),
    } });
    var id = try rpc.messageId(allocator, from, seqno);
    defer id.deinit(allocator);
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.message_cache.get(id.bytes) != null) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(router.message_cache.get(id.bytes) != null);

    // One heartbeat: P receives an IHAVE(t, [id]).
    try beatHeartbeats(io, router, 1);
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordHasIHave(conn_p.record.written.items, "t", id.bytes)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(recordHasIHave(conn_p.record.written.items, "t", id.bytes));
}

test "inbound IHAVE: unseen id triggers an IWANT; a seen id triggers none" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    const peer_p = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);

    // P announces an IHAVE for an id we have NOT seen → we reply with an IWANT.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundIHave(allocator, "t", &[_]?[]const u8{"unseen-id"}),
    } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountIWants(conn_p.record.written.items, "unseen-id") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountIWants(conn_p.record.written.items, "unseen-id"));

    // Now an IHAVE for an id we HAVE seen: insert it into the seen-cache by way of
    // an inbound publish whose (from, seqno) computes to "seen-id-marker". Build
    // the from/seqno so messageId yields exactly that id, then IHAVE it.
    const from = "fromX";
    const seqno = "sq";
    var id = try rpc.messageId(allocator, from, seqno);
    defer id.deinit(allocator);
    // Deliver the publish so the id is marked seen (no mesh, just dedup state).
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "seenmsg"),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.message_cache.get(id.bytes) != null) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // IHAVE the now-seen id; no new IWANT for it must be sent.
    const iwants_before = recordCountIWants(conn_p.record.written.items, id.bytes);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundIHave(allocator, "t", &[_]?[]const u8{id.bytes}),
    } });
    // Drive a heartbeat afterward: the single-fiber FIFO guarantees the IHAVE is
    // fully processed by the time the heartbeat is, so any spurious IWANT shows.
    try beatHeartbeats(io, router, 1);
    try std.testing.expectEqual(iwants_before, recordCountIWants(conn_p.record.written.items, id.bytes));
}

test "inbound IWANT: a cached id is served as the full publish; an unknown id is not" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    // Subscribe to "t" and connect P (the requester). Publish locally so the
    // message is cached and addressable by its id.
    try subscribeAndWait(io, allocator, router, "t");
    const peer_p = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);

    // Our local publish uses (local_peer, seqno 0) as its id.
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "served-data"),
    } });
    const from = local_test_peer.bytes[0..local_test_peer.len];
    var id = try rpc.messageId(allocator, from, "\x00\x00\x00\x00\x00\x00\x00\x00");
    defer id.deinit(allocator);
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.message_cache.get(id.bytes) != null) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(router.message_cache.get(id.bytes) != null);

    // P sends an IWANT for the cached id AND an unknown id. Only the cached one is
    // served (as the full publish on P's data lane); the unknown id is ignored.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundIWant(allocator, &[_]?[]const u8{ id.bytes, "no-such-id" }),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_p.record.written.items, "t", "served-data") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_p.record.written.items, "t", "served-data"));
}

test "recovery: IHAVE -> IWANT -> served publish is delivered to a node that missed the mesh" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Single-router view of the recovery chain from the RECEIVER's side. The
    // router subscribes to "t" but missed the message via the mesh. Peer P (which
    // holds the message) gossips an IHAVE; the router replies with an IWANT; P then
    // "serves" the message as a normal publish, which the router delivers locally
    // through its standard inbound path (no special delivery route).
    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{});
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");
    const peer_p = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);

    const from = "publisher";
    const seqno = "\x00\x00\x00\x2a";
    var id = try rpc.messageId(allocator, from, seqno);
    defer id.deinit(allocator);

    // Leg 1: P gossips IHAVE(t, [id]) for an id we have not seen.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundIHave(allocator, "t", &[_]?[]const u8{id.bytes}),
    } });
    // Leg 2: the router must IWANT the id from P.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountIWants(conn_p.record.written.items, id.bytes) >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountIWants(conn_p.record.written.items, id.bytes));

    // Leg 3+4: P serves the requested message as a normal publish; the router's
    // standard inbound path delivers it locally (the handler fires) and caches it.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "recovered"),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (rec.calls > 0) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
    try std.testing.expectEqualSlices(u8, "t", rec.topic.?);
    try std.testing.expectEqualSlices(u8, "recovered", rec.data.?);
    // And the recovered message is now cached locally too.
    try std.testing.expect(router.message_cache.get(id.bytes) != null);
}

// --- peer-scoring gate fake tests ------------------------------------------
//
// These tests ENABLE scoring (pass a ScoreConfig to create) to exercise the
// gates. Every other router test leaves scoring disabled (the trailing `null`),
// so this block alone proves the gates fire while the rest proves disabled =
// unchanged behaviour. Scores are driven deterministically through the wiring:
// a peer's invalid-message penalty (P4) is raised by sending it unsigned inbound
// publishes through a router that has StrictSign enabled — each fails
// verification and fires `rejectMessage` against the SENDING peer, dropping its
// score. (Clean, signed deliveries raise it via P2.) All `*_decay` are 1.0 and
// the P1/P3/P3b/P6 weights are zero, so a peer's score is exactly
//   -(invalid_count^2) - (behaviour_excess^2) + first_deliveries,
// stable across heartbeats — making the gate boundaries deterministic.

/// A scoring config whose only live terms are P4 (invalid messages, weight -1),
/// P2 (first deliveries, weight +1), and P7 (behaviour penalty, weight -1), each
/// undecayed, with the thresholds the gate tests assert against:
///   graylist -10, gossip 0 (so a clean peer at 0 is exactly at the bar and
///   passes, a negative peer is denied), publish/px/opportunistic out of the way.
fn scoringConfig() ScoreConfig {
    return .{
        .params = .{
            .app_specific_weight = 0,
            .ip_colocation_factor_weight = 0,
            .ip_colocation_factor_threshold = 0,
            .behaviour_penalty_weight = -1.0,
            .behaviour_penalty_threshold = 0,
            .behaviour_penalty_decay = 1.0,
            .decay_interval_ticks = 1,
            .decay_to_zero = 0.0001,
            .retain_score_ticks = 1000,
            .topic_default = .{
                .topic_weight = 1.0,
                .time_in_mesh_weight = 0,
                .time_in_mesh_quantum_ticks = 1,
                .time_in_mesh_cap = 0,
                .first_message_deliveries_weight = 1.0,
                .first_message_deliveries_decay = 1.0,
                .first_message_deliveries_cap = 100.0,
                .mesh_message_deliveries_weight = 0,
                .mesh_message_deliveries_decay = 1.0,
                .mesh_message_deliveries_threshold = 0,
                .mesh_message_deliveries_cap = 100.0,
                .mesh_message_deliveries_activation_ticks = 100,
                .mesh_message_deliveries_window_ticks = 0,
                .mesh_failure_penalty_weight = 0,
                .mesh_failure_penalty_decay = 1.0,
                .invalid_message_deliveries_weight = -1.0,
                .invalid_message_deliveries_decay = 1.0,
            },
        },
        .thresholds = .{
            .gossip_threshold = 0,
            .publish_threshold = -100.0,
            .graylist_threshold = -10.0,
            .accept_px_threshold = 1000.0,
            .opportunistic_graft_threshold = 1000.0,
        },
    };
}

/// Create a router with scoring ENABLED and StrictSign ON (so unsigned inbound
/// publishes fail verification and fire `rejectMessage`, the lever the gate
/// tests use to drive a peer's score down). The caller owns `host_key`.
fn scoringRouter(
    allocator: std.mem.Allocator,
    io: std.Io,
    host_key: *const identity.KeyPair,
    message_handler: ?MessageHandler,
) !*FakeRouter {
    return FakeRouter.create(allocator, io, .{}, local_test_peer, message_handler, 0, host_key, scoringConfig(), .{});
}

/// Drive `peer`'s score down by `n` invalid-message rejects: send `n` UNSIGNED
/// inbound publishes (distinct seqnos so each is a fresh message, not a dedup'd
/// duplicate) from `peer`. With StrictSign on, each fails verification → the
/// router fires `rejectMessage(peer, topic)` → P4 grows. After `n` rejects the
/// peer's invalid counter is `n`, so its score contribution is `-(n^2)`. Waits
/// until the engine reflects the target so the caller can rely on it.
fn driveInvalid(io: std.Io, allocator: std.mem.Allocator, router: *FakeRouter, peer: PeerId, topic: []const u8, n: usize) !void {
    var i: usize = 0;
    while (i < n) : (i += 1) {
        // A distinct seqno per call keeps every publish a NEW message (the seen
        // cache would otherwise suppress a repeat before verification).
        var seqno: [8]u8 = undefined;
        std.mem.writeInt(u64, &seqno, @as(u64, 0xA000) + i, .big);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{
            .peer = peer,
            .rpc = try buildInboundPublish(allocator, "publisher", &seqno, topic, "bad"),
        } });
    }
    // Post a heartbeat as a fence and wait for it to advance the tick: the inbox
    // is a single-fiber FIFO, so once the heartbeat is processed all `n` rejects
    // are too, and the score snapshot reflects them.
    try beatHeartbeats(io, router, 1);
}

/// The router fiber owns the scoring engine; while it is parked (no inbox
/// command pending) reading the engine from the test fiber is race-free, the
/// same property the mesh/cache assertions rely on. This recomputes a peer's
/// live score off the engine.
fn liveScore(router: *FakeRouter, peer: PeerId) f64 {
    return router.score.?.score(peer);
}

test "scoring graylist gate: an RPC from a below-graylist peer is ignored" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const router = try scoringRouter(allocator, io, &host_key, null);
    defer router.destroy();
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // Drive the peer below the graylist threshold (-10): 4 invalid messages give
    // score -(4^2) = -16 < -10.
    try driveInvalid(io, allocator, router, peer, "t", 4);
    try std.testing.expect(liveScore(router, peer) <= -10.0);

    // Now a SUBSCRIBE RPC from the graylisted peer must be IGNORED entirely: the
    // peer's announced-topics set must not gain "t". Send it and fence with a
    // heartbeat so the (dropped) RPC is fully processed before asserting.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", true) } });
    try beatHeartbeats(io, router, 1);
    try std.testing.expect(!peerTracksTopic(router, peer, "t"));

    // A GRAFT from the graylisted peer is likewise dropped: no PRUNE is sent back
    // (a graylisted peer is ignored, not even rejected). We do not subscribe to
    // "t2", so absent the graylist a GRAFT would have drawn a PRUNE.
    const prunes_before = recordCountPrunes(conn.record.written.items, "t2");
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundGraft(allocator, "t2") } });
    try beatHeartbeats(io, router, 1);
    try std.testing.expectEqual(prunes_before, recordCountPrunes(conn.record.written.items, "t2"));
    try std.testing.expect(!router.meshContains("t2", peer));
}

test "scoring prune gate: a negative-score mesh peer is pruned at the heartbeat" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const router = try scoringRouter(allocator, io, &host_key, null);
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // The peer GRAFTs in (score still 0, accepted) and becomes a mesh member.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try std.testing.expect(router.meshContains("t", peer));

    // Drive it negative (2 invalid → -4 < 0) but keep it above graylist (-10) so
    // its inbound RPCs are still processed (this is the prune gate, not graylist).
    try driveInvalid(io, allocator, router, peer, "t", 2);
    try std.testing.expect(liveScore(router, peer) < 0 and liveScore(router, peer) > -10.0);

    // One heartbeat prunes the negative peer out of the mesh + sends it a PRUNE.
    try beatHeartbeats(io, router, 1);
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (!router.meshContains("t", peer)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(waitPruneSent(io, conn, "t"));
}

test "scoring graft gate: maintenance does not graft a negative-score candidate" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const router = try scoringRouter(allocator, io, &host_key, null);
    defer router.destroy();
    try router.start();

    // Subscribe to "t" with no peers yet so the eager join grafts nobody; the
    // heartbeat would normally graft the subscribed candidate below.
    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // The peer announces it subscribes to "t" (a graft candidate) but we drive it
    // negative first (2 invalid → -4 < 0, still above graylist so the SUBSCRIBE is
    // processed). Send the subscribe BEFORE the rejects so it is recorded while
    // the peer is still at score 0.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer, "t"));

    try driveInvalid(io, allocator, router, peer, "t", 2);
    try std.testing.expect(liveScore(router, peer) < 0 and liveScore(router, peer) > -10.0);

    // The mesh is empty (< D_low); a heartbeat would graft a non-negative
    // candidate, but this one is negative so it must NOT be grafted.
    try std.testing.expectEqual(@as(usize, 0), router.meshSize("t"));
    try beatHeartbeats(io, router, 1);
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expectEqual(@as(usize, 0), router.meshSize("t"));
    // No GRAFT was sent to it.
    try std.testing.expectEqual(@as(usize, 0), recordCountGrafts(conn.record.written.items, "t"));
}

test "scoring graft gate: an inbound GRAFT from a negative-score peer is rejected" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const router = try scoringRouter(allocator, io, &host_key, null);
    defer router.destroy();
    try router.start();

    // We DO subscribe to "t", so absent the score gate a GRAFT would be accepted.
    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // Drive the peer negative (2 invalid → -4) but above graylist.
    try driveInvalid(io, allocator, router, peer, "t", 2);
    try std.testing.expect(liveScore(router, peer) < 0 and liveScore(router, peer) > -10.0);

    // Its GRAFT is rejected purely on score: PRUNE back, not added to the mesh.
    const outcome = try graftAndWait(io, allocator, router, conn, peer, "t");
    try std.testing.expectEqual(GraftOutcome.rejected, outcome);
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(waitPruneSent(io, conn, "t"));
}

test "scoring gossip gate: IHAVE goes to an above-threshold peer, not a below one" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const router = try scoringRouter(allocator, io, &host_key, null);
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // Two lazy (non-mesh) subscribers: GOOD stays at score 0 (>= gossip 0), BAD is
    // driven below the gossip threshold. A relay source S caches a message so the
    // heartbeat has an id to advertise.
    const good = testPeer(1);
    const bad = testPeer(2);
    const source = testPeer(3);
    const conn_good = try connectFakePeer(io, allocator, router, good);
    defer destroyFakeConn(allocator, conn_good);
    const conn_bad = try connectFakePeer(io, allocator, router, bad);
    defer destroyFakeConn(allocator, conn_bad);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);

    // Both announce they subscribe to "t" (so both are gossip candidates). PRUNE
    // each for "t" so the heartbeat's mesh maintenance does not graft them (a
    // backed-off peer is not a graft candidate); they stay lazy subscribers.
    for ([_]PeerId{ good, bad }) |p| {
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, "t", true) } });
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundPrune(allocator, "t", 0) } });
    }
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, good, "t") and peerTracksTopic(router, bad, "t") and
            router.inBackoff("t", good) and router.inBackoff("t", bad)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, good, "t") and peerTracksTopic(router, bad, "t"));

    // Drive BAD below the gossip threshold (1 invalid → -1 < 0), still above
    // graylist so its earlier subscribe stuck and it stays a tracked subscriber.
    try driveInvalid(io, allocator, router, bad, "t", 1);
    try std.testing.expect(liveScore(router, bad) < 0);

    // A relay source delivers a publish so there is a cached id to gossip. The
    // FakeRouter has StrictSign on, so the publish must be SIGNED to verify and
    // be cached. It is signed by `host_key`, so the message's `from` is that
    // key's peer id — the id is keyed under THAT from, not a synthetic origin.
    const signer_from = host_key_from: {
        var s = try signing.Signer.init(allocator, &host_key);
        defer s.deinit();
        break :host_key_from try allocator.dupe(u8, s.fromBytes());
    };
    defer allocator.free(signer_from);
    const seqno = "\x00\x00\x00\x55";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try signedInboundPublish(allocator, &host_key, seqno, "t", "gossiped"),
    } });
    var id = try rpc.messageId(allocator, signer_from, seqno);
    defer id.deinit(allocator);
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.message_cache.get(id.bytes) != null) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(router.message_cache.get(id.bytes) != null);

    // Heartbeat: GOOD gets the IHAVE, BAD does not.
    try beatHeartbeats(io, router, 1);
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordHasIHave(conn_good.record.written.items, "t", id.bytes)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(recordHasIHave(conn_good.record.written.items, "t", id.bytes));
    // Give a (wrong) IHAVE to BAD a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expect(!recordHasIHave(conn_bad.record.written.items, "t", id.bytes));
}

test "scoring events move the score: clean delivery raises it, sig-failure lowers it" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const router = try scoringRouter(allocator, io, &host_key, null);
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // A clean, signed delivery credits P2 (first delivery, weight +1) to the
    // SENDING peer → score rises to +1. (The message's `from`/signer is a separate
    // publisher; the credit goes to the stream's peer regardless.)
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer,
        .rpc = try signedInboundPublish(allocator, &host_key, "\x00\x00\x00\x01", "t", "clean"),
    } });
    try beatHeartbeats(io, router, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), liveScore(router, peer), 1e-9);

    // A signature failure (an unsigned publish) charges P4 to the SENDING peer:
    // invalid count 1 → P4 = -1, net score 1 (P2) - 1 (P4) = 0.
    try driveInvalid(io, allocator, router, peer, "t", 1);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), liveScore(router, peer), 1e-9);

    // A second sig-failure → invalid count 2 → P4 = -4, net 1 - 4 = -3.
    try driveInvalid(io, allocator, router, peer, "t", 1);
    try std.testing.expectApproxEqAbs(@as(f64, -3.0), liveScore(router, peer), 1e-9);
}

/// Build an OwnedRpc carrying a single message SIGNED by `host_key` (so it passes
/// StrictSign verification). The message's `from`/`key` are the signer's, so the
/// from↔key bind holds (verification requires `from` to derive from the signing
/// key); the resulting message id is `signer_from ++ seqno`.
fn signedInboundPublish(
    allocator: std.mem.Allocator,
    host_key: *const identity.KeyPair,
    seqno: []const u8,
    topic: []const u8,
    data: []const u8,
) !pubsub.OwnedRpc {
    var signer = try signing.Signer.init(allocator, host_key);
    defer signer.deinit();
    const from = signer.fromBytes();
    const sig = try signer.sign(from, seqno, topic, data);
    defer allocator.free(sig);
    const msg = rpc_pb.Message{
        .from = from,
        .seqno = seqno,
        .topic = topic,
        .data = data,
        .signature = sig,
        .key = signer.keyBytes(),
    };
    const frame = rpc_pb.RPC{ .publish = &[_]?rpc_pb.Message{msg} };
    return ownedFromRpc(allocator, frame);
}
