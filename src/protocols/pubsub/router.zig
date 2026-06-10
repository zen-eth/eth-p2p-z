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
const intern = @import("intern.zig");
const MsgId = intern.MsgId;
const InternedId = intern.InternedId;
const InternTable = intern.InternTable;
const IdEntry = intern.IdEntry;
const rpc = @import("rpc.zig");
const rpc_pb = @import("../../protobuf.zig").rpc;
const signing = @import("signing.zig");
const identity = @import("../../identity.zig");
const io_time = @import("../../quic/io/time.zig");
const score_mod = @import("score.zig");
const peer_record = @import("../../peer_record.zig");
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;
const PeerId = @import("peer_id").PeerId;

/// The libp2p pubsub message-signature policy. Picks how a published message is
/// stamped, whether an inbound message is verified, and how its message-id is
/// derived:
///   - `strict_sign`: every published message carries `from`+`seqno`+`signature`
///     +`key` and every inbound published message is verified (an invalid or
///     unsigned one is dropped). The message-id is `from`++`seqno`. Requires a
///     host key. This is go-libp2p's default and what cross-impl interop uses.
///   - `none`: a published message carries `from`+`seqno` but no signature, and
///     inbound messages are not verified. The message-id is `from`++`seqno`. A
///     local convenience (used when no host key is configured); NOT a privacy
///     mode — the peer-id still rides on the wire as `from`.
///   - `anonymous` (libp2p StrictNoSign): a published message carries ONLY
///     `topic`+`data` — no `from`/`seqno`/`signature`/`key`, so the publisher's
///     peer-id never appears on the wire. Inbound messages are not verified.
///     Because there is no `from`/`seqno` to key on, the message-id MUST be
///     content-derived (see `message_id_fn` / the default `sha256(topic++data)`);
///     every node in a topic must agree on the same id function. Requires NO key.
pub const SignaturePolicy = enum { strict_sign, none, anonymous };

/// A custom message-id function. Given a message's fields (any of which may be
/// empty — under the anonymous policy `from`/`seqno` are absent), it returns an
/// owned `MessageId` the router frees after use. `ctx` is the opaque pointer the
/// caller registered alongside the function (its own state, e.g. a domain tag).
/// Provided via `RouterConfig.message_id_fn`; when set it overrides the policy's
/// built-in id derivation for BOTH publish and receive, so all nodes in a topic
/// must use the same function for dedup to line up.
pub const MessageIdFn = fn (
    ctx: ?*anyopaque,
    topic: []const u8,
    from: []const u8,
    seqno: []const u8,
    data: []const u8,
    allocator: std.mem.Allocator,
) anyerror!rpc.MessageId;

/// A custom message-id function plus its opaque context, bundled so the router
/// stores one optional value. See `MessageIdFn`.
pub const MessageIdConfig = struct {
    ctx: ?*anyopaque = null,
    func: *const MessageIdFn,
};

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

/// A direct (explicit) peer: its id plus the multiaddr STRING to dial it at.
/// The router keeps the address so it can (re)connect a disconnected direct peer
/// itself (go-libp2p's `DirectConnectTicks` connection maintenance). Both fields
/// are BORROWED for the `create` call only; `create` copies the id and the
/// address into router-owned storage, so the caller may free either afterward.
pub const DirectPeer = struct {
    id: PeerId,
    /// Multiaddr string to dial (e.g. `/ip4/127.0.0.1/udp/4001/quic-v1/p2p/...`).
    addr: []const u8,
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
    /// Local-delivery decoupling (go's Subscription model). > 0 (the default):
    /// accepted messages are COPIED into a bounded delivery queue drained by a
    /// dedicated delivery fiber, which invokes `MessageHandler.on_message` OFF
    /// the router fiber — a slow or blocking handler can then never stall mesh
    /// maintenance or message processing, and a handler that (re)publishes is
    /// safe: it blocks at worst the delivery fiber, whose inbox post the router
    /// keeps draining (no self-deadlock cycle). A FULL queue drops the new
    /// message for the local subscriber, exactly like go's bounded subscriber
    /// channel (forwarding is unaffected; `delivery_drops` counts them).
    /// 0 = the historic INLINE mode: zero-copy, on the router fiber — the
    /// handler must be cheap and MUST NOT call publish/subscribe/unsubscribe
    /// synchronously (those post blocking into the inbox only the router fiber
    /// drains; a full inbox under flood deadlocks the router on itself).
    delivery_queue_len: usize = 256,
    /// Size threshold (bytes of message data) at or above which accepting a NEW
    /// received message broadcasts an IDONTWANT for it to our v1.2 mesh peers on
    /// the topic — telling them "I already have this large message, don't send it
    /// to me", which saves bandwidth on big messages (go-libp2p
    /// `IDontWantMessageThreshold`, default 1 KiB).
    idontwant_message_threshold: usize = 1024,
    /// The signature policy. Leave null to infer it from the host key (the
    /// backward-compatible default): a key present means `strict_sign`, a key
    /// absent means `none`. Set it explicitly to select a policy regardless —
    /// notably `anonymous` (which requires NO host key). The resolved policy is
    /// validated against the host key in `Router.create`: `strict_sign` requires a
    /// key; `none`/`anonymous` require the key to be absent.
    signature_policy: ?SignaturePolicy = null,
    /// Optional override for how a message-id is computed. When set it is used for
    /// every publish and every inbound message, replacing the policy's built-in
    /// derivation (`from`++`seqno` for strict_sign/none, `sha256(topic++data)` for
    /// anonymous). All nodes in a topic must use the SAME function. The default
    /// (null) leaves the policy's built-in derivation in place.
    message_id_fn: ?MessageIdConfig = null,
    /// Direct (explicit) peers: peers with an out-of-band peering agreement
    /// (go-libp2p `WithDirectPeers`). A connected direct peer is treated as
    /// trusted and OUTSIDE the mesh: every valid message on a topic it subscribes
    /// to is forwarded to it unconditionally (alongside the mesh, deduped), it is
    /// NEVER grafted/pruned into a mesh (a GRAFT from it is refused with a PRUNE
    /// back, signalling a non-reciprocal config), and it bypasses the score gates
    /// (graylist, the GRAFT negative-score reject, and the publish threshold). The
    /// peering should be reciprocal — both ends configure each other as direct.
    /// Each entry carries the peer id AND a multiaddr string to dial it at. The
    /// slice is BORROWED for the `create` call only: `create` copies each id and
    /// address into Router-owned storage, so the caller may free the slice
    /// afterward. An empty slice means no direct peers (the default).
    ///
    /// The router keeps each direct peer connected itself (go-libp2p's
    /// `DirectConnectTicks`): it dials every configured direct peer once at start,
    /// then every `MeshParams.direct_connect_ticks` heartbeats re-dials any that
    /// are NOT currently connected. A dial is fire-and-forget — success surfaces
    /// later as the normal connect event; a failure is logged and retried next tick.
    direct_peers: []const DirectPeer = &.{},
    /// Peer exchange (PX) on PRUNE (go-libp2p `WithPeerExchange`, default OFF —
    /// opt-in). When ON, a PRUNE we send on a doPX-eligible path (an over-degree
    /// heartbeat prune) carries a sample of OTHER peers in the topic as signed
    /// peer records, so the pruned peer has alternatives to graft toward — and an
    /// inbound PRUNE that carries such records, from a peer whose score clears the
    /// accept-PX threshold, makes us dial the suggested peers. Disabled is exactly
    /// the historic behaviour: PRUNEs carry no PX peers and inbound PX is ignored.
    /// PX is NOT emitted on a LEAVE (unsubscribe), a GRAFT-reject, or a
    /// negative-score prune (go suppresses PX on those paths to avoid leaking the
    /// mesh to a peer we are cutting off for cause).
    peer_exchange_enabled: bool = false,
    /// Optional application topic-message validator (go-libp2p-pubsub
    /// `RegisterTopicValidator` / rust-libp2p `report_message_validation_result`).
    /// When set, a received published message — once it passes the signature check
    /// and is found new — is handed to it; the verdict gates delivery + forwarding
    /// and, on `reject`, charges the relaying peer the P4 penalty. Null (the
    /// default) accepts every message: behaviour is exactly as it was before the
    /// validator existed. See `MessageValidator` / `ValidationResult`.
    validator: ?MessageValidator = null,
    /// How many message validations may run OFF the router fiber at once
    /// (go-libp2p-pubsub's validation-worker model, bounded by its
    /// `validateThrottle` semaphore). Zero (the default) runs everything INLINE
    /// on the single router fiber — the historic, backward-compatible
    /// behaviour, fine for a cheap validator and light message rates. A value
    /// > 0 enables the ASYNC pipeline for BOTH the StrictSign SIGNATURE check
    /// and the app `validator` (go's worker validate() runs exactly these two,
    /// in this order; its processLoop never touches crypto): a received message
    /// that passes the seen check is snapshotted and handed to a validation
    /// fiber (spawned in a router-owned group, up to this many in flight), and
    /// the combined outcome is posted back as a command which applies ALL
    /// effects — the seen mark, scoring, delivery, and forwarding. Under
    /// strict_sign this is the throughput lever: inline Ed25519 (~60-100µs per
    /// message) otherwise serializes every peer's traffic on the one router
    /// fiber. When this many validations are already in flight a further
    /// message is throttle-DROPPED exactly like go ("validation throttled:
    /// queue full; dropping") — neither marked seen (a later copy can still be
    /// validated) nor penalized; the old inline-fallback let a validation flood
    /// stall the router fiber, the very thing the offload exists to prevent.
    /// No effect when there is nothing to offload (no signer AND no validator).
    /// See `MessageValidator`.
    validation_concurrency: usize = 0,
    /// Optional override of the per-topic mesh DEGREE targets (`d`, `d_low`,
    /// `d_high`) the heartbeat's mesh maintenance uses to decide when to graft
    /// more peers in (mesh below `d_low`, top up to `d`) and when to prune excess
    /// out (mesh above `d_high`, shrink to `d`). Null (the default) keeps the
    /// go-libp2p baseline (`d` 6, `d_low` 5, `d_high` 12), so behaviour is exactly
    /// as before this knob existed. It exists so a SMALL topology can be driven
    /// over-degree on purpose — e.g. a three-node star where the centre meshes
    /// with two leaves can be pushed past `d_high = 1` so the heartbeat prunes one
    /// (the peer-exchange path). Only the maintenance degree decision reads it; the
    /// inbound-GRAFT path is unaffected (it never rejects on size, by design — the
    /// heartbeat is what shrinks an over-full mesh). The three values must be
    /// consistent (`d_low <= d <= d_high`); inconsistent values are not validated
    /// here (a misconfiguration only mis-sizes this node's own mesh).
    mesh_degree: ?MeshDegree = null,
};

/// An override of the mesh DEGREE targets (see `RouterConfig.mesh_degree`). The
/// fields mirror `MeshParams.d`/`d_low`/`d_high`; supplying this replaces those
/// three values for one router's mesh maintenance, leaving every other mesh
/// parameter at its baseline. Used to drive a small topology over-degree in a
/// test/interop node without touching the global defaults.
pub const MeshDegree = struct {
    /// Target mesh degree the heartbeat grafts up to / prunes down to.
    d: usize,
    /// Below this mesh size the heartbeat grafts more peers in (toward `d`).
    d_low: usize,
    /// Above this mesh size the heartbeat prunes excess peers out (toward `d`).
    d_high: usize,
};

/// Invoked on the router fiber for each delivered message on a topic WE
/// subscribe to. The `topic`/`from`/`data` slices are only valid for the
/// duration of the call; a handler that needs to retain them must copy. Keep it
/// cheap: it runs inline on the single router fiber and stalls every other event
/// while it executes.
pub const MessageHandler = struct {
    ctx: *anyopaque,
    /// Invoked once per locally-delivered message. Execution context depends on
    /// `RouterConfig.delivery_queue_len`: by DEFAULT (> 0) it runs on the
    /// router's single DELIVERY fiber — calls are serialized with each other
    /// but run CONCURRENTLY with the router fiber, so the handler may safely
    /// block or call publish/subscribe; in INLINE mode (0) it runs on the
    /// router fiber itself and must be cheap and must not post router commands
    /// (see the config field). The slices are valid only for the call; copy to
    /// retain.
    on_message: *const fn (ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) void,
};

/// The application's verdict on a received message, mirroring go-libp2p-pubsub's
/// `ValidationResult` (Accept/Reject/Ignore) and rust-libp2p's `MessageAcceptance`
/// (the two implementations agree exactly on each verdict's effect):
///   - `accept`: the message is valid — deliver it locally (if we subscribe) and
///     forward it over the mesh (the default, accept-all behaviour).
///   - `reject`: the message is application-invalid — do NOT deliver or forward,
///     and PENALIZE the relaying peer with the squared invalid-message-delivery
///     penalty (P4), exactly as a failed signature check does. Use this only for
///     a message that is genuinely malformed/invalid, since it costs the sender
///     score.
///   - `ignore`: drop the message (no deliver, no forward) but do NOT penalize the
///     relaying peer — the sender did nothing wrong; we simply choose not to
///     propagate it (e.g. a stale-but-well-formed message).
/// In all three cases the message ends up marked seen, so a later duplicate is
/// suppressed without re-validating (go marks seen right after the signature
/// check, before invoking user validators). One async-path nuance, shared with
/// go: copies of one id that arrive while its validation is still IN FLIGHT
/// (before the verdict marks it seen) are validated too — the verdicts converge
/// on the router fiber, where every copy after the first is handled as a
/// duplicate. A validator must therefore tolerate being invoked more than once
/// for the same message under duplicate-heavy arrival.
pub const ValidationResult = enum { accept, reject, ignore };

/// An optional, application-supplied topic message validator, matching the
/// validate-then-forward gate of go-libp2p-pubsub (`RegisterTopicValidator`) and
/// rust-libp2p (`report_message_validation_result`). When set on `RouterConfig`,
/// every received published message — found to be new (not a seen duplicate) and
/// past the StrictSign signature check — is handed to `validate`, whose verdict
/// gates delivery + forwarding (see `ValidationResult`). When null, every message
/// is accepted (the historic accept-all behaviour, unchanged).
///
/// A SINGLE validator covers all topics: an application that wants per-topic logic
/// dispatches on `topic` inside its own `validate` (mirroring how an app registers
/// a function and switches on the topic — we do not maintain a per-topic registry).
///
/// By default (`RouterConfig.validation_concurrency == 0`) this is called INLINE on
/// the single router fiber, like `MessageHandler.on_message`, so it must be cheap:
/// it stalls every other peer's events while it runs (matching go's `validateInline`
/// path). An app with an EXPENSIVE validator should instead set
/// `RouterConfig.validation_concurrency > 0` to run it ASYNCHRONOUSLY on a validation
/// fiber off the router fiber (go-libp2p's validator-worker model), so other peers'
/// events are not blocked; the verdict is then applied back on the router fiber. The
/// `topic`/`from`/`data` slices are valid only for the call; a validator that needs
/// to retain them must copy (the async path hands it copies that live only for the
/// call, exactly like the inline path).
pub const MessageValidator = struct {
    ctx: *anyopaque,
    validate: *const fn (ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) ValidationResult,
};

// The interned message-id machinery — `MsgId`, `InternedId` (= `RefCount(MsgId)`),
// the `InternTable` index, and the `IdEntry` map-value helper — lives in
// `intern.zig` and is imported above. It is shared by this file's per-id maps
// (`seen`, each peer's `dont_send`/`iwant_counts`/`iwant_promises`) AND the
// message cache, so a single id present in several of them is ONE allocation;
// `RefCount.release` is the single free path and the table the single live-id
// index.

/// A TIME-bounded set of owned message-id byte copies, used to suppress
/// duplicate/looping messages — the go-libp2p-pubsub seen-cache model. Each id
/// maps to an EXPIRY heartbeat tick (the tick it was first added plus the TTL);
/// the id reads as "seen" until that tick passes, then a per-heartbeat sweep
/// drops it. First-seen semantics (go's `Strategy_FirstSeen`, the pubsub
/// default): a re-add of an id already present does NOT extend its expiry, so an
/// id stays seen for the TTL measured from the FIRST time it was observed.
///
/// Unlike the old count-bounded FIFO this has no hard count cap: it is bounded by
/// TIME like go, and the per-heartbeat flood defenses (max_ihave / max_idontwant /
/// scoring) bound the inflow of new ids within any TTL window. Each entry holds
/// one reference on a SHARED interned id (the same id in a peer's `dont_send` or
/// `iwant_promises` is one allocation); the sweep `release`s an expired entry's
/// reference and `deinit` releases them all — no per-id `free` here.
/// One unit of work for the delivery fiber: either a delivered message
/// (topic ++ from ++ data packed into one owned allocation, split by the
/// recorded lengths) or a sync fence the fiber sets once every delivery queued
/// before it has been invoked (this is what keeps `sync`'s "everything prior
/// has fully happened" test contract intact in queued-delivery mode).
const Delivery = union(enum) {
    message: struct {
        bytes: []u8,
        topic_len: usize,
        from_len: usize,
    },
    fence: *std.Io.Event,
};

const SeenCache = struct {
    /// The router-shared intern table. `add` interns (retains) an id; the sweep
    /// and `deinit` release. The id bytes are freed only when the LAST holder
    /// across all maps releases (see `InternTable`).
    intern_table: *InternTable,
    allocator: std.mem.Allocator,
    /// How long (in heartbeat ticks) an id stays remembered after it is first
    /// added. Set from `MeshParams.seen_ttl_ticks` at construction.
    ttl_ticks: u64,
    /// id bytes (aliasing the entry's interned box) → an entry holding the box's
    /// reference and the heartbeat tick at which it expires (= first-added tick +
    /// `ttl_ticks`). "Seen" while `expiry` is still in the future; the sweep
    /// removes (and releases) it once `expiry <= now`.
    entries: std.StringHashMapUnmanaged(IdEntry(u64)) = .empty,
    /// The expiry wheel: `ttl_ticks + 1` per-tick buckets; an id added with
    /// expiry tick E lands in `buckets[E % buckets.len]`, and `sweep(T)` drains
    /// exactly `buckets[T % buckets.len]` — O(expired-this-tick), no map scans.
    /// (The old shape iterated the WHOLE map per heartbeat and restarted the
    /// iteration after every removal: O(n*k). At eth2 rates — ~180k retained
    /// ids, ~1.5k expiring per tick — that was a measured multi-SECOND
    /// router-fiber stall per heartbeat; the wheel makes it ~k list pops.)
    ///
    /// Bucket slots are NON-owning copies of the entry's `rc` pointer: the map
    /// entry holds the one reference, and nothing but the sweep ever removes a
    /// live entry, so a bucket slot stays valid exactly until its own drain.
    /// Generation safety: with `ttl + 1` buckets, a bucket index is drained
    /// (at its expiry tick E) strictly before any id that would reuse the same
    /// index can be added (the earliest such add is at tick E+1), so
    /// generations never mix; an id re-added after its sweep simply starts a
    /// new life in a new bucket.
    buckets: []Bucket,
    /// The last tick `sweep` ran for; the next sweep drains every bucket in
    /// (last_swept, now] so the PUBLIC contract stays "remove everything
    /// expired at now" even when ticks jump (tests do; production advances by
    /// one). A jump of a full revolution or more means every live id has
    /// expired, so all buckets drain.
    last_swept: u64 = 0,

    const Bucket = std.ArrayListUnmanaged(*InternedId);

    fn init(allocator: std.mem.Allocator, intern_table: *InternTable, ttl_ticks: u64) std.mem.Allocator.Error!SeenCache {
        const buckets = try allocator.alloc(Bucket, @intCast(ttl_ticks +| 1));
        for (buckets) |*bucket| bucket.* = .empty;
        return .{ .intern_table = intern_table, .allocator = allocator, .ttl_ticks = ttl_ticks, .buckets = buckets };
    }

    fn deinit(self: *SeenCache) void {
        // The map holds the references (bucket slots are non-owning aliases).
        var it = self.entries.valueIterator();
        while (it.next()) |entry| entry.rc.release();
        self.entries.deinit(self.allocator);
        for (self.buckets) |*bucket| bucket.deinit(self.allocator);
        self.allocator.free(self.buckets);
        self.* = undefined;
    }

    /// Whether `id` is currently seen: present AND not yet expired at `now_tick`.
    /// An entry whose expiry has already passed but has not yet been swept reads
    /// as NOT seen, so dedup never relies on the sweep having run.
    fn contains(self: *const SeenCache, id: []const u8, now_tick: u64) bool {
        const entry = self.entries.get(id) orelse return false;
        return entry.payload > now_tick;
    }

    /// Remember `id` until `now_tick + ttl_ticks`, interning it (one shared
    /// allocation across all maps). First-seen: if the id is already present its
    /// expiry is LEFT UNCHANGED (a re-add does not extend it), matching go's
    /// default `Strategy_FirstSeen`. On OOM the id is simply not remembered (dedup
    /// degrades to forwarding a possible duplicate — safe, and the only
    /// alternative is to drop the message, which is worse).
    fn add(self: *SeenCache, id: []const u8, now_tick: u64) void {
        if (self.entries.contains(id)) return;
        const rc = self.intern_table.intern(id) orelse return;
        const expiry = now_tick +| self.ttl_ticks;
        self.entries.put(self.allocator, rc.value.bytes, .{ .rc = rc, .payload = expiry }) catch {
            rc.release();
            return;
        };
        self.buckets[@intCast(expiry % self.buckets.len)].append(self.allocator, rc) catch {
            // No wheel slot means no sweep would ever free it: forget the id
            // now instead (remove BEFORE release — the key aliases the rc's
            // bytes). Dedup degrades to best-effort exactly like the put-OOM
            // path above.
            _ = self.entries.remove(rc.value.bytes);
            rc.release();
        };
    }

    /// Drop every id whose expiry has passed at `now_tick`: drain the wheel
    /// buckets for (last_swept, now] — O(expired this span), no map iteration.
    /// Each map entry is removed BEFORE its `rc` is released (the key aliases
    /// the rc's bytes, which the release may free).
    fn sweep(self: *SeenCache, now_tick: u64) void {
        if (now_tick <= self.last_swept) return;
        if (now_tick - self.last_swept >= self.buckets.len) {
            // The jump spans a full wheel revolution: every live id's expiry
            // (at most last_swept's adds + ttl) is in the past — drain all.
            for (self.buckets) |*bucket| self.drainBucket(bucket, now_tick);
        } else {
            var t = self.last_swept + 1;
            while (t <= now_tick) : (t += 1) {
                self.drainBucket(&self.buckets[@intCast(t % self.buckets.len)], now_tick);
            }
        }
        self.last_swept = now_tick;
    }

    fn drainBucket(self: *SeenCache, bucket: *Bucket, now_tick: u64) void {
        for (bucket.items) |rc| {
            std.debug.assert(blk: {
                const entry = self.entries.get(rc.value.bytes) orelse break :blk false;
                break :blk entry.payload <= now_tick;
            });
            _ = self.entries.remove(rc.value.bytes);
            rc.release();
        }
        // Retains capacity: each bucket pins its historical per-tick expiry
        // peak (8 bytes/slot) for the cache's lifetime — the same non-shrinking
        // trade the id map itself makes, in exchange for allocation-free
        // steady-state churn.
        bucket.clearRetainingCapacity();
    }
};

/// The router inbox holds at most this many un-processed commands. The single
/// fiber drains it continuously, so this only needs to absorb bursts of
/// connect/disconnect/inbound-rpc events between drains.
const inbox_capacity = 256;
/// The control inbox is sized for its bounded producers: validation verdicts
/// (at most `validation_concurrency` in flight), peer lifecycle (churn-bound),
/// one heartbeat, one shutdown. It must never backpressure against a data
/// flood — that coupling is the thing the second queue exists to remove.
const control_inbox_capacity = 256;
/// Bound on peers with a pre-connect subscription stash (see
/// stashPendingSubscription) — garbage-bounded, churn-tolerant.
const max_pending_subscription_peers = 512;

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
    /// How many peers a PX'd PRUNE offers (go-libp2p `GossipSubPrunePeers`, 16).
    /// On emit we select up to this many OTHER topic peers to advertise as signed
    /// records; on consume we process at most this many PX entries from one PRUNE.
    prune_peers: usize = 16,
    /// How long (in heartbeat ticks; one tick is one second) a PRUNE keeps us
    /// from re-grafting the pruned peer for that topic.
    prune_backoff_ticks: u64 = 60,
    /// Shorter backoff (in heartbeat ticks) used when the PRUNE is because we are
    /// LEAVING the topic (unsubscribe), not pruning for mesh maintenance. Both the
    /// wire backoff in the emitted PRUNE and our own local backoff for the departing
    /// peer use this value, so a node that unsubscribes then quickly resubscribes
    /// can re-mesh sooner. (go-libp2p-pubsub `GossipSubUnsubscribeBackoff`, 10s.)
    unsubscribe_backoff_ticks: u64 = 10,
    /// How long (in heartbeat ticks) a fanout topic survives without a publish.
    /// Once `heartbeat_tick - fanout_last_pub[topic]` exceeds this the fanout
    /// peer set is dropped (we stopped publishing to a topic we don't subscribe).
    fanout_ttl_ticks: u64 = 60,
    /// How long (in heartbeat ticks, one tick ≈ one second) a message-id stays in
    /// the seen-cache for duplicate suppression, measured from when the id was
    /// FIRST observed (go's `TimeCacheDuration`, default 2 minutes / 120s; the
    /// cache uses go's first-seen strategy, so a re-add never extends this).
    seen_ttl_ticks: u64 = 120,
    /// Floor on the number of gossip (IHAVE) targets per topic per heartbeat:
    /// peers subscribed to the topic but NOT in its mesh/fanout. The actual count
    /// is `max(gossip_factor * eligible, d_lazy)` (see `gossip_factor`), so a
    /// topic with few eligible peers still gossips to at least this many.
    /// (go-libp2p `D_lazy`.)
    d_lazy: usize = 6,
    /// Fraction of gossip-eligible (subscribed, non-mesh, non-fanout, above the
    /// gossip threshold when scoring) peers that receive IHAVE each heartbeat,
    /// when that fraction exceeds `d_lazy`. The target count is
    /// `max(gossip_factor * eligible_count, d_lazy)`, so large topics gossip to a
    /// proportional slice rather than a fixed floor. (go GossipSubGossipFactor.)
    gossip_factor: f64 = 0.25,
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
    /// Maximum times we serve the SAME message id to ONE peer in response to its
    /// IWANTs, counted within the gossip window (the counts reset each heartbeat
    /// alongside the message-cache window shift). Beyond this, further IWANTs for
    /// that id from that peer are ignored — an anti-spam bound so a peer cannot
    /// make us re-send a message endlessly. (go GossipSubGossipRetransmission.)
    gossip_retransmission: u64 = 3,
    /// Maximum IHAVE messages we process from ONE peer per heartbeat. Once a peer
    /// has sent this many IHAVEs in a heartbeat window, further IHAVEs from it are
    /// ignored until the per-peer counter resets at the next heartbeat. An
    /// anti-spam bound (distinct from `max_ihave_length`, which caps the ids in a
    /// single IHAVE). (go GossipSubMaxIHaveMessages.)
    max_ihave_messages: usize = 10,
    /// How long (in heartbeat ticks) a per-peer IDONTWANT entry suppresses sending
    /// that message id to the peer. After this many heartbeats the entry expires
    /// and we may send the message again. A few heartbeats is enough to cover the
    /// window in which the large message would otherwise be forwarded.
    dont_send_ttl_ticks: u64 = 3,
    /// Upper bound on per-peer IDONTWANT entries. A peer that floods IDONTWANTs
    /// cannot make us hold an unbounded id set: once full, further new ids are
    /// refused (the worst case is we still send a message the peer already has —
    /// safe, just slightly wasteful). Stale entries are reclaimed each heartbeat.
    dont_send_cap: usize = 10000,
    /// Maximum number of message-ids we process from a SINGLE inbound IDONTWANT
    /// control message; ids past this cap are ignored (not recorded, not purged).
    /// Bounds the work and memory one IDONTWANT can cost us, so a peer cannot
    /// flood us with an oversized id list. (go GossipSubMaxIDontWantLength.)
    max_idontwant_length: usize = 10,
    /// Maximum number of IDONTWANT control messages we accept from ONE peer per
    /// heartbeat. Once a peer has sent this many IDONTWANTs in a heartbeat window,
    /// further IDONTWANTs from it are ignored until the per-peer counter resets at
    /// the next heartbeat. An anti-flood bound (distinct from `max_idontwant_length`,
    /// which caps the ids in a single IDONTWANT). (go GossipSubMaxIDontWantMessages.)
    max_idontwant_messages: usize = 1000,
    /// How long (in heartbeat ticks, one tick ≈ one second) a peer has to deliver a
    /// message it implicitly promised by advertising it (IHAVE) and that we then
    /// requested (IWANT). When we send the IWANT we record a promise with deadline
    /// `heartbeat_tick + iwant_followup_ticks`; if the message is not delivered (by
    /// anyone) before the deadline, the heartbeat charges the promising peer a P7
    /// behaviour penalty. This deters peers that advertise ids they do not serve.
    /// (go GossipSubIWantFollowupTime, default 3 seconds.)
    iwant_followup_ticks: u64 = 3,
    /// How often (in heartbeat ticks) the router re-dials any configured direct
    /// peer that is not currently connected, keeping the explicit peering up
    /// (go GossipSubDirectConnectTicks, default 300 ticks ≈ 5 minutes). Direct
    /// peers are ALSO dialed once at start (go dials them after an initial delay);
    /// the periodic re-dial covers reconnect after a drop. Only the disconnected
    /// ones are dialed — a still-connected direct peer is left alone.
    direct_connect_ticks: u64 = 300,
};

const mesh_params: MeshParams = .{};

/// A set of peers (mesh membership), keyed by the zero-padded peer bytes.
const PeerSet = std.AutoHashMapUnmanaged(PeerKey, void);

/// A set of backed-off peers for one topic: peer → the heartbeat tick at which
/// the backoff expires (the peer becomes graftable again once the tick passes).
const BackoffSet = std.AutoHashMapUnmanaged(PeerKey, u64);

/// A verified signed peer record kept in the certified-record store, used to
/// vouch for a peer when offering it through peer-exchange (PX). The store owns
/// the original `envelope_bytes` (the exact wire bytes we re-emit on PX, so a
/// receiver can re-verify the signature) plus the decoded `seq` (the record's
/// monotonic version, used to keep only the newest) and `addrs` (the marshaled
/// multiaddrs, retained for dialing). All owned; `deinit` frees them.
const StoredRecord = struct {
    seq: u64,
    /// Marshaled-multiaddr byte slices, each an owned copy.
    addrs: [][]u8,
    /// The full signed Envelope wire bytes, an owned copy; re-emitted verbatim on
    /// PX (so the offered peer's record stays verifiable end-to-end).
    envelope_bytes: []u8,

    fn deinit(self: *StoredRecord, allocator: std.mem.Allocator) void {
        for (self.addrs) |a| allocator.free(a);
        allocator.free(self.addrs);
        allocator.free(self.envelope_bytes);
        self.* = undefined;
    }
};

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
///
///   pub fn dial(self: *Transport, io: std.Io, addr: []const u8) void;
///       Fire-and-forget: kick off a connection to the multiaddr STRING `addr`
///       and return IMMEDIATELY. The router calls this to (re)connect a direct
///       peer; it does NOT wait and gets no result. Success surfaces later as the
///       normal connect event (a `peer_connected` Command → `onPeerConnected`,
///       which sets up the per-peer streams); a failure is the transport's to log
///       and drop (the router re-dials a still-disconnected direct peer on its
///       next direct-connect tick). The transport owns any fiber it spawns to do
///       the dial and must join it on its own teardown (the router never does).
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
            /// A connection to a peer is gone. Fired once per CONNECTION, so
            /// with two connections to one peer (simultaneous dial) each close
            /// posts its own event. `conn` is an identity token only — it names
            /// WHICH connection died (the handler compares it against the
            /// PeerState's bound connection and ignores a non-matching one) and
            /// must not be dereferenced: the connection may already be freed.
            /// The Switch's unregister callback must be this command's ONLY
            /// producer: it posts before the connection's memory is freed, so
            /// the FIFO inbox processes the event before any later event can
            /// name a recycled address. A producer that cannot order its post
            /// before the free (e.g. a writer fiber) must NOT post this —
            /// see `reap_dead_writers` / `onWriterDisconnect` for the ABA it
            /// would reintroduce.
            peer_disconnected: struct { peer: PeerId, conn: ConnHandle },
            /// The /meshsub protocol version a peer negotiated, learned when it
            /// opens an inbound stream to us (the peer proposes its best version;
            /// we accept it, so this is the highest version that peer supports).
            /// Recorded per peer so version-gated control (e.g. IDONTWANT, 1.2+)
            /// only targets peers that can parse it. May arrive before or after
            /// `peer_connected`; the handler tolerates either order.
            peer_protocol: struct { peer: PeerId, version: pubsub.Version },
            /// A verified signed peer record for `peer` (its `signedPeerRecord`
            /// from libp2p identify, or a PX offer). The router owns
            /// `envelope_bytes` and frees them once processed: it re-verifies the
            /// envelope (the same statelessly-checkable bytes a producer could send)
            /// and, on success, stores it in the certified-record store so PX can
            /// vouch for the peer. Posted by the identify binding, off the router
            /// fiber, so the cert store stays router-fiber-owned.
            peer_record: struct { peer: PeerId, envelope_bytes: []u8 },
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
            /// Test-only barrier: the handler does nothing but set `reply`. Because
            /// the inbox is FIFO and the router is single-consumer, when this reply
            /// fires every command posted before it has been fully processed and the
            /// router fiber is back parked on the inbox (it will not touch its state
            /// again until the next command). A test posts this after its commands
            /// and awaits the reply, then reads router-owned state race-free — the
            /// router fiber owns that state exclusively, so a test must never read it
            /// concurrently with the fiber; this barrier is how a test serializes.
            sync: struct { reply: *std.Io.Event },
            /// An ASYNC message validation finished off the router fiber: its
            /// outcome is `verdict` (signature check + app validator combined —
            /// see `ValidationOutcome`) and `ctx` holds the owned message context
            /// the post-verdict effects need (built before the validation fiber
            /// spawned; freed once this command is processed, or on the
            /// inbox-drain at teardown). Posted by a validation fiber; only
            /// enabled when `validation_concurrency > 0`. See `ValidationContext`.
            validation_result: struct { ctx: *ValidationContext, verdict: ValidationOutcome },
            /// Prompt-wake for the dead-writer reap (see `reapDeadWriters`).
            /// Posted by a writer fiber after it sets its PeerState's
            /// `writer_dead` flag. Deliberately carries NO payload — in
            /// particular no connection handle: a writer-sourced event cannot be
            /// ordered before its connection's free, so a carried pointer could
            /// alias a recycled address and tear down a just-rebound live peer
            /// (ABA). The flag lives on the PeerState itself, so the reap always
            /// acts on current identity. Best-effort: dropped when the inbox is
            /// full, the heartbeat's own reap pass is the lossless fallback.
            reap_dead_writers,
            /// Wake marker a control post drops into the DATA inbox so a
            /// consumer parked in `getOne` re-runs its control drain. Carries
            /// nothing and is dispatched as a no-op; dropped (best-effort)
            /// when the data inbox is full — the consumer is awake then.
            control_ready,
        };

        /// The combined outcome of an ASYNC validation fiber: the StrictSign
        /// signature check folded together with the app validator's verdict
        /// (go's worker validate() runs exactly these two, in this order).
        /// `reject_signature` is distinct from `reject_validator` because their
        /// seen-cache effects differ: a validator-rejected message IS marked
        /// seen (later copies must not re-validate), while a signature-rejected
        /// one is NOT (a forged (from, seqno) must not censor the real message).
        const ValidationOutcome = enum { accept, reject_validator, ignore, reject_signature };

        /// An owned, self-contained snapshot of a received message, HELD from the
        /// moment an async validation is spawned until its verdict is applied (then
        /// freed). It exists because the wire slices a received message borrows
        /// (the `inbound_rpc`'s `OwnedRpc` bytes) are freed as soon as
        /// `onInboundRpc` returns, which is BEFORE the off-fiber validation finishes
        /// — so the validation fiber and the post-verdict accept/reject/ignore
        /// effects must own everything they touch. Owns copies of the message
        /// fields, the relaying peer id (the P4 reject target / forward exclusion),
        /// and the derived message id. Freed exactly once via `deinit` — by the
        /// router fiber when the `validation_result` command is processed, or by the
        /// inbox drain at teardown for a result that never got processed.
        pub const ValidationContext = struct {
            /// The peer the message arrived from (the inbound stream's peer): the
            /// forward-exclusion target and, on reject, the P4 penalty target.
            exclude: PeerId,
            from: []u8,
            seqno: []u8,
            topic: []u8,
            data: []u8,
            /// The message signature/key as carried on the wire; empty (`len == 0`)
            /// when absent (under the none/anonymous policy), mapped back to a null
            /// field on forward exactly as the inline path does.
            signature: []u8,
            key: []u8,
            /// The policy-derived message id (owned bytes), used for the seen-cache,
            /// the IDONTWANT-on-large broadcast, the cache, and the forward frame.
            id: []u8,

            fn deinit(self: *ValidationContext, allocator: std.mem.Allocator) void {
                allocator.free(self.from);
                allocator.free(self.seqno);
                allocator.free(self.topic);
                allocator.free(self.data);
                allocator.free(self.signature);
                allocator.free(self.key);
                allocator.free(self.id);
                allocator.destroy(self);
            }
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
            /// The /meshsub version in effect for this peer, learned from the
            /// version the peer negotiated on its inbound stream. Defaults to the
            /// 1.1 baseline until the inbound stream reports otherwise (an inbound
            /// stream that arrives before this peer_connected seeds it via
            /// `peer_versions`). Gates version-specific control toward the peer.
            protocol_version: pubsub.Version = .v1_1,
            /// Message ids this peer told us (via IDONTWANT) it already has, so we
            /// must NOT send it those messages. Maps id → expiry heartbeat tick;
            /// the entry suppresses sending until `heartbeat_tick > expiry`, at
            /// which point the heartbeat reclaims it. Each entry holds one
            /// reference on the SHARED interned id (released on expiry and on
            /// teardown — never an explicit byte free). Bounded by `dont_send_cap`.
            dont_send: std.StringHashMapUnmanaged(IdEntry(u64)) = .empty,
            /// How many times we have served each message id to this peer via
            /// IWANT within the current gossip window. Incremented each time we
            /// serve the id; once it reaches `gossip_retransmission` we stop
            /// serving that id to this peer (anti-spam). Each entry holds one
            /// reference on the SHARED interned id (released on clear and on
            /// teardown); the whole map is cleared each heartbeat (mirroring go,
            /// which ages these with the mcache window).
            iwant_counts: std.StringHashMapUnmanaged(IdEntry(u64)) = .empty,
            /// Outstanding IWANT promises this peer made: message id → the
            /// heartbeat tick by which the peer must deliver the message. A promise
            /// is recorded when we send the peer an IWANT for an id it advertised
            /// (IHAVE), fulfilled (removed) when any peer delivers a message with
            /// that id, and — if still outstanding once the tick passes the
            /// deadline — harvested by the heartbeat into a P7 behaviour penalty
            /// (go's "broken promise"). Each entry holds one reference on the SHARED
            /// interned id (released on fulfill, on harvest, and on teardown).
            /// Populated only when scoring is enabled.
            iwant_promises: std.StringHashMapUnmanaged(IdEntry(u64)) = .empty,
            /// How many IHAVE messages this peer has sent us in the current
            /// heartbeat window. Incremented on each inbound IHAVE; once it
            /// reaches `max_ihave_messages` we ignore further IHAVEs from the peer
            /// until the heartbeat resets it to zero (anti-spam).
            ihave_received_this_window: usize = 0,
            /// How many IDONTWANT control messages this peer has sent us in the
            /// current heartbeat window. Incremented on each accepted inbound
            /// IDONTWANT; once it reaches `max_idontwant_messages` we ignore further
            /// IDONTWANTs from the peer until the heartbeat resets it to zero
            /// (anti-flood).
            idontwant_received_this_window: usize = 0,
            /// The connection this peer's state is bound to — the one whose
            /// sink the writer drains into. Identity token only (never
            /// dereferenced after connect): `onPeerDisconnected` tears the peer
            /// down only when THIS connection dies, so the close of a dedup'd
            /// duplicate connection (simultaneous dial) leaves the live peer
            /// untouched. Set before the writer fiber spawns; immutable after.
            conn: ConnHandle,
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
            /// Set (release) by the writer fiber when it gives up (open
            /// exhaustion) — the LOSSLESS disconnect signal. The writer's inbox
            /// post is only a best-effort wake and is dropped when the inbox is
            /// full, because the router JOINS the writer in teardownPeer: a
            /// writer parked on the router's own full inbox would deadlock the
            /// whole router (it would await a fiber that waits for the router
            /// to drain the queue it is parked on). The heartbeat sweeps this
            /// flag and tears down flagged peers, so a dropped post only delays
            /// the teardown by up to one heartbeat. Atomic: written on the
            /// writer fiber, read on the router fiber.
            writer_dead: std.atomic.Value(bool) = .init(false),
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
        /// Cached `GS_DEBUG` env flag, resolved once at create. Gates the verbose
        /// per-message/per-peer diagnostic logs without a `getenv` syscall on the
        /// hot fan-out/inbound/heartbeat paths.
        gs_debug: bool,
        /// The per-peer sink factory. Held by value: the real `SwitchTransport`
        /// is empty, so by-value avoids an extra borrowed pointer.
        transport: Transport,
        inbox_storage: []Command,
        inbox: std.Io.Queue(Command),
        control_storage: []Command,
        /// Drained COMPLETELY before each data command (see mainLoop). Carries
        /// validation verdicts, peer lifecycle, heartbeat, and shutdown.
        control_inbox: std.Io.Queue(Command),
        /// Keyed by zero-padded peer bytes (see PeerKey). Values are heap-owned.
        peers: std.AutoHashMap(PeerKey, *PeerState),
        /// Negotiated /meshsub version for peers whose inbound stream reported it
        /// BEFORE their `peer_connected` arrived (the inbound handler and the
        /// peer-event callback fire on independent fibers, so either can win the
        /// race). Holds the version until `peer_connected` creates the PeerState,
        /// which then adopts it and removes the entry; entries are also dropped on
        /// disconnect. Without a matching PeerState an entry is a transient note,
        /// not a leak — disconnect always purges it. Keyed like `peers`.
        peer_versions: std.AutoHashMap(PeerKey, pubsub.Version),
        /// Topic subscriptions a peer announced on its inbound stream BEFORE its
        /// `peer_connected` arrived (the same independent-fiber race that
        /// `peer_versions` handles). Each value is the set of topics the peer is
        /// currently subscribed to (its own owned key copies); `peer_connected`
        /// drains it into the new PeerState's `topics` and removes the entry.
        /// Without this the early SUBSCRIBE is dropped (the peer is untracked) and
        /// never re-sent, so the peer is never recorded as a subscriber — it is
        /// excluded from flood-publish targets AND mesh GRAFT candidates, and an
        /// all-same-impl network fails to propagate. Keyed like `peers`; purged on
        /// disconnect.
        pending_subscriptions: std.AutoHashMap(PeerKey, std.StringHashMapUnmanaged(void)),
        /// Direct (explicit) peers — the go-libp2p `direct` set. A peer whose key
        /// is in here is treated as a trusted out-of-mesh forward target: it
        /// receives every valid message on a topic it subscribes to, is never
        /// added to a mesh (a GRAFT from it draws a PRUNE back), and bypasses the
        /// score gates. The value is the owned multiaddr-string copy used to
        /// (re)dial the peer for connection maintenance (`DirectConnectTicks`).
        /// Populated once in `create` from `RouterConfig.direct_peers` (each id
        /// copied into an owned key, each address into an owned value) and never
        /// mutated; both the keys and the address values are freed on teardown.
        /// Keyed like `peers` (zero-padded peer bytes).
        direct: std.AutoHashMapUnmanaged(PeerKey, []const u8) = .empty,
        /// Whether peer-exchange (PX) is enabled (go-libp2p `WithPeerExchange`,
        /// default OFF). Gates both PX emit (attaching signed records to a
        /// doPX-eligible PRUNE) and PX consume (dialing peers offered in an
        /// inbound PRUNE above the accept-PX score). See
        /// `RouterConfig.peer_exchange_enabled`.
        peer_exchange_enabled: bool,
        /// The resolved mesh DEGREE targets (`d`/`d_low`/`d_high`) the heartbeat's
        /// mesh maintenance uses to decide graft-up / prune-down. Set once in
        /// `create` from `RouterConfig.mesh_degree` (or the `mesh_params` baseline
        /// when that is null) and never mutated. Only the maintenance degree
        /// decision reads it; every other mesh parameter stays at the baseline.
        mesh_degree: MeshDegree,
        /// Certified-record store: peer → its newest verified signed peer record.
        /// Populated by PX consume (a verified inbound record is kept here) and
        /// read by PX emit (`getRecord`) to vouch for an offered peer. A later
        /// step also populates it from identify. `putRecord` replaces an entry
        /// only with a strictly-newer `seq`. Keyed like `peers`; every entry owns
        /// its bytes and is freed on replacement and on teardown.
        cert_store: std.AutoHashMapUnmanaged(PeerKey, StoredRecord) = .empty,
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
        /// The single index of live interned message ids. `seen`, every peer's
        /// `dont_send`/`iwant_counts`/`iwant_promises` all intern through this, so
        /// an id referenced by several maps is ONE allocation shared via reference
        /// counting (freed when the last holder releases). Address-stable because
        /// the Router itself is heap-allocated, so `&router.intern_table` is the
        /// stable `*InternTable` the maps and `MsgId.deinit` hold. Asserts empty at
        /// destroy (every interned id was released).
        intern_table: InternTable,
        /// Time-bounded dedup window over recently-seen message ids (TTL =
        /// `seen_ttl_ticks` heartbeats from when each id was first observed).
        seen: SeenCache,
        /// Windowed cache of recently-forwarded/published messages (keyed by
        /// message id), holding one reference on each message's shared frame.
        /// Serves later gossip needs — IWANT (the full message via `get`) and
        /// IHAVE (recent ids per topic via `getGossipIDs`). Distinct from `seen`
        /// (which only remembers ids for dedup). The window slides once per
        /// heartbeat (`shift`).
        message_cache: mcache.MessageCache,
        /// Monotonic sequence number for messages WE originate; encoded big-endian
        /// into Message.seqno so (from, seqno) is a unique message id. Seeded at
        /// create with wall-clock nanoseconds (`initialSeqno`), NOT 0: peers
        /// remember message ids for the seen TTL, so a counter restarting at 0
        /// would make every publish after a process restart reuse an already-seen
        /// id and be silently dropped network-wide until the TTL expires.
        seqno: u64,
        /// Our own peer id, used as Message.from on publish (under strict_sign /
        /// none; under anonymous publish omits `from` entirely).
        local_peer: PeerId,
        /// The resolved signature policy (see `SignaturePolicy`). Determines how
        /// `onPublish` stamps a message, whether `handleIncomingMessage` verifies,
        /// and — together with `message_id_fn` — how `computeMessageId` derives the
        /// id. Set once in `create` and never mutated.
        signature_policy: SignaturePolicy,
        /// The signing engine, present only under `strict_sign`. When present every
        /// published message is signed and every inbound published message is
        /// verified — invalid or unsigned inbound messages are rejected (dropped,
        /// never delivered or forwarded). Under `none` and `anonymous` it is null:
        /// messages are not signed and inbound messages are not verified. Owns its
        /// cached pubkey bytes; freed on destroy.
        signer: ?signing.Signer,
        /// Optional message-id override (see `RouterConfig.message_id_fn`). When
        /// set, `computeMessageId` calls it instead of the policy's built-in
        /// derivation. Borrowed: the function pointer + ctx must outlive the router.
        message_id_fn: ?MessageIdConfig,
        /// Optional sink for messages delivered on topics we subscribe to.
        message_handler: ?MessageHandler,
        /// Optional application topic-message validator (see `MessageValidator`).
        /// When set, gates delivery + forwarding of each received published message
        /// on its verdict (accept/reject/ignore) after the signature + seen checks.
        /// Null means accept-all (the historic behaviour). Set once in `create`
        /// from `RouterConfig.validator` and never mutated; called only on the
        /// router fiber (inline) or on a validation fiber (async), so it must be
        /// thread-safe when `validation_concurrency > 0`.
        validator: ?MessageValidator,
        /// Cap on validations running OFF the router fiber at once (0 = inline; see
        /// `RouterConfig.validation_concurrency`). Set once in `create`.
        validation_concurrency: usize,
        /// How many async validations are currently in flight (spawned, verdict not
        /// yet applied). Incremented when a validation fiber is spawned, decremented
        /// when its `validation_result` is processed. (The teardown drain does NOT
        /// decrement — by then the main loop has exited and nothing reads the
        /// counter again, so the stale value is dead state.)
        /// Mutated ONLY on the router fiber (the spawn happens in
        /// `handleIncomingMessage`, the decrement in `onValidationResult` / the
        /// drain), so it needs no atomic. Bounds spawning against
        /// `validation_concurrency`; over the cap a message is throttle-DROPPED
        /// (go parity), never validated inline.
        validations_in_flight: usize = 0,
        /// Owns every async validation fiber. Cancelled + awaited in `destroy`
        /// BEFORE any router state the post-verdict effects touch (peers, intern
        /// table, score, maps) is freed, so a still-running validation fiber cannot
        /// race the teardown. A validation fiber only computes a verdict and posts a
        /// `validation_result` command (it touches no router state directly), but it
        /// holds the `*ValidationContext` it allocated; joining it before the inbox
        /// drain guarantees no fiber is mid-post when the drain frees pending
        /// contexts. Empty (no resources) when `validation_concurrency == 0`.
        validation_group: std.Io.Group = .init,
        /// Bounded local-delivery queue (see RouterConfig.delivery_queue_len).
        /// Zero-length storage = inline mode (the queue is never used). The
        /// router fiber is the only producer; the delivery fiber the only
        /// consumer.
        delivery_storage: []Delivery,
        delivery_queue: std.Io.Queue(Delivery),
        delivery_future: ?std.Io.Future(void) = null,
        /// Messages dropped because the delivery queue was full (the local
        /// subscriber lagged) or the copy failed. Router-fiber-only writes.
        delivery_drops: u64 = 0,
        /// Outbound frames dropped because a peer's lane was full (slow peer)
        /// or its queue closed mid-push. Router-fiber-only writes.
        lane_drops: u64 = 0,
        /// When true (go-libp2p default), a message the local node ORIGINATES is
        /// flooded to every topic subscriber above the publish threshold rather
        /// than only its mesh/fanout. Relayed messages are unaffected (always
        /// mesh-only). See `RouterConfig.flood_publish`.
        flood_publish: bool,
        /// Data-size threshold (bytes) at or above which accepting a NEW received
        /// message broadcasts an IDONTWANT for it to our v1.2 mesh peers on the
        /// topic. See `RouterConfig.idontwant_message_threshold`.
        idontwant_message_threshold: usize,
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

        /// The signature policy is resolved from `config.signature_policy` and
        /// `host_key`: a null `signature_policy` is inferred from the key
        /// (`strict_sign` when present, `none` when absent), keeping the historic
        /// behaviour; an explicit policy overrides. The two must be consistent —
        /// `strict_sign` requires a key, `none`/`anonymous` require the key to be
        /// absent — or `create` returns `error.InvalidSignaturePolicy`.
        ///
        /// Under `strict_sign` the router derives + uses the key's peer-id as
        /// `local_peer` (ignoring the passed `local_peer`, which must match — they
        /// are the same node). Under `none`/`anonymous` `local_peer` is used as
        /// given (and under `anonymous` it never reaches the wire). The KeyPair is
        /// borrowed and must outlive the router.
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
            const control_storage = try allocator.alloc(Command, control_inbox_capacity);
            errdefer allocator.free(control_storage);
            const delivery_storage = try allocator.alloc(Delivery, config.delivery_queue_len);
            errdefer allocator.free(delivery_storage);

            // Resolve the policy: an explicit one is used as-is; a null one is
            // inferred from the key (strict_sign with a key, none without) to keep
            // the historic host_key-drives-the-policy default. Then validate the
            // policy against the key so a caller cannot ask for an unsignable
            // strict_sign (no key) or a key-bearing anonymous/none.
            const policy: SignaturePolicy = config.signature_policy orelse
                (if (host_key != null) .strict_sign else .none);
            switch (policy) {
                .strict_sign => if (host_key == null) return error.InvalidSignaturePolicy,
                .none, .anonymous => if (host_key != null) return error.InvalidSignaturePolicy,
            }

            // The Signer exists only under strict_sign: it caches the marshaled
            // pubkey + peer-id once so per-publish signing is cheap, and its
            // peer-id is authoritative for `from` on publish.
            var signer: ?signing.Signer = if (policy == .strict_sign) try signing.Signer.init(allocator, host_key.?) else null;
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
                .gs_debug = std.c.getenv("GS_DEBUG") != null,
                .transport = transport,
                .inbox_storage = inbox_storage,
                .inbox = std.Io.Queue(Command).init(inbox_storage),
                .control_storage = control_storage,
                .delivery_storage = delivery_storage,
                .delivery_queue = std.Io.Queue(Delivery).init(delivery_storage),
                .control_inbox = std.Io.Queue(Command).init(control_storage),
                .peers = std.AutoHashMap(PeerKey, *PeerState).init(allocator),
                .peer_versions = std.AutoHashMap(PeerKey, pubsub.Version).init(allocator),
                .pending_subscriptions = std.AutoHashMap(PeerKey, std.StringHashMapUnmanaged(void)).init(allocator),
                // `intern_table`, `seen`, and `message_cache` are wired up just
                // below: `seen` and `message_cache` each hold a `*InternTable` into
                // `router.intern_table`, which only has a stable address once
                // `router` itself is allocated.
                .intern_table = InternTable.init(allocator),
                .seen = undefined,
                .message_cache = undefined,
                .local_peer = effective_peer,
                .signature_policy = policy,
                .signer = signer,
                .message_id_fn = config.message_id_fn,
                .message_handler = message_handler,
                .validator = config.validator,
                .validation_concurrency = config.validation_concurrency,
                .flood_publish = config.flood_publish,
                .idontwant_message_threshold = config.idontwant_message_threshold,
                .peer_exchange_enabled = config.peer_exchange_enabled,
                // Resolve the mesh-degree targets: an explicit override is used as
                // given; null keeps the `mesh_params` baseline, so the default path
                // is byte-for-byte the historic behaviour.
                .mesh_degree = config.mesh_degree orelse .{
                    .d = mesh_params.d,
                    .d_low = mesh_params.d_low,
                    .d_high = mesh_params.d_high,
                },
                .score = score_engine,
                .heartbeat_interval_ms = heartbeat_interval_ms,
                .seqno = initialSeqno(io),
            };

            // `seen` and `message_cache` share the router's intern table (now
            // address-stable); every id they hold is the same allocation a peer's
            // dont_send / iwant_promises would intern, so an id in both the cache
            // and seen is ONE heap copy freed only on the last release.
            router.seen = try SeenCache.init(allocator, &router.intern_table, mesh_params.seen_ttl_ticks);
            router.message_cache = mcache.MessageCache.init(allocator, &router.intern_table);

            // Copy the configured direct peers into the router-owned set (the
            // config slice is borrowed only for this call). Each id is stored as a
            // zero-padded PeerKey (the same key the peer map / mesh use, so a
            // direct lookup is a plain map probe) mapped to an owned copy of its
            // dial address. A failed copy/insert (OOM) just leaves that peer
            // non-direct — degraded but safe (it falls back to ordinary mesh
            // treatment, and is not auto-dialed); the rest still load.
            for (config.direct_peers) |peer| {
                const addr_owned = allocator.dupe(u8, peer.addr) catch continue;
                router.direct.put(allocator, peerKey(&peer.id), addr_owned) catch {
                    allocator.free(addr_owned);
                };
            }
            return router;
        }

        /// Seed for the publish seqno: wall-clock nanoseconds since the Unix
        /// epoch (go-libp2p parity). Under strict_sign/none the message-id is
        /// `from ++ seqno` and peers hold ids for the seen TTL, so a counter
        /// restarting at 0 would self-censor every publish for up to the TTL
        /// after a process restart. Wall time is unique AND increasing across
        /// restarts (a random seed would only be unique). Falls back to 0 only
        /// if the platform reports a pre-epoch wall clock.
        fn initialSeqno(io: std.Io) u64 {
            const wall_ns: i96 = std.Io.Clock.real.now(io).toNanoseconds();
            if (wall_ns <= 0) return 0;
            return @intCast(@min(wall_ns, @as(i96, std.math.maxInt(u64))));
        }

        /// Spawn the main fiber (and, when the heartbeat interval is non-zero, the
        /// heartbeat fiber). Call once after `create`.
        ///
        /// Also dials every configured direct peer once, so an explicit peering is
        /// established at startup without waiting for the first direct-connect tick
        /// (go-libp2p dials its direct peers once after an initial delay, then the
        /// heartbeat re-dials disconnected ones). No peer is connected yet at this
        /// point — and the binding registers the peer-event callback only AFTER
        /// `start`, so no connect can race this — making the `peers` read here safe
        /// from the caller fiber. Each dial is fire-and-forget.
        pub fn start(router: *Self) std.Io.ConcurrentError!void {
            if (router.delivery_storage.len > 0) {
                router.delivery_future = try std.Io.concurrent(router.io, deliveryLoop, .{router});
            }
            router.main_future = try std.Io.concurrent(router.io, mainLoop, .{router});
            if (router.heartbeat_interval_ms > 0) {
                router.heartbeat_future = try std.Io.concurrent(router.io, heartbeatLoop, .{router});
            }
            router.dialDisconnectedDirectPeers();
        }

        /// Heartbeat fiber body: post a `heartbeat` command every interval until
        /// the router is stopping (or the inbox closes). The interval `sleep` is a
        /// cancellation point, so `destroy`'s cancel collapses the wait. The post
        /// is uncancelable + best-effort: a closed inbox (shutdown) just ends the
        /// loop. Only spawned when `heartbeat_interval_ms > 0`.
        fn heartbeatLoop(router: *Self) void {
            while (!router.stopping.load(.acquire)) {
                io_time.ms(router.heartbeat_interval_ms).sleep(router.io) catch break;
                router.control_inbox.putOneUncancelable(router.io, .heartbeat) catch break;
                router.notifyControl();
            }
        }

        /// Delivery fiber body: invoke the application handler for each queued
        /// message, OFF the router fiber. Single consumer; calls are serialized.
        /// Parks UNCANCELABLY on the queue — `destroy` closes the queue before
        /// joining (close-then-join), so the park always wakes; the close also
        /// flushes: `getUncancelable` keeps returning queued items until the
        /// closed queue is empty, so nothing is stranded.
        fn deliveryLoop(router: *Self) void {
            var buf: [1]Delivery = undefined;
            while (true) {
                const n = router.delivery_queue.getUncancelable(router.io, &buf, 1) catch return;
                if (n == 0) return;
                router.runDelivery(buf[0]);
            }
        }

        fn runDelivery(router: *Self, item: Delivery) void {
            switch (item) {
                .message => |m| {
                    defer router.allocator.free(m.bytes);
                    const h = router.message_handler orelse return;
                    const topic = m.bytes[0..m.topic_len];
                    const from = m.bytes[m.topic_len .. m.topic_len + m.from_len];
                    const data = m.bytes[m.topic_len + m.from_len ..];
                    h.on_message(h.ctx, topic, from, data);
                },
                .fence => |reply| reply.set(router.io),
            }
        }

        /// Teardown backstop: free anything still queued if the delivery fiber
        /// was never spawned (a created-but-never-started router) or already
        /// gone. Fences are SET, not dropped, so no syncing fiber hangs.
        fn drainDeliveries(router: *Self) void {
            var buf: [1]Delivery = undefined;
            while (true) {
                const n = router.delivery_queue.getUncancelable(router.io, &buf, 0) catch return;
                if (n == 0) return;
                switch (buf[0]) {
                    .message => |m| router.allocator.free(m.bytes),
                    .fence => |reply| reply.set(router.io),
                }
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
            router.control_inbox.putOneUncancelable(router.io, .shutdown) catch {};
            router.notifyControl();

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

            // Cancel + join every async validation fiber AFTER the main loop has
            // fully exited (its `defer` ran `teardownAllPeers` + `drainInbox`, so the
            // inbox is now CLOSED) and BEFORE any router state the post-verdict
            // effects touch (peers, intern table, score, maps) is freed below. The
            // main loop is the only producer that spawns into this group, so once it
            // has exited no `concurrent` can race this `cancel` (the group's
            // not-threadsafe contract). Each still-running validation fiber is
            // collapsed at its `validate` sleep / its result post (both cancellation
            // points); its post then fails against the now-closed inbox, so the fiber
            // frees its own held context — no leak, no UAF, freed exactly once.
            // (A result the fiber posted BEFORE the inbox closed was already freed by
            // `drainInbox`.) Idempotent and free when no validation fiber ever ran
            // (the inline default, or an async router that was never flooded).
            router.validation_group.cancel(router.io);

            // Retire the delivery fiber AFTER the main loop and the validation
            // group: only the (now-exited) router fiber produced deliveries, so
            // closing the queue here is final. The fiber drains every queued
            // item (freeing payloads, setting fences) and exits on Closed; the
            // close-then-join order is what makes its uncancelable park safe.
            router.delivery_queue.close(router.io);
            if (router.delivery_future) |*future| {
                future.await(router.io);
                router.delivery_future = null;
            }
            router.drainDeliveries();

            router.peers.deinit();
            router.peer_versions.deinit();
            // Free any pre-connect subscription stashes no `peer_connected` drained
            // (each inner set owns its topic-key copies).
            var pend_it = router.pending_subscriptions.valueIterator();
            while (pend_it.next()) |set| {
                var kit = set.keyIterator();
                while (kit.next()) |tkey| router.allocator.free(tkey.*);
                set.deinit(router.allocator);
            }
            router.pending_subscriptions.deinit();
            // Free each direct peer's owned dial-address copy before the map.
            var direct_it = router.direct.valueIterator();
            while (direct_it.next()) |addr| router.allocator.free(addr.*);
            router.direct.deinit(router.allocator);
            router.freeCertStore();
            router.freeMyTopics();
            router.freeMesh();
            router.freeBackoff();
            router.freeFanout();
            router.seen.deinit();
            // Release the message cache's interned-id holders BEFORE asserting the
            // table is empty: the cache shares the same interned ids as seen / the
            // per-peer maps, so its release must happen before the empty-check.
            router.message_cache.deinit();
            // Every interned id is released by now (seen.deinit + message_cache.deinit
            // above + each peer's dont_send/iwant_counts/iwant_promises released in
            // teardownPeer), so the intern table is empty: its deinit asserts this (a
            // straggler would be a leaked id byte copy / a missed release).
            router.intern_table.deinit();
            if (router.signer) |*s| s.deinit();
            if (router.score) |ps| {
                ps.deinit();
                router.allocator.destroy(ps);
            }
            router.score_snapshot.deinit(router.allocator);
            router.allocator.free(router.inbox_storage);
            router.allocator.free(router.control_storage);
            router.allocator.free(router.delivery_storage);
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

        /// Free every stored record (its addrs + envelope bytes) and the cert-store
        /// map itself. The main fiber is joined by the time this runs.
        fn freeCertStore(router: *Self) void {
            var it = router.cert_store.valueIterator();
            while (it.next()) |rec| rec.deinit(router.allocator);
            router.cert_store.deinit(router.allocator);
        }

        // ----- certified-record store (peer exchange) ---------------------

        /// Store `consumed` (a verified peer record) for `peer`, taking ownership
        /// of owned copies of its addresses and envelope bytes. An existing entry
        /// is REPLACED only when the new record is strictly newer (`seq` greater),
        /// so an older or equal record is ignored (go keeps the certified address
        /// book monotonic by seq). On replacement the old entry's owned bytes are
        /// freed. Best-effort: on allocation failure nothing is stored (PX simply
        /// has no record to vouch for that peer — safe). `consumed` itself is NOT
        /// freed here; the caller owns and frees it.
        fn putRecord(router: *Self, peer: PeerId, consumed: *const peer_record.ConsumedRecord, envelope_bytes: []const u8) void {
            const key = peerKey(&peer);
            if (router.cert_store.get(key)) |existing| {
                // Keep the newest record only; an older/equal seq is ignored.
                if (consumed.seq <= existing.seq) return;
            }

            // Build the owned StoredRecord in a fallible helper so its `errdefer`
            // unwinds every partial allocation on OOM (an `errdefer` in this void
            // function would never fire — `catch return` is not an error return).
            var stored = buildStoredRecord(router.allocator, consumed, envelope_bytes) catch return;

            const gop = router.cert_store.getOrPut(router.allocator, key) catch {
                stored.deinit(router.allocator);
                return;
            };
            if (gop.found_existing) gop.value_ptr.deinit(router.allocator);
            gop.value_ptr.* = stored;
        }

        /// Build an owned `StoredRecord` from a verified record + its envelope
        /// bytes (copying every byte). On any allocation failure the partials are
        /// freed via `errdefer` and the error is propagated, so the caller never
        /// leaks and never stores a half-built record.
        fn buildStoredRecord(allocator: std.mem.Allocator, consumed: *const peer_record.ConsumedRecord, envelope_bytes: []const u8) std.mem.Allocator.Error!StoredRecord {
            const addrs = try allocator.alloc([]u8, consumed.addrs.len);
            var filled: usize = 0;
            errdefer {
                for (addrs[0..filled]) |a| allocator.free(a);
                allocator.free(addrs);
            }
            for (consumed.addrs) |src| {
                addrs[filled] = try allocator.dupe(u8, src);
                filled += 1;
            }

            const env_owned = try allocator.dupe(u8, envelope_bytes);
            return .{ .seq = consumed.seq, .addrs = addrs, .envelope_bytes = env_owned };
        }

        /// The stored signed-envelope bytes for `peer`, or null if we hold no
        /// record. Borrowed from the store (valid until the entry is replaced or
        /// the store is freed); PX emit copies them into the frame.
        fn getRecord(router: *Self, peer: PeerId) ?[]const u8 {
            const rec = router.cert_store.getPtr(peerKey(&peer)) orelse return null;
            return rec.envelope_bytes;
        }

        /// Handle a `peer_record` command: re-verify the signed envelope and, on
        /// success, store it for `peer` so PX can vouch for it. `envelope_bytes`
        /// are router-owned and freed here in every case (the store keeps its own
        /// copy via `putRecord`). The envelope is re-verified even though the
        /// poster already did (the verification is cheap and stateless, and it
        /// keeps the router self-defending: a bad or peer-mismatched record is
        /// dropped, never stored). The record's peer-id must equal `peer` — a
        /// validly-signed record for a DIFFERENT peer would otherwise vouch for
        /// `peer` with another node's addresses.
        fn onPeerRecord(router: *Self, peer: PeerId, envelope_bytes: []u8) void {
            defer router.allocator.free(envelope_bytes);

            var consumed = peer_record.consumeEnvelope(router.allocator, envelope_bytes) catch return;
            defer consumed.deinit(router.allocator);

            var expected = peer;
            if (!consumed.peer_id.eql(&expected)) return;
            router.putRecord(consumed.peer_id, &consumed, envelope_bytes);
        }

        /// Free every owned topic key + nested set (PeerSet/BackoffSet) in a
        /// topic-keyed map, then the map itself. The main fiber is joined by the
        /// time these run, so the maps are quiescent.
        fn freeTopicKeyedMap(router: *Self, map: anytype) void {
            var it = map.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit(router.allocator);
                router.allocator.free(entry.key_ptr.*);
            }
            map.deinit(router.allocator);
        }

        fn freeMesh(router: *Self) void {
            router.freeTopicKeyedMap(&router.mesh);
        }

        fn freeBackoff(router: *Self) void {
            router.freeTopicKeyedMap(&router.backoff);
        }

        fn freeFanout(router: *Self) void {
            router.freeTopicKeyedMap(&router.fanout);
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

        /// Test-only: whether `id` is currently in the seen-cache (present and not
        /// yet expired at the current heartbeat tick). Lets tests assert dedup
        /// state directly without driving a forward through a mesh peer.
        fn seenContains(router: *Self, id: []const u8) bool {
            return router.seen.contains(id, router.heartbeat_tick);
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

        /// Drop `peer` from every topic's mesh and every topic's fanout set.
        /// Called when a peer disconnects so no stale membership survives it.
        ///
        /// Backoff is intentionally NOT cleared here: prune-backoff is keyed by
        /// peer+topic and is independent of connection state. If we PRUNEd a peer
        /// and backed it off, that backoff must persist across a disconnect —
        /// otherwise the peer could disconnect and immediately reconnect to bypass
        /// the backoff and re-GRAFT straight back into our mesh. A disconnected
        /// peer's backoff entry is time-bounded and expires naturally on schedule
        /// via the heartbeat's expiry scan (no leak), so dropping it here is both
        /// unnecessary and exploitable.
        fn dropPeerFromMeshAndFanout(router: *Self, peer: PeerId) void {
            const key = peerKey(&peer);
            var mesh_it = router.mesh.valueIterator();
            while (mesh_it.next()) |set| _ = set.remove(key);
            var fanout_it = router.fanout.valueIterator();
            while (fanout_it.next()) |set| _ = set.remove(key);
        }

        // ----- direct-peer helpers ----------------------------------------

        /// Whether `peer` is a configured direct (explicit) peer. A direct peer is
        /// treated as trusted and out-of-mesh: it receives every valid message on a
        /// topic it subscribes to, is never grafted/pruned into a mesh, and bypasses
        /// the score gates. Inert (always false) when no direct peers are configured.
        fn isDirect(router: *const Self, peer: PeerId) bool {
            return router.direct.contains(peerKey(&peer));
        }

        /// Fire-and-forget (re)dial every configured direct peer that is NOT
        /// currently connected, via `transport.dial`. Keeps the explicit peering
        /// up (go-libp2p `directConnect`): a direct peer that drops is reconnected,
        /// a still-connected one is left alone (no redundant dial). Runs on the
        /// router fiber, so the `peers`/`direct` reads are race-free; each dial
        /// returns immediately (the transport owns the connection attempt), so
        /// this never blocks the fiber. Inert when no direct peers are configured.
        fn dialDisconnectedDirectPeers(router: *Self) void {
            var it = router.direct.iterator();
            while (it.next()) |entry| {
                // entry.key_ptr is the zero-padded PeerKey; `peers` is keyed the
                // same way, so a plain probe tells us if the peer is connected.
                if (router.peers.contains(entry.key_ptr.*)) continue;
                router.transport.dial(router.io, entry.value_ptr.*);
            }
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
                // Never graft a direct (explicit) peer into a mesh — it is an
                // out-of-mesh trusted forward target (go-libp2p filters direct
                // peers out of every mesh-maintenance candidate selection).
                if (router.isDirect(peer)) continue;
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
                // A direct (explicit) peer is never placed in a fanout set: it is
                // an out-of-mesh trusted forward target, reached via the direct
                // path on every publish (go filters direct peers out of fanout).
                if (router.isDirect(peer)) continue;
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
                // A direct peer is always flooded to (go floods to `direct ||
                // score >= publishThreshold`), so it skips the publish-threshold
                // gate; any other peer must be at or above the threshold.
                if (!router.isDirect(peer) and !router.abovePublishThreshold(peer)) continue;
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
            // On any exit (shutdown, closed inboxes, cancellation) tear down
            // every peer and drain both inboxes so nothing leaks. Close the
            // inboxes FIRST: close wakes every producer parked on a full queue
            // with error.Closed — including a writer's disconnect post —
            // BEFORE teardownAllPeers joins those writer fibers. The reverse
            // order would await a fiber parked on a queue that only this (now
            // tearing-down) fiber drains. drainInbox's own closes are
            // idempotent.
            defer {
                router.control_inbox.close(router.io);
                router.inbox.close(router.io);
                router.teardownAllPeers();
                router.drainInbox();
            }

            // TWO inboxes, control before data: every iteration drains ALL
            // queued CONTROL commands (validation verdicts — which free the
            // bounded in-flight validation slots — peer lifecycle, the
            // heartbeat, shutdown) before taking ONE data command. With a
            // single FIFO inbox a data flood queued ahead of the verdicts that
            // recycle validation slots, so a burst starved the async pipeline
            // into throttle-dropping ~97% of itself (measured in
            // bench-gossipsub); lifecycle events were likewise backpressured
            // behind data. Control latency is now bounded by ONE data
            // command's processing time. Wakeups need no extra primitive: a
            // control producer follows its post with a best-effort
            // `.control_ready` marker into the DATA queue (see notifyControl)
            // — if the data queue is full the consumer is necessarily awake
            // already, so the dropped marker loses nothing.
            while (true) {
                if (router.drainControl()) return;
                const command = router.inbox.getOne(router.io) catch return; // Closed/Canceled
                if (command == .control_ready) continue; // wake marker; control drained above
                if (router.dispatch(command)) return;
            }
        }

        /// Drain every queued control command. Returns true when the loop must
        /// exit (shutdown dispatched, or the control inbox is closed).
        fn drainControl(router: *Self) bool {
            var buf: [16]Command = undefined;
            while (true) {
                const n = router.control_inbox.getUncancelable(router.io, &buf, 0) catch return true;
                if (n == 0) return false;
                for (buf[0..n], 0..) |command, i| {
                    if (router.dispatch(command)) {
                        // Commands batched BEHIND the shutdown are already out
                        // of the queue — the teardown drain can never see them,
                        // so free them here (a validation_result's context, a
                        // peer_record's bytes) instead of leaking.
                        for (buf[i + 1 .. n]) |stranded| router.freeCommand(stranded);
                        return true;
                    }
                }
            }
        }

        /// Apply one command on the router fiber (the single shared dispatch
        /// for both inboxes — the queues split PRIORITY, not meaning, so a
        /// command works from either; tests post everything to the data inbox
        /// for strict FIFO determinism). Returns true for shutdown.
        fn dispatch(router: *Self, command: Command) bool {
            switch (command) {
                .peer_connected => |c| router.onPeerConnected(c.peer, c.conn, c.remote_addr),
                .peer_disconnected => |c| router.onPeerDisconnected(c.peer, c.conn),
                .peer_protocol => |c| router.onPeerProtocol(c.peer, c.version),
                .peer_record => |c| router.onPeerRecord(c.peer, c.envelope_bytes),
                .inbound_rpc => |in| router.onInboundRpc(in),
                .subscribe => |s| router.onSubscribe(s.topic),
                .unsubscribe => |u| router.onUnsubscribe(u.topic),
                .publish => |p| router.onPublish(p.topic, p.data),
                .enqueue_for_test => |e| router.onEnqueueForTest(e.peer, e.frame, e.reply),
                .sync => |s| router.fenceSync(s.reply),
                .validation_result => |r| router.onValidationResult(r.ctx, r.verdict),
                .reap_dead_writers => router.reapDeadWriters(),
                .heartbeat => router.onHeartbeat(),
                .control_ready => {},
                .shutdown => return true,
            }
            return false;
        }

        /// Best-effort wake for a control post: a `.control_ready` marker into
        /// the data inbox unparks a consumer blocked in `getOne`. Non-blocking
        /// (`min = 0`): a FULL data inbox means the consumer is awake and will
        /// drain control at its next loop top, so dropping the marker is safe —
        /// the marker is ONLY a waker, never a carrier.
        /// Resolve a `sync` barrier. In queued-delivery mode the reply is
        /// ROUTED THROUGH the delivery queue as a fence, so "sync returned"
        /// keeps meaning "every prior command AND every delivery it produced
        /// has fully happened" — the contract the test suite is built on. The
        /// put is cancelable: at teardown-cancel (or a closed queue) nothing
        /// is pending for the syncing fiber to observe, so set directly.
        fn fenceSync(router: *Self, reply: *std.Io.Event) void {
            if (router.delivery_storage.len == 0) {
                reply.set(router.io);
                return;
            }
            router.delivery_queue.putOne(router.io, .{ .fence = reply }) catch reply.set(router.io);
        }

        pub fn notifyControl(router: *Self) void {
            _ = router.inbox.putUncancelable(router.io, &.{.control_ready}, 0) catch 0;
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
                .conn = conn,
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

            // Adopt a version the peer's inbound stream reported before this
            // connect arrived (the two events race on independent fibers). The
            // pending note is consumed here so it cannot outlive the PeerState.
            if (router.peer_versions.fetchRemove(key)) |kv| {
                state.protocol_version = kv.value;
            }

            // Adopt subscriptions the peer announced before this connect (stashed
            // by applyPeerSubscription because the PeerState did not exist yet).
            // Apply each now that it does, then free the stash. Without this the
            // early SUBSCRIBE is lost and the peer is never seen as a subscriber,
            // so it is excluded from flood-publish + mesh GRAFT (all-same-impl
            // networks then fail to propagate).
            if (router.pending_subscriptions.fetchRemove(key)) |kv| {
                var set = kv.value;
                var kit = set.keyIterator();
                while (kit.next()) |tkey| {
                    router.applyPeerSubscription(peer, tkey.*, true);
                    router.allocator.free(tkey.*);
                }
                set.deinit(router.allocator);
            }

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
        /// The frames built here carry no message id: only data frames need one
        /// (for IDONTWANT matching) and the sole data-frame producer is
        /// `cacheAndForward`, which builds its frame directly. Best-effort
        /// throughout (a void return): a framing failure or a per-peer push
        /// failure logs nothing and simply drops that copy.
        fn fanOut(router: *Self, lane: peer_io.Lane, rpc_frame: rpc_pb.RPC, targets: Targets) void {
            const framed = pubsub.frameRpc(router.allocator, rpc_frame) catch return;
            const frame = peer_io.OutboundFrame.create(router.allocator, framed, null, 1) catch {
                router.allocator.free(framed);
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
        ///
        /// Honours the peer's IDONTWANT (`dont_send`): if the id the frame carries
        /// is one the peer told us it already has, the frame is SKIPPED (not
        /// enqueued) — saving the bandwidth IDONTWANT is for. A skipped target
        /// simply does not `retain`, so the refcount stays balanced. Only data
        /// frames carry an id, so control/subscribe pushes are never skipped.
        fn pushTo(router: *Self, state: *PeerState, lane: peer_io.Lane, frame: *peer_io.OutboundFrame) void {
            if (frame.message_id) |id| {
                if (state.dont_send.contains(id)) {
                    if (router.gs_debug) std.log.info("GS_DEBUG pushTo SKIP lane={s} (dont_send)", .{@tagName(lane)});
                    return;
                }
            }
            if (router.gs_debug and lane == .data) {
                std.log.info("GS_DEBUG pushTo PUSH data frame bytes={d}", .{frame.bytes.len});
            }
            frame.retain();
            state.queue.push(router.io, lane, frame) catch {
                frame.release();
                // Full lane or closed queue: the frame is dropped for THIS peer
                // only (go's bounded-outbound-queue semantics). Router-fiber-only
                // counter; the slow peer itself is reaped by the writer's
                // give-up paths, this just makes the drops observable.
                router.lane_drops += 1;
            };
        }

        /// Send the local node's full current subscription set to one peer's
        /// `.subscribe` lane as a single subscription RPC. No-op when we have no
        /// subscriptions. Best-effort: drops the frame on a push failure.
        ///
        /// The transient SubOpts array (one entry per local topic) is the only
        /// genuinely multi-allocation scratch on this path, so it goes in a
        /// per-command arena that is freed in one shot — no per-entry bookkeeping.
        fn sendCurrentSubscriptions(router: *Self, state: *PeerState) void {
            if (router.gs_debug) {
                std.log.info("GS_DEBUG sendCurrentSubscriptions my_topics={d} peers={d}", .{ router.my_topics.count(), router.peers.count() });
            }
            if (router.my_topics.count() == 0) return;

            var arena = std.heap.ArenaAllocator.init(router.allocator);
            defer arena.deinit();
            const scratch = arena.allocator();

            var subs: std.ArrayListUnmanaged(?rpc_pb.RPC.SubOpts) = .empty;
            var it = router.my_topics.keyIterator();
            while (it.next()) |key| {
                subs.append(scratch, rpc.buildSubscription(key.*, true)) catch return;
            }

            router.fanOut(.subscribe, (rpc.RpcOut{ .subscriptions = subs.items }).toRpc(), .{ .one = state.peer });
        }

        /// Handle a connection to a peer dying: tear the peer down ONLY when the
        /// dead connection is the one its PeerState is bound to. The event fires
        /// once per connection, so in the simultaneous-dial case (both sides
        /// dial; the second connect was dedup'd by onPeerConnected) the dedup'd
        /// duplicate's close arrives here too — keyed on PeerId alone it would
        /// destroy the LIVE peer's mesh/writer/score state out from under its
        /// healthy connection. An absent peer (already removed) is a no-op.
        fn onPeerDisconnected(router: *Self, peer: PeerId, conn: ConnHandle) void {
            const key = peerKey(&peer);
            if (router.peers.get(key)) |state| {
                if (state.conn != conn) {
                    // A connection this peer's state never adopted died. The
                    // duplicate produced no router state of its own (dedup'd at
                    // connect, before makeSink), so there is nothing to clean —
                    // and the pre-connect stashes below belong to the live
                    // peer's streams, not to it. Ignore entirely.
                    return;
                }
            }
            // Drop any pending version note even if the peer was never fully
            // tracked (an inbound stream reported a version but peer_connected
            // never arrived), so it can't outlive the connection.
            _ = router.peer_versions.remove(key);
            // Drop any undrained pre-connect subscription stash too (it must not
            // outlive the connection); free the inner set's topic keys first.
            if (router.pending_subscriptions.fetchRemove(key)) |kv| {
                var set = kv.value;
                var kit = set.keyIterator();
                while (kit.next()) |tkey| router.allocator.free(tkey.*);
                set.deinit(router.allocator);
            }
            const entry = router.peers.fetchRemove(key) orelse return;
            router.dropPeerFromMeshAndFanout(peer);
            router.teardownPeer(entry.value);
            _ = router.peer_count.fetchSub(1, .release);
        }

        /// Record the /meshsub version a peer negotiated on its inbound stream.
        /// If the peer is already tracked, update its PeerState directly;
        /// otherwise stash it in `peer_versions` so the pending `peer_connected`
        /// adopts it (the inbound stream can be negotiated before the peer-event
        /// callback fires). Best-effort: a failed stash just leaves the peer at
        /// the 1.1 default until it reconnects or sends again.
        fn onPeerProtocol(router: *Self, peer: PeerId, version: pubsub.Version) void {
            const key = peerKey(&peer);
            if (router.peers.get(key)) |state| {
                state.protocol_version = version;
                return;
            }
            router.peer_versions.put(key, version) catch {};
        }

        /// Whether `peer` negotiated /meshsub 1.2.0 or newer, i.e. supports the
        /// 1.2 control messages (IDONTWANT). False for an untracked peer or one
        /// still at the pre-1.2 baseline. The gate later layers use before
        /// emitting 1.2-only control toward a peer.
        fn peerSupportsV12(router: *Self, peer: PeerId) bool {
            const state = router.peers.get(peerKey(&peer)) orelse return false;
            return state.protocol_version == .v1_2;
        }

        /// Post the /meshsub version a peer negotiated on its inbound stream onto
        /// the router inbox (the single ordered path into router state). Called
        /// from the inbound stream handler — which runs on a Switch-owned fiber,
        /// not the router fiber — so it must only post. Best-effort on a closed
        /// inbox (the router is shutting down).
        pub fn postPeerProtocol(router: *Self, io: std.Io, peer: PeerId, version: pubsub.Version) void {
            router.control_inbox.putOne(io, .{ .peer_protocol = .{ .peer = peer, .version = version } }) catch {};
            router.notifyControl();
        }

        /// Post a peer's signed peer record (from libp2p identify) onto the router
        /// inbox so the router fiber verifies it and stores it in the certified-
        /// record store. `envelope_bytes` MUST be a router-allocator-owned copy:
        /// ownership transfers to the router, which frees it after processing (and
        /// frees it too if the inbox is closed at shutdown). Called from the
        /// identify binding — off the router fiber — so it only posts. Best-effort:
        /// on a closed inbox (shutting down) the bytes are freed and the post is a
        /// no-op.
        pub fn postPeerRecord(router: *Self, io: std.Io, peer: PeerId, envelope_bytes: []u8) void {
            router.control_inbox.putOne(io, .{ .peer_record = .{ .peer = peer, .envelope_bytes = envelope_bytes } }) catch {
                router.allocator.free(envelope_bytes);
                return;
            };
            router.notifyControl();
        }

        /// Tear down every peer whose `writer_dead` flag is set — the writer
        /// fiber gave up (open exhaustion) and signalled via the flag. Runs on
        /// the router fiber, invoked by the writer's `reap_dead_writers` wake
        /// and by every heartbeat (the lossless fallback when that wake was
        /// dropped on a full inbox). The teardown passes the PeerState's OWN
        /// bound connection, so identity always matches — unlike a
        /// writer-carried handle, this cannot alias a recycled address (ABA).
        /// Teardown mutates the map, so restart the iteration after each
        /// removal — flagged peers are rare (k is almost always 0), so this
        /// stays one O(peers) scan per call.
        fn reapDeadWriters(router: *Self) void {
            reap: while (true) {
                var it = router.peers.valueIterator();
                while (it.next()) |state_ptr| {
                    const state = state_ptr.*;
                    if (state.writer_dead.load(.acquire)) {
                        router.onPeerDisconnected(state.peer, state.conn);
                        continue :reap;
                    }
                }
                break;
            }
        }

        /// Handle one heartbeat tick: advance the tick counter, expire stale
        /// backoffs and per-peer IDONTWANT (`dont_send`) entries, then run mesh
        /// maintenance (graft below D_low / prune above D_high for each subscribed
        /// topic) and fanout maintenance (expire stale fanout topics, replenish
        /// short ones), gossip emission (IHAVE), and the message-cache window shift.
        fn onHeartbeat(router: *Self) void {
            router.heartbeat_tick += 1;
            const tick = router.heartbeat_tick;

            // Reap dead-writer peers first so the rest of the tick (mesh
            // maintenance, gossip emission) never grafts toward or gossips at a
            // peer whose writer is gone. Lossless fallback for a dropped
            // `reap_dead_writers` wake.
            router.reapDeadWriters();

            // Ensure direct peers are connected (go-libp2p `directConnect`): every
            // `direct_connect_ticks` heartbeats, (re)dial any direct peer that is
            // not currently connected. The tick was just incremented (matching go,
            // which bumps `heartbeatTicks` before the check), so the first dial
            // pass lands at tick `direct_connect_ticks`, not tick 0 — the start-time
            // dial in `start` covers the initial connect. Cheap + inert when no
            // direct peers are configured.
            if (mesh_params.direct_connect_ticks > 0 and tick % mesh_params.direct_connect_ticks == 0) {
                router.dialDisconnectedDirectPeers();
            }

            // Expire seen-cache ids whose TTL has elapsed (an id added at tick T
            // expires once the tick reaches T + seen_ttl_ticks); the same id may
            // then be processed again as new, exactly like go's time-cache sweep.
            router.seen.sweep(tick);

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

            // Expire stale per-peer IDONTWANT entries (the id is now sendable
            // again). Each removed entry releases its interned reference (no
            // explicit byte free). Same restart-after-removal pattern as the
            // backoff scan: a removal can move the unscanned tail in an
            // open-addressing map. The map entry is removed BEFORE its rc is
            // released (the key aliases the rc's bytes).
            var peer_it = router.peers.valueIterator();
            while (peer_it.next()) |state_ptr| {
                const dont_send = &state_ptr.*.dont_send;
                var changed = true;
                while (changed) {
                    changed = false;
                    var entry_it = dont_send.iterator();
                    while (entry_it.next()) |entry| {
                        if (entry.value_ptr.payload < tick) {
                            const rc = entry.value_ptr.rc;
                            _ = dont_send.remove(entry.key_ptr.*);
                            rc.release();
                            changed = true;
                            break;
                        }
                    }
                }

                // Reset the per-heartbeat anti-spam counters. The IHAVE-message and
                // IDONTWANT-message budgets reopen for the new window (go resets
                // both per heartbeat), and the IWANT retransmission counts age out
                // with the gossip window (go clears these alongside the mcache
                // slide). Freeing each owned id key keeps `iwant_counts` from
                // leaking.
                state_ptr.*.ihave_received_this_window = 0;
                state_ptr.*.idontwant_received_this_window = 0;
                router.clearIWantCounts(state_ptr.*);

                // Harvest broken IWANT promises: any promise whose deadline tick
                // has passed (`deadline < tick`, i.e. the peer did not deliver in
                // time — go's strict `expire.Before(now)`) is broken. Count this
                // peer's broken promises and release each removed entry's interned
                // reference; the single per-peer penalty is applied below (go's
                // GetBrokenPromises returns a per-peer count, then
                // AddBehaviourPenalty(p, count) once each). No-op when scoring is
                // disabled (the map is always empty then). Entry removed BEFORE its
                // rc is released (the key aliases the rc's bytes).
                const promises = &state_ptr.*.iwant_promises;
                var broken: f64 = 0;
                var changed_p = true;
                while (changed_p) {
                    changed_p = false;
                    var promise_it = promises.iterator();
                    while (promise_it.next()) |entry| {
                        if (entry.value_ptr.payload < tick) {
                            const rc = entry.value_ptr.rc;
                            _ = promises.remove(entry.key_ptr.*);
                            rc.release();
                            broken += 1;
                            changed_p = true;
                            break;
                        }
                    }
                }
                if (broken > 0) {
                    if (router.score) |sc| sc.addPenalty(state_ptr.*.peer, broken);
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

            // TEMP DEBUG: log mesh/subscription state per topic each heartbeat.
            if (router.gs_debug) {
                var tit = router.my_topics.keyIterator();
                while (tit.next()) |tk| {
                    const topic = tk.*;
                    var subs: usize = 0;
                    var pit = router.peers.valueIterator();
                    while (pit.next()) |st| {
                        if (st.*.topics.contains(topic)) subs += 1;
                    }
                    std.log.info("GS_DEBUG tick={d} topic={s} peers={d} subscribers={d} mesh={d}", .{
                        tick, topic, router.peers.count(), subs, router.meshSize(topic),
                    });
                }
            }

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
        /// in the cache's gossipable windows to a slice of the peers that subscribe
        /// to the topic but are NOT in its mesh (and, for a fanout topic, not in its
        /// fanout set) — the "lazy" peers that get gossip rather than full messages.
        /// The slice size is `max(gossip_factor * eligible, d_lazy)`.
        ///
        /// Selection is deterministic (peer-map iteration order); a random shuffle
        /// for anti-eclipse fairness is a future refinement. Score-gated emission is
        /// applied (peers below the gossip threshold are excluded).
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
        /// (truncated at `max_ihave_length`), then send an IHAVE carrying them to the
        /// gossip-eligible peers chosen by `selectGossipTargets`. No-op if the cache
        /// has nothing for the topic. The borrowed ids stay valid through the sends
        /// (no `shift` happens before this returns) and `frameRpc` copies them into
        /// each frame.
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
            if (targets.items.len == 0) return;

            // Build the IHAVE frame ONCE and hand each target a refcounted
            // reference: every target gets the identical id list today (the cap
            // is a deterministic prefix), so the old per-peer sendIHave
            // protobuf-encoded the same bytes once per target — at the eth2
            // shape (5000 ids ≈ 170 KB per encode × ~16 targets per topic per
            // heartbeat) that was megabytes of redundant encoding every second.
            // If per-target shuffle SAMPLING of the over-cap id set is added
            // for go-parity later, this sharing must give way for that case.
            router.sendIHaveShared(topic, capped, targets.items);
        }

        /// Append the gossip targets for `topic` to `out`: a slice of the peers
        /// that announced they subscribe to `topic` but are NOT in its mesh and NOT
        /// in its fanout set (those already get full messages) and clear the gossip
        /// score threshold. The slice size is `max(gossip_factor * eligible_count,
        /// d_lazy)` — a proportional fan-out that still gossips to at least `d_lazy`
        /// peers in small topics. All eligible peers are gathered first (so the
        /// count is exact), then the list is truncated to the target. Peer-map
        /// iteration order is the selection order (deterministic; shuffle is a
        /// refinement).
        fn selectGossipTargets(router: *Self, topic: []const u8, out: *std.ArrayListUnmanaged(PeerId)) void {
            const mesh_set = router.mesh.getPtr(topic);
            const fanout_set = router.fanout.getPtr(topic);
            var it = router.peers.iterator();
            while (it.next()) |entry| {
                const peer = entry.value_ptr.*.peer;
                if (!entry.value_ptr.*.topics.contains(topic)) continue;
                const key = peerKey(&peer);
                if (mesh_set) |s| if (s.contains(key)) continue;
                if (fanout_set) |s| if (s.contains(key)) continue;
                // Never emit IHAVE to a direct (explicit) peer: it already
                // receives every full message on the topic via the direct forward
                // path, so gossiping ids to it is pointless (go excludes direct
                // peers from gossip emission for exactly this reason).
                if (router.isDirect(peer)) continue;
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

            // Target = max(gossip_factor * eligible, d_lazy). `out.items.len` is the
            // eligible count (every appended peer is gossip-eligible). When the
            // proportional slice is smaller than what we gathered, truncate to it;
            // otherwise keep everyone (eligible <= target).
            const eligible: f64 = @floatFromInt(out.items.len);
            const scaled: usize = @intFromFloat(mesh_params.gossip_factor * eligible);
            const target = @max(scaled, mesh_params.d_lazy);
            if (out.items.len > target) out.shrinkRetainingCapacity(target);
        }

        /// Send one IHAVE(topic, ids) frame to every peer in `targets` on their
        /// control lanes: the RPC is protobuf-encoded once and the resulting
        /// frame shared by reference (`pushTo` retains per accepted push; the
        /// builder reference is released at the end, so a total push failure
        /// frees the frame). The borrowed `ids` are copied by `frameRpc`, so
        /// they need only outlive this call. The IHAVE builder takes
        /// `[]const ?[]const u8`, so the ids are wrapped through a small
        /// scratch array of optionals (freed once the frame is built).
        fn sendIHaveShared(router: *Self, topic: []const u8, ids: []const []const u8, targets: []const PeerId) void {
            const opt_ids = router.allocator.alloc(?[]const u8, ids.len) catch return;
            defer router.allocator.free(opt_ids);
            for (ids, 0..) |id, i| opt_ids[i] = id;

            const ihave = rpc.buildIHave(topic, opt_ids);
            const ctrl = rpc_pb.ControlMessage{ .ihave = &.{ihave} };
            const framed = pubsub.frameRpc(router.allocator, (rpc.RpcOut{ .control = ctrl }).toRpc()) catch return;
            const frame = peer_io.OutboundFrame.create(router.allocator, framed, null, 1) catch {
                router.allocator.free(framed);
                return;
            };
            defer frame.release();
            for (targets) |peer| {
                const state = router.peers.get(peerKey(&peer)) orelse continue;
                router.pushTo(state, .control, frame);
            }
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
                // The degree targets come from `router.mesh_degree` (the
                // `mesh_params` baseline unless overridden via config), so a small
                // topology can be driven over-degree on purpose. Every other mesh
                // parameter stays at the module baseline.
                const degree = router.mesh_degree;
                if (size < degree.d_low) {
                    router.graftToTarget(topic, degree.d - size);
                } else if (size > degree.d_high) {
                    router.pruneToTarget(topic, size - degree.d);
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
                // Negative-score prune: NO PX (go's `noPX[p]`) — we never offer
                // our mesh to a peer we are cutting off for misbehaviour.
                router.sendPrune(state.peer, topic, mesh_params.prune_backoff_ticks, false);
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

            // Remove + back off EVERY victim FIRST, then send the PRUNEs. This
            // matches go (the heartbeat decides all prunes, then `sendGraftPrune`
            // builds each PRUNE against the FINAL mesh): peer-exchange offers in a
            // PRUNE are drawn from the SURVIVING mesh peers only, never a peer we
            // are simultaneously pruning. Doing the removal in a first pass keeps
            // the PX selection deterministic and leak-free.
            for (victims.items) |key| {
                if (router.peers.get(key)) |peer| {
                    router.meshRemove(topic, peer.peer);
                    router.setBackoff(topic, peer.peer, mesh_params.prune_backoff_ticks);
                } else if (router.mesh.getPtr(topic)) |set| {
                    // The mesh holds the key even if the peer just disconnected;
                    // drop it (no frame to send for a gone peer).
                    _ = set.remove(key);
                }
            }
            for (victims.items) |key| {
                const peer = router.peers.get(key) orelse continue;
                // Over-degree heartbeat prune: doPX-eligible (go sets doPX here),
                // so when peer exchange is on this PRUNE offers alternative peers.
                router.sendPrune(peer.peer, topic, mesh_params.prune_backoff_ticks, true);
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
        /// IHAVE/IWANT/IDONTWANT drive gossip and bandwidth control. The OwnedRpc
        /// is freed only after all parsing AND forward-frame construction, since
        /// its bytes back the readers and are copied by frameRpc.
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
            // shut out immediately, matching go-libp2p's per-RPC check.) A direct
            // (explicit) peer is exempt — go-libp2p's `AcceptFrom` returns
            // AcceptAll for a direct peer, so its RPCs are processed regardless of
            // score (it is trusted by configuration).
            if (router.score) |sc| {
                if (!router.isDirect(source) and sc.belowGraylist(source)) return;
            }

            // Subscription changes update the source peer's announced topics.
            while (reader.subscriptionsNext()) |sub| {
                router.applyPeerSubscription(source, sub.getTopicid(), sub.getSubscribe());
            }

            // Published messages: dedup, deliver locally, forward over the mesh.
            while (reader.publishNext()) |msg| {
                if (router.gs_debug) {
                    std.log.info("GS_DEBUG onInboundRpc got publish topic={s} data_len={d}", .{ msg.getTopic(), msg.getData().len });
                }
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
            // IDONTWANT records the ids the source already has (so we stop sending
            // them) and purges any still-queued copies to it. Any control replies
            // (a PRUNE rejecting a GRAFT, an IWANT, a served message) are built +
            // framed inside the handlers, which copy the bytes, so freeing the
            // OwnedRpc after this returns is safe.
            if (reader.getControl()) |ctrl_reader| {
                var control = ctrl_reader;
                while (control.graftNext()) |graft| {
                    router.handleGraft(source, graft.getTopicID());
                }
                while (control.pruneNext()) |prune| {
                    var pr = prune;
                    router.handlePrune(source, &pr);
                }
                while (control.ihaveNext()) |ihave| {
                    var ih = ihave;
                    router.handleIHave(source, &ih);
                }
                while (control.iwantNext()) |iwant| {
                    var iw = iwant;
                    router.handleIWant(source, &iw);
                }
                while (control.idontwantNext()) |idontwant| {
                    var idw = idontwant;
                    router.handleIDontWant(source, &idw);
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
        ///
        /// Anti-spam: each inbound IHAVE counts against the peer's per-heartbeat
        /// budget (`max_ihave_messages`). Once the peer has spent it for this
        /// heartbeat window, further IHAVEs from it are dropped (no IWANT) until
        /// the next heartbeat resets the counter.
        fn handleIHave(router: *Self, source: PeerId, ihave: *rpc_pb.ControlIHaveReader) void {
            const state = router.peers.get(peerKey(&source)) orelse return;

            // Per-heartbeat IHAVE-message budget (go GossipSubMaxIHaveMessages):
            // ignore this IHAVE once the peer has reached its cap for the window
            // (the counter resets each heartbeat).
            if (state.ihave_received_this_window >= mesh_params.max_ihave_messages) return;
            state.ihave_received_this_window += 1;

            var wanted: std.ArrayListUnmanaged(?[]const u8) = .empty;
            defer wanted.deinit(router.allocator);
            while (ihave.messageIDsNext()) |id| {
                if (wanted.items.len >= mesh_params.max_iwant_request_ids) break;
                if (router.seen.contains(id, router.heartbeat_tick)) continue;
                wanted.append(router.allocator, id) catch break;
            }
            if (wanted.items.len == 0) return;

            // Track an IWANT promise so a peer that advertises an id (IHAVE) it then
            // fails to serve is charged a P7 behaviour penalty. go records exactly
            // ONE promise per IWANT (a random id from the requested list); we record
            // the first requested id. Only when scoring is enabled — the penalty has
            // nowhere to land otherwise, so we skip the bookkeeping entirely.
            if (router.score != null) {
                if (wanted.items[0]) |first_id| router.addPromise(state, first_id);
            }

            const iwant = rpc.buildIWant(wanted.items);
            const ctrl = rpc_pb.ControlMessage{ .iwant = &.{iwant} };
            router.fanOut(.control, (rpc.RpcOut{ .control = ctrl }).toRpc(), .{ .one = source });
        }

        /// Record an IWANT promise: `state`'s peer must deliver message id `id` by
        /// `heartbeat_tick + iwant_followup_ticks` or the heartbeat charges it a P7
        /// behaviour penalty. An existing promise for the id is left as-is (its
        /// earlier deadline stands, matching go, which does not refresh a promise a
        /// peer already made). The first promise for an id owns a copy of the key
        /// (freed on fulfill, on harvest, or on teardown). Best-effort on OOM (a
        /// failed insert just means no promise — no penalty, which is safe).
        fn addPromise(router: *Self, state: *PeerState, id: []const u8) void {
            if (state.iwant_promises.contains(id)) return;
            const rc = router.intern_table.intern(id) orelse return;
            const deadline = router.heartbeat_tick +| mesh_params.iwant_followup_ticks;
            state.iwant_promises.put(router.allocator, rc.value.bytes, .{ .rc = rc, .payload = deadline }) catch {
                rc.release();
            };
        }

        /// Fulfill (clear) every outstanding IWANT promise for message id `id`
        /// across all peers: a delivery of the message satisfies whoever promised
        /// it, so no penalty is owed (go removes the promise for all peers on
        /// fulfill). Releases each removed entry's interned reference. Cheap when no
        /// peer promised the id.
        fn fulfillPromise(router: *Self, id: []const u8) void {
            var it = router.peers.valueIterator();
            while (it.next()) |state_ptr| {
                if (state_ptr.*.iwant_promises.fetchRemove(id)) |kv| {
                    kv.value.rc.release();
                }
            }
        }

        /// Handle an inbound IWANT: the source peer requests the listed message
        /// ids. For each id still in the cache, retain its frame and push it onto
        /// the source's `.data` lane — the cached frame is already a complete
        /// single-message publish RPC, so the served message re-enters the peer's
        /// normal inbound path on the other side (dedup, deliver, cache, forward).
        /// Ids we do not have are ignored. Capped at `max_iwant_to_serve` messages
        /// per request so one peer cannot make us serve an unbounded set.
        ///
        /// Anti-spam: we serve the same id to the same peer at most
        /// `gossip_retransmission` times within the gossip window (tracked in the
        /// peer's `iwant_counts`, reset each heartbeat). A repeat request for an id
        /// already served that many times is ignored.
        fn handleIWant(router: *Self, source: PeerId, iwant: *rpc_pb.ControlIWantReader) void {
            const state = router.peers.get(peerKey(&source)) orelse return;
            var served: usize = 0;
            while (iwant.messageIDsNext()) |id| {
                if (served >= mesh_params.max_iwant_to_serve) break;
                // Retransmission cap: skip an id we have already served this peer
                // `gossip_retransmission` times in the current window. Checked
                // before the cache lookup so a flooded id is cheap to reject.
                if (state.iwant_counts.get(id)) |entry| {
                    if (entry.payload >= mesh_params.gossip_retransmission) continue;
                }
                const frame = router.message_cache.get(id) orelse continue;
                // Retain before pushing (the queue holds the reference; the cache
                // keeps its own). `pushTo` releases on a rejected push, so a full
                // queue does not leak the retained reference.
                router.pushTo(state, .data, frame);
                router.bumpIWantCount(state, id);
                served += 1;
            }
        }

        /// Increment the per-peer served count for message id `id`. The first time
        /// an id is served it is interned (one reference held by this entry,
        /// released when `iwant_counts` is cleared each heartbeat, or on teardown);
        /// afterwards we just bump the existing entry's count. Best-effort on OOM (a
        /// failed insert just means we may serve the id one extra time — safe).
        fn bumpIWantCount(router: *Self, state: *PeerState, id: []const u8) void {
            const gop = state.iwant_counts.getOrPut(router.allocator, id) catch return;
            if (!gop.found_existing) {
                const rc = router.intern_table.intern(id) orelse {
                    // Undo the placeholder entry (its key aliases borrowed bytes).
                    _ = state.iwant_counts.remove(id);
                    return;
                };
                // Re-key to the interned box's owned bytes (the placeholder key
                // aliased the borrowed id), then start the count at zero.
                gop.key_ptr.* = rc.value.bytes;
                gop.value_ptr.* = .{ .rc = rc, .payload = 0 };
            }
            gop.value_ptr.payload += 1;
        }

        /// Handle an inbound IDONTWANT(ids) from `source`: the peer already holds
        /// those (typically large) messages and does not want us to send them. For
        /// each id we (a) record it in the peer's `dont_send` set with a TTL so the
        /// send-side skip honours it going forward, and (b) purge any not-yet-sent
        /// copies already queued for the peer.
        ///
        /// The ids are gathered into a borrowed-key set (keys point into the wire
        /// bytes, which outlive this call) so the purge predicate can test a
        /// frame's carried id(s) against the whole IDONTWANT set in one queue scan.
        /// The same set drives the per-id `dont_send` inserts. Honouring IDONTWANT
        /// from any peer is harmless, so this is not version-gated. Untracked
        /// source is ignored.
        ///
        /// Flood protection mirrors go-libp2p: we accept at most
        /// `max_idontwant_messages` IDONTWANT control messages from one peer per
        /// heartbeat (tracked in the peer's `idontwant_received_this_window`, reset
        /// each heartbeat) — once spent, further IDONTWANTs from the peer are
        /// dropped — and we process at most `max_idontwant_length` ids from a single
        /// IDONTWANT (ids past the cap are ignored).
        fn handleIDontWant(router: *Self, source: PeerId, idontwant: *rpc_pb.ControlIDontWantReader) void {
            const state = router.peers.get(peerKey(&source)) orelse return;

            // Per-heartbeat IDONTWANT-message budget (go GossipSubMaxIDontWantMessages):
            // drop this IDONTWANT once the peer has reached its cap for the window
            // (the counter resets each heartbeat). Checked-then-incremented so the
            // cap-th message is still processed.
            if (state.idontwant_received_this_window >= mesh_params.max_idontwant_messages) return;
            state.idontwant_received_this_window += 1;

            // Borrowed-key set of the IDONTWANT ids: keys alias the OwnedRpc's wire
            // bytes (valid until onInboundRpc frees them, after this returns), so no
            // copies here. Drives both the per-id dont_send inserts (which DO own a
            // copy of the id) and the single-pass queue purge below.
            var ids: std.StringHashMapUnmanaged(void) = .empty;
            defer ids.deinit(router.allocator);

            // Per-message id budget (go GossipSubMaxIDontWantLength): stop after the
            // cap so a peer cannot make us record/purge an unbounded id list from
            // one IDONTWANT. Ids past the cap are ignored entirely.
            var processed: usize = 0;
            const expiry = router.heartbeat_tick +| mesh_params.dont_send_ttl_ticks;
            while (idontwant.messageIDsNext()) |id| {
                if (processed >= mesh_params.max_idontwant_length) break;
                processed += 1;
                ids.put(router.allocator, id, {}) catch continue;
                router.recordDontSend(state, id, expiry);
            }
            if (ids.count() == 0) return;

            // Purge every queued DATA frame whose carried id(s) the peer just said
            // it does not want. `removeData` releases each removed frame's
            // reference, keeping the shared-frame refcount balanced. The ctx is a
            // const pointer to the id set (the predicate only reads it).
            const ids_ptr: *const std.StringHashMapUnmanaged(void) = &ids;
            _ = state.queue.removeData(router.io, ids_ptr, dontWantPred);
        }

        /// `removeData` predicate: true if the frame's carried message id is in
        /// the IDONTWANT id set `ids` (so the frame must be dropped from the
        /// peer's queue). Frames carry the single id of the message they hold (or
        /// none for control frames, which never reach the data lane).
        fn dontWantPred(ids: *const std.StringHashMapUnmanaged(void), frame: *const peer_io.OutboundFrame) bool {
            const id = frame.message_id orelse return false;
            return ids.contains(id);
        }

        /// Record (a copy of) message id `id` in the peer's `dont_send` set with
        /// the given expiry tick, so the send-side skip suppresses sending it to
        /// the peer until the heartbeat reclaims the entry. A no-op if already
        /// present (refreshing the expiry of an existing entry is not needed — the
        /// peer re-IDONTWANTs if it still has the message). Refused (and freed) once
        /// the set is at `dont_send_cap` so a flooding peer cannot grow it without
        /// bound. Best-effort on OOM (the message may still be sent — safe).
        fn recordDontSend(router: *Self, state: *PeerState, id: []const u8, expiry: u64) void {
            if (state.dont_send.contains(id)) return;
            if (state.dont_send.count() >= mesh_params.dont_send_cap) return;
            const rc = router.intern_table.intern(id) orelse return;
            state.dont_send.put(router.allocator, rc.value.bytes, .{ .rc = rc, .payload = expiry }) catch {
                rc.release();
            };
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

            // A direct (explicit) peer is never grafted into a mesh — it is an
            // out-of-mesh trusted forward target. A GRAFT from one signals a
            // non-reciprocal peering config (the remote thinks we are a regular
            // mesh peer); reply with a PRUNE so it stops, but do NOT back it off
            // (it stays a direct forward target). Matches go-libp2p, which warns
            // and PRUNEs the direct peer back without adding it to the mesh.
            if (router.isDirect(source)) {
                // GRAFT-reject: NO PX (go sets doPX=false on every reject branch).
                router.sendPrune(source, topic, mesh_params.prune_backoff_ticks, false);
                return;
            }

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
                // GRAFT-reject (backoff/negative-score): NO PX (go doPX=false).
                router.sendPrune(source, topic, mesh_params.prune_backoff_ticks, false);
            }
        }

        /// Handle an inbound PRUNE from `source`: drop the peer from the topic's
        /// mesh and back it off. The wire `backoff` is in seconds (≈ ticks at one
        /// tick per second). When the peer specifies a backoff (> 0) we obey it
        /// exactly — including a shorter one, e.g. the 10s a peer sends when it is
        /// unsubscribing — matching go (`handlePrune`: "is there a backoff specified
        /// by the peer? if so obey it"). Flooring it up to `prune_backoff_ticks` would
        /// defeat the unsubscribe backoff. A PRUNE without a backoff (0) falls back to
        /// `prune_backoff_ticks`. `setBackoff` never SHORTENS an already-later expiry
        /// (go `doAddBackoff`), so a peer cannot use this to cut an existing backoff.
        /// Untracked source is ignored.
        ///
        /// Peer exchange (PX): if the PRUNE carries PX peer offers AND peer exchange
        /// is enabled AND `source`'s score clears the accept-PX threshold (go's
        /// `score < acceptPXThreshold` gate; with scoring disabled this is score 0
        /// vs the threshold default, so it depends on the threshold), each offer is
        /// consumed via `consumePxPeers`. A PRUNE with PX from a too-low-scoring
        /// peer has its offers ignored entirely (we still apply the prune+backoff).
        fn handlePrune(router: *Self, source: PeerId, prune: *rpc_pb.ControlPruneReader) void {
            if (!router.peers.contains(peerKey(&source))) return;
            const topic = prune.getTopicID();
            router.meshRemove(topic, source);
            const backoff_secs = prune.getBackoff();
            const ticks = if (backoff_secs > 0) backoff_secs else mesh_params.prune_backoff_ticks;
            router.setBackoff(topic, source, ticks);

            // Peer exchange consume. Gate on the config flag first (default OFF), so
            // a non-PX deployment never touches the offers. Then the accept-PX score
            // gate: ignore offers from a peer we do not trust enough (go's
            // `score < acceptPXThreshold` → "ignoring PX"). When scoring is off
            // there is no engine, so we fall back to score 0 implicitly — but the
            // gate still depends on the threshold (go behaves the same: with no
            // scoring the score is 0 and the default threshold is 0, so PX is
            // accepted; a positive threshold rejects it).
            if (!router.peer_exchange_enabled) return;
            if (prune.peersCount() == 0) return;
            if (router.score) |sc| {
                if (!sc.aboveAcceptPX(source)) return;
            }
            router.consumePxPeers(prune);
        }

        /// Consume the PX peer offers in `prune` (already gated on enable +
        /// accept-PX score). For each offer carrying a signed peer record, verify
        /// the envelope (`consumeEnvelope`), confirm the record's peer-id matches
        /// the offer's `peerID` (an offer cannot vouch for a different peer than the
        /// record proves — go's `rec.PeerID != p` check), store the verified record
        /// (`putRecord`), and fire a fire-and-forget `dial` toward each of the
        /// record's addresses so we connect to the suggested peer. An offer with an
        /// invalid record, a peer-id mismatch, or no record at all is skipped (a
        /// record-less offer is just a peer id — go would consult the DHT for an
        /// address; we have none, so we cannot dial it). At most `prune_peers`
        /// offers are processed (go shuffles + caps to PrunePeers); we cap in
        /// iteration order (a randomised sample is a refinement).
        fn consumePxPeers(router: *Self, prune: *rpc_pb.ControlPruneReader) void {
            var processed: usize = 0;
            while (prune.peersNext()) |info| {
                if (processed >= mesh_params.prune_peers) break;
                processed += 1;

                const record_bytes = info.getSignedPeerRecord();
                if (record_bytes.len == 0) continue; // bare peer id, no address to dial

                // Verify the envelope (signature + key↔peer-id binding). A bad
                // record is dropped; the rest of the offers still process.
                var consumed = peer_record.consumeEnvelope(router.allocator, record_bytes) catch continue;
                defer consumed.deinit(router.allocator);

                // The record must vouch for the SAME peer the offer names; a
                // mismatch means the offer is trying to attach another peer's
                // (validly signed) record to a different id — reject it.
                const offered = PeerId.fromBytes(info.getPeerID()) catch continue;
                if (!consumed.peer_id.eql(&offered)) continue;

                // Keep the verified record (newest-seq wins) so we can vouch for
                // this peer in our own future PX, then dial each advertised address
                // (fire-and-forget — a connect surfaces later as the normal connect
                // event). The signed addresses are BINARY multiaddrs (the libp2p
                // wire form go/rust emit); decode each to the STRING form
                // `transport.dial` expects. An address we cannot decode (an
                // unsupported transport) is skipped; `dial` copies the string, so
                // the temporary is freed immediately after.
                router.putRecord(consumed.peer_id, &consumed, record_bytes);
                for (consumed.addrs) |addr| {
                    var ma = Multiaddr.fromBytes(router.allocator, addr) catch continue;
                    defer ma.deinit(router.allocator);
                    router.transport.dial(router.io, ma.bytes);
                }
            }
        }

        /// Send a PRUNE(topic) to `peer` on its control lane, carrying `backoff_ticks`
        /// as the wire backoff (in seconds, ≈ ticks). The caller passes
        /// `prune_backoff_ticks` for a mesh-maintenance prune and the shorter
        /// `unsubscribe_backoff_ticks` when LEAVING the topic, so the receiver backs
        /// us off for the matching duration.
        ///
        /// When `do_px` is set AND peer exchange is enabled, the PRUNE also carries
        /// up to `prune_peers` OTHER peers in the topic as peer-exchange offers
        /// (each a `PeerInfo{ peerID, signed_peer_record }`, the record present only
        /// if we hold one for that peer) — go's PX-on-PRUNE. Only doPX-eligible
        /// paths pass `do_px = true` (an over-degree heartbeat prune); a LEAVE, a
        /// GRAFT-reject and a negative-score prune pass false so we never leak our
        /// mesh to a peer we are cutting off. `frameRpc` copies the offer bytes
        /// synchronously inside `fanOut`, so the transient offer list is freed here.
        /// Framed once via `fanOut` to the single target.
        fn sendPrune(router: *Self, peer: PeerId, topic: []const u8, backoff_ticks: u64, do_px: bool) void {
            var px: std.ArrayListUnmanaged(?rpc_pb.PeerInfo) = .empty;
            defer px.deinit(router.allocator);
            if (do_px and router.peer_exchange_enabled) router.selectPxPeers(topic, peer, &px);

            const prune = rpc.buildPrune(topic, px.items, backoff_ticks);
            const ctrl = rpc_pb.ControlMessage{ .prune = &.{prune} };
            router.fanOut(.control, (rpc.RpcOut{ .control = ctrl }).toRpc(), .{ .one = peer });
        }

        /// Append up to `prune_peers` peer-exchange offers for `topic` to `out`:
        /// other peers in the topic's mesh, EXCLUDING the peer being pruned and any
        /// peer with a negative score (go's `p != xp && score(xp) >= 0` filter), and
        /// direct peers (never offered). Each offer is a `PeerInfo` borrowing the
        /// peer's id bytes (from the live `PeerState`, valid for this call) and, if
        /// we hold a certified record for the peer, its stored signed-envelope bytes
        /// (otherwise only the peer id — go sends the bare id and lets the receiver
        /// find addresses elsewhere). Iteration order is the mesh-set order;
        /// randomised sampling is a refinement. The borrowed slices stay valid until
        /// `fanOut` returns (it copies them), which is why `out` is freed there.
        fn selectPxPeers(router: *Self, topic: []const u8, pruned: PeerId, out: *std.ArrayListUnmanaged(?rpc_pb.PeerInfo)) void {
            const set = router.mesh.getPtr(topic) orelse return;
            var it = set.keyIterator();
            while (it.next()) |key_ptr| {
                if (out.items.len >= mesh_params.prune_peers) break;
                const state = router.peers.get(key_ptr.*) orelse continue;
                const peer = state.peer;
                if (peer.eql(&pruned)) continue;
                if (router.isDirect(peer)) continue;
                if (router.peerScore(peer) < 0) continue;
                // Borrow the peer-id bytes from the HEAP-stable PeerState (not the
                // stack-local `peer` copy, which dies when this returns) so the
                // slice stays valid until `fanOut`'s `frameRpc` copies it. The
                // record bytes (when present) are owned by the cert store, so they
                // are stable too.
                out.append(router.allocator, .{
                    .peer_i_d = state.peer.bytes[0..state.peer.len],
                    .signed_peer_record = router.getRecord(peer),
                }) catch break;
            }
        }

        /// Send a GRAFT(topic) to `peer` on its control lane (telling it we have
        /// added it to our mesh for the topic). Framed once via `fanOut` to the
        /// single target.
        fn sendGraft(router: *Self, peer: PeerId, topic: []const u8) void {
            const graft = rpc.buildGraft(topic);
            const ctrl = rpc_pb.ControlMessage{ .graft = &.{graft} };
            router.fanOut(.control, (rpc.RpcOut{ .control = ctrl }).toRpc(), .{ .one = peer });
        }

        /// Apply one inbound SUBSCRIBE/UNSUBSCRIBE from `source` to that peer's
        /// announced-topics set. Untracked source → ignored. Subscribe inserts an
        /// owned key copy (no-op if already present); unsubscribe removes + frees
        /// the stored key.
        fn applyPeerSubscription(router: *Self, source: PeerId, topic: []const u8, subscribe: bool) void {
            if (router.gs_debug) {
                std.log.info("GS_DEBUG applyPeerSubscription topic={s} subscribe={} tracked={}", .{ topic, subscribe, router.peers.contains(peerKey(&source)) });
            }
            const state = router.peers.get(peerKey(&source)) orelse {
                // The peer's SUBSCRIBE arrived before its `peer_connected` (the
                // inbound-stream-vs-peer-event race). Stash it so `peer_connected`
                // applies it once the PeerState exists, rather than dropping it —
                // it is never re-sent, so a drop permanently loses the peer's
                // subscription. Mirrors the `peer_versions` stash.
                router.stashPendingSubscription(source, topic, subscribe);
                return;
            };
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

        /// Record a subscription change from a not-yet-tracked peer (see
        /// `pending_subscriptions`): `subscribe` adds the topic to the peer's
        /// pending set, unsubscribe removes it, so a sub-then-unsub before connect
        /// nets out. Best-effort — a failed alloc just drops that one note (no leak,
        /// no corruption; the peer simply isn't recorded for that topic, the same
        /// outcome as before this stash existed).
        fn stashPendingSubscription(router: *Self, source: PeerId, topic: []const u8, subscribe: bool) void {
            const key = peerKey(&source);
            // Cap the stash map: a SUBSCRIBE that trails its peer's disconnect
            // (the disconnect rides the priority control inbox, so it can
            // overtake queued data from the same peer) creates an entry no
            // `peer_connected` will ever adopt and no disconnect will sweep
            // again. The cap bounds that garbage — and adversarial
            // connect/SUBSCRIBE/disconnect churn — at a fixed size; dropping a
            // stash for a genuinely-pending peer is the pre-existing
            // best-effort behaviour (the peer re-announces on its next
            // subscription change or reconnect).
            if (!router.pending_subscriptions.contains(key) and
                router.pending_subscriptions.count() >= max_pending_subscription_peers)
            {
                return;
            }
            const gop = router.pending_subscriptions.getOrPut(key) catch return;
            if (!gop.found_existing) gop.value_ptr.* = .empty;
            if (subscribe) {
                if (gop.value_ptr.contains(topic)) return;
                const tkey = router.allocator.dupe(u8, topic) catch return;
                gop.value_ptr.put(router.allocator, tkey, {}) catch router.allocator.free(tkey);
            } else if (gop.value_ptr.fetchRemove(topic)) |kv| {
                router.allocator.free(kv.key);
            }
        }

        /// Compute a message's id under the active policy. The one place both the
        /// publish and the receive paths derive an id, so the two always agree:
        ///   - a configured `message_id_fn` wins (apps override id derivation),
        ///   - else under `anonymous` the id is content-derived
        ///     (`sha256(topic ++ data)`), because an anonymous message has no
        ///     `from`/`seqno` to key on (they would be empty and collide),
        ///   - else (strict_sign / none) the id is the default `from ++ seqno`.
        /// The caller owns the returned MessageId and frees it.
        fn computeMessageId(
            router: *Self,
            topic: []const u8,
            from: []const u8,
            seqno: []const u8,
            data: []const u8,
        ) !rpc.MessageId {
            if (router.message_id_fn) |cfg| {
                return cfg.func(cfg.ctx, topic, from, seqno, data, router.allocator);
            }
            if (router.signature_policy == .anonymous) {
                return rpc.contentMessageId(router.allocator, topic, data);
            }
            return rpc.messageId(router.allocator, from, seqno);
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
            // The id is policy-derived: content-based under anonymous (where the
            // message has no from/seqno), from++seqno otherwise. The receive and
            // publish paths both go through computeMessageId so they always agree.
            // Computed FIRST — before any crypto — so a duplicate can be dropped
            // without paying signature verification (go checks seen in shouldPush
            // before pushing to its validation workers; a gossipsub mesh delivers
            // each message up to D times, so for sub-IDONTWANT-threshold messages
            // this is the difference between 1 verify and ~D verifies per id).
            var id = router.computeMessageId(topic, from, seqno, data) catch return;
            defer id.deinit(router.allocator);

            // IWANT-promise fulfilment follows go's gossip tracer exactly: a
            // promise is fulfilled by a duplicate delivery (DuplicateMessage), a
            // throttle-dropped delivery (RejectValidationThrottled is NOT in the
            // tracer's carve-out), and any post-signature verdict — but NOT by a
            // message whose SIGNATURE fails (go's RejectMessage explicitly skips
            // fulfilment for RejectInvalidSignature/RejectMissingSignature, so
            // the promise stays pending, expires, and draws the P7 broken-promise
            // penalty: a peer cannot make good on an IHAVE with garbage). So the
            // fulfilment sites are: the duplicate return + the throttle-drop
            // return + the inline post-verify point below, and the verdict
            // re-entry for the async path (`onValidationResult`) — never before
            // verification.

            if (router.seen.contains(id.bytes, router.heartbeat_tick)) {
                if (router.score != null) router.fulfillPromise(id.bytes);
                // A duplicate of an already-seen message. If the relaying peer is
                // a current mesh member for this topic, credit it the P3
                // mesh-delivery counter (it did relay the message to us, just not
                // first). The engine itself no-ops for a non-mesh peer.
                if (router.score) |sc| {
                    if (router.meshContains(topic, exclude)) sc.duplicateMessage(exclude, topic);
                }
                return;
            }

            // ASYNC pipeline (go-libp2p's validation-worker model): with
            // `validation_concurrency > 0`, BOTH the StrictSign signature check
            // and the app validator run OFF the router fiber — go's validate()
            // does exactly this (signature first, then user validators) on its
            // NumCPU worker pool, and its processLoop never touches crypto. We
            // HOLD an owned copy of the message (the wire slices here are freed
            // when `onInboundRpc` returns), spawn a validation fiber, and DEFER
            // every effect — including the seen MARK — to its `validation_result`
            // command (see onValidationResult for why seen is marked there).
            //
            // At the in-flight cap the message is throttle-DROPPED, exactly like
            // go ("message validation throttled: queue full; dropping"): the
            // inline fallback we used to run here let a validation flood stall
            // the router fiber — the very thing the offload exists to prevent.
            // The drop neither marks seen (a later copy can still be validated)
            // nor penalizes the sender (the message was never judged).
            const needs_signature_check = router.signer != null;
            if ((needs_signature_check or router.validator != null) and
                router.validation_concurrency > 0)
            {
                if (router.validations_in_flight >= router.validation_concurrency) {
                    // The peer did deliver — a throttled drop still fulfils its
                    // IWANT promise (go: RejectValidationThrottled fulfils).
                    if (router.score != null) router.fulfillPromise(id.bytes);
                    if (router.gs_debug) {
                        std.log.debug("gossipsub: validation throttled (in-flight {d} >= cap {d}); dropping message", .{ router.validations_in_flight, router.validation_concurrency });
                    }
                    return;
                }
                if (router.spawnValidation(exclude, from, seqno, topic, data, signature, key, id.bytes)) {
                    // Spawned: verdict + ALL effects come via the result command.
                    return;
                }
                // Building the owned snapshot / spawning failed (OOM — not
                // attacker-drivable load, which the cap already bounds): degrade
                // to the inline path below rather than dropping outright.
            }

            // INLINE path (`validation_concurrency == 0`, or the OOM fallback).
            //
            // StrictSign: reject (drop — no deliver, no cache, no forward) any
            // message whose signature does not verify against the key carried on
            // the wire AND whose `from` is not that key's peer-id. An unsigned
            // message (empty signature/key) also fails verification, so it is
            // dropped. Under the none policy we accept everything and the
            // message's signature/key — if any — pass through unchanged on
            // forward. Runs AFTER the seen check (above): duplicates never pay
            // verification. P4: the SENDING peer (the inbound stream's peer, not
            // the message's `from`) relayed the invalid message and is charged
            // the squared invalid-delivery penalty. The id is deliberately NOT
            // marked seen on a bad signature — a forged (from, seqno) must not
            // be able to censor the real message (go returns before markSeen).
            if (needs_signature_check) {
                if (!signing.verifyMessage(router.allocator, from, seqno, topic, data, signature, key)) {
                    // Deliberately NO fulfillPromise here: a garbage-signed
                    // message must not make good on an IHAVE promise (P7).
                    if (router.score) |sc| sc.rejectMessage(exclude, topic);
                    return;
                }
            }

            // Past the signature gate: the delivery now counts for any pending
            // IWANT promise regardless of the app validator's verdict (go
            // fulfils on deliver, duplicate, AND validator-reject/ignore).
            if (router.score != null) router.fulfillPromise(id.bytes);

            // Marked seen only now — after the signature verified (go: "we can
            // mark the message as seen now that we have verified the signature
            // and avoid invoking user validators more than once") and before the
            // app validator, so a duplicate of an ignored/rejected message is
            // suppressed by the seen-cache and never re-validated or re-forwarded.
            router.seen.add(id.bytes, router.heartbeat_tick);

            // Application topic-message validator (go-libp2p-pubsub's
            // validate-then-forward gate / rust-libp2p's MessageAcceptance). The
            // message is new and signature-checked; the app's verdict decides
            // whether we propagate it.
            //   - reject: the message is invalid; do NOT deliver/forward, and charge
            //     the relaying peer the squared invalid-delivery penalty (P4), the
            //     same penalty a bad signature draws. Keyed on the SENDING peer
            //     (`exclude`, the inbound stream's peer), not the message's `from`.
            //   - ignore: do NOT deliver/forward, but do NOT penalize the sender.
            //   - accept: fall through to the normal P2/deliver/IDONTWANT/forward
            //     path below (the historic behaviour).
            // No validator (null) means accept-all, so the behaviour is unchanged.
            if (router.validator) |v| {
                switch (v.validate(v.ctx, topic, from, data)) {
                    .accept => {},
                    .reject => {
                        if (router.score) |sc| sc.rejectMessage(exclude, topic);
                        return;
                    },
                    .ignore => return,
                }
            }

            router.applyAccept(exclude, from, seqno, topic, data, signature, key, id.bytes);
        }

        /// Apply the ACCEPT effects of a NEW, signature-checked, accepted message:
        /// the P2/P3 first-/mesh-delivery score credit, local delivery, the
        /// IDONTWANT-on-large broadcast, and the cache + mesh forward. Shared by the
        /// inline accept path (`handleIncomingMessage`) and the async accept path
        /// (`onValidationResult`) so both forward through IDENTICAL logic — the one
        /// place an accepted received message is delivered + propagated. The
        /// signature/key are the raw wire slices (empty when absent under the
        /// none/anonymous policy); an empty slice maps to a null Message field on
        /// forward (the field is absent), keeping relayed copies byte-identical.
        fn applyAccept(
            router: *Self,
            exclude: PeerId,
            from: []const u8,
            seqno: []const u8,
            topic: []const u8,
            data: []const u8,
            signature: []const u8,
            key: []const u8,
            id: []const u8,
        ) void {
            // P2/P3: the relaying peer delivered a NEW, accepted message — credit
            // its first-delivery (and, if it is a mesh member, mesh-delivery)
            // counters. Keyed on the SENDING peer, not the message's `from`.
            if (router.score) |sc| sc.deliverMessage(exclude, topic);

            router.deliverLocal(topic, from, data);

            // For a large NEW message, tell our v1.2 mesh peers (except the sender)
            // that we already have it via an IDONTWANT, so they skip forwarding us
            // a redundant copy — the bandwidth saving this control message exists
            // for. Done only on RECEIVED messages (we are not the origin) and only
            // when the data is at or above the configured threshold.
            if (data.len >= router.idontwant_message_threshold) {
                router.broadcastIDontWant(topic, id, exclude);
            }

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
                id,
            );
        }

        /// Build an owned `ValidationContext` from a received message and spawn a
        /// validation fiber that runs the validator off the router fiber and posts
        /// the verdict back as a `validation_result` command. Returns true on
        /// success (the held context is now owned by the fiber → the result command
        /// → freed when the command is applied or drained) and false if the owned
        /// copy or the fiber spawn failed (NOTHING is held; the caller validates
        /// inline instead). Called only on the router fiber, so the in-flight
        /// counter increment is race-free; it is decremented when the result is
        /// applied (`onValidationResult`) or drained at teardown.
        fn spawnValidation(
            router: *Self,
            exclude: PeerId,
            from: []const u8,
            seqno: []const u8,
            topic: []const u8,
            data: []const u8,
            signature: []const u8,
            key: []const u8,
            id: []const u8,
        ) bool {
            const ctx = router.allocator.create(ValidationContext) catch return false;
            // On any partial-copy failure free what we have and report failure so the
            // caller falls back to inline. Each field is dup'd in turn; an errdefer
            // would not fire on the `catch` paths, so unwind explicitly.
            const from_owned = router.allocator.dupe(u8, from) catch {
                router.allocator.destroy(ctx);
                return false;
            };
            const seqno_owned = router.allocator.dupe(u8, seqno) catch {
                router.allocator.free(from_owned);
                router.allocator.destroy(ctx);
                return false;
            };
            const topic_owned = router.allocator.dupe(u8, topic) catch {
                router.allocator.free(seqno_owned);
                router.allocator.free(from_owned);
                router.allocator.destroy(ctx);
                return false;
            };
            const data_owned = router.allocator.dupe(u8, data) catch {
                router.allocator.free(topic_owned);
                router.allocator.free(seqno_owned);
                router.allocator.free(from_owned);
                router.allocator.destroy(ctx);
                return false;
            };
            const sig_owned = router.allocator.dupe(u8, signature) catch {
                router.allocator.free(data_owned);
                router.allocator.free(topic_owned);
                router.allocator.free(seqno_owned);
                router.allocator.free(from_owned);
                router.allocator.destroy(ctx);
                return false;
            };
            const key_owned = router.allocator.dupe(u8, key) catch {
                router.allocator.free(sig_owned);
                router.allocator.free(data_owned);
                router.allocator.free(topic_owned);
                router.allocator.free(seqno_owned);
                router.allocator.free(from_owned);
                router.allocator.destroy(ctx);
                return false;
            };
            const id_owned = router.allocator.dupe(u8, id) catch {
                router.allocator.free(key_owned);
                router.allocator.free(sig_owned);
                router.allocator.free(data_owned);
                router.allocator.free(topic_owned);
                router.allocator.free(seqno_owned);
                router.allocator.free(from_owned);
                router.allocator.destroy(ctx);
                return false;
            };
            ctx.* = .{
                .exclude = exclude,
                .from = from_owned,
                .seqno = seqno_owned,
                .topic = topic_owned,
                .data = data_owned,
                .signature = sig_owned,
                .key = key_owned,
                .id = id_owned,
            };

            // Spawn into the router-owned group (cancelled + joined in `destroy`).
            // On spawn failure free the context and report failure (inline fallback).
            router.validation_group.concurrent(router.io, validationFiber, .{ router, ctx }) catch {
                ctx.deinit(router.allocator);
                return false;
            };
            router.validations_in_flight += 1;
            return true;
        }

        /// Validation-fiber body (one per async-validated message; runs OFF the
        /// router fiber in `validation_group`). Runs go's worker pipeline on the
        /// held copy — the StrictSign SIGNATURE check first, then the app
        /// validator (go's validate() order; either may be absent) — and posts
        /// the combined outcome + the context back to the router inbox as a
        /// `validation_result` command. `router.signer` and `router.validator`
        /// are set once in `create` and never reassigned, so reading them off
        /// the router fiber is safe; `verifyMessage`'s scratch allocation uses
        /// the same thread-safe-allocator contract as `ctx.deinit` below. The
        /// crypto/validator calls are CPU-bound (no cancellation point); the
        /// inbox post IS a cancellation point, so a teardown `cancel` collapses
        /// it — on a closed/cancelled post we free the held context HERE (this
        /// fiber is then its sole owner). On a successful post the router (or
        /// the inbox drain) owns + frees the context: freed EXACTLY once.
        /// The validator must be thread-safe (it can run on this fiber
        /// concurrently with the router fiber).
        fn validationFiber(router: *Self, ctx: *ValidationContext) void {
            const outcome: ValidationOutcome = blk: {
                if (router.signer != null and
                    !signing.verifyMessage(router.allocator, ctx.from, ctx.seqno, ctx.topic, ctx.data, ctx.signature, ctx.key))
                {
                    break :blk .reject_signature;
                }
                if (router.validator) |v| {
                    break :blk switch (v.validate(v.ctx, ctx.topic, ctx.from, ctx.data)) {
                        .accept => .accept,
                        .reject => .reject_validator,
                        .ignore => .ignore,
                    };
                }
                // Signature-only offload (no app validator configured).
                break :blk .accept;
            };
            router.control_inbox.putOne(router.io, .{ .validation_result = .{ .ctx = ctx, .verdict = outcome } }) catch {
                // Closed (shutting down) or Canceled (teardown): no one will process
                // the result, so this fiber frees the held context.
                ctx.deinit(router.allocator);
                return;
            };
            router.notifyControl();
        }

        /// Apply an async validation outcome on the router fiber (the deferred
        /// tail of `handleIncomingMessage`, now that the off-fiber signature
        /// check + validator have returned). This is also where the seen MARK
        /// happens for the async path: `seen` (and the intern table under it)
        /// is router-fiber-confined, so the worker cannot mark it — go marks
        /// seen inside its worker because its timecache is locked; ours moves
        /// the mark to the verdict re-entry instead. Consequences, both matching
        /// go's semantics:
        ///   - A bad-signature message is NEVER marked (go returns before
        ///     markSeen): a forged (from, seqno) cannot censor the real message.
        ///   - Two copies of one id can both be IN FLIGHT (both passed the seen
        ///     CHECK before either was MARKED — go has the same window between
        ///     shouldPush and the worker's markSeen). Verdicts apply serially
        ///     here: the first marks seen and applies; a later one finds the id
        ///     seen and is handled as a duplicate (go: markSeen=false →
        ///     DuplicateMessage), crediting the relayer's P3 like any duplicate.
        /// ACCEPT runs the SHARED accept effects (`applyAccept`) — identical to
        /// the inline accept path; a validator REJECT charges the relaying peer
        /// the P4 penalty (and IS marked seen, so later copies never
        /// re-validate); IGNORE just marks seen. Always decrements the in-flight
        /// counter and frees the held context (freed exactly once — the teardown
        /// drain frees only contexts that never reach here).
        fn onValidationResult(router: *Self, ctx: *ValidationContext, verdict: ValidationOutcome) void {
            defer {
                router.validations_in_flight -= 1;
                ctx.deinit(router.allocator);
            }
            if (verdict == .reject_signature) {
                // Deliberately NO fulfillPromise: a garbage-signed message must
                // not make good on an IHAVE promise — the promise expires and
                // draws P7 (go's tracer carve-out for RejectInvalidSignature).
                if (router.score) |sc| sc.rejectMessage(ctx.exclude, ctx.topic);
                return;
            }
            // Past the signature gate: this delivery fulfils any pending IWANT
            // promise whatever the verdict (accept / validator-reject / ignore /
            // duplicate — go fulfils on all four).
            if (router.score != null) router.fulfillPromise(ctx.id);
            if (router.seen.contains(ctx.id, router.heartbeat_tick)) {
                if (router.score) |sc| {
                    if (router.meshContains(ctx.topic, ctx.exclude)) sc.duplicateMessage(ctx.exclude, ctx.topic);
                }
                return;
            }
            router.seen.add(ctx.id, router.heartbeat_tick);
            switch (verdict) {
                .accept => router.applyAccept(ctx.exclude, ctx.from, ctx.seqno, ctx.topic, ctx.data, ctx.signature, ctx.key, ctx.id),
                .reject_validator => if (router.score) |sc| sc.rejectMessage(ctx.exclude, ctx.topic),
                .ignore => {},
                .reject_signature => unreachable, // handled above, before the seen mark
            }
        }

        /// Invoke the message handler if the local node subscribes to `topic`.
        /// The slices are valid only for the call (the handler copies to retain).
        fn deliverLocal(router: *Self, topic: []const u8, from: []const u8, data: []const u8) void {
            if (!router.my_topics.contains(topic)) return;
            const h = router.message_handler orelse return;
            if (router.delivery_storage.len == 0) {
                // Inline mode (opt-in): zero-copy, ON the router fiber. The
                // handler must be cheap and must not post router commands (see
                // RouterConfig.delivery_queue_len).
                h.on_message(h.ctx, topic, from, data);
                return;
            }
            // Queued mode: copy topic++from++data into one allocation and hand
            // it to the delivery fiber. Non-blocking put — a full queue means
            // the local subscriber lagged; drop the message for it (go's
            // bounded subscriber-channel semantics; forwarding is unaffected).
            const bytes = router.allocator.alloc(u8, topic.len + from.len + data.len) catch {
                router.delivery_drops += 1;
                return;
            };
            @memcpy(bytes[0..topic.len], topic);
            @memcpy(bytes[topic.len .. topic.len + from.len], from);
            @memcpy(bytes[topic.len + from.len ..], data);
            const item = Delivery{ .message = .{ .bytes = bytes, .topic_len = topic.len, .from_len = from.len } };
            const queued = router.delivery_queue.putUncancelable(router.io, &.{item}, 0) catch 0;
            if (queued == 0) {
                router.allocator.free(bytes);
                router.delivery_drops += 1;
            }
        }

        /// Broadcast an IDONTWANT(`id`) on the control lane to every member of
        /// `topic`'s mesh that negotiated v1.2 and is NOT `exclude` (the peer the
        /// message arrived from). Tells those peers we already hold the message so
        /// they skip forwarding it to us. No-op if the topic has no mesh.
        ///
        /// v1.2 gating: a pre-1.2 peer would not understand (and would reject) the
        /// control message, so it is wasteful to send. Honouring an inbound
        /// IDONTWANT is unconditional (see `handleIDontWant`); only EMITTING one is
        /// gated. Each eligible peer is targeted individually so the gate applies
        /// per peer. The id is borrowed (valid for the call) and `frameRpc` copies
        /// it into each frame.
        fn broadcastIDontWant(router: *Self, topic: []const u8, id: []const u8, exclude: PeerId) void {
            const set = router.mesh.getPtr(topic) orelse return;
            var it = set.keyIterator();
            while (it.next()) |key_ptr| {
                const state = router.peers.get(key_ptr.*) orelse continue;
                if (state.peer.eql(&exclude)) continue;
                if (!router.peerSupportsV12(state.peer)) continue;
                router.sendIDontWant(state.peer, id);
            }
        }

        /// Send an IDONTWANT(`id`) to `peer` on its control lane. The id is wrapped
        /// in a one-element optional array for the builder (which `frameRpc`
        /// copies), and the frame carries no message ids of its own (control frames
        /// are never IDONTWANT-purged).
        fn sendIDontWant(router: *Self, peer: PeerId, id: []const u8) void {
            const idontwant = rpc.buildIDontWant(&[_]?[]const u8{id});
            const ctrl = rpc_pb.ControlMessage{ .idontwant = &.{idontwant} };
            router.fanOut(.control, (rpc.RpcOut{ .control = ctrl }).toRpc(), .{ .one = peer });
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
            // The data frame carries one PRIVATE copy of the message id (for a
            // later IDONTWANT purge). `id` is interned, and an interned id's
            // LAST release unlinks it from the unsynchronized, router-fiber-
            // owned intern table — but a frame's last reference is usually
            // dropped on a writer fiber (the refcount is atomic; the table is
            // not), so a frame must never hold an interned reference. The one
            // small dupe per accepted message is the price of that confinement.
            // Freed on any pre-frame failure.
            const id_copy = router.allocator.dupe(u8, id) catch return;
            // The cached/forwarded frame carries whatever signature/key the
            // message was published or relayed with (null under the none policy),
            // so IWANT-served and mesh-forwarded copies stay byte-identical and
            // keep the original publisher's signature.
            const msg = rpc_pb.Message{ .from = from, .seqno = seqno, .topic = topic, .data = data, .signature = signature, .key = key };
            const framed = pubsub.frameRpc(router.allocator, (rpc.RpcOut{ .publish = &.{msg} }).toRpc()) catch {
                router.allocator.free(id_copy);
                return;
            };
            const frame = peer_io.OutboundFrame.create(router.allocator, framed, id_copy, 1) catch {
                router.allocator.free(framed);
                router.allocator.free(id_copy);
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

            // Forward over the mesh/fanout/flood set PLUS every connected direct
            // peer subscribed to the topic. Direct peers are out-of-mesh trusted
            // targets that go-libp2p adds to the send set alongside the mesh, so a
            // valid message always reaches them. Build a transient UNION of the
            // base set and those direct peers (a set, so a peer that is both a mesh
            // member and direct is targeted exactly once), then fan out over it.
            // When no direct peers are configured (the common case) this is a no-op
            // and we forward over the base set directly, unchanged.
            if (router.directSubscribers(topic, exclude)) |direct_targets| {
                var combined = direct_targets;
                defer combined.deinit(router.allocator);
                if (set) |s| {
                    var it = s.keyIterator();
                    while (it.next()) |key_ptr| combined.put(router.allocator, key_ptr.*, {}) catch {};
                }
                router.fanOutFrame(.data, frame, .{ .peer_set = .{ .set = &combined, .exclude = exclude } });
            } else if (set) |s| {
                router.fanOutFrame(.data, frame, .{ .peer_set = .{ .set = s, .exclude = exclude } });
            }
        }

        /// Build a transient PeerSet of every connected direct peer subscribed to
        /// `topic`, excluding `exclude` (the relay source — never echo a message
        /// back to where it came from). Returns null when no direct peer qualifies
        /// (no direct peers configured, none connected, or none subscribed) so the
        /// common no-direct-peers path stays allocation-free. The caller OWNS the
        /// returned set and must `deinit` it; it only TARGETS the fan-out (the
        /// frame's references live on the per-peer queues), so freeing it touches no
        /// frame. Mirrors go-libp2p, which forwards to a direct peer only for topics
        /// it has SUBSCRIBEd to (the `tmap`/`inTopic` check in Publish).
        fn directSubscribers(router: *Self, topic: []const u8, exclude: ?PeerId) ?PeerSet {
            if (router.direct.count() == 0) return null;
            var set: PeerSet = .empty;
            var it = router.direct.keyIterator();
            while (it.next()) |key_ptr| {
                const state = router.peers.get(key_ptr.*) orelse continue;
                if (exclude) |ex| if (state.peer.eql(&ex)) continue;
                if (!state.topics.contains(topic)) continue;
                set.put(router.allocator, key_ptr.*, {}) catch {};
            }
            if (set.count() == 0) {
                set.deinit(router.allocator);
                return null;
            }
            return set;
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
            if (router.gs_debug) {
                std.log.info("GS_DEBUG onSubscribe topic={s} announcing_to_peers={d}", .{ topic, router.peers.count() });
            }
            router.announceSubscription(topic, true);

            // Seed the mesh from any existing fanout peers (we were publishing to
            // this topic without subscribing), grafting each, then drop the fanout.
            if (router.fanout.getPtr(topic)) |fanout_set| {
                var it = fanout_set.keyIterator();
                while (it.next()) |key_ptr| {
                    if (router.meshSize(topic) >= mesh_params.d) break;
                    const state = router.peers.get(key_ptr.*) orelse continue;
                    const peer = state.peer;
                    // A direct peer is never placed in the mesh, even when seeding
                    // from a fanout set (which already excludes direct peers, so
                    // this is a belt-and-braces guard on the mesh-add invariant).
                    if (router.isDirect(peer)) continue;
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
                    // Leaving the topic uses the shorter unsubscribe backoff for both
                    // our local backoff and the wire PRUNE, so if we rejoin soon we
                    // (and the peer) can re-graft each other sooner than a normal prune.
                    router.setBackoff(topic, state.peer, mesh_params.unsubscribe_backoff_ticks);
                    // LEAVE (unsubscribe): NO PX — we are abandoning the topic, so
                    // we do not advertise its mesh to the peers we are pruning.
                    router.sendPrune(state.peer, topic, mesh_params.unsubscribe_backoff_ticks, false);
                }
            }
            set.deinit(router.allocator);
            router.allocator.free(kv.key);
        }

        /// Announce a single (un)subscription to every peer's `.subscribe` lane:
        /// framed once and fanned out (see `fanOut`).
        fn announceSubscription(router: *Self, topic: []const u8, subscribe: bool) void {
            const sub = rpc.buildSubscription(topic, subscribe);
            router.fanOut(.subscribe, (rpc.RpcOut{ .subscriptions = &.{sub} }).toRpc(), .all);
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

            // Anonymous (StrictNoSign) publishes carry ONLY topic+data — no
            // `from`/`seqno`/`signature`/`key`, so the publisher's peer-id never
            // reaches the wire (the privacy guarantee) and the seqno counter is
            // not advanced. Under strict_sign / none the message carries our
            // peer-id as `from` and the next seqno. Empty `from`/`seqno` encode to
            // absent fields (the protobuf encoder omits empty bytes), so the
            // anonymous frame is just topic+data.
            const anonymous = router.signature_policy == .anonymous;

            var seqno_buf: [8]u8 = undefined;
            var from: []const u8 = &.{};
            var seqno: []const u8 = &.{};
            if (!anonymous) {
                from = router.local_peer.bytes[0..router.local_peer.len];
                std.mem.writeInt(u64, &seqno_buf, router.seqno, .big);
                router.seqno += 1;
                seqno = seqno_buf[0..];
            }

            // Under StrictSign, sign the message and attach the signature + the
            // marshaled libp2p public key. A signing failure drops the publish (we
            // must not emit an unsigned message a StrictSign peer would reject).
            // Under none and anonymous both stay null. `sig` is owned here and
            // freed after framing (cacheAndForward copies the bytes into the frame).
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

            // Policy-derived id: content-based (sha256(topic++data)) under
            // anonymous (from/seqno are empty), from++seqno otherwise. The receive
            // path uses the same helper so the two always agree.
            var id = router.computeMessageId(topic, from, seqno, data) catch return;
            defer id.deinit(router.allocator);
            // Suppress a re-publish of a message already in the seen window. Under
            // strict_sign / none each publish carries a fresh seqno, so its id is
            // always new and this never triggers; under anonymous (or any
            // content-based id) two identical (topic, data) publishes share an id,
            // so the second is dropped here rather than forwarded twice — matching
            // go-libp2p, which checks the seen-cache before publishing.
            if (router.seen.contains(id.bytes, router.heartbeat_tick)) return;
            router.seen.add(id.bytes, router.heartbeat_tick);

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
                if (router.gs_debug) {
                    std.log.info("GS_DEBUG onPublish topic={s} peers={d} flood_targets={d} mesh={d}", .{ topic, router.peers.count(), flood_set.count(), router.meshSize(topic) });
                }
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
            router.freePeerDontSend(state);
            router.freePeerPromises(state);
            router.clearIWantCounts(state);
            state.iwant_counts.deinit(router.allocator);
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

        /// Release every interned reference in a peer's IDONTWANT (`dont_send`) map
        /// and deinit the map. Called on teardown so the shared id references this
        /// peer held are dropped (the id bytes free only if this was the last
        /// holder across all maps).
        fn freePeerDontSend(router: *Self, state: *PeerState) void {
            var it = state.dont_send.valueIterator();
            while (it.next()) |entry| entry.rc.release();
            state.dont_send.deinit(router.allocator);
        }

        /// Release every interned reference in a peer's outstanding IWANT-promise
        /// map and deinit the map. Called on teardown so the shared id references
        /// for promises not yet fulfilled or harvested are dropped.
        fn freePeerPromises(router: *Self, state: *PeerState) void {
            var it = state.iwant_promises.valueIterator();
            while (it.next()) |entry| entry.rc.release();
            state.iwant_promises.deinit(router.allocator);
        }

        /// Release every interned reference in a peer's IWANT retransmission-count
        /// map and empty the map (retaining its capacity). Called each heartbeat to
        /// age the counts out with the gossip window, and on teardown (followed
        /// there by a `deinit`). Bounds the map's lifetime to one heartbeat window.
        fn clearIWantCounts(router: *Self, state: *PeerState) void {
            _ = router; // releasing interned ids needs no allocator (kept a method
            // for call-site symmetry with the other per-peer free helpers).
            var it = state.iwant_counts.valueIterator();
            while (it.next()) |entry| entry.rc.release();
            state.iwant_counts.clearRetainingCapacity();
        }

        fn teardownAllPeers(router: *Self) void {
            var it = router.peers.iterator();
            while (it.next()) |entry| {
                router.teardownPeer(entry.value_ptr.*);
                _ = router.peer_count.fetchSub(1, .release);
            }
            router.peers.clearRetainingCapacity();
        }

        /// Drain and free any commands still buffered in BOTH inboxes after the
        /// loop exits, so an inbound RPC (data) or validation result / peer
        /// record (control) posted concurrently with teardown is not leaked.
        fn drainInbox(router: *Self) void {
            router.control_inbox.close(router.io);
            router.inbox.close(router.io);
            router.drainOneQueue(&router.control_inbox);
            router.drainOneQueue(&router.inbox);
        }

        fn drainOneQueue(router: *Self, queue: *std.Io.Queue(Command)) void {
            var buf: [16]Command = undefined;
            while (true) {
                const n = queue.getUncancelable(router.io, &buf, 0) catch return;
                if (n == 0) return;
                for (buf[0..n]) |command| router.freeCommand(command);
            }
        }

        /// Release whatever an UNDISPATCHED command owns (and wake any waiter
        /// embedded in it). Used by the teardown drain, and by drainControl for
        /// commands batched out of the queue BEHIND a shutdown — once a command
        /// has been dequeued, the drain can never see it again, so its owner of
        /// last resort is whoever holds the batch buffer.
        fn freeCommand(router: *Self, command: Command) void {
            switch (command) {
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
                .peer_record => |c| router.allocator.free(c.envelope_bytes),
                .enqueue_for_test => |e| {
                    // The peer map is (or is about to be) torn down; release the
                    // frame reference and wake any waiter so a test post in
                    // flight at shutdown neither leaks nor hangs.
                    e.frame.release();
                    e.reply.set(router.io);
                },
                // Wake a sync barrier in flight at shutdown so its awaiter
                // does not hang.
                .sync => |s| s.reply.set(router.io),
                // An async validation that posted its result before the inbox
                // closed but never got processed by the main loop: free its held
                // context here (the fiber that produced it handed off ownership on
                // the successful post, so the drain is the sole remaining owner).
                // Freed exactly once — a processed result is freed in
                // `onValidationResult`, a fiber whose post FAILS frees its own
                // context, and the shutdown-batch path in drainControl frees
                // exactly the commands it stranded. `destroy` cancels + joins the
                // validation group AFTER the teardown drain, so no fiber posts a
                // new result into the (now-closed) inbox past this point.
                .validation_result => |r| r.ctx.deinit(router.allocator),
                else => {},
            }
        }

        /// PeerWriter on_disconnect callback. The writer exhausted its open
        /// retries, so hand the peer back to the router. Runs on the WRITER
        /// fiber, so it must only signal — never free the state/sink (the
        /// writer's trailing `sink.close` still fires after this returns) —
        /// and the signal must satisfy TWO constraints:
        ///
        /// - It MUST NOT block: the router cancel+awaits this fiber in
        ///   teardownPeer, so parking (uncancelably) on the router's own full
        ///   inbox would wedge the router forever — it would join a fiber that
        ///   waits for the router to drain the very queue it is parked on.
        /// - It MUST NOT carry the connection handle (or any identity): a
        ///   writer-sourced event cannot be ordered before the connection's
        ///   free, so a carried pointer can alias a RECYCLED address — a stale
        ///   `peer_disconnected{peer, old_ptr}` processed after the peer
        ///   rebound to a new connection at the same address would pass the
        ///   identity check and destroy the live peer (ABA). This also fires
        ///   spuriously when teardownPeer cancels a writer parked in its
        ///   reopen backoff, which is exactly when the old handle is dangling.
        ///
        /// So: set the atomic flag on the PeerState (the lossless signal, tied
        /// to current identity by construction) and post a payload-free
        /// `reap_dead_writers` wake, non-blocking (`min = 0` never parks; a
        /// full inbox drops the wake and the heartbeat's reap pass covers it).
        fn onWriterDisconnect(ctx: ?*anyopaque) void {
            const state: *PeerState = @ptrCast(@alignCast(ctx.?));
            const router = state.router_for_disconnect;
            state.writer_dead.store(true, .release);
            _ = router.control_inbox.putUncancelable(router.io, &.{.reap_dead_writers}, 0) catch 0;
            router.notifyControl();
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
    /// Number of leading writeFrame() calls that should fail. `maxInt` = every
    /// write fails, driving the writer's consecutive-write-failure give-up.
    fail_write_count: usize = 0,

    pub fn open(self: *FakeSink, io: std.Io) anyerror!void {
        self.record.open_calls += 1;
        if (self.record.open_calls <= self.fail_open_count) return error.OpenFailed;
        // While the test holds `block_open`, park here (short poll) so the writer
        // does not drain the queue and a test can observe queued frames. The test
        // CLEARS the flag before teardown, so the writer exits this loop on its
        // own; the short sleep is also a cancellation point but the flag, not
        // cancel, is what releases it (cancel-driven teardown of a long sleep is
        // fragile, so this never depends on it).
        while (self.record.block_open.load(.acquire)) {
            io_time.ms(5).sleep(io) catch break;
        }
        self.record.streams_opened += 1;
    }

    pub fn writeFrame(self: *FakeSink, io: std.Io, bytes: []const u8) anyerror!void {
        // The append runs on the peer's writer fiber (a std.Io.Threaded executor
        // thread), while the test fiber reads `record.written` through the
        // recordCount*/recordHas* helpers. Both take `record.mutex` so the append
        // never races a read of the ArrayList's buffer/len.
        self.record.mutex.lockUncancelable(io);
        defer self.record.mutex.unlock(io);
        self.record.write_calls += 1;
        if (self.record.write_calls <= self.fail_write_count) return error.WriteFailed;
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
    write_calls: usize = 0,
    written: std.ArrayList(u8) = .empty,
    /// Guards `written` against the writer-fiber append vs. test-fiber read race.
    /// The peer's writer fiber appends under this lock in FakeSink.writeFrame; the
    /// recordCount*/recordHas* reader helpers below take it before walking the
    /// buffer. std.Io.Mutex is fiber/thread-safe under std.Io.Threaded.
    mutex: std.Io.Mutex = .init,
    /// While set, the peer's writer parks at the top of open() (short cancellable
    /// poll), so a test can observe frames sitting un-drained in the peer's queue
    /// (e.g. an IDONTWANT purge). The test CLEARS it before teardown so the writer
    /// exits open() on its own — never relying on cancel to collapse a long sleep
    /// (which the writer-teardown path treats as fragile). Written by the test
    /// fiber, read by the writer fiber, so atomic.
    block_open: std.atomic.Value(bool) = .init(false),

    fn deinit(self: *FakeRecord) void {
        self.written.deinit(self.allocator);
    }
};

/// Test-owned, thread-safe log of every address the router asked the transport to
/// dial. The router runs on a worker fiber, so `dial` appends under a mutex while
/// the test fiber reads via `count`/`contains`; both take `mutex`. The test owns
/// the log (so the recording outlives the router) and frees it with `deinit`.
const DialLog = struct {
    allocator: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    addrs: std.ArrayList([]u8) = .empty,

    fn deinit(self: *DialLog, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        for (self.addrs.items) |a| self.allocator.free(a);
        self.addrs.deinit(self.allocator);
    }

    /// Record one dialed address (copying it). Best-effort: on OOM the address is
    /// simply not recorded (the test then observes one fewer dial — a failure it
    /// can surface), never a crash.
    fn record(self: *DialLog, io: std.Io, addr: []const u8) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const owned = self.allocator.dupe(u8, addr) catch return;
        self.addrs.append(self.allocator, owned) catch self.allocator.free(owned);
    }

    /// How many times `addr` was dialed.
    fn count(self: *DialLog, io: std.Io, addr: []const u8) usize {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var n: usize = 0;
        for (self.addrs.items) |a| {
            if (std.mem.eql(u8, a, addr)) n += 1;
        }
        return n;
    }
};

/// An in-memory transport for router unit tests. `ConnHandle` is a tiny fake
/// connection that carries the per-peer recording; `Sink` is a FakeSink that
/// records into that connection's recording. `makeSink` allocates a FakeSink
/// and points it at the connection's transport-owned record. `dial` records the
/// requested address into the (optional) test-owned `DialLog` and opens NO real
/// socket — a test drives the resulting connect itself by posting peer_connected.
const FakeTransport = struct {
    /// When non-zero every sink made by this transport fails its first N opens
    /// (use `maxInt` for "always fail"), exercising the writer give-up path.
    fail_open_count: usize = 0,
    /// When non-zero every sink made by this transport fails its first N frame
    /// writes (use `maxInt` for "always fail"), exercising the writer's
    /// consecutive-write-failure give-up path (the stalled-peer defense).
    fail_write_count: usize = 0,
    /// Where `dial` records requested addresses, or null to ignore dials (the
    /// default, so existing `.{}` call sites that never dial still compile).
    dial_log: ?*DialLog = null,

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
            .fail_write_count = self.fail_write_count,
        };
        return sink;
    }

    /// Fire-and-forget dial: record the address (no socket). The test simulates
    /// the resulting connect by posting peer_connected itself, exactly as the
    /// other lifecycle tests do.
    pub fn dial(self: *FakeTransport, io: std.Io, addr: []const u8) void {
        if (self.dial_log) |log| log.record(io, addr);
    }
};

/// Build a FakeConn on the heap with a fresh recording. The test owns it and
/// frees it with `destroyFakeConn`. By test convention the conn is freed only
/// after the router has fully processed the peer's last operation and the peer's
/// writer fiber is idle (the tests poll the record for the expected frames, or
/// post no further outbound, before tearing down) — so the writer is parked, not
/// mid-append, when the record is freed.
fn makeFakeConn(allocator: std.mem.Allocator) !*FakeTransport.FakeConn {
    const conn = try allocator.create(FakeTransport.FakeConn);
    conn.* = .{ .record = .{ .allocator = allocator } };
    return conn;
}

/// Frees a FakeConn and its embedded FakeRecord. ORDERING INVARIANT: a peer's
/// writer fiber (router-owned) appends to this record in writeFrame; those fibers
/// are only cancelled+joined by `router.destroy()`. So for any conn whose peer is
/// still connected at end of test, `router.destroy()` MUST run before this frees
/// the record, or a writer mid-writeFrame touches freed memory (an `index 0xAA…`
/// panic in appendSlice). Tests guarantee this by declaring `defer router.destroy()`
/// AFTER the `defer destroyFakeConn(...)` calls (defers are LIFO, so destroy runs
/// first → joins writers → then records free). Records stay inspectable until freed.
/// (Tests that disconnect the peer first — peer_disconnected joins that writer — or
/// quiesce the writer before teardown may free inline / in any order safely.)
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

/// Barrier: post a `sync` command and await its reply, so every command posted
/// before it has been fully processed by the router fiber, which is then back
/// parked on the inbox. After this returns, the test fiber may read router-owned
/// state (mesh / peers / fanout / backoff / peer_versions / message_cache /
/// score / heartbeat_tick) race-free — but only because no other command is
/// posted and no heartbeat/inbound fiber is concurrently active during the read
/// window (the router tests run with heartbeat_interval_ms = 0).
///
/// This is the one safe way for a test to read the router's HashMaps: the router
/// is a single-fiber actor that owns that state exclusively, so a test must never
/// read it while the router fiber might mutate it (a read racing a HashMap resize
/// gets a torn metadata pointer and panics). `sync` serializes the test fiber
/// behind the router fiber's FIFO inbox; read only after it returns.
fn sync(router: *FakeRouter, io: std.Io) !void {
    var reply: std.Io.Event = .unset;
    try router.inbox.putOne(io, .{ .sync = .{ .reply = &reply } });
    reply.waitUncancelable(io);
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
    try router.start();

    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));

    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
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
    try router.start();

    const peer = try PeerId.random();
    const conn_a = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_b);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

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

    // The dedup'd duplicate's close must NOT tear down the live peer: the
    // event names the dying connection, and the PeerState is bound to conn_a.
    // (Simultaneous dial: both sides dial, one connection is redundant; its
    // close used to destroy the surviving peer's state keyed on PeerId alone.)
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn_b } });
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 1), router.peerCount());

    // The BOUND connection's close does tear it down.
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn_a } });
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
    try router.start();

    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

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

test "router tears the peer down after consecutive write failures (stalled peer)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Opens succeed but EVERY frame write fails (the fake stand-in for a
    // stalled peer's write timeouts). The writer closes + reopens after each
    // failure and, after max_write_failures consecutive ones, fires
    // on_disconnect — the router tears the peer down on its own.
    const router = try FakeRouter.create(allocator, io, .{ .fail_write_count = std.math.maxInt(usize) }, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));

    // Feed enough frames for the give-up: each failed write consumes one frame
    // (lost in-flight), and the writer gives up on the max_write_failures-th
    // consecutive failure.
    for (0..4) |_| try router.enqueueDataForTest(peer, try testDataFrame(allocator));

    try std.testing.expect(waitFor(io, peerCountIsZero, router));
    // Nothing was ever successfully written.
    try std.testing.expectEqual(@as(usize, 0), conn.record.written.items.len);
}

test "writer give-up with a FULL inbox neither parks the writer nor wedges teardown" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Deadlock regression: the writer's disconnect signal used to be a BLOCKING
    // uncancelable inbox post. With the inbox full, the writer parked forever
    // (uncancelable puts ignore cancel), and any teardown joining the writer —
    // teardownPeer / teardownAllPeers, both on the router fiber, the inbox's
    // only consumer — wedged the router permanently: it awaited a fiber that
    // was waiting for the router to drain the very queue it was parked on.
    // Post-fix the writer signals via the atomic `writer_dead` flag plus a
    // non-blocking post, and the heartbeat reaps flagged peers.
    //
    // start() is never called: with no main loop draining, the test fiber
    // stands in for the router fiber and the inbox stays deterministically FULL.
    const router = try FakeRouter.create(allocator, io, .{ .fail_open_count = std.math.maxInt(usize) }, local_test_peer, null, 0, null, null, .{});
    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);
    defer {
        // Orderly cleanup without a main loop: close the (full) inbox so
        // destroy's own shutdown post returns Closed instead of parking; with
        // main_future null, destroy then tears down directly and drains.
        router.inbox.close(io);
        router.destroy();
    }

    // Connect a peer directly on this fiber (it is "the router fiber" here);
    // the writer fiber spawns with every stream open failing.
    router.onPeerConnected(peer, conn, dummy_addr);
    try std.testing.expectEqual(@as(usize, 1), router.peerCount());
    const state = router.peers.get(peerKey(&peer)).?;

    // Fill the inbox to capacity with inert commands (drainInbox frees nothing
    // for .heartbeat), so the writer's disconnect post will find it full.
    while (true) {
        const n = try router.inbox.putUncancelable(io, &.{.heartbeat}, 0);
        if (n == 0) break;
    }

    // Trigger the writer's lazy stream open by pushing a frame straight onto
    // its queue (the inbox is full, so the enqueue_for_test path is unusable).
    // Every open fails -> the writer exhausts its retries -> fires
    // onWriterDisconnect against the FULL inbox. Pre-fix it parks here forever
    // and the await below hangs the test; post-fix it flags writer_dead and
    // exits.
    try state.queue.push(io, .data, try testDataFrame(allocator));

    var waited_ms: u64 = 0;
    while (waited_ms < 5000) : (waited_ms += 5) {
        if (state.writer_dead.load(.acquire)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(state.writer_dead.load(.acquire));

    // The heartbeat reap is the lossless fallback for the dropped post: it must
    // tear the flagged peer down (joining the now-exited writer without
    // wedging).
    router.onHeartbeat();
    try std.testing.expectEqual(@as(usize, 0), router.peerCount());
}

test "router frees an inbound RPC (peer tracked and peer absent)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    // Peer-absent case: no peer_connected was posted, so the inbound RPC arrives
    // for an untracked peer. The router must still free it.
    const stranger = try PeerId.random();
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = stranger, .rpc = try buildInboundRpc(allocator, "t-absent") } });

    // Peer-tracked case: connect first, then deliver an inbound RPC for it.
    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundRpc(allocator, "t-present") } });

    // Disconnect to flush + tear down; on test exit destroy() drains any leftover
    // inbox commands. std.testing.allocator confirms both RPCs were freed.
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));
}

test "router retains a SUBSCRIBE that arrives before peer_connected (race stash)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // The race: a peer's SUBSCRIBE arrives on its inbound stream BEFORE its
    // peer_connected (the two fire on independent fibers). Pre-fix the
    // subscription was dropped (peer untracked) and never re-sent, so the peer was
    // never recorded as a topic subscriber — excluding it from flood-publish AND
    // mesh GRAFT. The stash must retain it and peer_connected must apply it.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundRpc(allocator, "race-topic") } });
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try sync(router, io);

    // The early SUBSCRIBE must have landed on the now-tracked PeerState's topics
    // (so the peer is a flood-publish target + a GRAFT candidate for the topic).
    const st = router.peers.get(peerKey(&peer)) orelse return error.PeerNotTracked;
    try std.testing.expect(st.topics.contains("race-topic"));

    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
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
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

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
    return peer_io.OutboundFrame.create(allocator, bytes, null, 1);
}

/// Build a one-byte shared data frame (one reference) carrying a single owned
/// copy of `id` as its `message_id`, matching the shape `cacheAndForward`
/// produces — so a router IDONTWANT purge has a real id to match against.
fn testDataFrameWithId(allocator: std.mem.Allocator, id: []const u8) !*peer_io.OutboundFrame {
    const bytes = try allocator.alloc(u8, 1);
    errdefer allocator.free(bytes);
    bytes[0] = 0x7f;
    const owned_id = try allocator.dupe(u8, id);
    errdefer allocator.free(owned_id);
    return peer_io.OutboundFrame.create(allocator, bytes, owned_id, 1);
}

/// After a `sync`, the data-lane length of the peer tracked under `peer` (0 if
/// untracked). Router-owned state; read only after `sync` and with the peer's
/// writer parked (e.g. a block_open sink), so the read never races a drain.
fn peerDataLen(io: std.Io, router: *FakeRouter, peer: PeerId) usize {
    const state = router.peers.get(peerKey(&peer)) orelse return 0;
    return state.queue.dataLen(io);
}

/// After a `sync`, whether the peer tracked under `peer` has `id` recorded in its
/// IDONTWANT (`dont_send`) set. Router-owned state; read only after `sync`.
fn peerDontSendHas(router: *FakeRouter, peer: PeerId, id: []const u8) bool {
    const state = router.peers.get(peerKey(&peer)) orelse return false;
    return state.dont_send.contains(id);
}

/// After a `sync`, the number of entries in the peer's IDONTWANT (`dont_send`)
/// set (0 if untracked). Router-owned state; read only after `sync`.
fn peerDontSendCount(router: *FakeRouter, peer: PeerId) usize {
    const state = router.peers.get(peerKey(&peer)) orelse return 0;
    return state.dont_send.count();
}

/// After a `sync`, the number of outstanding IWANT promises recorded for the
/// peer (0 if untracked). Router-owned state; read only after `sync`.
fn peerPromiseCount(router: *FakeRouter, peer: PeerId) usize {
    const state = router.peers.get(peerKey(&peer)) orelse return 0;
    return state.iwant_promises.count();
}

/// After a `sync`, the number of DISTINCT live interned message ids (one per
/// allocation). Used to assert id sharing across the four maps. Router-owned
/// state; read only after `sync`.
fn internCount(router: *FakeRouter) usize {
    return router.intern_table.count();
}

/// After a `sync`, the reference count of the interned box for `id` (0 if the id
/// is not currently interned). Used to assert that an id held by several maps is
/// ONE allocation whose refs equal the holder count. Router-owned state; read
/// only after `sync`.
fn internRefs(router: *FakeRouter, id: []const u8) usize {
    const box = router.intern_table.entries.get(id) orelse return 0;
    return box.refs.load(.monotonic);
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

/// Like `buildInboundPublish` but carrying the publisher's signature + marshaled
/// public key, for StrictSign inbound paths (the wire shape a signing publisher
/// produces).
fn buildInboundPublishSigned(
    allocator: std.mem.Allocator,
    from: []const u8,
    seqno: []const u8,
    topic: []const u8,
    data: []const u8,
    signature: []const u8,
    key: []const u8,
) !pubsub.OwnedRpc {
    const msg = rpc_pb.Message{ .from = from, .seqno = seqno, .topic = topic, .data = data, .signature = signature, .key = key };
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

/// Build an OwnedRpc carrying a single control PRUNE for `topic` that carries the
/// given PX peer offers (`px`), mirroring what an inbound reader yields for a peer
/// that PRUNEd us WITH peer exchange. `backoff` is the wire backoff in seconds.
fn buildInboundPrunePx(allocator: std.mem.Allocator, topic: []const u8, backoff: u64, px: []const ?rpc_pb.PeerInfo) !pubsub.OwnedRpc {
    const ctrl = rpc_pb.ControlMessage{ .prune = &[_]?rpc_pb.ControlPrune{rpc.buildPrune(topic, px, backoff)} };
    return ownedFromRpc(allocator, rpc_pb.RPC{ .control = ctrl });
}

/// Seal a signed peer record for `key`'s own peer-id advertising the single
/// dialable multiaddr STRING `addr`, encoded to its BINARY multiaddr form (the
/// libp2p wire form a real peer signs, so the consume path's `fromBytes` decodes
/// it back to `addr`). Returns the marshaled Envelope bytes (caller owns + frees)
/// plus the peer-id, so a test can both put it on the wire and assert on the id.
const SealedRecord = struct { envelope: []u8, peer_id: PeerId };
fn sealTestRecord(allocator: std.mem.Allocator, key: *const identity.KeyPair, seq: u64, addr: []const u8) !SealedRecord {
    const id = try key.peerId(allocator);
    const bin = try (Multiaddr{ .bytes = addr }).toBytes(allocator);
    defer allocator.free(bin);
    const addrs = [_][]const u8{bin};
    const envelope = try peer_record.sealPeerRecord(allocator, key, .{ .peer_id = id, .seq = seq, .addrs = &addrs });
    return .{ .envelope = envelope, .peer_id = id };
}

/// Collect the PX peer offers (peer-id + signed-record bytes) from the FIRST
/// recorded PRUNE for `topic` into `out` (each entry borrows the record bytes,
/// valid while the record lock is held — so callers assert inside the closure or
/// copy). Returns the count found. Walks frames under the record lock.
const PxOffer = struct { peer_id: []const u8, record: []const u8 };
fn recordFirstPrunePxPeers(io: std.Io, record: *FakeRecord, topic: []const u8, out: *std.ArrayList(PxOffer), allocator: std.mem.Allocator) !usize {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var rest = record.written.items;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        if (reader.getControl()) |ctrl_reader| {
            var control = ctrl_reader;
            while (control.pruneNext()) |prune| {
                var pr = prune;
                if (!std.mem.eql(u8, pr.getTopicID(), topic)) continue;
                while (pr.peersNext()) |info| {
                    try out.append(allocator, .{ .peer_id = info.getPeerID(), .record = info.getSignedPeerRecord() });
                }
                return out.items.len;
            }
        } else |_| {}
        rest = rest[decoded.total_len..];
    }
    return 0;
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

/// Build an OwnedRpc carrying a single control IDONTWANT(`ids`).
fn buildInboundIDontWant(allocator: std.mem.Allocator, ids: []const ?[]const u8) !pubsub.OwnedRpc {
    const ctrl = rpc_pb.ControlMessage{ .idontwant = &[_]?rpc_pb.ControlIDontWant{rpc.buildIDontWant(ids)} };
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

/// Whether the recording contains a frame whose first subscription matches
/// (`topic`, `subscribe`). Walks every recorded frame under the record lock so
/// the read never races the writer fiber's append.
fn recordHasSubscription(io: std.Io, record: *FakeRecord, topic: []const u8, subscribe: bool) bool {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var rest = record.written.items;
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
/// `data`. Used to assert exactly-once forwarding (dedup). Reads under the record
/// lock so the walk never races the writer fiber's append.
fn recordCountPublishes(io: std.Io, record: *FakeRecord, topic: []const u8, data: []const u8) usize {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var count: usize = 0;
    var rest = record.written.items;
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
/// recorded frame and every prune in each frame's control message, under the
/// record lock so the walk never races the writer fiber's append.
fn recordCountPrunes(io: std.Io, record: *FakeRecord, topic: []const u8) usize {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var count: usize = 0;
    var rest = record.written.items;
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

/// The wire `backoff` (seconds) of the FIRST recorded PRUNE for `topic`, or null
/// if none was recorded. Walks frames under the record lock; used to assert the
/// emitted PRUNE carried the expected backoff (e.g. the shorter unsubscribe one).
fn recordFirstPruneBackoff(io: std.Io, record: *FakeRecord, topic: []const u8) ?u64 {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var rest = record.written.items;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        if (reader.getControl()) |ctrl_reader| {
            var control = ctrl_reader;
            while (control.pruneNext()) |prune| {
                if (std.mem.eql(u8, prune.getTopicID(), topic)) return prune.getBackoff();
            }
        } else |_| {}
        rest = rest[decoded.total_len..];
    }
    return null;
}

/// Count recorded frames carrying a control GRAFT for `topic`. Walks every
/// recorded frame and every graft in each frame's control message, under the
/// record lock so the walk never races the writer fiber's append.
fn recordCountGrafts(io: std.Io, record: *FakeRecord, topic: []const u8) usize {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var count: usize = 0;
    var rest = record.written.items;
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

/// Whether the recording contains a control IHAVE for `topic` that advertises
/// `id`. Walks every recorded frame and every IHAVE/message-id within each
/// control msg, under the record lock so the walk never races the writer append.
fn recordHasIHave(io: std.Io, record: *FakeRecord, topic: []const u8, id: []const u8) bool {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var rest = record.written.items;
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

/// Whether the recording contains any control IHAVE for `topic` (ignoring the
/// advertised ids). Used by gossip-fan-out tests that count how many peers got an
/// IHAVE, not which ids. Walks every recorded frame under the record lock.
fn recordHasAnyIHave(io: std.Io, record: *FakeRecord, topic: []const u8) bool {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var rest = record.written.items;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        if (reader.getControl()) |ctrl_reader| {
            var control = ctrl_reader;
            while (control.ihaveNext()) |ihave| {
                var ih = ihave;
                if (std.mem.eql(u8, ih.getTopicID(), topic)) return true;
            }
        } else |_| {}
        rest = rest[decoded.total_len..];
    }
    return false;
}

/// Count control IWANT messages in the recording that request `id`. Walks every
/// recorded frame and every IWANT/message-id within each control message, under
/// the record lock so the walk never races the writer fiber's append.
fn recordCountIWants(io: std.Io, record: *FakeRecord, id: []const u8) usize {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var count: usize = 0;
    var rest = record.written.items;
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

/// Count control IDONTWANT messages in the recording that announce `id`. Walks
/// every recorded frame and every IDONTWANT/message-id within each control
/// message, under the record lock so the walk never races the writer append.
fn recordCountIDontWants(io: std.Io, record: *FakeRecord, id: []const u8) usize {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var count: usize = 0;
    var rest = record.written.items;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        if (reader.getControl()) |ctrl_reader| {
            var control = ctrl_reader;
            while (control.idontwantNext()) |idontwant| {
                var idw = idontwant;
                while (idw.messageIDsNext()) |mid| {
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
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    // Sync so the connect is fully processed (peer wired into the map) before the
    // caller reads any router state for this peer.
    try sync(router, io);
    return conn;
}

test "subscribe announces the subscription to a connected peer" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Subscribe locally; the router announces it to every peer on the subscribe
    // lane. The writer fiber opens the fake stream and records the framed RPC.
    const topic = try allocator.dupe(u8, "t");
    try router.inbox.putOne(io, .{ .subscribe = .{ .topic = topic } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordHasSubscription(io, &conn.record, "t", true)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(recordHasSubscription(io, &conn.record, "t", true));
}

test "inbound subscription is tracked on the source peer" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Peer X announces it subscribes to "t"; the router records it on X's state.
    // Sync so the inbound is fully processed, then read the peer's topics once.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", true) } });
    try sync(router, io);
    try std.testing.expect(peerTracksTopic(router, peer, "t"));

    // And an unsubscribe removes it.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", false) } });
    try sync(router, io);
    try std.testing.expect(!peerTracksTopic(router, peer, "t"));
}

test "received message forwards over the mesh, not to all subscribers" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
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
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

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
        if (recordCountPublishes(io, &conn_a.record, "t", "hello") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give a wrong forward to B a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "hello"));
    // B is a subscriber but not in the mesh, so it gets nothing.
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_b.record, "t", "hello"));
}

test "mesh forwarding dedups a repeated publish (same from+seqno) to forward once" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    // Subscribe to "t" and graft Y into the mesh so a forward has a destination.
    try subscribeAndWait(io, allocator, router, "t");

    const peer_x = testPeer(1);
    const peer_y = testPeer(2);
    const conn_x = try connectFakePeer(io, allocator, router, peer_x);
    defer destroyFakeConn(allocator, conn_x);
    const conn_y = try connectFakePeer(io, allocator, router, peer_y);
    defer destroyFakeConn(allocator, conn_y);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

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
        if (recordCountPublishes(io, &conn_y.record, "t", "dup") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give the (suppressed) second a chance to wrongly land before asserting.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_y.record, "t", "dup"));
}

test "seen-cache TTL: a duplicate is suppressed within the window, re-processed after it expires" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    // Subscribe to "t" and graft Y in so a forward has a destination; X relays.
    try subscribeAndWait(io, allocator, router, "t");
    const peer_x = testPeer(1);
    const peer_y = testPeer(2);
    const conn_x = try connectFakePeer(io, allocator, router, peer_x);
    defer destroyFakeConn(allocator, conn_x);
    const conn_y = try connectFakePeer(io, allocator, router, peer_y);
    defer destroyFakeConn(allocator, conn_y);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_y, peer_y, "t"));

    // Receive the message at tick 0 (from++seqno id). It forwards once and is now
    // seen, so the duplicate posted right after is suppressed: Y sees exactly one.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_x,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x09", "t", "ttl"),
    } });
    try sync(router, io);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_x,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x09", "t", "ttl"),
    } });
    try sync(router, io);

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_y.record, "t", "ttl") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_y.record, "t", "ttl"));

    // Still seen at ticks 1..119 (a beat just short of the TTL leaves it in).
    try beatHeartbeats(io, router, mesh_params.seen_ttl_ticks - 1);
    try std.testing.expect(router.seenContains("origin\x00\x00\x00\x09"));

    // The TTL-th heartbeat sweeps the id (expiry = 0 + seen_ttl_ticks <= tick).
    try beatHeartbeats(io, router, 1);
    try std.testing.expect(!router.seenContains("origin\x00\x00\x00\x09"));

    // The same message is now processed again as NEW and forwarded a second time.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_x,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x09", "t", "ttl"),
    } });
    try sync(router, io);
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_y.record, "t", "ttl") >= 2) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 2), recordCountPublishes(io, &conn_y.record, "t", "ttl"));
}

test "seen-cache keeps more ids than the old FIFO cap within the TTL window (no premature eviction)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");
    const peer_x = testPeer(1);
    const conn_x = try connectFakePeer(io, allocator, router, peer_x);
    defer destroyFakeConn(allocator, conn_x);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Receive 2000 distinct messages (> the old 1024 FIFO cap) within one TTL
    // window. Each gets a unique 4-byte seqno, so a unique from++seqno id.
    const count: u32 = 2000;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        var seqno: [4]u8 = undefined;
        std.mem.writeInt(u32, &seqno, i, .big);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{
            .peer = peer_x,
            .rpc = try buildInboundPublish(allocator, "origin", &seqno, "t", "vol"),
        } });
    }
    try sync(router, io);

    // ALL 2000 are still seen: the time-cache (unlike the old FIFO) never evicted
    // the earliest ones to make room — they only expire on the TTL clock.
    i = 0;
    while (i < count) : (i += 1) {
        var seqno: [4]u8 = undefined;
        std.mem.writeInt(u32, &seqno, i, .big);
        var id: [10]u8 = undefined;
        @memcpy(id[0..6], "origin");
        @memcpy(id[6..10], &seqno);
        try std.testing.expect(router.seenContains(&id));
    }
}

test "seen-cache first-seen: re-adding an id does NOT extend its expiry" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");
    const peer_x = testPeer(1);
    const conn_x = try connectFakePeer(io, allocator, router, peer_x);
    defer destroyFakeConn(allocator, conn_x);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Add the id at tick 0 (expiry = seen_ttl_ticks).
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_x,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x01", "t", "fs"),
    } });
    try sync(router, io);
    try std.testing.expect(router.seenContains("origin\x00\x00\x00\x01"));

    // Beat partway, then re-receive the SAME id. Under first-seen (go's default)
    // the re-add leaves the original expiry untouched — it does NOT slide forward.
    try beatHeartbeats(io, router, 60);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_x,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x01", "t", "fs"),
    } });
    try sync(router, io);

    // It still expires at the ORIGINAL first-insert + TTL (tick seen_ttl_ticks):
    // after the remaining ticks the sweep drops it, even though it was re-added at
    // tick 60. A last-seen cache would have kept it alive until 60 + TTL.
    try beatHeartbeats(io, router, mesh_params.seen_ttl_ticks - 60);
    try std.testing.expect(!router.seenContains("origin\x00\x00\x00\x01"));
}

test "publish to a subscribed topic forwards over the mesh and delivers locally" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{});
    try router.start();

    // Subscribe locally so the publish is delivered to our own handler too.
    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // A announces it subscribes to "t" (so it is both a topic subscriber and,
    // once grafted, a mesh member). With flood-publish on (the default) an
    // originated message reaches A as a subscriber; with it off it reaches A as a
    // mesh member — either way A receives exactly one copy.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_a, .rpc = try buildInboundSub(allocator, "t", true) } });
    try sync(router, io);
    try std.testing.expect(peerTracksTopic(router, peer_a, "t"));

    // Graft A into the mesh so the local publish has a mesh destination.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expect(router.meshContains("t", peer_a));

    // Publish locally: A gets the forwarded frame and our handler fires. The local
    // delivery (onMessage) runs on the delivery fiber, but `sync` fences THROUGH
    // the delivery queue, so it still guarantees rec.* is written before the test
    // reads it. The forwarded frame to A is flushed asynchronously by A's writer,
    // so poll the record.
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "hello"),
    } });
    try sync(router, io);

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "hello") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "hello"));
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
    // Sync so both inbound subscriptions are fully processed, then read once.
    try sync(router, io);
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
        if (recordCountPublishes(io, &conn_a.record, "t", "flood") >= 1 and
            recordCountPublishes(io, &conn_b.record, "t", "flood") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "flood"));
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_b.record, "t", "flood"));

    // Contrast: a RELAYED message on "t" (from a third source) reaches only the
    // mesh member A, NOT the non-mesh subscriber B — relays stay mesh-only.
    const source = testPeer(3);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x01", "t", "relay"),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "relay") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give a wrong forward to B a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "relay"));
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_b.record, "t", "relay"));
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
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const peer_b = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try connectFakePeer(io, allocator, router, peer_b);
    defer destroyFakeConn(allocator, conn_b);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Both subscribe to "t".
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_a, .rpc = try buildInboundSub(allocator, "t", true) } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_b, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    // Sync so both inbound subscriptions are fully processed, then read once.
    try sync(router, io);
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
        if (recordCountPublishes(io, &conn_a.record, "t", "gated") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give a wrong forward to B a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "gated"));
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_b.record, "t", "gated"));
}

test "flood-publish OFF: an originated message reaches mesh members only, not other subscribers" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Flood-publish OFF → originated messages use the mesh (we subscribe to "t").
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{ .flood_publish = false });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const peer_b = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try connectFakePeer(io, allocator, router, peer_b);
    defer destroyFakeConn(allocator, conn_b);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Both subscribe to "t", but only A is grafted into the mesh.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_a, .rpc = try buildInboundSub(allocator, "t", true) } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_b, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    // Sync so both inbound subscriptions are fully processed, then read once.
    try sync(router, io);
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
        if (recordCountPublishes(io, &conn_a.record, "t", "meshonly") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give a wrong forward to B a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "meshonly"));
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_b.record, "t", "meshonly"));
}

test "a forwarded message lands in the message cache and is evicted after history_length heartbeats" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
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
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    // Source relays a publish; sync so the forward (and its message-cache
    // insertion) is fully applied on the router fiber before reading the cache.
    // The forwarded frame reaches A's record asynchronously (A's writer flushes
    // it), so poll the record for it; the cache state is settled by the sync.
    const from = "origin";
    const seqno = "\x00\x00\x00\x05";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "cached"),
    } });
    try sync(router, io);
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "cached") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "cached"));

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
    // beatHeartbeats syncs, so the eviction is fully applied; read the cache once.
    try std.testing.expect(router.message_cache.get(id.bytes) == null);
}

test "a forwarded message's id is interned ONCE across mcache + seen; freed only when both release" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");
    const peer_a = testPeer(1);
    const source = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    const from = "origin";
    const seqno = "\x00\x00\x00\x07";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "shared"),
    } });
    try sync(router, io);

    var id = try rpc.messageId(allocator, from, seqno);
    defer id.deinit(allocator);

    // The accepted message marks the id seen AND caches it. Both holders intern
    // through the router's one table, so the id is ONE allocation with TWO
    // references (seen + mcache), not two copies.
    try std.testing.expectEqual(@as(usize, 1), internCount(router));
    try std.testing.expectEqual(@as(usize, 2), internRefs(router, id.bytes));
    try std.testing.expect(router.seen.contains(id.bytes, router.heartbeat_tick));
    try std.testing.expect(router.message_cache.get(id.bytes) != null);

    // Beat history_length heartbeats: the mcache evicts the id (releasing its
    // reference), but seen's TTL (seen_ttl_ticks, far larger) keeps it. The id
    // survives in the table with one remaining holder (seen).
    const history_length = @import("mcache.zig").historyLengthForTest();
    try beatHeartbeats(io, router, history_length);
    try std.testing.expect(router.message_cache.get(id.bytes) == null);
    try std.testing.expect(router.seen.contains(id.bytes, router.heartbeat_tick));
    try std.testing.expectEqual(@as(usize, 1), internCount(router));
    try std.testing.expectEqual(@as(usize, 1), internRefs(router, id.bytes));

    // Expire the seen holder (the last one): the id reaches zero references and
    // is freed, leaving the intern table empty.
    router.seen.sweep(router.heartbeat_tick +| mesh_params.seen_ttl_ticks +| 1);
    try std.testing.expectEqual(@as(usize, 0), internCount(router));
    try std.testing.expectEqual(@as(usize, 0), internRefs(router, id.bytes));
}

test "queued delivery: a handler that re-publishes synchronously completes (H1)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // The eth2-natural pattern H1 flagged as a self-deadlock: receive ->
    // process -> (re)publish SYNCHRONOUSLY from the message handler. In
    // queued-delivery mode (the default) the handler runs on the delivery
    // fiber, so its blocking inbox post is drained by the running router
    // fiber — no cycle. (In inline mode the same handler would post into the
    // queue only its own fiber drains and deadlock exactly under flood.)
    const Republisher = struct {
        allocator: std.mem.Allocator,
        io: std.Io,
        router: *FakeRouter,
        delivered: std.atomic.Value(usize) = .init(0),
        republished: std.atomic.Value(bool) = .init(false),

        fn handler(self: *@This()) MessageHandler {
            return .{ .ctx = self, .on_message = onMessage };
        }

        fn onMessage(ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) void {
            _ = from;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.delivered.fetchAdd(1, .acq_rel);
            if (std.mem.eql(u8, data, "ping") and !self.republished.swap(true, .acq_rel)) {
                const t = self.allocator.dupe(u8, topic) catch return;
                const d = self.allocator.dupe(u8, "pong") catch {
                    self.allocator.free(t);
                    return;
                };
                self.router.inbox.putOne(self.io, .{ .publish = .{ .topic = t, .data = d } }) catch {
                    self.allocator.free(t);
                    self.allocator.free(d);
                };
            }
        }
    };

    var rep = Republisher{ .allocator = allocator, .io = io, .router = undefined };
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rep.handler(), 0, null, null, .{});
    rep.router = router;
    try router.start();
    defer router.destroy();
    try subscribeAndWait(io, allocator, router, "t");

    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "ping"),
    } });
    // First sync: fence1 is queued after the ping delivery, and the handler
    // posts its re-publish BEFORE the fence is reached, so when this returns
    // the pong publish is already in the inbox ahead of the next sync.
    try sync(router, io);
    // Second sync: fences the pong's own delivery.
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 2), rep.delivered.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), router.delivery_drops);
}

// --- mesh: GRAFT / PRUNE / backoff / heartbeat fake tests ------------------

/// Subscribe the local node to `topic`, then `sync` so the router has fully
/// processed it (recorded it in `my_topics`) before returning. The GRAFT
/// handler's `my_topics.contains` check is only meaningful once this has landed.
fn subscribeAndWait(io: std.Io, allocator: std.mem.Allocator, router: *FakeRouter, topic: []const u8) !void {
    try router.inbox.putOne(io, .{ .subscribe = .{ .topic = try allocator.dupe(u8, topic) } });
    try sync(router, io);
}

/// Spin (bounded) until P's recorded outbound bytes contain at least one PRUNE
/// for `topic`. Returns whether it held.
fn waitPruneSent(io: std.Io, conn: *FakeTransport.FakeConn, topic: []const u8) bool {
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPrunes(io, &conn.record, topic) >= 1) return true;
        io_time.ms(5).sleep(io) catch {};
    }
    return recordCountPrunes(io, &conn.record, topic) >= 1;
}

/// Spin (bounded) until P's recorded outbound bytes contain at least one GRAFT
/// for `topic`. Returns whether it held.
fn waitGraftSent(io: std.Io, conn: *FakeTransport.FakeConn, topic: []const u8) bool {
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountGrafts(io, &conn.record, topic) >= 1) return true;
        io_time.ms(5).sleep(io) catch {};
    }
    return recordCountGrafts(io, &conn.record, topic) >= 1;
}

/// Post a GRAFT(topic) inbound from `peer`, then `sync` so the GRAFT is fully
/// applied, and read the outcome once: mesh membership is authoritative —
/// accepted iff the mesh now contains `peer`, otherwise rejected (the router
/// either PRUNEd it back or it was graylisted/backed-off). The `conn` parameter
/// is kept so callers read `conn.record` separately (e.g. waitPruneSent) when the
/// test asserts the PRUNE actually reached the wire; the writer flush is
/// asynchronous from the router-state effect, so the record is not consulted for
/// the accept/reject decision.
const GraftOutcome = enum { accepted, rejected };
fn graftAndWait(
    io: std.Io,
    allocator: std.mem.Allocator,
    router: *FakeRouter,
    conn: *FakeTransport.FakeConn,
    peer: PeerId,
    topic: []const u8,
) !GraftOutcome {
    _ = conn;
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundGraft(allocator, topic) } });
    try sync(router, io);
    return if (router.meshContains(topic, peer)) .accepted else .rejected;
}

/// Drive `n` heartbeat ticks, then `sync` so all `n` ticks (and their decay /
/// mesh maintenance) have fully run on the router fiber before returning. Used to
/// age out backoffs deterministically with the heartbeat fiber disabled
/// (interval 0). After this returns `heartbeat_tick` has advanced by `n`.
fn beatHeartbeats(io: std.Io, router: *FakeRouter, n: u64) !void {
    var i: u64 = 0;
    while (i < n) : (i += 1) try router.inbox.putOne(io, .heartbeat);
    try sync(router, io);
}

test "GRAFT accepted: subscribed topic, peer joins the mesh, no PRUNE back" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    const outcome = try graftAndWait(io, allocator, router, conn, peer, "t");
    try std.testing.expectEqual(GraftOutcome.accepted, outcome);
    try std.testing.expect(router.meshContains("t", peer));
    // Accept sends nothing back: no PRUNE on the peer's recorded stream.
    try std.testing.expectEqual(@as(usize, 0), recordCountPrunes(io, &conn.record, "t"));
}

test "GRAFT rejected when we do not subscribe: PRUNE sent, peer not in mesh" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    // We do NOT subscribe to "t".
    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    const outcome = try graftAndWait(io, allocator, router, conn, peer, "t");
    try std.testing.expectEqual(GraftOutcome.rejected, outcome);
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(waitPruneSent(io, conn, "t"));
    try std.testing.expectEqual(@as(usize, 1), recordCountPrunes(io, &conn.record, "t"));
}

test "GRAFT can push the mesh past D_high; the heartbeat prunes it back to D" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // Drive the mesh ABOVE D_high with distinct peers, each via an accepted GRAFT
    // (the D_high cap is gone — go/rust-faithful, the heartbeat does the pruning).
    const n = mesh_params.d_high + 1; // 13: strictly above D_high
    var conns: [mesh_params.d_high + 1]*FakeTransport.FakeConn = undefined;
    var peers: [mesh_params.d_high + 1]PeerId = undefined;
    var filled: usize = 0;
    defer for (conns[0..filled]) |c| destroyFakeConn(allocator, c);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
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
    // beatHeartbeats syncs, so the prune-to-D is fully applied; read the size once.
    try beatHeartbeats(io, router, 1);
    try std.testing.expectEqual(mesh_params.d, router.meshSize("t"));

    // Wait for the PRUNE control frames to actually land on the pruned peers'
    // streams before counting (the writer fibers drain asynchronously). Once the
    // total reaches n - D the writers are quiescent, which also keeps the FakeSink
    // records from being mutated while the test tears down. The router-state
    // checks (meshContains / inBackoff) settle synchronously: beatHeartbeats
    // synced, and no command is posted before the reads below, so they are safe.
    const want_pruned = n - mesh_params.d;
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        var total: usize = 0;
        for (conns[0..n]) |c| total += recordCountPrunes(io, &c.record, "t");
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
            if (recordCountPrunes(io, &conns[i].record, "t") >= 1) pruned += 1;
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
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    while (i < n) : (i += 1) {
        const p = testPeer(@intCast(20 + i));
        peers[i] = p;
        conns[i] = try connectFakePeer(io, allocator, router, p);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, "t", true) } });
    }
    // Sync so all three inbound subscriptions are fully processed (the GRAFT
    // selection reads each peer's announced topics).
    try sync(router, io);

    // Mesh starts empty; one heartbeat grafts all three (3 < D_low).
    try std.testing.expectEqual(@as(usize, 0), router.meshSize("t"));
    try beatHeartbeats(io, router, 1);
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
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    while (i < n) : (i += 1) {
        const p = testPeer(@intCast(30 + i));
        peers[i] = p;
        conns[i] = try connectFakePeer(io, allocator, router, p);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, "t", true) } });
    }
    // Sync so all inbound subscriptions are fully processed before subscribing
    // locally (the eager subscribe-join reads each peer's announced topics).
    try sync(router, io);

    // The eager subscribe-join grafts up to D; subscribeAndWait syncs, so the
    // mesh is at D right after it returns.
    try subscribeAndWait(io, allocator, router, "t");
    try std.testing.expectEqual(mesh_params.d, router.meshSize("t"));
    // A heartbeat must not push it past D (D >= D_low → no further graft).
    try beatHeartbeats(io, router, 1);
    try std.testing.expectEqual(mesh_params.d, router.meshSize("t"));

    // Exactly D GRAFTs were sent in total (one per grafted peer); the other
    // n - D candidates got none. The writers flush asynchronously, so poll until
    // the total reaches D (the mesh-state assertions above already settled it).
    var waited: u64 = 0;
    var grafts: usize = 0;
    while (waited < 2000) : (waited += 5) {
        grafts = 0;
        for (conns) |c| grafts += recordCountGrafts(io, &c.record, "t");
        if (grafts >= mesh_params.d) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(mesh_params.d, grafts);
}

test "PRUNE removes peer from mesh and backs it off (a later GRAFT is rejected)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // P joins the mesh, then PRUNEs us (with a small backoff < default floor).
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundPrune(allocator, "t", 5) } });

    // Sync so the prune is fully applied (peer removed from mesh), then read once.
    try sync(router, io);
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
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

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

test "peer disconnect cleans the peer out of mesh but keeps its backoff" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    // We subscribe to "t" (so a GRAFT(t) is accepted into the mesh) but NOT to
    // "t2" (so a GRAFT(t2) is rejected, which backs P off for "t2"). After this P
    // has an entry in BOTH mesh["t"] and backoff["t2"].
    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try std.testing.expectEqual(GraftOutcome.rejected, try graftAndWait(io, allocator, router, conn, peer, "t2"));
    try std.testing.expect(router.meshContains("t", peer));
    try std.testing.expect(router.inBackoff("t2", peer));

    // Disconnect P: it must be removed from every mesh, but its prune-backoff
    // must PERSIST (backoff is keyed by peer+topic, independent of the
    // connection — otherwise a quick reconnect would bypass it).
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));

    // Sync so the disconnect (mesh cleanup) is fully applied before the reads.
    try sync(router, io);
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(router.inBackoff("t2", peer));
    // The testing allocator's end-of-test leak check confirms destroy() freed the
    // (now peer-less) mesh/backoff topic entries with no leak.
}

test "prune-backoff persists across a disconnect+reconnect (a reconnect cannot bypass it)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // P joins the mesh, then PRUNEs us (default-floor backoff) → P is removed from
    // the mesh and backed off for prune_backoff_ticks.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundPrune(allocator, "t", 0) } });
    try sync(router, io);
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(router.inBackoff("t", peer));

    // P disconnects, then immediately reconnects (fresh conn so its record starts
    // clean). The backoff must survive the disconnect: clearing it here is exactly
    // the bug — it would let a pruned peer bypass the backoff by reconnecting.
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));
    try sync(router, io);
    try std.testing.expect(router.inBackoff("t", peer));

    const conn2 = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn2);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try std.testing.expect(peerCountIsOne(router));

    // The first GRAFT after reconnect must be REJECTED (still in backoff) and earn
    // a PRUNE back — the reconnect did NOT reset the backoff.
    try std.testing.expectEqual(GraftOutcome.rejected, try graftAndWait(io, allocator, router, conn2, peer, "t"));
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(waitPruneSent(io, conn2, "t"));

    // Age the backoff out. The PRUNE set the expiry to prune_backoff_ticks (no
    // heartbeat had run, so heartbeat_tick was 0); graftAndWait posts no
    // heartbeats, so exactly prune_backoff_ticks beats reach the expiry tick and a
    // GRAFT is then accepted.
    try beatHeartbeats(io, router, mesh_params.prune_backoff_ticks);
    try std.testing.expect(!router.inBackoff("t", peer));
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn2, peer, "t"));
    try std.testing.expect(router.meshContains("t", peer));
}

test "disconnect+reconnect churn: mesh reforms and pub/sub resumes with clean state" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // P is a mesh member; S is a separate relay source feeding publishes in. (A
    // relayed publish forwards only over the mesh, so P — not S — receives it.)
    const peer = testPeer(1);
    const source = testPeer(2);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try std.testing.expect(router.meshContains("t", peer));

    // A relayed publish reaches mesh member P.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x01", "t", "before"),
    } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn.record, "t", "before") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn.record, "t", "before"));

    // P disconnects: it leaves the mesh, but only P (not S).
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router)); // only S remains
    try sync(router, io);
    try std.testing.expect(!router.meshContains("t", peer));
    // P was never pruned, so it is not backed off; a clean reconnect can re-graft.
    try std.testing.expect(!router.inBackoff("t", peer));

    // P reconnects (fresh conn) and re-grafts: the mesh reforms with exactly P.
    const conn2 = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn2);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn2, peer, "t"));
    try std.testing.expect(router.meshContains("t", peer));
    try std.testing.expectEqual(@as(usize, 1), router.meshSize("t"));

    // Pub/sub resumes: a second relayed publish reaches the re-grafted P (on its
    // new conn — the first publish landed on the old, now-destroyed conn's record).
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x02", "t", "after"),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn2.record, "t", "after") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn2.record, "t", "after"));
    // The testing allocator's end-of-test leak check confirms the churn left no
    // stale per-peer allocations (queues / sinks / topic sets / dont_send).
}

test "many disconnect+reconnect churn cycles leave no leak, no crash, and clean state" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    defer router.destroy();
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);

    // Loop connect → graft → disconnect many times for the same peer. Each cycle
    // fully tears the peer down (queue close + writer join + frees) and rebuilds
    // it; a leak in that path would trip the testing allocator at end of test, and
    // a use-after-free or double-free would crash. Backoff is untouched here (P is
    // never pruned), so every re-graft is accepted.
    var cycle: usize = 0;
    while (cycle < 20) : (cycle += 1) {
        const conn = try connectFakePeer(io, allocator, router, peer);
        try std.testing.expect(peerCountIsOne(router));
        try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
        try std.testing.expect(router.meshContains("t", peer));

        try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
        try std.testing.expect(waitFor(io, peerCountIsZero, router));
        try sync(router, io);
        try std.testing.expect(!router.meshContains("t", peer));
        // The conn is freed only after the peer is fully torn down (writer joined),
        // so its record is no longer mutated — safe to destroy here each cycle.
        destroyFakeConn(allocator, conn);
    }

    // Final state is clean: no peers, P not in the mesh, no backoff lingering.
    try std.testing.expect(peerCountIsZero(router));
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(!router.inBackoff("t", peer));
}

test "PRUNE with a max-u64 backoff does not crash and actually backs the peer off" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // P joins the mesh, then PRUNEs us with an attacker-controlled max-u64 backoff.
    // Computing the expiry as heartbeat_tick + backoff would overflow: a panic in
    // this Debug build, or (in release) a wrap to a near-zero expiry that silently
    // disables the backoff. The saturating add must avoid both.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try router.inbox.putOne(io, .{
        .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundPrune(allocator, "t", std.math.maxInt(u64)) },
    });

    // Sync so the prune is fully applied (peer removed from mesh) — proves we did
    // not crash processing it — then read once.
    try sync(router, io);
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
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // Fill the mesh to D_high with distinct peers, each via an accepted GRAFT.
    const d_high = mesh_params.d_high;
    var conns: [mesh_params.d_high]*FakeTransport.FakeConn = undefined;
    var peers: [mesh_params.d_high]PeerId = undefined;
    var filled: usize = 0;
    defer for (conns[0..filled]) |c| destroyFakeConn(allocator, c);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
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
    const prunes_before = recordCountPrunes(io, &member_conn.record, "t");
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = member, .rpc = try buildInboundGraft(allocator, "t") } });
    try beatHeartbeats(io, router, 1);
    try std.testing.expect(router.meshContains("t", member));
    try std.testing.expectEqual(d_high, router.meshSize("t"));
    try std.testing.expectEqual(prunes_before, recordCountPrunes(io, &member_conn.record, "t"));
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
    try router.start();

    // We do NOT subscribe to "t". Connect two peers that DO subscribe to "t".
    const peer_a = testPeer(1);
    const peer_b = testPeer(2);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try connectFakePeer(io, allocator, router, peer_b);
    defer destroyFakeConn(allocator, conn_b);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_a, .rpc = try buildInboundSub(allocator, "t", true) } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_b, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    // Sync so both inbound subscriptions are fully processed, then read once.
    try sync(router, io);
    try std.testing.expect(peerTracksTopic(router, peer_a, "t") and peerTracksTopic(router, peer_b, "t"));

    // Publish on the unsubscribed topic: a fanout["t"] forms (up to D) from the
    // subscribed peers and the message is forwarded to them.
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "fan"),
    } });

    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "fan") >= 1 and
            recordCountPublishes(io, &conn_b.record, "t", "fan") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "fan"));
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_b.record, "t", "fan"));
    // The fanout set exists and last-publish was stamped (we never subscribed, so
    // there is no mesh for "t"). Sync so the publish's fanout-state mutation is
    // fully applied before reading the router's fanout/mesh maps.
    try sync(router, io);
    try std.testing.expect(router.fanout.contains("t"));
    try std.testing.expect(router.fanout_last_pub.contains("t"));
    try std.testing.expect(!router.mesh.contains("t"));

    // Drive more than FanoutTTL heartbeats with no further publish: the fanout
    // topic must be dropped (freed). One extra beat past the TTL guarantees the
    // strict `> ttl` comparison fires. beatHeartbeats syncs, so the expiry is
    // fully applied; read once.
    try beatHeartbeats(io, router, mesh_params.fanout_ttl_ticks + 1);
    try std.testing.expect(!router.fanout.contains("t"));
    try std.testing.expect(!router.fanout_last_pub.contains("t"));
}

test "subscribe JOINs the mesh (eager GRAFTs); unsubscribe LEAVEs it (PRUNEs)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    // Connect three candidate peers, each subscribed to "t", BEFORE we subscribe,
    // so the eager subscribe-join finds them.
    const n = 3;
    var conns: [n]*FakeTransport.FakeConn = undefined;
    var peers: [n]PeerId = undefined;
    var i: usize = 0;
    defer for (conns[0..]) |c| destroyFakeConn(allocator, c);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    while (i < n) : (i += 1) {
        const p = testPeer(@intCast(40 + i));
        peers[i] = p;
        conns[i] = try connectFakePeer(io, allocator, router, p);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, "t", true) } });
    }
    // Sync so all three inbound subscriptions are fully processed (the eager
    // subscribe-join reads each peer's announced topics).
    try sync(router, io);

    // Subscribe → eager JOIN: all three (< D) are grafted into the mesh and each
    // gets a GRAFT. subscribeAndWait syncs, so the eager join is fully applied.
    try subscribeAndWait(io, allocator, router, "t");
    try std.testing.expectEqual(@as(usize, n), router.meshSize("t"));
    for (peers, 0..) |p, idx| {
        try std.testing.expect(router.meshContains("t", p));
        try std.testing.expect(waitGraftSent(io, conns[idx], "t"));
    }

    // Unsubscribe → LEAVE: a PRUNE is sent to every mesh member and mesh["t"] is
    // cleared (the topic key freed). Sync so the leave is fully applied, then read.
    try router.inbox.putOne(io, .{ .unsubscribe = .{ .topic = try allocator.dupe(u8, "t") } });
    try sync(router, io);
    try std.testing.expect(!router.my_topics.contains("t"));
    try std.testing.expect(!router.mesh.contains("t"));
    for (conns) |c| try std.testing.expect(waitPruneSent(io, c, "t"));
}

test "unsubscribe LEAVE uses the shorter unsubscribe backoff (wire + local), not prune_backoff" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // Graft P into the mesh for "t".
    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try std.testing.expect(router.meshContains("t", peer));

    // Unsubscribe → LEAVE: P gets a PRUNE and is backed off. Both the wire backoff
    // and our local backoff must be the SHORTER unsubscribe_backoff_ticks (10), not
    // prune_backoff_ticks (60).
    try router.inbox.putOne(io, .{ .unsubscribe = .{ .topic = try allocator.dupe(u8, "t") } });
    try sync(router, io);
    try std.testing.expect(waitPruneSent(io, conn, "t"));

    // The emitted PRUNE carries unsubscribe_backoff_ticks in its wire backoff.
    try std.testing.expectEqual(
        @as(?u64, mesh_params.unsubscribe_backoff_ticks),
        recordFirstPruneBackoff(io, &conn.record, "t"),
    );

    // The LOCAL backoff is the SHORT one (10), not prune_backoff_ticks (60). No
    // heartbeat has run (tick is 0), so the expiry is exactly
    // unsubscribe_backoff_ticks. After one fewer beat P is still backed off; after
    // the full unsubscribe window it has cleared — a normal prune (60) would still
    // be in backoff here.
    try std.testing.expect(router.inBackoff("t", peer));
    try beatHeartbeats(io, router, mesh_params.unsubscribe_backoff_ticks - 1);
    try std.testing.expect(router.inBackoff("t", peer));
    try beatHeartbeats(io, router, 1);
    try std.testing.expect(!router.inBackoff("t", peer));

    // Resubscribe (a GRAFT on an unsubscribed topic is rejected regardless of
    // backoff), then a GRAFT from P is now accepted — the short backoff has expired.
    try subscribeAndWait(io, allocator, router, "t");
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try std.testing.expect(router.meshContains("t", peer));
}

test "handlePrune honors a shorter advertised backoff (not floored to prune_backoff)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // P joins the mesh, then PRUNEs us advertising the SHORT unsubscribe backoff
    // (10) — as a peer that is leaving the topic does. We must honor 10, NOT floor
    // it up to prune_backoff_ticks (60), or the feature is defeated.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    const advertised = mesh_params.unsubscribe_backoff_ticks;
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundPrune(allocator, "t", advertised) } });
    try sync(router, io);
    try std.testing.expect(!router.meshContains("t", peer));
    try std.testing.expect(router.inBackoff("t", peer));

    // We backed off exactly the advertised ~10 ticks, NOT a floored 60: still in
    // backoff one beat short of the advertised window, cleared at the window. (No
    // heartbeat ran before the PRUNE, so the expiry is exactly `advertised`.)
    try beatHeartbeats(io, router, advertised - 1);
    try std.testing.expect(router.inBackoff("t", peer));
    try beatHeartbeats(io, router, 1);
    try std.testing.expect(!router.inBackoff("t", peer));

    // A GRAFT from P is now accepted — the short advertised backoff has expired.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try std.testing.expect(router.meshContains("t", peer));
}

test "over-degree heartbeat prune still carries prune_backoff_ticks (not the unsubscribe one)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // Connect d_high + 1 peers, each subscribed to "t", and graft them all in so
    // the mesh is over D_high. The heartbeat then prunes the excess down to D.
    const n = mesh_params.d_high + 1;
    var conns = try allocator.alloc(*FakeTransport.FakeConn, n);
    defer allocator.free(conns);
    var peers = try allocator.alloc(PeerId, n);
    defer allocator.free(peers);
    defer for (conns) |c| destroyFakeConn(allocator, c);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    for (0..n) |idx| {
        const p = testPeer(@intCast(70 + idx));
        peers[idx] = p;
        conns[idx] = try connectFakePeer(io, allocator, router, p);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, "t", true) } });
        try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conns[idx], p, "t"));
    }
    try std.testing.expectEqual(n, router.meshSize("t"));

    // One heartbeat prunes the mesh back to D. Each pruned victim got a PRUNE; any
    // such PRUNE must carry the NORMAL prune_backoff_ticks (60), not the unsubscribe
    // backoff — this is mesh maintenance, not a LEAVE.
    try beatHeartbeats(io, router, 1);
    try std.testing.expectEqual(mesh_params.d, router.meshSize("t"));

    // Wait for the PRUNE control frames to land on the pruned peers' streams (the
    // writer fibers drain asynchronously) before reading their backoff.
    const want_pruned = n - mesh_params.d;
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        var total: usize = 0;
        for (conns) |c| total += recordCountPrunes(io, &c.record, "t");
        if (total >= want_pruned) break;
        io_time.ms(5).sleep(io) catch {};
    }

    var found_prune = false;
    for (conns) |c| {
        if (recordFirstPruneBackoff(io, &c.record, "t")) |b| {
            try std.testing.expectEqual(mesh_params.prune_backoff_ticks, b);
            found_prune = true;
        }
    }
    try std.testing.expect(found_prune);
}

// --- gossip: cache-on-accept + IHAVE / IWANT fake tests --------------------

test "cache-on-accept: an accepted message with an EMPTY mesh is still cached" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
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
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    const from = "origin";
    const seqno = "\x00\x00\x00\x09";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "lonely"),
    } });

    var id = try rpc.messageId(allocator, from, seqno);
    defer id.deinit(allocator);

    // Sync so the inbound RPC is fully processed (and the id cached), then read.
    try sync(router, io);
    try std.testing.expect(router.message_cache.get(id.bytes) != null);
    // No mesh member existed, so nothing was forwarded to S either.
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_s.record, "t", "lonely"));
}

test "IHAVE emission: heartbeat advertises cached ids to a non-mesh subscriber" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
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
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // P announces it subscribes to "t" (so it is a gossip target) but never GRAFTs.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_p, .rpc = try buildInboundSub(allocator, "t", true) } });
    // P also PRUNEs us for "t" so the heartbeat's mesh maintenance will NOT graft
    // it into the mesh (a backed-off peer is not a graft candidate). It stays a
    // gossip target, since gossip-target selection ignores backoff. This keeps P a
    // lazy/non-mesh subscriber across the heartbeat the test then drives.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_p, .rpc = try buildInboundPrune(allocator, "t", 0) } });
    // Sync so P's subscribe + prune are fully processed, then read once.
    try sync(router, io);
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
    // Sync so the publish is fully processed (cached), then read the cache once.
    try sync(router, io);
    try std.testing.expect(router.message_cache.get(id.bytes) != null);

    // One heartbeat: P receives an IHAVE(t, [id]).
    try beatHeartbeats(io, router, 1);
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordHasIHave(io, &conn_p.record, "t", id.bytes)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(recordHasIHave(io, &conn_p.record, "t", id.bytes));
}

test "inbound IHAVE: unseen id triggers an IWANT; a seen id triggers none" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer_p = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // P announces an IHAVE for an id we have NOT seen → we reply with an IWANT.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundIHave(allocator, "t", &[_]?[]const u8{"unseen-id"}),
    } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountIWants(io, &conn_p.record, "unseen-id") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountIWants(io, &conn_p.record, "unseen-id"));

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
    // Sync so the publish is fully processed (id cached/seen) before reading.
    try sync(router, io);
    try std.testing.expect(router.message_cache.get(id.bytes) != null);
    // IHAVE the now-seen id; no new IWANT for it must be sent.
    const iwants_before = recordCountIWants(io, &conn_p.record, id.bytes);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundIHave(allocator, "t", &[_]?[]const u8{id.bytes}),
    } });
    // Drive a heartbeat afterward: the single-fiber FIFO guarantees the IHAVE is
    // fully processed by the time the heartbeat is, so any spurious IWANT shows.
    try beatHeartbeats(io, router, 1);
    try std.testing.expectEqual(iwants_before, recordCountIWants(io, &conn_p.record, id.bytes));
}

test "inbound IWANT: a cached id is served as the full publish; an unknown id is not" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    // Subscribe to "t" and connect P (the requester). Publish locally so the
    // message is cached and addressable by its id.
    try subscribeAndWait(io, allocator, router, "t");
    const peer_p = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Our local publish uses (local_peer, <the wall-clock-seeded initial seqno>)
    // as its id. The counter is seeded at create and only the router fiber
    // advances it, so reading it after the sync barrier (one publish processed)
    // gives initial = current - 1.
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "served-data"),
    } });
    const from = local_test_peer.bytes[0..local_test_peer.len];
    // Sync so the local publish is fully processed (cached), then read the cache.
    try sync(router, io);
    var seqno_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &seqno_buf, router.seqno - 1, .big);
    var id = try rpc.messageId(allocator, from, &seqno_buf);
    defer id.deinit(allocator);
    try std.testing.expect(router.message_cache.get(id.bytes) != null);
    var waited: u64 = 0;

    // P sends an IWANT for the cached id AND an unknown id. Only the cached one is
    // served (as the full publish on P's data lane); the unknown id is ignored.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundIWant(allocator, &[_]?[]const u8{ id.bytes, "no-such-id" }),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_p.record, "t", "served-data") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_p.record, "t", "served-data"));
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
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");
    const peer_p = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

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
        if (recordCountIWants(io, &conn_p.record, id.bytes) >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountIWants(io, &conn_p.record, id.bytes));

    // Leg 3+4: P serves the requested message as a normal publish; the router's
    // standard inbound path delivers it locally (the handler fires) and caches it.
    // The local delivery (onMessage) and the cache insert both run on the router
    // fiber inside onInboundRpc, so a sync guarantees both are done before reading.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "recovered"),
    } });
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
    try std.testing.expectEqualSlices(u8, "t", rec.topic.?);
    try std.testing.expectEqualSlices(u8, "recovered", rec.data.?);
    // And the recovered message is now cached locally too.
    try std.testing.expect(router.message_cache.get(id.bytes) != null);
}

/// Connect `n` peers (seeds `first_seed .. first_seed+n`), announce each as a
/// non-mesh subscriber of `topic` (inbound SUBSCRIBE) and PRUNE each so the
/// heartbeat's mesh maintenance never grafts it (a backed-off peer is not a graft
/// candidate). The peers stay lazy gossip-eligible subscribers. Conns are written
/// into `conns[0..n]`; the caller owns + frees them. Syncs once at the end so all
/// peers are fully wired before the caller reads state.
fn connectLazySubscribers(
    io: std.Io,
    allocator: std.mem.Allocator,
    router: *FakeRouter,
    topic: []const u8,
    first_seed: u8,
    n: usize,
    conns: []*FakeTransport.FakeConn,
) !void {
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const peer = testPeer(first_seed + @as(u8, @intCast(i)));
        conns[i] = try connectFakePeer(io, allocator, router, peer);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, topic, true) } });
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundPrune(allocator, topic, 0) } });
    }
    try sync(router, io);
}

/// Count how many of the `n` recordings hold at least one IHAVE for `topic`.
/// Spins (bounded) until the count stabilises at `expected` or the wait elapses,
/// so the asynchronous writer flush has time to land every IHAVE.
fn countRecordsWithIHave(io: std.Io, conns: []*FakeTransport.FakeConn, n: usize, topic: []const u8) usize {
    var count: usize = 0;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (recordHasAnyIHave(io, &conns[i].record, topic)) count += 1;
    }
    return count;
}

test "gossip factor: emits IHAVE to max(0.25*eligible, d_lazy) lazy subscribers" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // 40 non-mesh gossip-eligible subscribers. 0.25 * 40 = 10 > d_lazy (6), so the
    // heartbeat must IHAVE exactly 10 of them (gossip_factor scaling).
    const n = 40;
    var conns: [n]*FakeTransport.FakeConn = undefined;
    try connectLazySubscribers(io, allocator, router, "t", 1, n, &conns);
    defer for (conns) |c| destroyFakeConn(allocator, c);

    // A relay source delivers a publish so the heartbeat has a cached id to gossip.
    const source = testPeer(200);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    const from = "origin";
    const seqno = "\x00\x00\x00\x42";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "gossiped"),
    } });
    try sync(router, io);

    try beatHeartbeats(io, router, 1);
    // Let the asynchronous writer flush land every IHAVE before counting.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (countRecordsWithIHave(io, &conns, n, "t") >= 10) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 10), countRecordsWithIHave(io, &conns, n, "t"));
}

test "gossip factor floor: few eligible peers still gossips to d_lazy" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // 8 non-mesh subscribers: 0.25 * 8 = 2 < d_lazy (6), so the floor applies and
    // the heartbeat IHAVEs all 6 it can fill (max(2, 6) capped by the 8 available).
    const n = 8;
    var conns: [n]*FakeTransport.FakeConn = undefined;
    try connectLazySubscribers(io, allocator, router, "t", 1, n, &conns);
    defer for (conns) |c| destroyFakeConn(allocator, c);

    const source = testPeer(200);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    const from = "origin";
    const seqno = "\x00\x00\x00\x43";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "gossiped"),
    } });
    try sync(router, io);

    try beatHeartbeats(io, router, 1);
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (countRecordsWithIHave(io, &conns, n, "t") >= 6) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 6), countRecordsWithIHave(io, &conns, n, "t"));
}

test "gossip retransmission: same id served at most gossip_retransmission times per peer" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");
    const peer_p = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Cache a message (local publish) so its id is serveable by IWANT. The id
    // uses the wall-clock-seeded seqno: read it back after the sync barrier
    // (one publish processed → initial = current - 1).
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "served-data"),
    } });
    const from = local_test_peer.bytes[0..local_test_peer.len];
    try sync(router, io);
    var seqno_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &seqno_buf, router.seqno - 1, .big);
    var id = try rpc.messageId(allocator, from, &seqno_buf);
    defer id.deinit(allocator);
    try std.testing.expect(router.message_cache.get(id.bytes) != null);

    // P IWANTs the same id 4 times (gossip_retransmission is 3). Each IWANT is a
    // separate handleIWant call; sync between them so the served count is updated
    // before the next request is processed. The first 3 are served, the 4th is not.
    var k: usize = 0;
    while (k < 4) : (k += 1) {
        try router.inbox.putOne(io, .{ .inbound_rpc = .{
            .peer = peer_p,
            .rpc = try buildInboundIWant(allocator, &[_]?[]const u8{id.bytes}),
        } });
        try sync(router, io);
    }

    // The data lane carried the served publish exactly gossip_retransmission times.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_p.record, "t", "served-data") >= mesh_params.gossip_retransmission) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give any (wrong) 4th serve a chance to land before asserting the cap held.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(
        @as(usize, mesh_params.gossip_retransmission),
        recordCountPublishes(io, &conn_p.record, "t", "served-data"),
    );
}

test "max IHAVE messages: only the first max_ihave_messages IHAVEs per heartbeat are processed" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer_p = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // P sends 12 separate IHAVE messages, each advertising a distinct unseen id.
    // Only the first max_ihave_messages (10) are processed → exactly 10 ids draw an
    // IWANT; ids 11-12 are dropped (peer over its per-heartbeat IHAVE budget).
    const total = 12;
    var ids: [total][]u8 = undefined;
    var made: usize = 0;
    defer for (ids[0..made]) |b| allocator.free(b);
    var i: usize = 0;
    while (i < total) : (i += 1) {
        ids[i] = try std.fmt.allocPrint(allocator, "ihave-id-{d}", .{i});
        made += 1;
        try router.inbox.putOne(io, .{ .inbound_rpc = .{
            .peer = peer_p,
            .rpc = try buildInboundIHave(allocator, "t", &[_]?[]const u8{ids[i]}),
        } });
    }
    try sync(router, io);

    // Wait for the IWANTs for the first 10 ids to flush.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountIWants(io, &conn_p.record, ids[9]) >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Ids 0..9 each drew exactly one IWANT; ids 10..11 (over budget) drew none.
    i = 0;
    while (i < total) : (i += 1) {
        const expected: usize = if (i < mesh_params.max_ihave_messages) 1 else 0;
        try std.testing.expectEqual(expected, recordCountIWants(io, &conn_p.record, ids[i]));
    }

    // A heartbeat resets the per-peer IHAVE budget; P can be served IHAVEs again.
    try beatHeartbeats(io, router, 1);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundIHave(allocator, "t", &[_]?[]const u8{ids[11]}),
    } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountIWants(io, &conn_p.record, ids[11]) >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountIWants(io, &conn_p.record, ids[11]));
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
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

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
    const prunes_before = recordCountPrunes(io, &conn.record, "t2");
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundGraft(allocator, "t2") } });
    try beatHeartbeats(io, router, 1);
    try std.testing.expectEqual(prunes_before, recordCountPrunes(io, &conn.record, "t2"));
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
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // The peer GRAFTs in (score still 0, accepted) and becomes a mesh member.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn, peer, "t"));
    try std.testing.expect(router.meshContains("t", peer));

    // Drive it negative (2 invalid → -4 < 0) but keep it above graylist (-10) so
    // its inbound RPCs are still processed (this is the prune gate, not graylist).
    try driveInvalid(io, allocator, router, peer, "t", 2);
    try std.testing.expect(liveScore(router, peer) < 0 and liveScore(router, peer) > -10.0);

    // One heartbeat prunes the negative peer out of the mesh + sends it a PRUNE.
    // beatHeartbeats syncs, so the prune is fully applied; read once.
    try beatHeartbeats(io, router, 1);
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
    try router.start();

    // Subscribe to "t" with no peers yet so the eager join grafts nobody; the
    // heartbeat would normally graft the subscribed candidate below.
    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // The peer announces it subscribes to "t" (a graft candidate) but we drive it
    // negative first (2 invalid → -4 < 0, still above graylist so the SUBSCRIBE is
    // processed). Send the subscribe BEFORE the rejects so it is recorded while
    // the peer is still at score 0.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", true) } });
    try sync(router, io);
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
    try std.testing.expectEqual(@as(usize, 0), recordCountGrafts(io, &conn.record, "t"));
}

test "scoring graft gate: an inbound GRAFT from a negative-score peer is rejected" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const router = try scoringRouter(allocator, io, &host_key, null);
    try router.start();

    // We DO subscribe to "t", so absent the score gate a GRAFT would be accepted.
    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

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
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Both announce they subscribe to "t" (so both are gossip candidates). PRUNE
    // each for "t" so the heartbeat's mesh maintenance does not graft them (a
    // backed-off peer is not a graft candidate); they stay lazy subscribers.
    for ([_]PeerId{ good, bad }) |p| {
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, "t", true) } });
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundPrune(allocator, "t", 0) } });
    }
    // Sync so both peers' subscribe + prune are fully processed, then read once.
    try sync(router, io);
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
    // Sync so the signed publish is fully processed (cached) before reading.
    try sync(router, io);
    try std.testing.expect(router.message_cache.get(id.bytes) != null);

    // Heartbeat: GOOD gets the IHAVE, BAD does not.
    try beatHeartbeats(io, router, 1);
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordHasIHave(io, &conn_good.record, "t", id.bytes)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(recordHasIHave(io, &conn_good.record, "t", id.bytes));
    // Give a (wrong) IHAVE to BAD a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expect(!recordHasIHave(io, &conn_bad.record, "t", id.bytes));
}

test "scoring events move the score: clean delivery raises it, sig-failure lowers it" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const router = try scoringRouter(allocator, io, &host_key, null);
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

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

/// The message id a signed publish from `host_key` with `seqno` will produce
/// under StrictSign: `from ++ seqno`, where `from` is the signer's marshaled
/// peer-id (matching `computeMessageId` → `rpc.messageId`). Caller frees the
/// returned slice. Lets a test IHAVE the exact id a later delivery fulfills.
fn signedMessageId(allocator: std.mem.Allocator, host_key: *const identity.KeyPair, seqno: []const u8) ![]u8 {
    var signer = try signing.Signer.init(allocator, host_key);
    defer signer.deinit();
    const from = signer.fromBytes();
    const id = try allocator.alloc(u8, from.len + seqno.len);
    @memcpy(id[0..from.len], from);
    @memcpy(id[from.len..], seqno);
    return id;
}

test "broken IWANT promise: a peer that IHAVEs an id it never serves is penalized" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const router = try scoringRouter(allocator, io, &host_key, null);
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // P advertises an unseen id (IHAVE) → we IWANT it, recording a promise that P
    // must serve the message by `iwant_followup_ticks`. P never delivers it.
    const id = "promised-but-never-served";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer,
        .rpc = try buildInboundIHave(allocator, "t", &[_]?[]const u8{id}),
    } });
    try sync(router, io);

    // Confirm the IWANT actually went out (so a promise was created).
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountIWants(io, &conn.record, id) >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountIWants(io, &conn.record, id));

    // The score before any heartbeat passes the deadline is 0 (no P-terms yet).
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), liveScore(router, peer), 1e-9);

    // Beat past the deadline. The promise was recorded at tick 0, so its deadline
    // is `iwant_followup_ticks`; it breaks once the tick exceeds that. After
    // `iwant_followup_ticks + 1` heartbeats the harvest charges P7: behaviour
    // penalty 1 → P7 = -(1^2) = -1 (weight -1, threshold 0, no decay).
    try beatHeartbeats(io, router, mesh_params.iwant_followup_ticks + 1);
    try std.testing.expectApproxEqAbs(@as(f64, -1.0), liveScore(router, peer), 1e-9);
}

test "fulfilled IWANT promise: delivering the promised message before the deadline avoids the penalty" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const router = try scoringRouter(allocator, io, &host_key, null);
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // The id P will advertise is exactly the id the signed message below produces
    // (`from ++ seqno`), so the delivery fulfills the promise for that id.
    const seqno = "\x00\x00\x00\x07";
    const id = try signedMessageId(allocator, &host_key, seqno);
    defer allocator.free(id);

    // P IHAVEs the id (unseen) → we IWANT it, recording a promise.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer,
        .rpc = try buildInboundIHave(allocator, "t", &[_]?[]const u8{id}),
    } });
    try sync(router, io);

    // P then DELIVERS the (signed) message before the deadline, fulfilling the
    // promise. This is a clean first delivery, so it also credits P2 (+1).
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer,
        .rpc = try signedInboundPublish(allocator, &host_key, seqno, "t", "served"),
    } });
    try beatHeartbeats(io, router, 1);

    // Beat well past the deadline. No promise remains, so no P7 penalty is charged
    // — the only live term is the P2 first-delivery credit, leaving score +1.
    try beatHeartbeats(io, router, mesh_params.iwant_followup_ticks + 2);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), liveScore(router, peer), 1e-9);
}

test "IWANT promise tracking is skipped when scoring is disabled (no penalty, no leak)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // No score config → scoring disabled; promise bookkeeping must be skipped
    // entirely (and the leak-checking allocator proves no id key is allocated).
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();
    try std.testing.expect(router.score == null);

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // IHAVE → IWANT still works without scoring: the id is requested.
    const id = "unserved-no-scoring";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer,
        .rpc = try buildInboundIHave(allocator, "t", &[_]?[]const u8{id}),
    } });
    try sync(router, io);

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountIWants(io, &conn.record, id) >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountIWants(io, &conn.record, id));

    // No promise was recorded (scoring off), so beating past the followup window
    // does nothing — and harvests nothing (no `score` engine to penalize either).
    try beatHeartbeats(io, router, mesh_params.iwant_followup_ticks + 2);
    // The peer's promise map stays empty; teardown (on destroy) frees nothing,
    // and the testing allocator verifies no leak.
    try std.testing.expectEqual(@as(usize, 0), peerPromiseCount(router, peer));
}

// --- anonymous mode (StrictNoSign) -----------------------------------------

/// Build an OwnedRpc carrying a single ANONYMOUS published message: only
/// topic+data, no from/seqno/signature/key (what an anonymous node puts on the
/// wire). The empty identity fields encode to absent on the wire.
fn anonymousInboundPublish(
    allocator: std.mem.Allocator,
    topic: []const u8,
    data: []const u8,
) !pubsub.OwnedRpc {
    const msg = rpc_pb.Message{ .topic = topic, .data = data };
    const frame = rpc_pb.RPC{ .publish = &[_]?rpc_pb.Message{msg} };
    return ownedFromRpc(allocator, frame);
}

/// Whether ANY recorded published frame on `topic` carrying `data` has every
/// identity field absent (from/seqno/signature/key) — the anonymous wire shape.
/// Reads under the record lock so the walk never races the writer fiber.
fn recordHasAnonymousPublish(io: std.Io, record: *FakeRecord, topic: []const u8, data: []const u8) bool {
    record.mutex.lockUncancelable(io);
    defer record.mutex.unlock(io);
    var rest = record.written.items;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        while (reader.publishNext()) |msg| {
            if (std.mem.eql(u8, msg.getTopic(), topic) and std.mem.eql(u8, msg.getData(), data)) {
                // Absent fields parse to null on the MessageReader (the encoder
                // omits empty bytes), so an anonymous frame has all four null.
                if (msg._from == null and msg._seqno == null and
                    msg._signature == null and msg._key == null) return true;
            }
        }
        rest = rest[decoded.total_len..];
    }
    return false;
}

test "seqno is seeded with wall-clock time, not 0 (restart must not reuse seen ids)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // A "restarted" node is just a second router created later: its seed must
    // be strictly ahead of the first's so no (from, seqno) id is ever reused
    // while peers still hold the old ids in their seen caches. A counter that
    // restarts at 0 fails exactly this.
    const a = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    const seed_a = a.seqno;
    a.destroy();
    try std.testing.expect(seed_a != 0);

    // Wall clocks tick in ns; even back-to-back creates are >0ns apart, but
    // sleep a moment to make the strict ordering robust on coarse clocks.
    io_time.ms(2).sleep(io) catch {};

    const b = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    const seed_b = b.seqno;
    b.destroy();
    try std.testing.expect(seed_b > seed_a);
}

test "anonymous publish omits identity fields and uses a content-based id" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Anonymous policy, no host key. local_test_peer is ignored on the wire.
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{ .signature_policy = .anonymous });
    // The create-time (wall-clock-seeded) counter value; read before start()
    // spawns the router fiber, so the read cannot race.
    const seqno0 = router.seqno;
    try router.start();

    // A peer subscribed to "t" so the flood-publish has a target to forward to.
    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", true) } });
    try sync(router, io);
    try std.testing.expect(peerTracksTopic(router, peer, "t"));

    // Publish: the forwarded frame must carry only topic+data — no identity.
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "anon"),
    } });
    try sync(router, io);
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn.record, "t", "anon") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(recordHasAnonymousPublish(io, &conn.record, "t", "anon"));

    // The message id is the content id sha256(topic ++ data), NOT from++seqno.
    var id = try rpc.contentMessageId(allocator, "t", "anon");
    defer id.deinit(allocator);
    try std.testing.expect(router.message_cache.get(id.bytes) != null);
    // The seqno counter was never advanced past its create-time seed (anonymous
    // publishes do not use one).
    try std.testing.expectEqual(seqno0, router.seqno);
}

test "anonymous: two publishes of the same (topic,data) dedup on the content id" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{ .signature_policy = .anonymous });
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", true) } });
    try sync(router, io);

    // Publish the SAME (topic, data) twice. Under from++seqno the seqno would
    // differ each time → two distinct ids → two forwards. Under the content id
    // both hash to the same id, so the second is a seen-duplicate and is dropped.
    for (0..2) |_| {
        try router.inbox.putOne(io, .{ .publish = .{
            .topic = try allocator.dupe(u8, "t"),
            .data = try allocator.dupe(u8, "dup"),
        } });
    }
    try sync(router, io);
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn.record, "t", "dup") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Let any (wrongly) un-deduped second publish land before asserting.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn.record, "t", "dup"));
}

test "anonymous: an unsigned inbound message is accepted (no verification) and forwarded over the mesh" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{ .signature_policy = .anonymous });
    try router.start();

    // We subscribe to "t" (so we deliver locally). A is a mesh member; B is a
    // non-mesh subscriber; S relays the anonymous publish. A received anonymous
    // message (no signature) is ACCEPTED — under strict_sign it would be dropped.
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
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // B announces it subscribes to "t" but is never grafted (non-mesh subscriber).
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_b, .rpc = try buildInboundSub(allocator, "t", true) } });
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expect(router.meshContains("t", peer_a));

    // S relays an anonymous (unsigned, no from/seqno) publish on "t".
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try anonymousInboundPublish(allocator, "t", "relayed"),
    } });
    try sync(router, io);

    // It is delivered locally (we subscribe) with an EMPTY `from` (anonymous).
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (rec.calls >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
    try std.testing.expectEqualSlices(u8, "t", rec.topic.?);
    try std.testing.expectEqualSlices(u8, "relayed", rec.data.?);
    try std.testing.expectEqual(@as(usize, 0), rec.from.?.len);

    // It is forwarded to the mesh member A (still anonymous on the wire), not to
    // the non-mesh subscriber B.
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "relayed") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "relayed"));
    try std.testing.expect(recordHasAnonymousPublish(io, &conn_a.record, "t", "relayed"));
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_b.record, "t", "relayed"));

    // A duplicate anonymous message (same topic+data → same content id) is
    // deduped: still exactly one local delivery and one forward to A.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try anonymousInboundPublish(allocator, "t", "relayed"),
    } });
    try sync(router, io);
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "relayed"));
}

test "create rejects an inconsistent signature policy" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // anonymous + a host key is rejected (anonymous must carry NO key).
    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();
    try std.testing.expectError(error.InvalidSignaturePolicy, FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, &host_key, null, .{ .signature_policy = .anonymous }));

    // strict_sign without a host key is rejected (nothing to sign with).
    try std.testing.expectError(error.InvalidSignaturePolicy, FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{ .signature_policy = .strict_sign }));

    // none + a host key is rejected (none must carry NO key).
    try std.testing.expectError(error.InvalidSignaturePolicy, FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, &host_key, null, .{ .signature_policy = .none }));
}

/// A custom message-id function for the override test: ignores from/seqno and
/// returns a fixed prefix ++ data, proving the override is honoured (and that the
/// publish and receive paths agree, since both go through computeMessageId).
fn fixedPrefixId(
    ctx: ?*anyopaque,
    topic: []const u8,
    from: []const u8,
    seqno: []const u8,
    data: []const u8,
    allocator: std.mem.Allocator,
) anyerror!rpc.MessageId {
    _ = ctx;
    _ = topic;
    _ = from;
    _ = seqno;
    const prefix = "ID-";
    const buf = try allocator.alloc(u8, prefix.len + data.len);
    @memcpy(buf[0..prefix.len], prefix);
    @memcpy(buf[prefix.len..], data);
    return .{ .bytes = buf };
}

test "a configured message_id_fn overrides the policy's id derivation" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Anonymous policy but with a custom id function: the cached id must be the
    // function's output, not the default content id.
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{
        .signature_policy = .anonymous,
        .message_id_fn = .{ .func = fixedPrefixId },
    });
    defer router.destroy();
    try router.start();

    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "payload"),
    } });
    try sync(router, io);

    // Cached under "ID-payload" (the override), and NOT under the default
    // content id sha256("t"++"payload").
    try std.testing.expect(router.message_cache.get("ID-payload") != null);
    var content = try rpc.contentMessageId(allocator, "t", "payload");
    defer content.deinit(allocator);
    try std.testing.expect(router.message_cache.get(content.bytes) == null);
}

// --- per-peer protocol-version tracking ------------------------------------

/// The version recorded for the peer tracked under `peer`, or null if untracked.
fn peerVersionOf(router: *FakeRouter, peer: PeerId) ?pubsub.Version {
    const state = router.peers.get(peerKey(&peer)) orelse return null;
    return state.protocol_version;
}

/// Sync so any previously-posted `peer_protocol` (or `peer_connected`) command
/// is fully processed, then read the peer's recorded version once and compare to
/// `want`. Returns whether it matched. Caller must post the version-affecting
/// command before calling this.
fn waitForVersion(io: std.Io, router: *FakeRouter, peer: PeerId, want: pubsub.Version) bool {
    sync(router, io) catch return false;
    return if (peerVersionOf(router, peer)) |v| v == want else false;
}

test "router records each peer's negotiated /meshsub version; peerSupportsV12 only for 1.2" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    // One peer per version. Each connects, then its inbound stream reports the
    // version it negotiated (as the inbound handler would via peer_protocol).
    const peer_10 = testPeer(0x10);
    const peer_11 = testPeer(0x11);
    const peer_12 = testPeer(0x12);
    const conn_10 = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_10);
    const conn_11 = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_11);
    const conn_12 = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_12);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer_10, .conn = conn_10, .remote_addr = dummy_addr } });
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer_11, .conn = conn_11, .remote_addr = dummy_addr } });
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer_12, .conn = conn_12, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, struct {
        fn pred(r: *FakeRouter) bool {
            return r.peerCount() == 3;
        }
    }.pred, router));

    // Sync so all three connects are fully processed before reading the peer map.
    try sync(router, io);
    // Before any peer_protocol, every peer defaults to the 1.1 baseline.
    try std.testing.expectEqual(pubsub.Version.v1_1, peerVersionOf(router, peer_10).?);
    try std.testing.expectEqual(pubsub.Version.v1_1, peerVersionOf(router, peer_12).?);
    try std.testing.expect(!router.peerSupportsV12(peer_12));

    router.postPeerProtocol(io, peer_10, .v1_0);
    router.postPeerProtocol(io, peer_11, .v1_1);
    router.postPeerProtocol(io, peer_12, .v1_2);

    try std.testing.expect(waitForVersion(io, router, peer_10, .v1_0));
    try std.testing.expect(waitForVersion(io, router, peer_11, .v1_1));
    try std.testing.expect(waitForVersion(io, router, peer_12, .v1_2));

    // peerSupportsV12 is true ONLY for the 1.2 peer.
    try std.testing.expect(!router.peerSupportsV12(peer_10));
    try std.testing.expect(!router.peerSupportsV12(peer_11));
    try std.testing.expect(router.peerSupportsV12(peer_12));
    // An untracked peer never supports 1.2.
    try std.testing.expect(!router.peerSupportsV12(testPeer(0x99)));
}

test "router adopts a version reported before peer_connected (inbound-before-connect race)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = testPeer(0x42);
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // The inbound stream negotiates + reports 1.2 BEFORE the peer-event callback
    // posts peer_connected (both are independent fibers; either can win). The
    // version is stashed and adopted when peer_connected creates the PeerState.
    router.postPeerProtocol(io, peer, .v1_2);
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });

    try std.testing.expect(waitForVersion(io, router, peer, .v1_2));
    try std.testing.expect(router.peerSupportsV12(peer));

    // Disconnect purges the version (the pending map too); tear down leak-clean.
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));
}

test "router purges a pending version when the peer disconnects without ever connecting" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = testPeer(0x55);
    // The connection that broke; the peer was never tracked, so the handler's
    // identity check is moot — any handle names the dead connection.
    const broken_conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, broken_conn);

    // A version note arrives but the matching peer_connected never does; a later
    // disconnect (e.g. the stream broke during negotiation) must drop the note so
    // it cannot leak or be inherited by a future connection.
    router.postPeerProtocol(io, peer, .v1_2);
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = broken_conn } });

    // Now connect for real: the peer must start at the baseline, not the purged
    // 1.2 note. All three commands run in order on the one router fiber, so the
    // connect already observes the purge; waitFor(peerCountIsOne) then confirms
    // the connect was processed before we read the version.
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));
    // Sync so the connect (which adopted/purged the pending version) is fully
    // applied before reading the peer map.
    try sync(router, io);
    try std.testing.expectEqual(pubsub.Version.v1_1, peerVersionOf(router, peer).?);
    try std.testing.expect(!router.peerSupportsV12(peer));

    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));
}

// --- IDONTWANT (v1.2) fake tests -------------------------------------------

/// Spin (bounded) until the peer tracked under `peer` has exactly `want` frames
/// on its data lane, then return whether it held. Used with a block_open sink (so
/// the writer never drains) to wait for the writer to pop its one priming frame
/// and park, leaving the lane at a known length.
fn waitForDataLen(io: std.Io, router: *FakeRouter, peer: PeerId, want: usize) bool {
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerDataLen(io, router, peer) == want) return true;
        io_time.ms(5).sleep(io) catch {};
    }
    return peerDataLen(io, router, peer) == want;
}

/// Clear a peer's `block_open` hold and wait until its writer has drained the
/// queue and parked back in popBlocking (data lane empty + a short settle for the
/// in-flight writeFrame to finish under the record lock). After this the writer no
/// longer touches the record, so teardown can free the record and cancel the
/// (popBlocking-parked) writer with no use-after-free and no cancel-collapsed
/// wait. Used as a `defer` so it runs before destroyFakeConn / router.destroy.
fn releaseAndQuiesceWriter(io: std.Io, router: *FakeRouter, conn: *FakeTransport.FakeConn, peer: PeerId) void {
    conn.record.block_open.store(false, .release);
    _ = waitForDataLen(io, router, peer, 0);
    // A short settle so an in-flight writeFrame (record append) completes before
    // the record is freed; mirrors the margins the other writer tests use.
    io_time.ms(50).sleep(io) catch {};
}

test "inbound IDONTWANT records the ids and purges matching queued frames" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Hold the peer's writer at the top of open() so it does not drain the queue:
    // frames pushed after the writer pops its one priming frame stay queued, where
    // the IDONTWANT purge is observable. The `defer` (runs FIRST, before
    // destroyFakeConn / router.destroy, since defers are LIFO) releases the writer
    // and waits for it to drain + park back in popBlocking, so teardown frees the
    // record only once the writer is quiescent — no cancel-collapsed wait, no
    // use-after-free of the record. Fires on success AND an early assertion-failure
    // return.
    conn.record.block_open.store(true, .release);
    defer releaseAndQuiesceWriter(io, router, conn, peer);

    // Push a priming frame (absorbs the writer's single pop), then two id-carrying
    // data frames for X and Y. With the writer parked in open(), X and Y remain
    // queued: data-lane length settles at 2.
    try router.enqueueDataForTest(peer, try testDataFrame(allocator));
    try router.enqueueDataForTest(peer, try testDataFrameWithId(allocator, "id-X"));
    try router.enqueueDataForTest(peer, try testDataFrameWithId(allocator, "id-Y"));
    try std.testing.expect(waitForDataLen(io, router, peer, 2));

    // P sends IDONTWANT([id-X]): X is recorded in dont_send and its queued frame
    // is purged; Y survives (its id was not in the IDONTWANT set).
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer,
        .rpc = try buildInboundIDontWant(allocator, &[_]?[]const u8{"id-X"}),
    } });
    try sync(router, io);

    try std.testing.expect(peerDontSendHas(router, peer, "id-X"));
    // One frame purged (X), one survives (Y). The deferred writer release above
    // then lets the writer drain Y and park before teardown frees the record.
    try std.testing.expectEqual(@as(usize, 1), peerDataLen(io, router, peer));
}

test "IDONTWANT length cap: only the first max_idontwant_length ids of one message are recorded" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // One IDONTWANT carrying more ids than the per-message cap. Only the first
    // max_idontwant_length are recorded in dont_send; the rest are ignored.
    const total = mesh_params.max_idontwant_length + 5;
    var ids: [total][]u8 = undefined;
    var made: usize = 0;
    defer for (ids[0..made]) |b| allocator.free(b);
    var id_views: [total]?[]const u8 = undefined;
    var i: usize = 0;
    while (i < total) : (i += 1) {
        ids[i] = try std.fmt.allocPrint(allocator, "idw-len-{d}", .{i});
        made += 1;
        id_views[i] = ids[i];
    }

    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer,
        .rpc = try buildInboundIDontWant(allocator, &id_views),
    } });
    try sync(router, io);

    // Exactly max_idontwant_length entries recorded: the first cap ids present,
    // the over-cap ids absent.
    try std.testing.expectEqual(mesh_params.max_idontwant_length, peerDontSendCount(router, peer));
    i = 0;
    while (i < total) : (i += 1) {
        const expected = i < mesh_params.max_idontwant_length;
        try std.testing.expectEqual(expected, peerDontSendHas(router, peer, ids[i]));
    }
}

test "IDONTWANT message cap: only max_idontwant_messages per heartbeat are processed; resets after a heartbeat" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // The peer sends more IDONTWANT messages than the per-heartbeat cap, each
    // carrying a distinct id. Only the first max_idontwant_messages are processed
    // (their ids recorded); ids from messages past the cap are dropped.
    const total = mesh_params.max_idontwant_messages + 5;
    var i: usize = 0;
    while (i < total) : (i += 1) {
        const id = try std.fmt.allocPrint(allocator, "idw-msg-{d}", .{i});
        defer allocator.free(id);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{
            .peer = peer,
            .rpc = try buildInboundIDontWant(allocator, &[_]?[]const u8{id}),
        } });
    }
    try sync(router, io);

    // Exactly max_idontwant_messages ids recorded (one per accepted message);
    // the over-cap messages contributed nothing.
    try std.testing.expectEqual(mesh_params.max_idontwant_messages, peerDontSendCount(router, peer));

    // A heartbeat resets the per-peer IDONTWANT budget; a fresh IDONTWANT is
    // processed again (its id recorded), so the count grows by one.
    try beatHeartbeats(io, router, 1);
    const after_id = "idw-msg-after-reset";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer,
        .rpc = try buildInboundIDontWant(allocator, &[_]?[]const u8{after_id}),
    } });
    try sync(router, io);
    try std.testing.expect(peerDontSendHas(router, peer, after_id));
    try std.testing.expectEqual(mesh_params.max_idontwant_messages + 1, peerDontSendCount(router, peer));
}

test "send-side skip: a message whose id a peer IDONTWANTed is not enqueued to it" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    // We subscribe to "t" and graft two peers P and Q into the mesh. P will
    // IDONTWANT the message id; Q will not. A relay source S then delivers the
    // message: Q receives it (mesh forward), P does not (send-side skip).
    try subscribeAndWait(io, allocator, router, "t");

    const peer_p = testPeer(1);
    const peer_q = testPeer(2);
    const source = testPeer(3);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);
    const conn_q = try connectFakePeer(io, allocator, router, peer_q);
    defer destroyFakeConn(allocator, conn_q);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_p, peer_p, "t"));
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_q, peer_q, "t"));

    // The relayed message's id is `from ++ seqno` = "origin" ++ seqno.
    const from = "origin";
    const seqno = "\x00\x00\x00\x42";
    var id = try rpc.messageId(allocator, from, seqno);
    defer id.deinit(allocator);

    // P IDONTWANTs that id BEFORE the message arrives, so the forward to P is
    // skipped. Sync so it is recorded before the relay.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundIDontWant(allocator, &[_]?[]const u8{id.bytes}),
    } });
    try sync(router, io);
    try std.testing.expect(peerDontSendHas(router, peer_p, id.bytes));

    // S relays the message on "t": Q (mesh, no IDONTWANT) gets it; P is skipped.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "skipme"),
    } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_q.record, "t", "skipme") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give a wrong forward to P a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_q.record, "t", "skipme"));
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_p.record, "t", "skipme"));
}

test "emit IDONTWANT on a large received message: to v1.2 mesh peers, not v1.1 or the source" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // The threshold knob is left at the default (1024); the large message below
    // clears it, the small one does not.
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    // A is a v1.2 mesh peer (should receive IDONTWANT), C is a v1.1 mesh peer
    // (should NOT — version-gated), S is the v1.2 relay source (should NOT — it is
    // the sender). A and C are grafted into the mesh; S relays from outside it.
    const peer_a = testPeer(1);
    const peer_c = testPeer(2);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_c = try connectFakePeer(io, allocator, router, peer_c);
    defer destroyFakeConn(allocator, conn_c);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    router.postPeerProtocol(io, peer_a, .v1_2);
    router.postPeerProtocol(io, peer_c, .v1_1);
    router.postPeerProtocol(io, source, .v1_2);
    try sync(router, io);

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_c, peer_c, "t"));

    // A SMALL message (< threshold) must NOT trigger any IDONTWANT.
    const small_seqno = "\x00\x00\x00\x01";
    var small_id = try rpc.messageId(allocator, "origin", small_seqno);
    defer small_id.deinit(allocator);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", small_seqno, "t", "tiny"),
    } });
    try sync(router, io);

    // A LARGE message (>= threshold) triggers IDONTWANT(id) to A only.
    const big = try allocator.alloc(u8, 2048);
    defer allocator.free(big);
    @memset(big, 'Z');
    const big_seqno = "\x00\x00\x00\x02";
    var big_id = try rpc.messageId(allocator, "origin", big_seqno);
    defer big_id.deinit(allocator);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", big_seqno, "t", big),
    } });
    try sync(router, io);

    // A (v1.2 mesh peer, not the source) receives the IDONTWANT for the large id.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountIDontWants(io, &conn_a.record, big_id.bytes) >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give a wrong send to C/S a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountIDontWants(io, &conn_a.record, big_id.bytes));
    // C is v1.1: version-gated out. S is the source: excluded. Neither gets it.
    try std.testing.expectEqual(@as(usize, 0), recordCountIDontWants(io, &conn_c.record, big_id.bytes));
    try std.testing.expectEqual(@as(usize, 0), recordCountIDontWants(io, &conn_s.record, big_id.bytes));
    // The small message produced no IDONTWANT to anyone.
    try std.testing.expectEqual(@as(usize, 0), recordCountIDontWants(io, &conn_a.record, small_id.bytes));
}

test "IDONTWANT dont_send entry expires after its TTL, un-skipping the message" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    // Subscribe to "t", graft P (mesh) and connect S (relay source).
    try subscribeAndWait(io, allocator, router, "t");
    const peer_p = testPeer(1);
    const source = testPeer(2);
    const conn_p = try connectFakePeer(io, allocator, router, peer_p);
    defer destroyFakeConn(allocator, conn_p);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_p, peer_p, "t"));

    const from = "origin";
    const seqno = "\x00\x00\x00\x77";
    var id = try rpc.messageId(allocator, from, seqno);
    defer id.deinit(allocator);

    // P IDONTWANTs the id (recorded at tick 0 → expiry = dont_send_ttl_ticks).
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_p,
        .rpc = try buildInboundIDontWant(allocator, &[_]?[]const u8{id.bytes}),
    } });
    try sync(router, io);
    try std.testing.expect(peerDontSendHas(router, peer_p, id.bytes));

    // Beat past the TTL: ttl+1 heartbeats reclaim the entry.
    try beatHeartbeats(io, router, mesh_params.dont_send_ttl_ticks + 1);
    try std.testing.expect(!peerDontSendHas(router, peer_p, id.bytes));

    // A later relay of that id is no longer skipped: P now receives it.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, from, seqno, "t", "again"),
    } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_p.record, "t", "again") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_p.record, "t", "again"));
}

// --- direct (explicit) peers ----------------------------------------------
//
// A direct peer has an out-of-band peering agreement (go-libp2p WithDirectPeers).
// It is treated as a trusted out-of-mesh forward target: every valid message on a
// topic it subscribes to reaches it (alongside the mesh, deduped), it is never
// grafted/pruned into a mesh (a GRAFT from it draws a PRUNE back), and it bypasses
// the score gates (graylist / GRAFT-negative-reject / publish threshold). These
// forwarding tests pass its id (and a placeholder dial address) through
// `RouterConfig.direct_peers`; the connection itself is driven by the test (it
// posts peer_connected). The auto-dial behaviour is covered by its own tests
// below, which inspect the FakeTransport's DialLog.

/// Placeholder dial address for the forwarding tests above, which configure a
/// direct peer but drive its connect manually (so the FakeTransport's recorded
/// dial is never inspected). A well-formed multiaddr string keeps the config
/// realistic even though no socket is opened.
const direct_test_addr = "/ip4/127.0.0.1/udp/9999/quic-v1";

test "direct peer receives a relayed message though it is NOT in the mesh, and is not double-sent when also a mesh member" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // D is a direct peer; A is a regular mesh peer; S relays a publish.
    const peer_d = testPeer(1);
    const peer_a = testPeer(2);
    const source = testPeer(3);

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{
        .direct_peers = &.{.{ .id = peer_d, .addr = direct_test_addr }},
    });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const conn_d = try connectFakePeer(io, allocator, router, peer_d);
    defer destroyFakeConn(allocator, conn_d);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Both D and A announce they subscribe to "t".
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_d, .rpc = try buildInboundSub(allocator, "t", true) } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_a, .rpc = try buildInboundSub(allocator, "t", true) } });

    // A GRAFTs into the mesh; D is direct, so a GRAFT from it would be refused
    // (covered by another test) — here D never grafts, it stays out of the mesh.
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expect(router.meshContains("t", peer_a));
    try std.testing.expect(!router.meshContains("t", peer_d));

    // S relays a publish on "t": both mesh member A and direct peer D receive it,
    // each EXACTLY ONCE (D via the direct forward path, A via the mesh).
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x01", "t", "hi"),
    } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_d.record, "t", "hi") >= 1 and
            recordCountPublishes(io, &conn_a.record, "t", "hi") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give any erroneous extra copy a chance to land before asserting exactly-once.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_d.record, "t", "hi"));
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "hi"));
}

test "direct peer that is ALSO a mesh-eligible subscriber is forwarded to exactly once" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // D is direct AND subscribes to "t". The heartbeat must never graft it into
    // the mesh (it is filtered out of candidate selection), so it only ever
    // receives messages via the direct path — and exactly once.
    const peer_d = testPeer(1);
    const source = testPeer(2);

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{
        .direct_peers = &.{.{ .id = peer_d, .addr = direct_test_addr }},
    });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const conn_d = try connectFakePeer(io, allocator, router, peer_d);
    defer destroyFakeConn(allocator, conn_d);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_d, .rpc = try buildInboundSub(allocator, "t", true) } });
    try sync(router, io);

    // Several heartbeats: the mesh is below D_low, so maintenance tries to graft —
    // but D, the only candidate, is direct and must be skipped. The mesh stays
    // empty of D and no GRAFT is ever sent to it.
    try beatHeartbeats(io, router, 3);
    try std.testing.expect(!router.meshContains("t", peer_d));
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 0), recordCountGrafts(io, &conn_d.record, "t"));

    // A relayed publish still reaches D once via the direct path.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x09", "t", "z"),
    } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_d.record, "t", "z") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_d.record, "t", "z"));
}

test "GRAFT from a direct peer is refused: it does NOT join the mesh and is PRUNEd back" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const peer_d = testPeer(1);

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{
        .direct_peers = &.{.{ .id = peer_d, .addr = direct_test_addr }},
    });
    try router.start();

    // We subscribe to "t" so that, were D a regular peer, its GRAFT would be
    // ACCEPTED — isolating the direct-peer refusal as the cause.
    try subscribeAndWait(io, allocator, router, "t");

    const conn_d = try connectFakePeer(io, allocator, router, peer_d);
    defer destroyFakeConn(allocator, conn_d);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // A GRAFT from the direct peer is refused: D never enters the mesh.
    try std.testing.expectEqual(GraftOutcome.rejected, try graftAndWait(io, allocator, router, conn_d, peer_d, "t"));
    try std.testing.expect(!router.meshContains("t", peer_d));

    // And it draws a PRUNE back (go warns + PRUNEs a direct peer that GRAFTs).
    try std.testing.expect(waitPruneSent(io, conn_d, "t"));
    try std.testing.expectEqual(@as(usize, 1), recordCountPrunes(io, &conn_d.record, "t"));
}

test "heartbeat never grafts or prunes a direct peer (mesh maintenance leaves it untouched)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // D is direct and subscribed; R is a regular subscriber. With the mesh below
    // D_low the heartbeat grafts candidates — it must graft R but never D.
    const peer_d = testPeer(1);
    const peer_r = testPeer(2);

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{
        .direct_peers = &.{.{ .id = peer_d, .addr = direct_test_addr }},
    });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const conn_d = try connectFakePeer(io, allocator, router, peer_d);
    defer destroyFakeConn(allocator, conn_d);
    const conn_r = try connectFakePeer(io, allocator, router, peer_r);
    defer destroyFakeConn(allocator, conn_r);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_d, .rpc = try buildInboundSub(allocator, "t", true) } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_r, .rpc = try buildInboundSub(allocator, "t", true) } });
    try sync(router, io);

    // Drive heartbeats: maintenance grafts R into the mesh (and GRAFTs it) but
    // skips D entirely — D is never added to the mesh and never sent a GRAFT.
    try beatHeartbeats(io, router, 3);
    try std.testing.expect(router.meshContains("t", peer_r));
    try std.testing.expect(!router.meshContains("t", peer_d));
    try std.testing.expect(waitGraftSent(io, conn_r, "t"));
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 0), recordCountGrafts(io, &conn_d.record, "t"));
    // D is out of the mesh, so it can never be a prune victim either.
    try std.testing.expectEqual(@as(usize, 0), recordCountPrunes(io, &conn_d.record, "t"));
}

test "flood-publish reaches a direct peer for a topic it subscribes to" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // We do NOT subscribe to "t" (we only publish). Under flood-publish (the
    // default), an originated message goes to every subscriber — including the
    // direct peer D — even with no mesh for the topic.
    const peer_d = testPeer(1);

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{
        .direct_peers = &.{.{ .id = peer_d, .addr = direct_test_addr }},
    });
    try router.start();

    const conn_d = try connectFakePeer(io, allocator, router, peer_d);
    defer destroyFakeConn(allocator, conn_d);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_d, .rpc = try buildInboundSub(allocator, "t", true) } });
    try sync(router, io);

    // Publish locally; D receives the originated message exactly once.
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "flood"),
    } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_d.record, "t", "flood") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_d.record, "t", "flood"));
}

test "scoring bypass: a direct peer at a NEGATIVE score is still forwarded to and not graylisted" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    // D is a direct peer; S relays a (valid, signed) publish. Scoring is ON with
    // StrictSign, so D's score can be driven below the graylist threshold via
    // invalid (unsigned) deliveries — yet D must stay trusted: its RPCs are still
    // processed (not graylisted) and it still receives forwards.
    const peer_d = testPeer(1);
    const source = testPeer(2);

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, &host_key, scoringConfig(), .{
        .direct_peers = &.{.{ .id = peer_d, .addr = direct_test_addr }},
    });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const conn_d = try connectFakePeer(io, allocator, router, peer_d);
    defer destroyFakeConn(allocator, conn_d);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Drive D well below the graylist threshold (-10): 4 invalid messages → -16.
    try driveInvalid(io, allocator, router, peer_d, "t", 4);
    try std.testing.expect(liveScore(router, peer_d) <= -10.0);

    // A SUBSCRIBE RPC from the graylist-deep direct peer is STILL processed (a
    // regular peer this far down would be ignored): D's announced-topics gains "t".
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_d, .rpc = try buildInboundSub(allocator, "t", true) } });
    try beatHeartbeats(io, router, 1);
    try std.testing.expect(peerTracksTopic(router, peer_d, "t"));

    // S relays a valid signed publish on "t": the direct peer D — though deeply
    // negative — still receives it (scoring does not gate a direct forward target).
    // The publish is signed by the host key so it passes StrictSign verification.
    const signed = try signedInboundPublish(allocator, &host_key, "\x00\x00\x00\x05", "t", "scored");
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = source, .rpc = signed } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_d.record, "t", "scored") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_d.record, "t", "scored"));
}

// --- direct-peer auto-connect (go DirectConnectTicks) ----------------------
//
// The router keeps configured direct peers connected: it dials them once at
// start, then every `direct_connect_ticks` heartbeats re-dials any that are not
// currently connected (go-libp2p `directConnect`). The FakeTransport records each
// dialed address into a test-owned DialLog so these tests can assert the cadence
// without opening real sockets — the test still drives any resulting connect by
// posting peer_connected, exactly as the forwarding tests above do.

/// The distinct dial address for a direct peer in the auto-connect tests.
const direct_addr_d = "/ip4/127.0.0.1/udp/4001/quic-v1";
const direct_addr_e = "/ip4/127.0.0.1/udp/4002/quic-v1";

test "direct-peer auto-connect: a configured, disconnected direct peer is dialed on start" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var log = DialLog{ .allocator = allocator };
    defer log.deinit(io);

    const peer_d = testPeer(1);

    // Heartbeat fiber disabled (interval 0): start's one-shot dial is the only
    // dial we expect, so the count is exactly one.
    const router = try FakeRouter.create(allocator, io, .{ .dial_log = &log }, local_test_peer, null, 0, null, null, .{
        .direct_peers = &.{.{ .id = peer_d, .addr = direct_addr_d }},
    });
    defer router.destroy();
    try router.start();
    // start() dials on the caller fiber; sync settles any router-fiber work too.
    try sync(router, io);

    try std.testing.expectEqual(@as(usize, 1), log.count(io, direct_addr_d));
}

test "direct-peer auto-connect: heartbeat re-dials a disconnected direct peer but not a connected one" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var log = DialLog{ .allocator = allocator };
    defer log.deinit(io);

    // D will be connected after start; E stays disconnected. Both are dialed once
    // at start; only the still-disconnected E is re-dialed at the direct-connect
    // tick.
    const peer_d = testPeer(1);
    const peer_e = testPeer(2);

    const router = try FakeRouter.create(allocator, io, .{ .dial_log = &log }, local_test_peer, null, 0, null, null, .{
        .direct_peers = &.{
            .{ .id = peer_d, .addr = direct_addr_d },
            .{ .id = peer_e, .addr = direct_addr_e },
        },
    });
    try router.start();
    try sync(router, io);

    // Both were dialed once at start (neither was connected yet).
    try std.testing.expectEqual(@as(usize, 1), log.count(io, direct_addr_d));
    try std.testing.expectEqual(@as(usize, 1), log.count(io, direct_addr_e));

    // D connects (the dial "succeeded"); E remains disconnected.
    const conn_d = try connectFakePeer(io, allocator, router, peer_d);
    defer destroyFakeConn(allocator, conn_d);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Beat exactly `direct_connect_ticks` heartbeats: the re-dial pass lands at
    // that tick (the tick is incremented before the modulo check, matching go).
    try beatHeartbeats(io, router, mesh_params.direct_connect_ticks);

    // E (disconnected) was re-dialed; D (connected) was left alone.
    try std.testing.expectEqual(@as(usize, 2), log.count(io, direct_addr_e));
    try std.testing.expectEqual(@as(usize, 1), log.count(io, direct_addr_d));
}

test "direct-peer auto-connect: a dialed direct peer that then connects is treated as direct (forwarded to)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var log = DialLog{ .allocator = allocator };
    defer log.deinit(io);

    // D is a direct peer; S relays a publish. The router dials D at start; the
    // test then simulates D's connect (peer_connected) and confirms D is treated
    // as direct — a relayed message on a topic D subscribes to is forwarded to it.
    const peer_d = testPeer(1);
    const source = testPeer(2);

    const router = try FakeRouter.create(allocator, io, .{ .dial_log = &log }, local_test_peer, null, 0, null, null, .{
        .direct_peers = &.{.{ .id = peer_d, .addr = direct_addr_d }},
    });
    try router.start();
    try sync(router, io);

    // The dial was kicked off at start.
    try std.testing.expectEqual(@as(usize, 1), log.count(io, direct_addr_d));

    try subscribeAndWait(io, allocator, router, "t");

    // The dial "succeeds": the connect surfaces as peer_connected, the normal path.
    const conn_d = try connectFakePeer(io, allocator, router, peer_d);
    defer destroyFakeConn(allocator, conn_d);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // D announces it subscribes to "t".
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_d, .rpc = try buildInboundSub(allocator, "t", true) } });
    try sync(router, io);

    // S relays a publish on "t": D — direct, never grafted into the mesh —
    // receives it exactly once via the direct forward path.
    try std.testing.expect(!router.meshContains("t", peer_d));
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x07", "t", "direct"),
    } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_d.record, "t", "direct") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_d.record, "t", "direct"));
}

// --- peer exchange (PX): emit on PRUNE + consume/dial ----------------------
//
// go-libp2p `WithPeerExchange` (default OFF): an over-degree heartbeat prune (a
// doPX path) offers up to GossipSubPrunePeers (16) OTHER topic peers as signed
// records in the PRUNE; the receiver, if the pruner clears AcceptPXThreshold,
// verifies each record and dials the suggested peer. These tests exercise emit
// gating + selection and consume gating (accept-PX) + dial + invalid-record
// rejection, all on the in-memory FakeTransport (dials recorded in a DialLog).

/// Drive an over-degree mesh on `topic` with `n` grafted peers and beat one
/// heartbeat to prune the excess down to D. Returns the connections (caller frees
/// each with destroyFakeConn). Used by the emit tests, which then read the PRUNEs.
fn oversizeMeshAndPrune(io: std.Io, allocator: std.mem.Allocator, router: *FakeRouter, topic: []const u8, n: usize, conns: []*FakeTransport.FakeConn, peers: []PeerId) !void {
    for (0..n) |idx| {
        const p = testPeer(@intCast(120 + idx));
        peers[idx] = p;
        conns[idx] = try connectFakePeer(io, allocator, router, p);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, topic, true) } });
        try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conns[idx], p, topic));
    }
    try std.testing.expectEqual(n, router.meshSize(topic));
    try beatHeartbeats(io, router, 1);
    try std.testing.expectEqual(mesh_params.d, router.meshSize(topic));
}

/// An app-specific (P5) score source that pins ONE peer's score very high and
/// gives everyone else zero. Used by the PX-emit test to make the record-bearing
/// peer a GUARANTEED survivor of the lowest-score-first over-degree prune (so its
/// offer — the one carrying a stored record — is deterministically present).
const PinnedAppScore = struct {
    pinned: score_mod.PeerKey,
    fn scoreFn(ctx: *anyopaque, peer: score_mod.PeerKey) f64 {
        const self: *const PinnedAppScore = @ptrCast(@alignCast(ctx));
        return if (std.mem.eql(u8, &peer, &self.pinned)) 1_000_000.0 else 0.0;
    }
};

test "PX emit: an over-degree prune offers the surviving mesh peers (with a stored record) when PX is on" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // The record-bearing peer R is sealed under a known key (so we can store a
    // VALID record for it). Pin R's app score very high so the lowest-score-first
    // over-degree prune always RETAINS R — making R a deterministic survivor whose
    // PX offer (the one carrying the stored record) is present in every victim's
    // PRUNE. Scoring is otherwise neutral (all other peers at 0).
    var key = try identity.KeyPair.generate(.ED25519);
    defer key.deinit();
    const sealed = try sealTestRecord(allocator, &key, 1, "/ip4/127.0.0.1/udp/5500/quic-v1");
    defer allocator.free(sealed.envelope);

    var pinned = PinnedAppScore{ .pinned = score_mod.peerKey(&sealed.peer_id) };
    var cfg = scoringConfig();
    cfg.params.app_specific_weight = 1.0;

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, cfg, .{
        .peer_exchange_enabled = true,
    });
    // Wire the app-score source onto the engine before start() (no fiber is
    // running yet, so this is race-free): P5 pins R's score very high.
    router.score.?.app_score_fn = .{ .ctx = &pinned, .score = PinnedAppScore.scoreFn };
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const n = mesh_params.d_high + 1;
    const conns = try allocator.alloc(*FakeTransport.FakeConn, n);
    defer allocator.free(conns);
    const peers = try allocator.alloc(PeerId, n);
    defer allocator.free(peers);
    defer for (conns) |c| destroyFakeConn(allocator, c);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // Build the mesh with the FIRST peer being R (the record's peer-id), so its
    // offer carries the signed record; the rest carry only a peer id.
    peers[0] = sealed.peer_id;
    conns[0] = try connectFakePeer(io, allocator, router, sealed.peer_id);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = sealed.peer_id, .rpc = try buildInboundSub(allocator, "t", true) } });
    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conns[0], sealed.peer_id, "t"));
    for (1..n) |idx| {
        const p = testPeer(@intCast(120 + idx));
        peers[idx] = p;
        conns[idx] = try connectFakePeer(io, allocator, router, p);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = p, .rpc = try buildInboundSub(allocator, "t", true) } });
        try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conns[idx], p, "t"));
    }
    try std.testing.expectEqual(n, router.meshSize("t"));

    // Store the signed record for R so an offer for it includes the bytes. The
    // router fiber is parked after sync, so this map write is race-free (same
    // property the private-state reads rely on).
    try sync(router, io);
    var consumed = try peer_record.consumeEnvelope(allocator, sealed.envelope);
    defer consumed.deinit(allocator);
    router.putRecord(sealed.peer_id, &consumed, sealed.envelope);
    try std.testing.expect(router.getRecord(sealed.peer_id) != null); // stored

    // One heartbeat prunes the excess (a doPX path). R is pinned high, so it
    // survives; each PRUNE to a pruned victim carries PX offers for the SURVIVING
    // mesh peers — which include R with its record.
    try beatHeartbeats(io, router, 1);
    try std.testing.expectEqual(mesh_params.d, router.meshSize("t"));
    try std.testing.expect(router.meshContains("t", sealed.peer_id)); // R survived

    // Wait for the PRUNE control frames to land, then find a PRUNE that carries PX
    // offers and assert (a) it offers peers OTHER than its recipient, and (b) the
    // offer for R carries exactly the sealed record bytes.
    var found_px = false;
    var found_record = false;
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        for (conns, peers) |c, recipient| {
            var offers: std.ArrayList(PxOffer) = .empty;
            defer offers.deinit(allocator);
            _ = recordFirstPrunePxPeers(io, &c.record, "t", &offers, allocator) catch continue;
            if (offers.items.len == 0) continue;
            found_px = true;
            for (offers.items) |off| {
                // Never offer a peer back to itself.
                try std.testing.expect(!std.mem.eql(u8, off.peer_id, recipient.bytes[0..recipient.len]));
                if (std.mem.eql(u8, off.peer_id, sealed.peer_id.bytes[0..sealed.peer_id.len])) {
                    try std.testing.expectEqualSlices(u8, sealed.envelope, off.record);
                    found_record = true;
                }
            }
        }
        if (found_px and found_record) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(found_px);
    try std.testing.expect(found_record);
}

test "PX emit disabled: an over-degree prune carries NO PX peers" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // PX OFF (the default): the over-degree prune is identical to today — a bare
    // PRUNE with no peer offers.
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const n = mesh_params.d_high + 1;
    const conns = try allocator.alloc(*FakeTransport.FakeConn, n);
    defer allocator.free(conns);
    const peers = try allocator.alloc(PeerId, n);
    defer allocator.free(peers);
    defer for (conns) |c| destroyFakeConn(allocator, c);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try oversizeMeshAndPrune(io, allocator, router, "t", n, conns, peers);

    // Wait for the PRUNEs, then assert NONE of them carry any PX offer.
    const want_pruned = n - mesh_params.d;
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        var total: usize = 0;
        for (conns) |c| total += recordCountPrunes(io, &c.record, "t");
        if (total >= want_pruned) break;
        io_time.ms(5).sleep(io) catch {};
    }
    io_time.ms(50).sleep(io) catch {};
    for (conns) |c| {
        var offers: std.ArrayList(PxOffer) = .empty;
        defer offers.deinit(allocator);
        const cnt = try recordFirstPrunePxPeers(io, &c.record, "t", &offers, allocator);
        try std.testing.expectEqual(@as(usize, 0), cnt);
    }
}

test "PX consume + dial: a PRUNE with a signed record from an above-accept-PX peer stores it and dials its addr" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var log = DialLog{ .allocator = allocator };
    defer log.deinit(io);

    // Scoring ON with accept_px_threshold = 0, so a clean pruner (score 0) clears
    // it (the gate is `>=`). PX enabled.
    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();
    var cfg = scoringConfig();
    cfg.thresholds.accept_px_threshold = 0;
    const router = try FakeRouter.create(allocator, io, .{ .dial_log = &log }, local_test_peer, null, 0, &host_key, cfg, .{
        .peer_exchange_enabled = true,
    });
    try router.start();

    // The pruner P (clean, score 0) PRUNEs us on "t" with an offer for peer C: a
    // valid signed record advertising a dialable address.
    const pruner = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, pruner);
    defer destroyFakeConn(allocator, conn_p);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    var c_key = try identity.KeyPair.generate(.ED25519);
    defer c_key.deinit();
    const c_addr = "/ip4/127.0.0.1/udp/6001/quic-v1";
    const sealed = try sealTestRecord(allocator, &c_key, 1, c_addr);
    defer allocator.free(sealed.envelope);

    const px = [_]?rpc_pb.PeerInfo{.{
        .peer_i_d = sealed.peer_id.bytes[0..sealed.peer_id.len],
        .signed_peer_record = sealed.envelope,
    }};
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = pruner, .rpc = try buildInboundPrunePx(allocator, "t", 60, &px) } });
    try sync(router, io);

    // C's record is now in the cert store AND C's address was dialed.
    try std.testing.expect(router.getRecord(sealed.peer_id) != null);
    try std.testing.expectEqual(@as(usize, 1), log.count(io, c_addr));
}

test "PX consume gated by accept-PX: a PRUNE from a below-threshold peer is ignored (no store, no dial)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var log = DialLog{ .allocator = allocator };
    defer log.deinit(io);

    // accept_px_threshold = 1: a clean pruner (score 0) is BELOW it, so its PX
    // offers are ignored entirely (the prune+backoff still apply).
    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();
    var cfg = scoringConfig();
    cfg.thresholds.accept_px_threshold = 1;
    const router = try FakeRouter.create(allocator, io, .{ .dial_log = &log }, local_test_peer, null, 0, &host_key, cfg, .{
        .peer_exchange_enabled = true,
    });
    try router.start();

    const pruner = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, pruner);
    defer destroyFakeConn(allocator, conn_p);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    var c_key = try identity.KeyPair.generate(.ED25519);
    defer c_key.deinit();
    const c_addr = "/ip4/127.0.0.1/udp/6002/quic-v1";
    const sealed = try sealTestRecord(allocator, &c_key, 1, c_addr);
    defer allocator.free(sealed.envelope);

    const px = [_]?rpc_pb.PeerInfo{.{
        .peer_i_d = sealed.peer_id.bytes[0..sealed.peer_id.len],
        .signed_peer_record = sealed.envelope,
    }};
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = pruner, .rpc = try buildInboundPrunePx(allocator, "t", 60, &px) } });
    try sync(router, io);

    // Below the accept-PX threshold: nothing stored, nothing dialed. Give a stray
    // dial a chance to land before asserting it did not.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expect(router.getRecord(sealed.peer_id) == null);
    try std.testing.expectEqual(@as(usize, 0), log.count(io, c_addr));
}

test "PX consume: an invalid record is rejected (not stored, not dialed); a valid one beside it still processes" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var log = DialLog{ .allocator = allocator };
    defer log.deinit(io);

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();
    var cfg = scoringConfig();
    cfg.thresholds.accept_px_threshold = 0;
    const router = try FakeRouter.create(allocator, io, .{ .dial_log = &log }, local_test_peer, null, 0, &host_key, cfg, .{
        .peer_exchange_enabled = true,
    });
    try router.start();

    const pruner = testPeer(1);
    const conn_p = try connectFakePeer(io, allocator, router, pruner);
    defer destroyFakeConn(allocator, conn_p);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    // A valid offer for C and a TAMPERED offer for B (its record bytes are
    // corrupted so the signature fails). The valid one must still be processed.
    var c_key = try identity.KeyPair.generate(.ED25519);
    defer c_key.deinit();
    const c_addr = "/ip4/127.0.0.1/udp/6003/quic-v1";
    const sealed_c = try sealTestRecord(allocator, &c_key, 1, c_addr);
    defer allocator.free(sealed_c.envelope);

    var b_key = try identity.KeyPair.generate(.ED25519);
    defer b_key.deinit();
    const b_addr = "/ip4/127.0.0.1/udp/6004/quic-v1";
    const sealed_b = try sealTestRecord(allocator, &b_key, 1, b_addr);
    defer allocator.free(sealed_b.envelope);
    // Tamper: flip a byte in the middle of B's envelope so consumeEnvelope fails.
    const tampered = try allocator.dupe(u8, sealed_b.envelope);
    defer allocator.free(tampered);
    tampered[tampered.len / 2] ^= 0xff;

    const px = [_]?rpc_pb.PeerInfo{
        .{ .peer_i_d = sealed_b.peer_id.bytes[0..sealed_b.peer_id.len], .signed_peer_record = tampered },
        .{ .peer_i_d = sealed_c.peer_id.bytes[0..sealed_c.peer_id.len], .signed_peer_record = sealed_c.envelope },
    };
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = pruner, .rpc = try buildInboundPrunePx(allocator, "t", 60, &px) } });
    try sync(router, io);
    io_time.ms(50).sleep(io) catch {};

    // B (invalid) is neither stored nor dialed; C (valid) is both.
    try std.testing.expect(router.getRecord(sealed_b.peer_id) == null);
    try std.testing.expectEqual(@as(usize, 0), log.count(io, b_addr));
    try std.testing.expect(router.getRecord(sealed_c.peer_id) != null);
    try std.testing.expectEqual(@as(usize, 1), log.count(io, c_addr));
}

// ---------------------------------------------------------------------------
// Interned message-id tests: the same id in `seen` + a peer's `dont_send` +
// `iwant_promises` is ONE allocation shared via reference counting, freed only
// when the last holder releases. These read/mutate router-owned state directly
// AFTER a `sync` (the router fiber is then parked on the inbox and the tests run
// with heartbeat_interval_ms = 0, so no concurrent fiber mutates the maps — the
// same single-fiber-quiescent contract the accessor helpers above rely on).
// ---------------------------------------------------------------------------

test "interned id shared across seen + dont_send + iwant_promises is one allocation" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try sync(router, io); // router parked; its maps are quiescent below.

    const state = router.peers.get(peerKey(&peer)).?;
    const id = "shared-id-1";

    // Three independent holders intern the SAME id: seen, the peer's dont_send,
    // and the peer's iwant_promises. Interning coalesces onto one allocation.
    router.seen.add(id, router.heartbeat_tick);
    router.recordDontSend(state, id, router.heartbeat_tick +| 5);
    router.addPromise(state, id);

    // One table entry (one allocation) with three references (one per holder).
    try std.testing.expectEqual(@as(usize, 1), internCount(router));
    try std.testing.expectEqual(@as(usize, 3), internRefs(router, id));

    // Drop the dont_send holder: the id survives (still in seen + promises).
    {
        const e = state.dont_send.fetchRemove(id).?;
        e.value.rc.release();
    }
    try std.testing.expectEqual(@as(usize, 1), internCount(router));
    try std.testing.expectEqual(@as(usize, 2), internRefs(router, id));

    // Drop the promises holder: still live in seen alone.
    {
        const e = state.iwant_promises.fetchRemove(id).?;
        e.value.rc.release();
    }
    try std.testing.expectEqual(@as(usize, 1), internCount(router));
    try std.testing.expectEqual(@as(usize, 1), internRefs(router, id));

    // Drop the last holder (seen) THROUGH the wheel (a jump-sweep past expiry):
    // removing from `seen.entries` directly would leave a dangling non-owning
    // slot in a wheel bucket — only the sweep may remove live entries.
    router.seen.sweep(router.heartbeat_tick +| mesh_params.seen_ttl_ticks +| 1);
    try std.testing.expectEqual(@as(usize, 0), internCount(router));
    try std.testing.expectEqual(@as(usize, 0), internRefs(router, id));
}

test "tearing down a peer releases its id holders; ids also in seen survive" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try sync(router, io);

    const state = router.peers.get(peerKey(&peer)).?;

    // `shared` is held by seen AND the peer (dont_send + promises): refs 3.
    // `peer_only` is held by the peer alone (dont_send): refs 1.
    const shared = "shared-id";
    const peer_only = "peer-only-id";
    router.seen.add(shared, router.heartbeat_tick);
    router.recordDontSend(state, shared, router.heartbeat_tick +| 5);
    router.addPromise(state, shared);
    router.recordDontSend(state, peer_only, router.heartbeat_tick +| 5);

    try std.testing.expectEqual(@as(usize, 2), internCount(router));
    try std.testing.expectEqual(@as(usize, 3), internRefs(router, shared));
    try std.testing.expectEqual(@as(usize, 1), internRefs(router, peer_only));

    // Tear the peer down via a disconnect command: its two holders on `shared`
    // and its one on `peer_only` are released. `peer_only` (no other holder) is
    // freed and leaves the table; `shared` survives in seen (refs 3 → 1).
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
    try sync(router, io);

    try std.testing.expectEqual(@as(usize, 1), internCount(router));
    try std.testing.expectEqual(@as(usize, 1), internRefs(router, shared));
    try std.testing.expectEqual(@as(usize, 0), internRefs(router, peer_only));

    // Release the surviving seen holder so destroy's empty-table assert holds and
    // the testing allocator confirms no leak — THROUGH the wheel (a jump-sweep
    // past expiry); a direct map remove would leave a dangling bucket slot.
    router.seen.sweep(router.heartbeat_tick +| mesh_params.seen_ttl_ticks +| 1);
    try std.testing.expectEqual(@as(usize, 0), internCount(router));
}

test "intern churn through all four maps + peer teardown empties the table (no leak)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null, 0, null, null, .{});
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn
    try sync(router, io);

    const state = router.peers.get(peerKey(&peer)).?;

    // Push many distinct ids through all four maps. Each id is interned by up to
    // four holders; the table holds at most one entry per distinct id.
    var buf: [32]u8 = undefined;
    var i: usize = 0;
    while (i < 200) : (i += 1) {
        const id = std.fmt.bufPrint(&buf, "churn-{d}", .{i}) catch unreachable;
        router.seen.add(id, router.heartbeat_tick);
        router.recordDontSend(state, id, router.heartbeat_tick +| 3);
        router.addPromise(state, id);
        router.bumpIWantCount(state, id);
    }
    // 200 distinct ids interned (shared across the four maps, not 800 copies).
    try std.testing.expectEqual(@as(usize, 200), internCount(router));

    // Sweep seen (everything past expiry) and clear the peer's per-heartbeat
    // count map; both release their references.
    router.seen.sweep(router.heartbeat_tick +| 1000);
    router.clearIWantCounts(state);

    // Tear the peer down: dont_send + iwant_promises holders released. With seen
    // already swept and counts cleared, every id reaches refs 0 and the table is
    // empty — proving no id leaks and no holder is missed.
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer, .conn = conn } });
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 0), internCount(router));
}

// --- topic message validator (ACCEPT / REJECT / IGNORE) --------------------
//
// The validator gates delivery + forwarding of a NEW, signature-checked received
// message on the app's verdict, matching go-libp2p-pubsub's validate-then-forward
// and rust-libp2p's MessageAcceptance:
//   accept -> deliver locally (if subscribed) + forward over the mesh + the usual
//             P2 first-delivery credit (the historic accept-all behaviour);
//   reject -> neither deliver nor forward, and charge the relaying peer the P4
//             invalid-message-delivery penalty (score.rejectMessage);
//   ignore -> neither deliver nor forward, and do NOT penalize the sender.
// In every case the message is marked seen before the verdict, so a re-send of the
// same id is suppressed and the validator is not invoked twice for it.

/// A test validator returning a fixed verdict, counting its invocations so a test
/// can assert it ran (or did not). Owns no heap.
const FixedValidator = struct {
    verdict: ValidationResult,
    calls: usize = 0,

    fn validator(self: *FixedValidator) MessageValidator {
        return .{ .ctx = self, .validate = validate };
    }

    fn validate(ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) ValidationResult {
        const self: *FixedValidator = @ptrCast(@alignCast(ctx));
        _ = topic;
        _ = from;
        _ = data;
        self.calls += 1;
        return self.verdict;
    }
};

test "validator ACCEPT: a received message is delivered locally and forwarded over the mesh" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    var val = FixedValidator{ .verdict = .accept };

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{ .validator = val.validator() });
    try router.start();

    // We subscribe to "t" (so an accepted message is delivered locally) and graft
    // A into our mesh (so it is forwarded). S relays the publish.
    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x01", "t", "hello"),
    } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "hello") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    // Accepted: forwarded to mesh member A, delivered locally, and the validator
    // ran exactly once.
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "hello"));
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
    try std.testing.expectEqual(@as(usize, 1), val.calls);
}

test "validator REJECT: not delivered/forwarded and the sender is penalized (P4)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    var val = FixedValidator{ .verdict = .reject };

    // Scoring ENABLED (to observe P4) with the `none` policy (no host key), so the
    // inbound publish is NOT signature-checked and reaches the validator. The
    // scoringConfig gives each reject a score contribution of -(invalid^2): one
    // reject => -1.
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, scoringConfig(), .{ .validator = val.validator() });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    // The relaying peer S starts at score 0.
    try std.testing.expectEqual(@as(f64, 0), liveScore(router, source));

    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x02", "t", "bad"),
    } });
    // Fence so the reject is fully processed before asserting; give a wrong forward
    // a chance to (not) land.
    try sync(router, io);
    io_time.ms(50).sleep(io) catch {};

    // Rejected: nothing delivered locally, nothing forwarded to A, validator ran
    // once, and S took the P4 penalty (one invalid delivery => score -1).
    try std.testing.expectEqual(@as(usize, 1), val.calls);
    try std.testing.expectEqual(@as(usize, 0), rec.calls);
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_a.record, "t", "bad"));
    try std.testing.expectEqual(@as(f64, -1), liveScore(router, source));
}

test "validator IGNORE: not delivered/forwarded, sender NOT penalized, a re-send is seen-suppressed" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    var val = FixedValidator{ .verdict = .ignore };

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, scoringConfig(), .{ .validator = val.validator() });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expectEqual(@as(f64, 0), liveScore(router, source));

    // First send: the validator ignores it.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x03", "t", "stale"),
    } });
    try sync(router, io);

    // Re-send the IDENTICAL message (same from+seqno => same id). It is suppressed
    // by the seen-cache (the id was marked seen before the first verdict), so the
    // validator is NOT invoked a second time.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x03", "t", "stale"),
    } });
    try sync(router, io);
    io_time.ms(50).sleep(io) catch {};

    // Ignored: nothing delivered/forwarded, the validator ran exactly ONCE (the
    // re-send was seen-suppressed), and S took NO penalty (score still 0).
    try std.testing.expectEqual(@as(usize, 1), val.calls);
    try std.testing.expectEqual(@as(usize, 0), rec.calls);
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_a.record, "t", "stale"));
    try std.testing.expectEqual(@as(f64, 0), liveScore(router, source));
}

test "validator null (default): accept-all — delivered + forwarded, unchanged behaviour" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();

    // No validator (the default): every message is accepted, exactly as before the
    // validator existed.
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{});
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins writers before records free; see destroyFakeConn

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x04", "t", "world"),
    } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "world") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "world"));
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
}

// --- ASYNC topic message validation (off the router fiber) -----------------
//
// With `validation_concurrency > 0` the validator runs on a validation fiber off
// the single router fiber (go-libp2p's validator-worker model), and the verdict is
// posted back as a `validation_result` command the router applies: ACCEPT runs the
// same accept effects as the inline path (deliver + forward + score), REJECT
// charges P4, IGNORE does nothing. The effects are DEFERRED until the verdict, so a
// test posts the message, lets the validation fiber run, and polls the observable
// effect (a forwarded publish on the mesh peer's record, a score change) — then a
// final `sync` makes the router-state read race-free.

/// A thread-safe async-validation test validator. Its `validate` runs on a
/// validation fiber (a worker thread under `std.Io.Threaded`), so its call counter
/// is atomic and it may sleep (via the stored `io`) to model an EXPENSIVE/slow
/// validator. `started` is bumped on entry (before any sleep) so a test can observe
/// that a validation is in flight while the router fiber stays responsive.
const AsyncValidator = struct {
    io: std.Io,
    verdict: ValidationResult,
    sleep_ms: u64 = 0,
    calls: std.atomic.Value(usize) = .init(0),
    started: std.atomic.Value(usize) = .init(0),

    fn validator(self: *AsyncValidator) MessageValidator {
        return .{ .ctx = self, .validate = validate };
    }

    fn validate(ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) ValidationResult {
        const self: *AsyncValidator = @ptrCast(@alignCast(ctx));
        _ = topic;
        _ = from;
        _ = data;
        _ = self.started.fetchAdd(1, .acq_rel);
        if (self.sleep_ms > 0) io_time.ms(self.sleep_ms).sleep(self.io) catch {};
        _ = self.calls.fetchAdd(1, .acq_rel);
        return self.verdict;
    }
};

/// Sync the router fiber, then read `peer`'s live score — race-free because the
/// router fiber is parked after `sync` returns. Used to poll for an async verdict's
/// score effect: the `validation_result` command's score mutation is visible once a
/// `sync` posted AFTER it has been processed.
fn syncedScore(router: *FakeRouter, io: std.Io, peer: PeerId) f64 {
    sync(router, io) catch {};
    return liveScore(router, peer);
}

test "async validator ACCEPT: message is delivered + forwarded once the verdict lands" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    var val = AsyncValidator{ .io = io, .verdict = .accept };

    // validation_concurrency = 2 enables async validation (off the router fiber).
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{ .validator = val.validator(), .validation_concurrency = 2 });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy(); // joins validation + writer fibers before records free

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x01", "t", "hello"),
    } });

    // The verdict is applied off-line: poll the forwarded copy on A's record (set by
    // the deferred accept effects once the validation fiber's result is processed).
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "hello") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "hello"));
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
    try std.testing.expectEqual(@as(usize, 1), val.calls.load(.acquire));
}

test "async validator REJECT: not delivered/forwarded and the sender takes P4" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    var val = AsyncValidator{ .io = io, .verdict = .reject };

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, scoringConfig(), .{ .validator = val.validator(), .validation_concurrency = 2 });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy();

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expectEqual(@as(f64, 0), liveScore(router, source));

    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x02", "t", "bad"),
    } });

    // Poll (sync then read) until S takes the P4 penalty — the verdict's reject
    // effect lands once the validation fiber's result command is processed.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (syncedScore(router, io, source) == -1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 1), val.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), rec.calls);
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_a.record, "t", "bad"));
    try std.testing.expectEqual(@as(f64, -1), liveScore(router, source));
}

test "async validator IGNORE: not delivered/forwarded, sender NOT penalized" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    var val = AsyncValidator{ .io = io, .verdict = .ignore };

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, scoringConfig(), .{ .validator = val.validator(), .validation_concurrency = 2 });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy();

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x03", "t", "stale"),
    } });

    // Wait for the validation to run (atomic call count), then settle the result
    // command with a sync. IGNORE leaves no observable score/forward effect, so we
    // gate on the call count + a fence and assert the absence of effects.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (val.calls.load(.acquire) >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    io_time.ms(50).sleep(io) catch {};
    try sync(router, io);

    try std.testing.expectEqual(@as(usize, 1), val.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), rec.calls);
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_a.record, "t", "stale"));
    try std.testing.expectEqual(@as(f64, 0), liveScore(router, source));
}

test "async validator: a SLOW validation does not block other router commands" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    // A slow validator: it sleeps before returning accept, modelling an expensive
    // off-fiber validation. The router fiber must stay responsive while it runs.
    var val = AsyncValidator{ .io = io, .verdict = .accept, .sleep_ms = 300 };

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{ .validator = val.validator(), .validation_concurrency = 2 });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy();

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    // Kick off the slow validation.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x04", "t", "slow"),
    } });
    // Wait until the validation has STARTED (running off the router fiber), but is
    // still sleeping (its result is not yet posted, so nothing is forwarded yet).
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (val.started.load(.acquire) >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(val.started.load(.acquire) >= 1);

    // While the validation is still in flight, the router fiber must process other
    // commands. Subscribe to a SECOND topic and confirm the router announced it to
    // peer A — proving the router did NOT block behind the slow validation. This
    // sync completes well before the 300ms validator sleep, so it is processed
    // concurrently with the in-flight validation.
    try subscribeAndWait(io, allocator, router, "t2");
    var ann_waited: u64 = 0;
    while (ann_waited < 200) : (ann_waited += 5) {
        if (recordHasSubscription(io, &conn_a.record, "t2", true)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(recordHasSubscription(io, &conn_a.record, "t2", true));
    // The slow validation should still NOT have delivered (it is still sleeping).
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_a.record, "t", "slow"));

    // Eventually the verdict lands and the message is forwarded.
    waited = 0;
    while (waited < 3000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "slow") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "slow"));
}

test "async validation cap: over-cap messages are throttle-DROPPED (go parity) and NOT marked seen" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    // A slow ACCEPT validator with a cap of 1: the first message occupies the
    // single async slot (and sleeps); further messages posted while it is in
    // flight exceed the cap and are throttle-DROPPED, exactly like go
    // ("validation throttled: queue full; dropping") — never validated inline
    // (the old fallback let a validation flood stall the router fiber), never
    // delivered, never forwarded. The drop does NOT mark the id seen, so a
    // later re-delivery of a dropped message validates normally.
    var val = AsyncValidator{ .io = io, .verdict = .accept, .sleep_ms = 200 };

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{ .validator = val.validator(), .validation_concurrency = 1 });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy();

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    // Post several DISTINCT messages back-to-back. m0 takes the async slot; the
    // rest arrive while it is in flight and are dropped at the cap.
    const datas = [_][]const u8{ "m0", "m1", "m2", "m3", "m4" };
    for (datas, 0..) |d, i| {
        var seqno: [4]u8 = undefined;
        std.mem.writeInt(u32, &seqno, @as(u32, @intCast(0x10 + i)), .big);
        try router.inbox.putOne(io, .{ .inbound_rpc = .{
            .peer = source,
            .rpc = try buildInboundPublish(allocator, "origin", &seqno, "t", d),
        } });
    }

    // m0's verdict eventually lands and it is forwarded; the over-cap rest never are.
    var waited: u64 = 0;
    while (waited < 3000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "m0") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    io_time.ms(50).sleep(io) catch {}; // grace: a wrong forward would land here
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "m0"));
    for (datas[1..]) |d| {
        try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_a.record, "t", d));
    }
    // Exactly one validation ran: the dropped messages were never validated
    // (neither async nor inline).
    try std.testing.expectEqual(@as(usize, 1), val.calls.load(.acquire));

    // The drop did not mark m1 seen: re-delivering it (same from/seqno => same
    // id) with the slot now free validates and forwards normally.
    var seqno1: [4]u8 = undefined;
    std.mem.writeInt(u32, &seqno1, @as(u32, 0x11), .big);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", &seqno1, "t", "m1"),
    } });
    waited = 0;
    while (waited < 3000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "m1") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "m1"));
}

test "async validation: two in-flight copies of one id converge to a single forward" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    // Seen is MARKED at verdict re-entry (it is router-fiber-confined, so the
    // worker cannot mark it). Two copies of one id that both pass the seen CHECK
    // before either verdict lands are BOTH validated — go has the same window
    // between shouldPush and the worker's markSeen — but the verdicts apply
    // serially on the router fiber: the first marks seen + forwards, the second
    // is handled as a duplicate. The message must be forwarded exactly once.
    var val = AsyncValidator{ .io = io, .verdict = .accept, .sleep_ms = 150 };

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{ .validator = val.validator(), .validation_concurrency = 4 });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const s1 = testPeer(3);
    const s2 = testPeer(4);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s1 = try connectFakePeer(io, allocator, router, s1);
    defer destroyFakeConn(allocator, conn_s1);
    const conn_s2 = try connectFakePeer(io, allocator, router, s2);
    defer destroyFakeConn(allocator, conn_s2);
    defer router.destroy();

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    // The SAME message (same from/seqno => same id) relayed by two peers
    // back-to-back, while the slow validator holds both verdicts in flight.
    const seqno = "\x00\x00\x00\x77";
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = s1,
        .rpc = try buildInboundPublish(allocator, "origin", seqno, "t", "dup-race"),
    } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = s2,
        .rpc = try buildInboundPublish(allocator, "origin", seqno, "t", "dup-race"),
    } });

    var waited: u64 = 0;
    while (waited < 3000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "dup-race") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    io_time.ms(50).sleep(io) catch {}; // grace: a duplicate forward would land here
    // Both copies were validated (both were in flight before either was seen)...
    try std.testing.expectEqual(@as(usize, 2), val.calls.load(.acquire));
    // ...but the message was forwarded and delivered exactly once.
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "dup-race"));
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
}

test "async signature check: a bad signature is rejected off-fiber (P4) and NOT marked seen" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    // strict_sign + scoring + concurrency=1, NO app validator: the SIGNATURE
    // check itself runs on the validation fiber (go's worker validate() order).
    // An unsigned message under strict_sign fails verification off-fiber; the
    // verdict charges the relayer P4. Crucially the id is NOT marked seen — a
    // forged (from, seqno) must not censor the real message — so a second copy
    // is re-verified and re-penalized rather than seen-suppressed.
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, &host_key, scoringConfig(), .{ .validation_concurrency = 1 });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy();

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));
    try std.testing.expectEqual(@as(f64, 0), syncedScore(router, io, source));

    // First unsigned copy: verified off-fiber, rejected, P4 (-1 = -(1 invalid)^2).
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x21", "t", "forged"),
    } });
    var waited: u64 = 0;
    while (waited < 3000) : (waited += 5) {
        if (syncedScore(router, io, source) < 0) break;
        io_time.ms(5).sleep(io) catch {};
    }
    const score_after_first = syncedScore(router, io, source);
    try std.testing.expect(score_after_first < 0);

    // The SAME message again (same from/seqno => same id): NOT seen-suppressed —
    // it is re-verified and the score strictly worsens (a seen-marked id would
    // have been dropped as a duplicate with no further penalty).
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x21", "t", "forged"),
    } });
    waited = 0;
    while (waited < 3000) : (waited += 5) {
        if (syncedScore(router, io, source) < score_after_first) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(syncedScore(router, io, source) < score_after_first);

    // Nothing was ever delivered or forwarded.
    try std.testing.expectEqual(@as(usize, 0), rec.calls);
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(io, &conn_a.record, "t", "forged"));
}

test "async signature check: a correctly signed message verifies off-fiber and is forwarded" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    // The POSITIVE async-signature path: with concurrency>0 and no app
    // validator, a properly signed message is verified on the validation fiber
    // and its accept verdict delivers + forwards it — the router fiber itself
    // never runs the Ed25519 verify.
    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, &host_key, null, .{ .validation_concurrency = 2 });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy();

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    // A second identity is the message's ORIGIN publisher: sign exactly the
    // fields the wire carries, with its own from/key bytes.
    var origin_key = try identity.KeyPair.generate(.ED25519);
    defer origin_key.deinit();
    var origin_signer = try signing.Signer.init(allocator, &origin_key);
    defer origin_signer.deinit();
    const seqno = "\x00\x00\x00\x42";
    const sig = try origin_signer.sign(origin_signer.fromBytes(), seqno, "t", "signed-data");
    defer allocator.free(sig);

    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublishSigned(allocator, origin_signer.fromBytes(), seqno, "t", "signed-data", sig, origin_signer.keyBytes()),
    } });

    var waited: u64 = 0;
    while (waited < 3000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "signed-data") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "signed-data"));
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
}

/// A validator that BLOCKS forever (until cancelled) on entry, to model an
/// in-flight async validation that never returns a verdict — so teardown must
/// cancel + join its fiber and free its held context. `started` lets the test wait
/// until the validation is actually running before it drops the router.
const BlockingValidator = struct {
    io: std.Io,
    started: std.atomic.Value(usize) = .init(0),

    fn validator(self: *BlockingValidator) MessageValidator {
        return .{ .ctx = self, .validate = validate };
    }

    fn validate(ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) ValidationResult {
        const self: *BlockingValidator = @ptrCast(@alignCast(ctx));
        _ = topic;
        _ = from;
        _ = data;
        _ = self.started.fetchAdd(1, .acq_rel);
        // Sleep in a loop until the fiber is cancelled (the sleep is a cancellation
        // point, so teardown's group cancel collapses it). A long bound is a
        // backstop so a test bug cannot hang forever.
        var elapsed: u64 = 0;
        while (elapsed < 30_000) : (elapsed += 50) {
            io_time.ms(50).sleep(self.io) catch return .accept;
        }
        return .accept;
    }
};

test "async validator teardown: a never-returning in-flight validation is cancelled + freed (no leak/UAF)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    var val = BlockingValidator{ .io = io };

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{ .validator = val.validator(), .validation_concurrency = 4 });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const source = testPeer(3);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);

    // Post a message whose validation will block forever (until teardown cancels
    // it). The held context lives on the validation fiber.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x05", "t", "blocked"),
    } });

    // Wait until the validation is actually running (so it is genuinely in flight
    // when we tear down — exercising the cancel-mid-validate path).
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (val.started.load(.acquire) >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(val.started.load(.acquire) >= 1);

    // Drop the router with the validation still in flight: destroy must cancel +
    // join the validation fiber (collapsing its sleep) and free the held context —
    // before the peer/intern/score state is freed. std.testing.allocator + the
    // intern-table empty-assert confirm no leak/UAF.
    router.destroy();
}

test "async validator config (validation_concurrency=0) is the inline default — delivered + forwarded" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();
    // Concurrency 0 (the default) runs the validator INLINE on the router fiber —
    // identical to the historic behaviour. A FixedValidator (no thread-safety) is
    // fine because it is never called off the router fiber.
    var val = FixedValidator{ .verdict = .accept };

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler(), 0, null, null, .{ .validator = val.validator(), .validation_concurrency = 0 });
    try router.start();

    try subscribeAndWait(io, allocator, router, "t");

    const peer_a = testPeer(1);
    const source = testPeer(3);
    const conn_a = try connectFakePeer(io, allocator, router, peer_a);
    defer destroyFakeConn(allocator, conn_a);
    const conn_s = try connectFakePeer(io, allocator, router, source);
    defer destroyFakeConn(allocator, conn_s);
    defer router.destroy();

    try std.testing.expectEqual(GraftOutcome.accepted, try graftAndWait(io, allocator, router, conn_a, peer_a, "t"));

    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = source,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x06", "t", "inline"),
    } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(io, &conn_a.record, "t", "inline") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try sync(router, io);
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(io, &conn_a.record, "t", "inline"));
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
    // Inline path: the validator ran synchronously on the router fiber, exactly once.
    try std.testing.expectEqual(@as(usize, 1), val.calls);
}
