//! The gossipsub peer scoring engine: a standalone, lock-free accountant that
//! assigns every peer a real-valued score from the gossipsub v1.1 spec's P1-P7
//! terms. All formulas and field semantics here byte-match go-libp2p-pubsub's
//! `score.go`/`params.go`. The router drives it (graft/prune/delivery/penalty/IP
//! events + a per-heartbeat `decay` tick) and gates decisions on the thresholds;
//! this file knows nothing about the router, mesh, or wire. All math is f64.
//!
//!   score = Σ_topic topicScore + appScore·appWeight + ipColocation + behaviour
//!
//! The per-topic term sums five clamped, weighted sub-scores; the rest are global:
//!
//!   P1  time in mesh             — positive; rewards staying grafted.
//!   P2  first message deliveries — positive; rewards being first to relay a msg.
//!   P3  mesh message deliveries  — NEGATIVE; penalises a mesh peer below the
//!                                  delivery threshold, only after an activation
//!                                  grace period.
//!   P3b mesh failure penalty     — NEGATIVE; sticky, captured when a peer is
//!                                  pruned while carrying a P3 deficit.
//!   P4  invalid messages         — NEGATIVE; squared count of failed-validation msgs.
//!   P5  application-specific      — host callback × weight; optional, default zero.
//!   P6  IP colocation            — NEGATIVE; many peers sharing one IP (sybil signal).
//!   P7  behaviour penalty        — NEGATIVE; squared excess of a misbehaviour counter.
//!
//! Fading counters decay geometrically once per heartbeat (factor in (0,1)); a
//! value below `decay_to_zero` snaps to zero so it does not linger as float dust.
//!
//! The default parameters/thresholds below are a self-consistent baseline that
//! exercises every term, NOT production values; real deployments tune per-topic.
//! All-zero weights disable scoring (peers score 0).

const std = @import("std");
const PeerId = @import("peer_id").PeerId;

/// A PeerId as an AutoHashMap key. PeerId's tail past `len` is undefined, so we
/// zero-pad to a content-defined key (same meaningful prefix + len → equal key).
pub const PeerKey = [64]u8;

pub fn peerKey(peer: *const PeerId) PeerKey {
    var key: PeerKey = [_]u8{0} ** 64;
    @memcpy(key[0..peer.len], peer.bytes[0..peer.len]);
    return key;
}

/// A port-independent IP-address key. P6 counts peers per ADDRESS (one host's
/// many connections share an address, differ in port), so the port is stripped:
/// byte 0 tags the family (4 or 6), bytes 1.. are the raw address (4 for v4 with
/// the tail zeroed, 16 for v6).
pub const IpKey = [17]u8;

pub fn ipKey(addr: std.Io.net.IpAddress) IpKey {
    var key: IpKey = [_]u8{0} ** 17;
    switch (addr) {
        .ip4 => |a| {
            key[0] = 4;
            @memcpy(key[1..5], &a.bytes);
        },
        .ip6 => |a| {
            key[0] = 6;
            @memcpy(key[1..17], &a.bytes);
        },
    }
    return key;
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// Per-topic scoring parameters (go-libp2p `TopicScoreParams`). P1-P4 are summed
/// then scaled by `topic_weight`. Every `*_decay` is a per-heartbeat factor in
/// (0,1); a zero weight disables that term for the topic.
pub const TopicScoreParams = struct {
    /// Overall multiplier for this topic's P1-P4 sum.
    topic_weight: f64,

    // P1: time in mesh.
    time_in_mesh_weight: f64,
    /// One unit of P1 is earned per this many ticks in the mesh.
    time_in_mesh_quantum_ticks: u64,
    /// Upper bound on the P1 quantum count (so old peers don't dominate).
    time_in_mesh_cap: f64,

    // P2: first message deliveries.
    first_message_deliveries_weight: f64,
    first_message_deliveries_decay: f64,
    first_message_deliveries_cap: f64,

    // P3: mesh message deliveries (deficit below a threshold).
    mesh_message_deliveries_weight: f64,
    mesh_message_deliveries_decay: f64,
    /// Expected mesh-delivery floor; a counter below this is in deficit.
    mesh_message_deliveries_threshold: f64,
    mesh_message_deliveries_cap: f64,
    /// Grace period: ticks in the mesh before P3 is evaluated, so a fresh peer is
    /// not penalised before it can deliver anything.
    mesh_message_deliveries_activation_ticks: u64,
    /// Ticks after a message is first seen during which a duplicate from a mesh
    /// peer still counts toward its mesh-delivery credit.
    mesh_message_deliveries_window_ticks: u64,

    // P3b: mesh failure penalty.
    mesh_failure_penalty_weight: f64,
    mesh_failure_penalty_decay: f64,

    // P4: invalid message deliveries.
    invalid_message_deliveries_weight: f64,
    invalid_message_deliveries_decay: f64,
};

/// Global (non-topic) scoring parameters (go-libp2p `PeerScoreParams`): the P5
/// app-score weight, P6/P7 parameters, and decay/retention bookkeeping. A single
/// `topic_default` is applied to every topic (per-topic overrides are a future
/// refinement; per-peer per-topic STATE is stored regardless).
pub const ScoreParams = struct {
    /// P5 multiplier: appScore (from `app_score_fn`) is scaled by this.
    app_specific_weight: f64,

    // P6: IP colocation.
    ip_colocation_factor_weight: f64,
    /// Peers-per-IP allowed before the penalty kicks in (surplus = count above this).
    ip_colocation_factor_threshold: f64,

    // P7: behaviour penalty.
    behaviour_penalty_weight: f64,
    /// Counter level below which P7 is zero (excess = counter above this).
    behaviour_penalty_threshold: f64,
    behaviour_penalty_decay: f64,

    // Decay / retention bookkeeping.
    /// Decay (and mesh-time/window/activation accounting) runs once every this many
    /// heartbeat ticks. 1 = every heartbeat (the common case).
    decay_interval_ticks: u64,
    /// A decaying counter at or below this snaps to zero (avoids float dust keeping
    /// a long-dead counter marginally non-zero forever).
    decay_to_zero: f64,
    /// Ticks to retain a disconnected peer's stats before purging, so a quick
    /// reconnect keeps its (negative) score (go-libp2p `RetainScore`).
    retain_score_ticks: u64,

    /// The parameters applied to every topic.
    topic_default: TopicScoreParams,
};

/// The five score thresholds the router gates decisions on (go-libp2p
/// `PeerScoreThresholds`).
pub const PeerScoreThresholds = struct {
    /// Below this, do not emit/accept gossip (IHAVE/IWANT) to/from the peer.
    gossip_threshold: f64,
    /// Below this, do not publish messages through the peer.
    publish_threshold: f64,
    /// At or below this, graylist the peer (ignore its RPCs entirely).
    graylist_threshold: f64,
    /// At or above this, accept peer-exchange (PX) info from the peer on PRUNE.
    accept_px_threshold: f64,
    /// At or above this, a mesh peer is eligible to be kept by opportunistic graft.
    opportunistic_graft_threshold: f64,
};

/// A baseline (non-production) parameter set exercising every term: modest
/// positive mesh-time/first-delivery rewards, the standard negative penalty
/// weights, and near-unity decays (a counter keeps most value across a heartbeat).
pub const default_params: ScoreParams = .{
    .app_specific_weight = 1.0,
    .ip_colocation_factor_weight = -5.0,
    .ip_colocation_factor_threshold = 3.0,
    .behaviour_penalty_weight = -10.0,
    .behaviour_penalty_threshold = 0.0,
    .behaviour_penalty_decay = 0.95,
    .decay_interval_ticks = 1,
    .decay_to_zero = 0.01,
    .retain_score_ticks = 3600,
    .topic_default = .{
        .topic_weight = 1.0,
        .time_in_mesh_weight = 0.01,
        .time_in_mesh_quantum_ticks = 1,
        .time_in_mesh_cap = 100.0,
        .first_message_deliveries_weight = 1.0,
        .first_message_deliveries_decay = 0.99,
        .first_message_deliveries_cap = 100.0,
        .mesh_message_deliveries_weight = -1.0,
        .mesh_message_deliveries_decay = 0.97,
        .mesh_message_deliveries_threshold = 10.0,
        .mesh_message_deliveries_cap = 100.0,
        .mesh_message_deliveries_activation_ticks = 10,
        .mesh_message_deliveries_window_ticks = 5,
        .mesh_failure_penalty_weight = -1.0,
        .mesh_failure_penalty_decay = 0.97,
        .invalid_message_deliveries_weight = -1.0,
        .invalid_message_deliveries_decay = 0.99,
    },
};

/// A baseline (non-production) threshold set matching `default_params`' scale.
pub const default_thresholds: PeerScoreThresholds = .{
    .gossip_threshold = -10.0,
    .publish_threshold = -50.0,
    .graylist_threshold = -80.0,
    .accept_px_threshold = 10.0,
    .opportunistic_graft_threshold = 20.0,
};

/// The optional P5 app-score source: a host-supplied context + function mapping a
/// peer to its app score, which the engine scales by `app_specific_weight`.
pub const AppScoreFn = struct {
    ctx: *anyopaque,
    score: *const fn (ctx: *anyopaque, peer: PeerKey) f64,
};

// ---------------------------------------------------------------------------
// Per-peer state
// ---------------------------------------------------------------------------

/// Per-peer, per-topic accounting (go-libp2p `topicStats`). One per (peer, topic)
/// the peer has grafted into or delivered for; the parent `PeerStats` owns the
/// topic-key copy.
pub const TopicStats = struct {
    /// Whether the peer is currently in this topic's mesh (grafted, not pruned).
    in_mesh: bool = false,
    /// Total ticks in the mesh (P1 input + P3-activation gate). Reset on graft;
    /// accrued by `decay` each interval the peer is `in_mesh`.
    mesh_time_ticks: u64 = 0,
    /// P2 first-delivery credit: incremented (capped) on a delivery, decayed.
    first_message_deliveries: f64 = 0,
    /// P3 mesh-delivery credit: incremented (capped) on a delivery or in-window
    /// duplicate while in the mesh, decayed.
    mesh_message_deliveries: f64 = 0,
    /// Whether P3 is active here (in the mesh past the activation grace period).
    /// Set by `decay`; cleared on graft.
    mesh_message_deliveries_active: bool = false,
    /// P3b sticky penalty: a deficit snapshot captured on prune, decayed.
    mesh_failure_penalty: f64 = 0,
    /// P4 invalid-message count: incremented on a rejected message, decayed.
    invalid_message_deliveries: f64 = 0,
};

/// All scoring state for one peer (go-libp2p `peerStats`). Owns its topic-key
/// copies and IP-key list. A disconnected peer is retained (for its score) until
/// `retain_score_ticks` after `disconnected_at_tick`, then `decay` purges it.
pub const PeerStats = struct {
    connected: bool = true,
    /// The tick the peer disconnected at (only meaningful when `!connected`).
    disconnected_at_tick: u64 = 0,
    /// IP keys this peer uses (one per `addIP`); each is also tallied in the global
    /// `ip_counts`, which `removeIP`/purge decrement.
    ips: std.ArrayListUnmanaged(IpKey) = .empty,
    /// P7 misbehaviour counter, decayed.
    behaviour_penalty: f64 = 0,
    /// Per-topic stats; keys are owned copies (freed on purge).
    topics: std.StringHashMapUnmanaged(TopicStats) = .empty,
};

// ---------------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------------

/// The peer scoring engine. Single-owner (the router fiber): no internal locking.
/// All mutation goes through the event hooks; `score` and the threshold helpers
/// are read-only.
pub const PeerScore = struct {
    allocator: std.mem.Allocator,
    params: ScoreParams,
    thresholds: PeerScoreThresholds,
    /// Per-peer stats, keyed by the zero-padded peer bytes. Nested maps/lists are
    /// freed on purge/deinit.
    peers: std.AutoHashMapUnmanaged(PeerKey, PeerStats) = .empty,
    /// P6 input: global IP → count of tracked peers using it; an entry is removed
    /// when its count reaches zero.
    ip_counts: std.AutoHashMapUnmanaged(IpKey, usize) = .empty,
    /// Monotonic tick advanced by `decay`. All time-based accounting (mesh time,
    /// activation, window, retention) is in these ticks.
    tick: u64 = 0,
    /// Optional P5 source; null means P5 is zero.
    app_score_fn: ?AppScoreFn = null,

    pub fn init(
        allocator: std.mem.Allocator,
        params: ScoreParams,
        thresholds: PeerScoreThresholds,
    ) PeerScore {
        return .{
            .allocator = allocator,
            .params = params,
            .thresholds = thresholds,
        };
    }

    /// Free every peer's stats (topic-key copies, topic map, IP list) and the
    /// engine's two maps. After this the engine is unusable.
    pub fn deinit(self: *PeerScore) void {
        var it = self.peers.valueIterator();
        while (it.next()) |stats| self.freePeerStats(stats);
        self.peers.deinit(self.allocator);
        self.ip_counts.deinit(self.allocator);
        self.* = undefined;
    }

    /// Free one peer's owned topic-key copies, its topic map, and its IP list.
    /// Does NOT touch `ip_counts` (callers that purge a peer decrement the global
    /// counts first via `releasePeerIps`).
    fn freePeerStats(self: *PeerScore, stats: *PeerStats) void {
        var it = stats.topics.keyIterator();
        while (it.next()) |key| self.allocator.free(key.*);
        stats.topics.deinit(self.allocator);
        stats.ips.deinit(self.allocator);
    }

    /// Decrement the global IP count for each of a peer's IPs (used on purge). The
    /// per-peer IP list itself is freed separately by `freePeerStats`.
    fn releasePeerIps(self: *PeerScore, stats: *PeerStats) void {
        for (stats.ips.items) |key| self.decIp(key);
    }

    /// Drop one reference to an IP in the global count, removing the entry when it
    /// hits zero.
    fn decIp(self: *PeerScore, key: IpKey) void {
        const entry = self.ip_counts.getPtr(key) orelse return;
        if (entry.* <= 1) {
            _ = self.ip_counts.remove(key);
        } else {
            entry.* -= 1;
        }
    }

    // ----- peer lifecycle ---------------------------------------------------

    /// Begin (or re-activate) scoring for a peer. A still-retained peer is just
    /// marked connected again, keeping its accumulated (negative) score. Best-effort:
    /// on allocation failure the peer stays untracked and scores 0 (safe).
    pub fn addPeer(self: *PeerScore, peer: PeerId) void {
        const key = peerKey(&peer);
        const gop = self.peers.getOrPut(self.allocator, key) catch return;
        if (!gop.found_existing) {
            gop.value_ptr.* = .{};
        } else {
            gop.value_ptr.connected = true;
        }
    }

    /// Mark a peer disconnected. Its stats are RETAINED (so a quick reconnect keeps
    /// its score) until `retain_score_ticks` elapse, then `decay` purges it. IPs are
    /// released now — a disconnected peer no longer occupies a colocation slot
    /// (go-libp2p). No-op for an untracked peer.
    pub fn removePeer(self: *PeerScore, peer: PeerId) void {
        const stats = self.peers.getPtr(peerKey(&peer)) orelse return;
        stats.connected = false;
        stats.disconnected_at_tick = self.tick;
        self.releasePeerIps(stats);
        // Clear the list so a later purge does not double-decrement the global count.
        stats.ips.clearRetainingCapacity();
    }

    /// Fully remove and free a peer's stats now (used by `decay` on retention
    /// expiry). Releases any still-held IPs first.
    fn purgePeer(self: *PeerScore, key: PeerKey) void {
        const stats = self.peers.getPtr(key) orelse return;
        self.releasePeerIps(stats);
        self.freePeerStats(stats);
        _ = self.peers.remove(key);
    }

    // ----- IP tracking (P6) -------------------------------------------------

    /// Record that `peer` uses `ip`: append to the peer's IP list and bump the
    /// global per-IP count. Duplicate IPs for one peer are NOT collapsed — go-libp2p
    /// counts per connection, so two connections from one IP count twice. No-op for
    /// an untracked peer; best-effort on allocation failure.
    pub fn addIP(self: *PeerScore, peer: PeerId, ip: std.Io.net.IpAddress) void {
        const stats = self.peers.getPtr(peerKey(&peer)) orelse return;
        const key = ipKey(ip);
        stats.ips.append(self.allocator, key) catch return;
        const gop = self.ip_counts.getOrPut(self.allocator, key) catch {
            // Undo the per-peer append so the two stay consistent.
            _ = stats.ips.pop();
            return;
        };
        if (!gop.found_existing) gop.value_ptr.* = 0;
        gop.value_ptr.* += 1;
    }

    /// Drop one record of `peer` using `ip`: remove one matching entry from the
    /// peer's IP list and decrement the global count. No-op if the peer is
    /// untracked or does not have that IP.
    pub fn removeIP(self: *PeerScore, peer: PeerId, ip: std.Io.net.IpAddress) void {
        const stats = self.peers.getPtr(peerKey(&peer)) orelse return;
        const key = ipKey(ip);
        for (stats.ips.items, 0..) |existing, i| {
            if (std.mem.eql(u8, &existing, &key)) {
                _ = stats.ips.swapRemove(i);
                self.decIp(key);
                return;
            }
        }
    }

    // ----- topic stats helpers ---------------------------------------------

    /// Get (creating on first use) the per-topic stats for a peer. Returns null
    /// for an untracked peer or on allocation failure. On creation the topic key
    /// is copied (owned by the peer's topic map).
    fn topicStats(self: *PeerScore, peer: PeerKey, topic: []const u8) ?*TopicStats {
        const stats = self.peers.getPtr(peer) orelse return null;
        const gop = stats.topics.getOrPut(self.allocator, topic) catch return null;
        if (!gop.found_existing) {
            const owned = self.allocator.dupe(u8, topic) catch {
                stats.topics.removeByPtr(gop.key_ptr);
                return null;
            };
            gop.key_ptr.* = owned;
            gop.value_ptr.* = .{};
        }
        return gop.value_ptr;
    }

    // ----- mesh events ------------------------------------------------------

    /// Record that `peer` was grafted into `topic`'s mesh: mark in-mesh, restart
    /// mesh time, and deactivate P3 until the grace period re-elapses. The decayed
    /// `mesh_message_deliveries` counter is intentionally preserved (go-libp2p): a
    /// re-grafted peer keeps its accrued delivery credit. No-op for an untracked peer.
    pub fn graft(self: *PeerScore, peer: PeerId, topic: []const u8) void {
        const ts = self.topicStats(peerKey(&peer), topic) orelse return;
        ts.in_mesh = true;
        ts.mesh_time_ticks = 0;
        ts.mesh_message_deliveries_active = false;
    }

    /// Record that `peer` was pruned from `topic`'s mesh. If P3 was active and the
    /// peer was below the delivery threshold, capture the squared deficit into the
    /// sticky P3b penalty — leaving the mesh under-delivering keeps costing the peer
    /// as that penalty decays. No-op if untracked or no stats for the topic.
    pub fn prune(self: *PeerScore, peer: PeerId, topic: []const u8) void {
        const stats = self.peers.getPtr(peerKey(&peer)) orelse return;
        const ts = stats.topics.getPtr(topic) orelse return;
        if (ts.mesh_message_deliveries_active) {
            const p = self.params.topic_default;
            if (ts.mesh_message_deliveries < p.mesh_message_deliveries_threshold) {
                const deficit = p.mesh_message_deliveries_threshold - ts.mesh_message_deliveries;
                ts.mesh_failure_penalty += deficit * deficit;
            }
        }
        ts.in_mesh = false;
        ts.mesh_message_deliveries_active = false;
    }

    // ----- message events ---------------------------------------------------

    /// Record that `peer` was first (or first-in-window) to relay an ACCEPTED
    /// message on `topic`. Credits P2 (capped) and, if in the mesh, P3 (capped).
    /// Simplified vs go-libp2p's per-message validation state machine: the router
    /// calls this once per accepted message per relaying peer. No-op if untracked.
    pub fn deliverMessage(self: *PeerScore, peer: PeerId, topic: []const u8) void {
        const ts = self.topicStats(peerKey(&peer), topic) orelse return;
        const p = self.params.topic_default;

        ts.first_message_deliveries = @min(
            ts.first_message_deliveries + 1,
            p.first_message_deliveries_cap,
        );

        if (ts.in_mesh) {
            ts.mesh_message_deliveries = @min(
                ts.mesh_message_deliveries + 1,
                p.mesh_message_deliveries_cap,
            );
        }
    }

    /// Record an in-window DUPLICATE from a mesh `peer` on `topic`: it relayed the
    /// message, just not first, so credit P3 (capped) but not P2. The router enforces
    /// the `mesh_message_deliveries_window_ticks` window before calling. No-op if the
    /// peer is not in the mesh or is untracked.
    pub fn duplicateMessage(self: *PeerScore, peer: PeerId, topic: []const u8) void {
        const ts = self.topicStats(peerKey(&peer), topic) orelse return;
        if (!ts.in_mesh) return;
        const p = self.params.topic_default;
        ts.mesh_message_deliveries = @min(
            ts.mesh_message_deliveries + 1,
            p.mesh_message_deliveries_cap,
        );
    }

    /// Record that `peer` sent an INVALID message on `topic` (P4): bump its
    /// invalid-delivery counter (the score squares it). Fires on signature/validation
    /// failure against the SENDING peer. No-op for an untracked peer.
    pub fn rejectMessage(self: *PeerScore, peer: PeerId, topic: []const u8) void {
        const ts = self.topicStats(peerKey(&peer), topic) orelse return;
        ts.invalid_message_deliveries += 1;
    }

    /// Add `count` to a peer's behaviour-penalty counter (P7): broken IWANT
    /// promises, too-frequent grafts, etc. The score squares the excess above the
    /// threshold. No-op for an untracked peer.
    pub fn addPenalty(self: *PeerScore, peer: PeerId, count: f64) void {
        const stats = self.peers.getPtr(peerKey(&peer)) orelse return;
        stats.behaviour_penalty += count;
    }

    // ----- decay ------------------------------------------------------------

    /// Advance to `tick` (the heartbeat counter) and, once a full
    /// `decay_interval_ticks` has elapsed, run the per-interval bookkeeping:
    /// geometrically decay every fading counter (snapping sub-`decay_to_zero` to
    /// zero), accrue mesh time + activate P3 for in-mesh topics, and purge
    /// disconnected peers past their retention window. Within an interval only
    /// `self.tick` advances and no counters move. Caller invokes every heartbeat
    /// with a monotonic tick.
    pub fn decay(self: *PeerScore, tick: u64) void {
        const elapsed = tick -| self.tick;
        self.tick = tick;
        if (elapsed < self.params.decay_interval_ticks) return;

        const gp = self.params;
        const tp = gp.topic_default;

        // Collect peers to purge after the walk: removing from the map we are
        // iterating would invalidate the iterator.
        var to_purge: std.ArrayListUnmanaged(PeerKey) = .empty;
        defer to_purge.deinit(self.allocator);

        var it = self.peers.iterator();
        while (it.next()) |entry| {
            const stats = entry.value_ptr;

            if (!stats.connected) {
                // A disconnected peer past its retention window is purged; until
                // then its score (and counters) are kept but still decayed below.
                if (self.tick -| stats.disconnected_at_tick >= gp.retain_score_ticks) {
                    to_purge.append(self.allocator, entry.key_ptr.*) catch {};
                    continue;
                }
            }

            stats.behaviour_penalty = decayValue(
                stats.behaviour_penalty,
                gp.behaviour_penalty_decay,
                gp.decay_to_zero,
            );

            var topic_it = stats.topics.valueIterator();
            while (topic_it.next()) |ts| {
                ts.first_message_deliveries = decayValue(
                    ts.first_message_deliveries,
                    tp.first_message_deliveries_decay,
                    gp.decay_to_zero,
                );
                ts.mesh_message_deliveries = decayValue(
                    ts.mesh_message_deliveries,
                    tp.mesh_message_deliveries_decay,
                    gp.decay_to_zero,
                );
                ts.mesh_failure_penalty = decayValue(
                    ts.mesh_failure_penalty,
                    tp.mesh_failure_penalty_decay,
                    gp.decay_to_zero,
                );
                ts.invalid_message_deliveries = decayValue(
                    ts.invalid_message_deliveries,
                    tp.invalid_message_deliveries_decay,
                    gp.decay_to_zero,
                );

                // Accrue mesh time and activate P3 once past the grace period.
                // Only connected peers accrue: a disconnected peer's stats may still
                // record `in_mesh` from before, but it is no longer in any live mesh.
                if (stats.connected and ts.in_mesh) {
                    ts.mesh_time_ticks += gp.decay_interval_ticks;
                    if (ts.mesh_time_ticks > tp.mesh_message_deliveries_activation_ticks) {
                        ts.mesh_message_deliveries_active = true;
                    }
                }
            }
        }

        for (to_purge.items) |key| self.purgePeer(key);
    }

    /// Multiply `value` by `factor`, snapping the result to zero if it falls to or
    /// below `to_zero` (so a decaying counter does not linger as float dust).
    fn decayValue(value: f64, factor: f64, to_zero: f64) f64 {
        const decayed = value * factor;
        if (decayed <= to_zero) return 0;
        return decayed;
    }

    // ----- score ------------------------------------------------------------

    /// Compute the peer's total score: Σ_topic topicScore + appScore·weight +
    /// ipColocation + behaviour. An untracked peer scores 0.
    pub fn score(self: *PeerScore, peer: PeerId) f64 {
        const key = peerKey(&peer);
        const stats = self.peers.getPtr(key) orelse return 0;

        var total: f64 = 0;

        // Per-topic P1-P4.
        var topic_it = stats.topics.valueIterator();
        while (topic_it.next()) |ts| total += self.topicScore(ts);

        // P5: application-specific.
        if (self.app_score_fn) |f| {
            total += f.score(f.ctx, key) * self.params.app_specific_weight;
        }

        // P6: IP colocation.
        total += self.ipColocationScore(stats);

        // P7: behaviour penalty.
        total += self.behaviourScore(stats);

        return total;
    }

    /// The per-topic contribution: topic_weight · (P1·w1 + P2·w2 + P3·w3 + P3b·w3b
    /// + P4·w4), with each Pn clamped per the params.
    fn topicScore(self: *PeerScore, ts: *const TopicStats) f64 {
        const p = self.params.topic_default;
        var s: f64 = 0;

        // P1: time in mesh, in quantum units, capped. Both the accrued mesh time
        // and the quantum are tick counts, so the division is integer (truncating)
        // before the float cast — 5 ticks / quantum 2 yields 2, not 2.5.
        if (p.time_in_mesh_weight != 0) {
            const quantum = @max(p.time_in_mesh_quantum_ticks, 1); // avoid div-by-zero
            const quanta: f64 = @floatFromInt(ts.mesh_time_ticks / quantum);
            const p1 = @min(quanta, p.time_in_mesh_cap);
            s += p1 * p.time_in_mesh_weight;
        }

        // P2: first message deliveries (the stored, already-capped counter).
        if (p.first_message_deliveries_weight != 0) {
            s += ts.first_message_deliveries * p.first_message_deliveries_weight;
        }

        // P3: mesh delivery deficit (only once active, only below threshold).
        if (p.mesh_message_deliveries_weight != 0 and ts.mesh_message_deliveries_active) {
            if (ts.mesh_message_deliveries < p.mesh_message_deliveries_threshold) {
                const deficit = p.mesh_message_deliveries_threshold - ts.mesh_message_deliveries;
                s += deficit * deficit * p.mesh_message_deliveries_weight;
            }
        }

        // P3b: sticky mesh-failure penalty.
        if (p.mesh_failure_penalty_weight != 0) {
            s += ts.mesh_failure_penalty * p.mesh_failure_penalty_weight;
        }

        // P4: invalid message deliveries, squared.
        if (p.invalid_message_deliveries_weight != 0) {
            const invalid = ts.invalid_message_deliveries;
            s += invalid * invalid * p.invalid_message_deliveries_weight;
        }

        return s * p.topic_weight;
    }

    /// P6: for each of the peer's IP entries whose global count exceeds the
    /// colocation threshold, add the squared surplus times the (negative) weight.
    /// Repeated entries for one IP count once each (per-connection bookkeeping).
    fn ipColocationScore(self: *PeerScore, stats: *const PeerStats) f64 {
        if (self.params.ip_colocation_factor_weight == 0) return 0;
        var s: f64 = 0;
        for (stats.ips.items) |key| {
            const count: f64 = @floatFromInt(self.ip_counts.get(key) orelse 0);
            const surplus = count - self.params.ip_colocation_factor_threshold;
            if (surplus > 0) {
                s += surplus * surplus * self.params.ip_colocation_factor_weight;
            }
        }
        return s;
    }

    /// P7: squared excess of the behaviour counter above its threshold, times the
    /// (negative) weight. Zero when at or below the threshold.
    fn behaviourScore(self: *PeerScore, stats: *const PeerStats) f64 {
        if (self.params.behaviour_penalty_weight == 0) return 0;
        const excess = stats.behaviour_penalty - self.params.behaviour_penalty_threshold;
        if (excess <= 0) return 0;
        return excess * excess * self.params.behaviour_penalty_weight;
    }

    // ----- threshold helpers ------------------------------------------------

    /// Whether the peer's score is high enough to exchange gossip (IHAVE/IWANT).
    pub fn aboveGossipThreshold(self: *PeerScore, peer: PeerId) bool {
        return self.score(peer) >= self.thresholds.gossip_threshold;
    }

    /// Whether the peer's score is high enough to publish messages through.
    pub fn abovePublishThreshold(self: *PeerScore, peer: PeerId) bool {
        return self.score(peer) >= self.thresholds.publish_threshold;
    }

    /// Whether the peer should be graylisted (score at or below the graylist
    /// threshold — its RPCs are ignored entirely).
    pub fn belowGraylist(self: *PeerScore, peer: PeerId) bool {
        return self.score(peer) <= self.thresholds.graylist_threshold;
    }

    /// Whether the peer's score is high enough to accept peer-exchange info from.
    pub fn aboveAcceptPX(self: *PeerScore, peer: PeerId) bool {
        return self.score(peer) >= self.thresholds.accept_px_threshold;
    }

    /// Whether the peer's score clears the opportunistic-graft bar.
    pub fn aboveOpportunisticGraft(self: *PeerScore, peer: PeerId) bool {
        return self.score(peer) >= self.thresholds.opportunistic_graft_threshold;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// Each test isolates ONE score term by zeroing every other weight, then asserts
// the exact go-libp2p formula. A small helper builds distinct peer ids.

const testing = std.testing;

/// A deterministic distinct PeerId for tests (one meaningful byte). Two calls with
/// the same byte produce equal ids (so the same peer can be referenced repeatedly).
fn testPeer(byte: u8) PeerId {
    var p: PeerId = .{ .bytes = [_]u8{0} ** 64, .len = 4 };
    p.bytes[0] = byte;
    return p;
}

/// Params with every weight zeroed; tests turn on exactly the term they exercise.
fn zeroParams() ScoreParams {
    return .{
        .app_specific_weight = 0,
        .ip_colocation_factor_weight = 0,
        .ip_colocation_factor_threshold = 0,
        .behaviour_penalty_weight = 0,
        .behaviour_penalty_threshold = 0,
        .behaviour_penalty_decay = 1.0,
        .decay_interval_ticks = 1,
        .decay_to_zero = 0.01,
        .retain_score_ticks = 100,
        .topic_default = .{
            .topic_weight = 1.0,
            .time_in_mesh_weight = 0,
            .time_in_mesh_quantum_ticks = 1,
            .time_in_mesh_cap = 0,
            .first_message_deliveries_weight = 0,
            .first_message_deliveries_decay = 1.0,
            .first_message_deliveries_cap = 0,
            .mesh_message_deliveries_weight = 0,
            .mesh_message_deliveries_decay = 1.0,
            .mesh_message_deliveries_threshold = 0,
            .mesh_message_deliveries_cap = 0,
            .mesh_message_deliveries_activation_ticks = 0,
            .mesh_message_deliveries_window_ticks = 0,
            .mesh_failure_penalty_weight = 0,
            .mesh_failure_penalty_decay = 1.0,
            .invalid_message_deliveries_weight = 0,
            .invalid_message_deliveries_decay = 1.0,
        },
    };
}

fn zeroThresholds() PeerScoreThresholds {
    return .{
        .gossip_threshold = 0,
        .publish_threshold = 0,
        .graylist_threshold = 0,
        .accept_px_threshold = 0,
        .opportunistic_graft_threshold = 0,
    };
}

test "P1 time in mesh rises with accrued mesh time and is capped" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.topic_default.time_in_mesh_weight = 0.5;
    params.topic_default.time_in_mesh_quantum_ticks = 2;
    params.topic_default.time_in_mesh_cap = 3.0; // cap the quantum count at 3.

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(1);
    ps.addPeer(peer);
    ps.graft(peer, "topic");

    // Freshly grafted: no mesh time yet.
    try testing.expectEqual(@as(f64, 0), ps.score(peer));

    // 4 decay ticks → mesh_time = 4 ticks → quantum count = 4/2 = 2 → 2·0.5 = 1.0.
    var t: u64 = 0;
    while (t < 4) : (t += 1) ps.decay(ps.tick + 1);
    try testing.expectApproxEqAbs(@as(f64, 1.0), ps.score(peer), 1e-9);

    // Run far past the cap: quantum count saturates at 3 → 3·0.5 = 1.5.
    var i: u64 = 0;
    while (i < 100) : (i += 1) ps.decay(ps.tick + 1);
    try testing.expectApproxEqAbs(@as(f64, 1.5), ps.score(peer), 1e-9);
}

test "P1 quantum division truncates before the float cast (go parity)" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.topic_default.time_in_mesh_weight = 1.0;
    params.topic_default.time_in_mesh_quantum_ticks = 2;
    params.topic_default.time_in_mesh_cap = 100.0; // well above the value under test.

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(9);
    ps.addPeer(peer);
    ps.graft(peer, "topic");

    // Accrue exactly 5 ticks of mesh time. With quantum 2 the count is the integer
    // 5 / 2 = 2 (NOT 2.5): truncation happens before the float cast. P1 = 2·1.0 = 2.0.
    var t: u64 = 0;
    while (t < 5) : (t += 1) ps.decay(ps.tick + 1);
    try testing.expectApproxEqAbs(@as(f64, 2.0), ps.score(peer), 1e-9);
}

test "graft preserves the decayed mesh-delivery counter (go parity)" {
    const allocator = testing.allocator;
    var params = zeroParams();
    // Drive P3 off the mesh-delivery counter so the score reflects its value.
    params.topic_default.mesh_message_deliveries_weight = -1.0;
    params.topic_default.mesh_message_deliveries_threshold = 10.0;
    params.topic_default.mesh_message_deliveries_cap = 100.0;
    params.topic_default.mesh_message_deliveries_activation_ticks = 1;
    params.topic_default.mesh_message_deliveries_decay = 0.5;

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(10);
    ps.addPeer(peer);
    ps.graft(peer, "topic");

    // Build up 4 mesh deliveries, then decay once (factor 0.5) → counter = 2.0.
    var i: u32 = 0;
    while (i < 4) : (i += 1) ps.deliverMessage(peer, "topic");
    ps.decay(ps.tick + 1);
    try testing.expectApproxEqAbs(@as(f64, 2.0), ps.topicStats(peerKey(&peer), "topic").?.mesh_message_deliveries, 1e-9);

    // Re-graft: the decayed counter must survive (go does NOT zero it); only the
    // mesh time restarts and P3 deactivates.
    ps.graft(peer, "topic");
    const ts = ps.topicStats(peerKey(&peer), "topic").?;
    try testing.expectApproxEqAbs(@as(f64, 2.0), ts.mesh_message_deliveries, 1e-9);
    try testing.expectEqual(@as(u64, 0), ts.mesh_time_ticks);
    try testing.expectEqual(false, ts.mesh_message_deliveries_active);
}

test "P2 first deliveries rise (capped) then shrink by the decay factor" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.topic_default.first_message_deliveries_weight = 2.0;
    params.topic_default.first_message_deliveries_cap = 3.0;
    params.topic_default.first_message_deliveries_decay = 0.5;

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(2);
    ps.addPeer(peer);
    ps.graft(peer, "topic");

    // 5 deliveries but the counter caps at 3 → 3·2.0 = 6.0.
    var i: u32 = 0;
    while (i < 5) : (i += 1) ps.deliverMessage(peer, "topic");
    try testing.expectApproxEqAbs(@as(f64, 6.0), ps.score(peer), 1e-9);

    // One decay (factor 0.5): counter 3 → 1.5 → score 1.5·2.0 = 3.0.
    ps.decay(ps.tick + 1);
    try testing.expectApproxEqAbs(@as(f64, 3.0), ps.score(peer), 1e-9);
}

test "P3 mesh delivery deficit is negative below threshold, zero above" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.topic_default.mesh_message_deliveries_weight = -1.0;
    params.topic_default.mesh_message_deliveries_threshold = 5.0;
    params.topic_default.mesh_message_deliveries_cap = 100.0;
    params.topic_default.mesh_message_deliveries_activation_ticks = 2;

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(3);
    ps.addPeer(peer);
    ps.graft(peer, "topic");

    // Before activation (mesh_time not past 2): P3 is inert even with no deliveries.
    ps.decay(ps.tick + 1); // mesh_time = 1, not > 2 → still inactive
    try testing.expectApproxEqAbs(@as(f64, 0), ps.score(peer), 1e-9);

    // Drive mesh_time past the activation window (need > 2).
    ps.decay(ps.tick + 1); // mesh_time = 2, not > 2
    ps.decay(ps.tick + 1); // mesh_time = 3 > 2 → active
    // 0 deliveries, threshold 5 → deficit 5 → 25·(-1) = -25.
    try testing.expectApproxEqAbs(@as(f64, -25.0), ps.score(peer), 1e-9);

    // Deliver above threshold → deficit gone → P3 contributes 0.
    var i: u32 = 0;
    while (i < 6) : (i += 1) ps.deliverMessage(peer, "topic");
    try testing.expectApproxEqAbs(@as(f64, 0), ps.score(peer), 1e-9);
}

test "P3b mesh failure penalty is captured on prune and decays" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.topic_default.mesh_message_deliveries_weight = 0; // isolate P3b from P3
    params.topic_default.mesh_message_deliveries_threshold = 4.0;
    params.topic_default.mesh_message_deliveries_cap = 100.0;
    params.topic_default.mesh_message_deliveries_activation_ticks = 1;
    params.topic_default.mesh_failure_penalty_weight = -2.0;
    params.topic_default.mesh_failure_penalty_decay = 0.5;

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(4);
    ps.addPeer(peer);
    ps.graft(peer, "topic");

    // Activate P3 (mesh_time must exceed 1).
    ps.decay(ps.tick + 1); // mesh_time = 1, not > 1
    ps.decay(ps.tick + 1); // mesh_time = 2 > 1 → active
    // 0 deliveries, threshold 4 → on prune capture deficit^2 = 16.
    ps.prune(peer, "topic");
    // P3b = 16·(-2) = -32 (P3 weight is 0, so only P3b shows).
    try testing.expectApproxEqAbs(@as(f64, -32.0), ps.score(peer), 1e-9);

    // One decay halves the penalty: 16 → 8 → 8·(-2) = -16.
    ps.decay(ps.tick + 1);
    try testing.expectApproxEqAbs(@as(f64, -16.0), ps.score(peer), 1e-9);
}

test "P4 invalid messages give a squared negative penalty that decays" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.topic_default.invalid_message_deliveries_weight = -1.0;
    params.topic_default.invalid_message_deliveries_decay = 0.5;

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(5);
    ps.addPeer(peer);
    ps.graft(peer, "topic");

    // 3 invalid messages → 3^2·(-1) = -9.
    var i: u32 = 0;
    while (i < 3) : (i += 1) ps.rejectMessage(peer, "topic");
    try testing.expectApproxEqAbs(@as(f64, -9.0), ps.score(peer), 1e-9);

    // One decay (0.5): counter 3 → 1.5 → 1.5^2·(-1) = -2.25.
    ps.decay(ps.tick + 1);
    try testing.expectApproxEqAbs(@as(f64, -2.25), ps.score(peer), 1e-9);
}

test "P6 IP colocation penalises peers sharing an IP and eases on removeIP" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.ip_colocation_factor_weight = -1.0;
    params.ip_colocation_factor_threshold = 2.0; // surplus is count above 2.

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const shared: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 10, 0, 0, 1 }, .port = 1111 } };

    // Five peers on one IP. The scored peer (peer 10) sees count = 5 → surplus 3.
    const scored = testPeer(10);
    ps.addPeer(scored);
    ps.addIP(scored, shared);
    var i: u8 = 0;
    while (i < 4) : (i += 1) {
        const other = testPeer(20 + i);
        ps.addPeer(other);
        // A different port on the same address must map to the same IP key.
        ps.addIP(other, .{ .ip4 = .{ .bytes = .{ 10, 0, 0, 1 }, .port = 2000 + @as(u16, i) } });
    }
    // surplus = 5 - 2 = 3 → 3^2·(-1) = -9.
    try testing.expectApproxEqAbs(@as(f64, -9.0), ps.score(scored), 1e-9);

    // Remove two peers' IPs → count = 3 → surplus 1 → 1^2·(-1) = -1.
    ps.removeIP(testPeer(20), .{ .ip4 = .{ .bytes = .{ 10, 0, 0, 1 }, .port = 2000 } });
    ps.removeIP(testPeer(21), .{ .ip4 = .{ .bytes = .{ 10, 0, 0, 1 }, .port = 2001 } });
    try testing.expectApproxEqAbs(@as(f64, -1.0), ps.score(scored), 1e-9);

    // Drop one more → count = 2 = threshold → surplus 0 → no penalty.
    ps.removeIP(testPeer(22), .{ .ip4 = .{ .bytes = .{ 10, 0, 0, 1 }, .port = 2002 } });
    try testing.expectApproxEqAbs(@as(f64, 0), ps.score(scored), 1e-9);
}

test "P7 behaviour penalty is squared excess above threshold and decays away" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.behaviour_penalty_weight = -1.0;
    params.behaviour_penalty_threshold = 1.0; // excess is counter above 1.
    params.behaviour_penalty_decay = 0.5;

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(6);
    ps.addPeer(peer);

    // Penalty 3, threshold 1 → excess 2 → 2^2·(-1) = -4.
    ps.addPenalty(peer, 3.0);
    try testing.expectApproxEqAbs(@as(f64, -4.0), ps.score(peer), 1e-9);

    // One decay (0.5): counter 3 → 1.5 → excess 0.5 → 0.25·(-1) = -0.25.
    ps.decay(ps.tick + 1);
    try testing.expectApproxEqAbs(@as(f64, -0.25), ps.score(peer), 1e-9);

    // Another decay: counter 1.5 → 0.75 ≤ threshold 1 → excess 0 → no penalty.
    ps.decay(ps.tick + 1);
    try testing.expectApproxEqAbs(@as(f64, 0), ps.score(peer), 1e-9);
}

test "thresholds classify a negative-scoring peer" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.behaviour_penalty_weight = -1.0;
    params.behaviour_penalty_threshold = 0;

    var thresholds = zeroThresholds();
    thresholds.gossip_threshold = -10.0;
    thresholds.publish_threshold = -20.0;
    thresholds.graylist_threshold = -40.0;
    thresholds.accept_px_threshold = 0;

    var ps = PeerScore.init(allocator, params, thresholds);
    defer ps.deinit();

    const peer = testPeer(7);
    ps.addPeer(peer);
    // Penalty 7 → excess 7 → 49·(-1) = -49: below graylist (-40), below gossip.
    ps.addPenalty(peer, 7.0);
    try testing.expectApproxEqAbs(@as(f64, -49.0), ps.score(peer), 1e-9);

    try testing.expect(ps.belowGraylist(peer));
    try testing.expect(!ps.aboveGossipThreshold(peer));
    try testing.expect(!ps.abovePublishThreshold(peer));
    try testing.expect(!ps.aboveAcceptPX(peer));

    // A fresh, unpenalised peer scores 0: above gossip/publish, not graylisted.
    const good = testPeer(8);
    ps.addPeer(good);
    try testing.expect(ps.aboveGossipThreshold(good));
    try testing.expect(ps.abovePublishThreshold(good));
    try testing.expect(!ps.belowGraylist(good));
    try testing.expect(ps.aboveAcceptPX(good)); // 0 >= 0
}

test "decay snaps a tiny value to zero and purges a disconnected peer after retention" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.behaviour_penalty_weight = -1.0;
    params.behaviour_penalty_threshold = 0;
    params.behaviour_penalty_decay = 0.5;
    params.decay_to_zero = 0.1; // anything <= 0.1 snaps to 0
    params.retain_score_ticks = 3;

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(9);
    ps.addPeer(peer);
    // Start at 0.15: one decay → 0.075 <= 0.1 → snaps to 0.
    ps.addPenalty(peer, 0.15);
    ps.decay(ps.tick + 1);
    // Counter is exactly zero now → excess 0 → score 0 (and not lingering dust).
    try testing.expectApproxEqAbs(@as(f64, 0), ps.score(peer), 1e-12);

    // Disconnect, then advance past the retention window: the peer is purged and
    // scores 0 (untracked). disconnected_at_tick = current tick (1).
    ps.removePeer(peer);
    // retain_score_ticks = 3: purge once tick - disconnected_at >= 3.
    ps.decay(ps.tick + 1); // tick 2, elapsed 1 < 3 → retained
    try testing.expect(ps.peers.contains(peerKey(&peer)));
    ps.decay(ps.tick + 1); // tick 3, elapsed 2 < 3 → retained
    try testing.expect(ps.peers.contains(peerKey(&peer)));
    ps.decay(ps.tick + 1); // tick 4, elapsed 3 >= 3 → purged
    try testing.expect(!ps.peers.contains(peerKey(&peer)));
    try testing.expectApproxEqAbs(@as(f64, 0), ps.score(peer), 1e-12);
}

test "duplicateMessage credits mesh deliveries only for in-mesh peers" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.topic_default.mesh_message_deliveries_weight = -1.0;
    params.topic_default.mesh_message_deliveries_threshold = 5.0;
    params.topic_default.mesh_message_deliveries_cap = 100.0;
    params.topic_default.mesh_message_deliveries_activation_ticks = 1;

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(11);
    ps.addPeer(peer);
    ps.graft(peer, "topic");
    ps.decay(ps.tick + 1); // mesh_time 1, not > 1
    ps.decay(ps.tick + 1); // mesh_time 2 > 1 → active

    // Duplicates count toward mesh deliveries while in mesh: 3 duplicates → counter
    // 3, threshold 5 → deficit 2 → 4·(-1) = -4.
    var i: u32 = 0;
    while (i < 3) : (i += 1) ps.duplicateMessage(peer, "topic");
    try testing.expectApproxEqAbs(@as(f64, -4.0), ps.score(peer), 1e-9);

    // After prune (out of mesh), duplicates do not count anymore: counter unchanged.
    ps.prune(peer, "topic");
    ps.duplicateMessage(peer, "topic");
    const ts = ps.peers.getPtr(peerKey(&peer)).?.topics.getPtr("topic").?;
    try testing.expectApproxEqAbs(@as(f64, 3.0), ts.mesh_message_deliveries, 1e-9);
}

test "appScore is added through the optional callback and weight" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.app_specific_weight = 3.0;

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const Ctx = struct {
        fn score(_: *anyopaque, _: PeerKey) f64 {
            return 4.0;
        }
    };
    var dummy: u8 = 0;
    ps.app_score_fn = .{ .ctx = &dummy, .score = Ctx.score };

    const peer = testPeer(12);
    ps.addPeer(peer);
    // appScore 4 · weight 3 = 12.
    try testing.expectApproxEqAbs(@as(f64, 12.0), ps.score(peer), 1e-9);
}

test "addPeer on a retained disconnected peer keeps its accumulated score" {
    const allocator = testing.allocator;
    var params = zeroParams();
    params.behaviour_penalty_weight = -1.0;
    params.behaviour_penalty_threshold = 0;
    params.behaviour_penalty_decay = 1.0; // no decay, so the score is stable
    params.retain_score_ticks = 100;

    var ps = PeerScore.init(allocator, params, zeroThresholds());
    defer ps.deinit();

    const peer = testPeer(13);
    ps.addPeer(peer);
    ps.addPenalty(peer, 4.0); // excess 4 → 16·(-1) = -16
    try testing.expectApproxEqAbs(@as(f64, -16.0), ps.score(peer), 1e-9);

    ps.removePeer(peer); // retained, not purged
    // Reconnect within the retention window: stats (and the -16) survive.
    ps.addPeer(peer);
    try testing.expectApproxEqAbs(@as(f64, -16.0), ps.score(peer), 1e-9);
}
