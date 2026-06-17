const std = @import("std");
const Allocator = std.mem.Allocator;
const PeerId = @import("peer_id").PeerId;

pub const DEFAULT_DECAY_INTERVAL_MS: i64 = 1000;
pub const DEFAULT_DECAY_TO_ZERO: f64 = 0.1;

pub fn scoreParameterDecay(decay_ms: i64) f64 {
    return scoreParameterDecayWithBase(decay_ms, DEFAULT_DECAY_INTERVAL_MS, DEFAULT_DECAY_TO_ZERO);
}

/// Returns the per-tick decay factor such that after `decay_ms / base_ms` ticks
/// a value reaches `decay_to_zero`. Solves `factor^n = decay_to_zero` for factor.
pub fn scoreParameterDecayWithBase(decay_ms: i64, base_ms: i64, decay_to_zero: f64) f64 {
    const ticks = @as(f64, @floatFromInt(decay_ms)) / @as(f64, @floatFromInt(base_ms));
    return std.math.pow(f64, decay_to_zero, 1.0 / ticks);
}

pub const PeerScoreThresholds = struct {
    gossip_threshold: f64 = -10.0,
    publish_threshold: f64 = -50.0,
    graylist_threshold: f64 = -80.0,
    accept_px_threshold: f64 = 10.0,
    opportunistic_graft_threshold: f64 = 20.0,

    pub fn validate(self: PeerScoreThresholds) ?[]const u8 {
        if (self.gossip_threshold > 0) return "invalid gossip_threshold; must be <= 0";
        if (self.publish_threshold > 0 or self.publish_threshold > self.gossip_threshold)
            return "invalid publish_threshold; must be <= 0 and <= gossip_threshold";
        if (self.graylist_threshold > 0 or self.graylist_threshold > self.publish_threshold)
            return "invalid graylist_threshold; must be <= 0 and <= publish_threshold";
        if (self.accept_px_threshold < 0) return "invalid accept_px_threshold; must be >= 0";
        if (self.opportunistic_graft_threshold < 0)
            return "invalid opportunistic_graft_threshold; must be >= 0";
        return null;
    }
};

pub const TopicScoreParams = struct {
    topic_weight: f64 = 0.5,

    time_in_mesh_weight: f64 = 1.0,
    time_in_mesh_quantum_ms: i64 = 1,
    time_in_mesh_cap: f64 = 3600.0,

    first_message_deliveries_weight: f64 = 1.0,
    first_message_deliveries_decay: f64 = 0.5,
    first_message_deliveries_cap: f64 = 2000.0,

    mesh_message_deliveries_weight: f64 = -1.0,
    mesh_message_deliveries_decay: f64 = 0.5,
    mesh_message_deliveries_cap: f64 = 100.0,
    mesh_message_deliveries_threshold: f64 = 20.0,
    mesh_message_deliveries_window_ms: i64 = 10,
    mesh_message_deliveries_activation_ms: i64 = 5000,

    mesh_failure_penalty_weight: f64 = -1.0,
    mesh_failure_penalty_decay: f64 = 0.5,

    invalid_message_deliveries_weight: f64 = -1.0,
    invalid_message_deliveries_decay: f64 = 0.3,

    pub fn validate(self: TopicScoreParams) ?[]const u8 {
        if (self.topic_weight < 0) return "invalid topic_weight; must be >= 0";
        if (self.time_in_mesh_quantum_ms == 0)
            return "invalid time_in_mesh_quantum; must be non-zero";
        if (self.time_in_mesh_weight < 0)
            return "invalid time_in_mesh_weight; must be >= 0";
        if (self.time_in_mesh_weight != 0 and self.time_in_mesh_cap <= 0)
            return "invalid time_in_mesh_cap; must be positive";

        if (self.first_message_deliveries_weight < 0)
            return "invalid first_message_deliveries_weight; must be >= 0";
        if (self.first_message_deliveries_weight != 0 and
            (self.first_message_deliveries_decay <= 0 or self.first_message_deliveries_decay >= 1))
            return "invalid first_message_deliveries_decay; must be in (0, 1)";
        if (self.first_message_deliveries_weight != 0 and self.first_message_deliveries_cap <= 0)
            return "invalid first_message_deliveries_cap; must be positive";

        if (self.mesh_message_deliveries_weight > 0)
            return "invalid mesh_message_deliveries_weight; must be <= 0";
        if (self.mesh_message_deliveries_weight != 0 and
            (self.mesh_message_deliveries_decay <= 0 or self.mesh_message_deliveries_decay >= 1))
            return "invalid mesh_message_deliveries_decay; must be in (0, 1)";
        if (self.mesh_message_deliveries_weight != 0 and self.mesh_message_deliveries_cap <= 0)
            return "invalid mesh_message_deliveries_cap; must be positive";
        if (self.mesh_message_deliveries_weight != 0 and self.mesh_message_deliveries_threshold <= 0)
            return "invalid mesh_message_deliveries_threshold; must be positive";
        if (self.mesh_message_deliveries_weight != 0 and self.mesh_message_deliveries_activation_ms < 1000)
            return "invalid mesh_message_deliveries_activation; must be at least 1000 ms";

        if (self.mesh_failure_penalty_weight > 0)
            return "invalid mesh_failure_penalty_weight; must be <= 0";
        if (self.mesh_failure_penalty_weight != 0 and
            (self.mesh_failure_penalty_decay <= 0 or self.mesh_failure_penalty_decay >= 1))
            return "invalid mesh_failure_penalty_decay; must be in (0, 1)";

        if (self.invalid_message_deliveries_weight > 0)
            return "invalid invalid_message_deliveries_weight; must be <= 0";
        if (self.invalid_message_deliveries_decay <= 0 or self.invalid_message_deliveries_decay >= 1)
            return "invalid invalid_message_deliveries_decay; must be in (0, 1)";
        return null;
    }
};

pub const PeerScoreParams = struct {
    topics: std.StringHashMapUnmanaged(TopicScoreParams) = .empty,
    topic_score_cap: f64 = 3600.0,
    app_specific_weight: f64 = 10.0,

    ip_colocation_factor_weight: f64 = -5.0,
    ip_colocation_factor_threshold: f64 = 10.0,
    ip_colocation_factor_whitelist: std.StringHashMapUnmanaged(void) = .empty,

    behaviour_penalty_weight: f64 = -10.0,
    behaviour_penalty_threshold: f64 = 0.0,
    behaviour_penalty_decay: f64 = 0.2,

    decay_interval_ms: i64 = DEFAULT_DECAY_INTERVAL_MS,
    decay_to_zero: f64 = DEFAULT_DECAY_TO_ZERO,

    retain_score_ms: i64 = 3600 * 1000,

    slow_peer_weight: f64 = -0.2,
    slow_peer_threshold: f64 = 0.0,
    slow_peer_decay: f64 = 0.2,

    pub fn deinit(self: *PeerScoreParams, allocator: Allocator) void {
        var it = self.topics.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.topics.deinit(allocator);

        var wl_it = self.ip_colocation_factor_whitelist.iterator();
        while (wl_it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.ip_colocation_factor_whitelist.deinit(allocator);
    }

    pub fn validate(self: PeerScoreParams) ?[]const u8 {
        if (self.topic_score_cap < 0) return "invalid topic_score_cap; must be >= 0";
        if (self.ip_colocation_factor_weight > 0)
            return "invalid ip_colocation_factor_weight; must be <= 0";
        if (self.ip_colocation_factor_weight != 0 and self.ip_colocation_factor_threshold < 1)
            return "invalid ip_colocation_factor_threshold; must be >= 1";
        if (self.behaviour_penalty_weight > 0)
            return "invalid behaviour_penalty_weight; must be <= 0";
        if (self.behaviour_penalty_weight != 0 and
            (self.behaviour_penalty_decay <= 0 or self.behaviour_penalty_decay >= 1))
            return "invalid behaviour_penalty_decay; must be in (0, 1)";
        if (self.behaviour_penalty_threshold < 0)
            return "invalid behaviour_penalty_threshold; must be >= 0";
        if (self.decay_interval_ms < 1000)
            return "invalid decay_interval; must be at least 1000 ms";
        if (self.decay_to_zero <= 0 or self.decay_to_zero >= 1)
            return "invalid decay_to_zero; must be in (0, 1)";

        var it = self.topics.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.validate()) |msg| return msg;
        }
        return null;
    }
};

const MeshStatus = union(enum) {
    inactive,
    active: struct {
        graft_time_ms: i64,
        mesh_time_ms: i64,
    },

    fn isActive(self: MeshStatus) bool {
        return self == .active;
    }
};

const TopicStats = struct {
    mesh_status: MeshStatus = .inactive,
    first_message_deliveries: f64 = 0,
    mesh_message_deliveries_active: bool = false,
    mesh_message_deliveries: f64 = 0,
    mesh_failure_penalty: f64 = 0,
    invalid_message_deliveries: f64 = 0,
};

const ConnectionStatus = union(enum) {
    connected,
    disconnected: struct { expire_ms: i64 },
};

const PeerStats = struct {
    status: ConnectionStatus = .connected,
    topics: std.StringHashMapUnmanaged(TopicStats) = .empty,
    known_ips: std.StringHashMapUnmanaged(void) = .empty,
    behaviour_penalty: f64 = 0,
    application_score: f64 = 0,
    slow_peer_penalty: f64 = 0,

    fn deinit(self: *PeerStats, allocator: Allocator) void {
        var it = self.topics.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        self.topics.deinit(allocator);

        var ip_it = self.known_ips.iterator();
        while (ip_it.next()) |entry| allocator.free(entry.key_ptr.*);
        self.known_ips.deinit(allocator);
    }
};

pub const PeerScore = struct {
    allocator: Allocator,
    params: PeerScoreParams,
    thresholds: PeerScoreThresholds,
    peer_stats: std.AutoHashMapUnmanaged(PeerId, PeerStats) = .empty,
    peer_ips: std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(PeerId, void)) = .empty,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        params: PeerScoreParams,
        thresholds: PeerScoreThresholds,
    ) Self {
        return .{
            .allocator = allocator,
            .params = params,
            .thresholds = thresholds,
        };
    }

    pub fn deinit(self: *Self) void {
        var stats_it = self.peer_stats.iterator();
        while (stats_it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.peer_stats.deinit(self.allocator);

        var ip_it = self.peer_ips.iterator();
        while (ip_it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.peer_ips.deinit(self.allocator);

        self.params.deinit(self.allocator);
    }

    pub fn setTopicParams(self: *Self, topic: []const u8, params: TopicScoreParams) !void {
        const gop = try self.params.topics.getOrPut(self.allocator, topic);
        if (gop.found_existing) {
            const old = gop.value_ptr.*;
            gop.value_ptr.* = params;
            if (old.first_message_deliveries_cap > params.first_message_deliveries_cap) {
                var it = self.peer_stats.valueIterator();
                while (it.next()) |peer| {
                    if (peer.topics.getPtr(topic)) |ts| {
                        if (ts.first_message_deliveries > params.first_message_deliveries_cap) {
                            ts.first_message_deliveries = params.first_message_deliveries_cap;
                        }
                    }
                }
            }
            if (old.mesh_message_deliveries_cap > params.mesh_message_deliveries_cap) {
                var it = self.peer_stats.valueIterator();
                while (it.next()) |peer| {
                    if (peer.topics.getPtr(topic)) |ts| {
                        if (ts.mesh_message_deliveries > params.mesh_message_deliveries_cap) {
                            ts.mesh_message_deliveries = params.mesh_message_deliveries_cap;
                        }
                    }
                }
            }
        } else {
            gop.key_ptr.* = try self.allocator.dupe(u8, topic);
            gop.value_ptr.* = params;
        }
    }

    pub fn getTopicParams(self: *const Self, topic: []const u8) ?TopicScoreParams {
        return self.params.topics.get(topic);
    }

    pub fn addPeer(self: *Self, peer_id: PeerId) !void {
        const gop = try self.peer_stats.getOrPut(self.allocator, peer_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{};
        }
        gop.value_ptr.status = .connected;
    }

    pub fn addIp(self: *Self, peer_id: PeerId, ip: []const u8) !void {
        const peer_gop = try self.peer_stats.getOrPut(self.allocator, peer_id);
        if (!peer_gop.found_existing) {
            peer_gop.value_ptr.* = .{};
        }
        peer_gop.value_ptr.status = .connected;

        const known_gop = try peer_gop.value_ptr.known_ips.getOrPut(self.allocator, ip);
        if (!known_gop.found_existing) {
            known_gop.key_ptr.* = try self.allocator.dupe(u8, ip);
        }

        const set_gop = try self.peer_ips.getOrPut(self.allocator, ip);
        if (!set_gop.found_existing) {
            set_gop.key_ptr.* = try self.allocator.dupe(u8, ip);
            set_gop.value_ptr.* = .empty;
        }
        try set_gop.value_ptr.put(self.allocator, peer_id, {});
    }

    pub fn removeIp(self: *Self, peer_id: PeerId, ip: []const u8) void {
        if (self.peer_stats.getPtr(peer_id)) |peer| {
            if (peer.known_ips.fetchRemove(ip)) |kv| {
                self.allocator.free(kv.key);
            }
        }
        if (self.peer_ips.getPtr(ip)) |set| {
            _ = set.remove(peer_id);
            if (set.count() == 0) {
                if (self.peer_ips.fetchRemove(ip)) |kv| {
                    var s = kv.value;
                    s.deinit(self.allocator);
                    self.allocator.free(kv.key);
                }
            }
        }
    }

    /// Number of distinct peers currently associated with `ip`.
    pub fn peerCountAtIp(self: *const Self, ip: []const u8) usize {
        const set = self.peer_ips.get(ip) orelse return 0;
        return set.count();
    }

    pub fn addIpToWhitelist(self: *Self, ip: []const u8) !void {
        const gop = try self.params.ip_colocation_factor_whitelist.getOrPut(self.allocator, ip);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, ip);
        }
    }

    /// Drops the peer from every `peer_ips` reverse-index set. Used whenever
    /// a peer's `peer_stats` entry is about to be reaped.
    fn unindexPeerIps(self: *Self, peer_id: PeerId, peer: *const PeerStats) void {
        var it = peer.known_ips.iterator();
        while (it.next()) |entry| {
            const ip = entry.key_ptr.*;
            if (self.peer_ips.getPtr(ip)) |set| {
                _ = set.remove(peer_id);
                if (set.count() == 0) {
                    if (self.peer_ips.fetchRemove(ip)) |kv| {
                        var s = kv.value;
                        s.deinit(self.allocator);
                        self.allocator.free(kv.key);
                    }
                }
            }
        }
    }

    pub fn removePeer(self: *Self, peer_id: PeerId) void {
        const current_score = self.score(peer_id);
        const peer = self.peer_stats.getPtr(peer_id) orelse return;

        if (current_score > 0) {
            self.unindexPeerIps(peer_id, peer);
            if (self.peer_stats.fetchRemove(peer_id)) |kv| {
                var ps = kv.value;
                ps.deinit(self.allocator);
            }
            return;
        }

        var t_it = peer.topics.iterator();
        while (t_it.next()) |entry| {
            const topic = entry.key_ptr.*;
            const ts = entry.value_ptr;
            ts.first_message_deliveries = 0;

            if (self.params.topics.get(topic)) |tp| {
                if (ts.mesh_status.isActive() and
                    ts.mesh_message_deliveries_active and
                    ts.mesh_message_deliveries < tp.mesh_message_deliveries_threshold)
                {
                    const deficit = tp.mesh_message_deliveries_threshold - ts.mesh_message_deliveries;
                    ts.mesh_failure_penalty += deficit * deficit;
                }
            }

            ts.mesh_status = .inactive;
            ts.mesh_message_deliveries_active = false;
        }

        const now_ms = std.time.milliTimestamp();
        peer.status = .{ .disconnected = .{ .expire_ms = now_ms + self.params.retain_score_ms } };
    }

    pub fn setApplicationScore(self: *Self, peer_id: PeerId, score_value: f64) bool {
        if (self.peer_stats.getPtr(peer_id)) |peer| {
            peer.application_score = score_value;
            return true;
        }
        return false;
    }

    pub fn addPenalty(self: *Self, peer_id: PeerId, count: usize) void {
        if (self.peer_stats.getPtr(peer_id)) |peer| {
            peer.behaviour_penalty += @as(f64, @floatFromInt(count));
        }
    }

    pub fn graft(self: *Self, peer_id: PeerId, topic: []const u8, now_ms: i64) !void {
        if (!self.params.topics.contains(topic)) return;
        const peer = self.peer_stats.getPtr(peer_id) orelse return;
        const ts = try self.topicStatsOrDefault(peer, topic);
        ts.mesh_status = .{ .active = .{ .graft_time_ms = now_ms, .mesh_time_ms = 0 } };
        ts.mesh_message_deliveries_active = false;
    }

    pub fn prune(self: *Self, peer_id: PeerId, topic: []const u8) !void {
        const topic_params = self.params.topics.get(topic) orelse return;
        const peer = self.peer_stats.getPtr(peer_id) orelse return;
        const ts = try self.topicStatsOrDefault(peer, topic);
        if (ts.mesh_message_deliveries_active and
            ts.mesh_message_deliveries < topic_params.mesh_message_deliveries_threshold)
        {
            const deficit = topic_params.mesh_message_deliveries_threshold - ts.mesh_message_deliveries;
            ts.mesh_failure_penalty += deficit * deficit;
        }
        ts.mesh_message_deliveries_active = false;
        ts.mesh_status = .inactive;
    }

    pub fn markFirstMessageDelivery(self: *Self, peer_id: PeerId, topic: []const u8) !void {
        const topic_params = self.params.topics.get(topic) orelse return;
        const peer = self.peer_stats.getPtr(peer_id) orelse return;
        const ts = try self.topicStatsOrDefault(peer, topic);

        const fmd_cap = topic_params.first_message_deliveries_cap;
        ts.first_message_deliveries = @min(ts.first_message_deliveries + 1, fmd_cap);

        if (ts.mesh_status.isActive()) {
            const mmd_cap = topic_params.mesh_message_deliveries_cap;
            ts.mesh_message_deliveries = @min(ts.mesh_message_deliveries + 1, mmd_cap);
        }
    }

    pub fn markDuplicateMessageDelivery(self: *Self, peer_id: PeerId, topic: []const u8) !void {
        const topic_params = self.params.topics.get(topic) orelse return;
        const peer = self.peer_stats.getPtr(peer_id) orelse return;
        const ts = try self.topicStatsOrDefault(peer, topic);
        if (!ts.mesh_status.isActive()) return;
        const cap = topic_params.mesh_message_deliveries_cap;
        ts.mesh_message_deliveries = @min(ts.mesh_message_deliveries + 1, cap);
    }

    pub fn markInvalidMessageDelivery(self: *Self, peer_id: PeerId, topic: []const u8) !void {
        if (!self.params.topics.contains(topic)) return;
        const peer = self.peer_stats.getPtr(peer_id) orelse return;
        const ts = try self.topicStatsOrDefault(peer, topic);
        ts.invalid_message_deliveries += 1;
    }

    fn topicStatsOrDefault(self: *Self, peer: *PeerStats, topic: []const u8) !*TopicStats {
        const gop = try peer.topics.getOrPut(self.allocator, topic);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.allocator.dupe(u8, topic);
            gop.value_ptr.* = .{};
        }
        return gop.value_ptr;
    }

    pub fn refreshScores(self: *Self, now_ms: i64) void {
        var rm_list = std.ArrayList(PeerId).empty;
        defer rm_list.deinit(self.allocator);

        var it = self.peer_stats.iterator();
        while (it.next()) |entry| {
            const peer = entry.value_ptr;

            switch (peer.status) {
                .disconnected => |d| {
                    if (now_ms > d.expire_ms) {
                        rm_list.append(self.allocator, entry.key_ptr.*) catch {};
                    }
                    continue;
                },
                .connected => {},
            }

            var t_it = peer.topics.iterator();
            while (t_it.next()) |t_entry| {
                const topic = t_entry.key_ptr.*;
                const ts = t_entry.value_ptr;
                const topic_params = self.params.topics.get(topic) orelse continue;

                ts.first_message_deliveries *= topic_params.first_message_deliveries_decay;
                if (ts.first_message_deliveries < self.params.decay_to_zero) ts.first_message_deliveries = 0;

                ts.mesh_message_deliveries *= topic_params.mesh_message_deliveries_decay;
                if (ts.mesh_message_deliveries < self.params.decay_to_zero) ts.mesh_message_deliveries = 0;

                ts.mesh_failure_penalty *= topic_params.mesh_failure_penalty_decay;
                if (ts.mesh_failure_penalty < self.params.decay_to_zero) ts.mesh_failure_penalty = 0;

                ts.invalid_message_deliveries *= topic_params.invalid_message_deliveries_decay;
                if (ts.invalid_message_deliveries < self.params.decay_to_zero) ts.invalid_message_deliveries = 0;

                switch (ts.mesh_status) {
                    .active => |*a| {
                        a.mesh_time_ms = now_ms - a.graft_time_ms;
                        if (a.mesh_time_ms > topic_params.mesh_message_deliveries_activation_ms) {
                            ts.mesh_message_deliveries_active = true;
                        }
                    },
                    .inactive => {},
                }
            }

            peer.behaviour_penalty *= self.params.behaviour_penalty_decay;
            if (peer.behaviour_penalty < self.params.decay_to_zero) peer.behaviour_penalty = 0;

            peer.slow_peer_penalty *= self.params.slow_peer_decay;
            if (peer.slow_peer_penalty < self.params.decay_to_zero) peer.slow_peer_penalty = 0;
        }

        for (rm_list.items) |pid| {
            if (self.peer_stats.fetchRemove(pid)) |kv| {
                var ps = kv.value;
                self.unindexPeerIps(pid, &ps);
                ps.deinit(self.allocator);
            }
        }
    }

    pub fn score(self: *const Self, peer_id: PeerId) f64 {
        const peer = self.peer_stats.getPtr(peer_id) orelse return 0;

        var s: f64 = 0;
        var t_it = peer.topics.iterator();
        while (t_it.next()) |t_entry| {
            const topic = t_entry.key_ptr.*;
            const ts = t_entry.value_ptr;
            const tp = self.params.topics.get(topic) orelse continue;

            var topic_score: f64 = 0;

            switch (ts.mesh_status) {
                .active => |a| {
                    const quantum = @as(f64, @floatFromInt(tp.time_in_mesh_quantum_ms));
                    const time_f = @as(f64, @floatFromInt(a.mesh_time_ms));
                    const v = time_f / quantum;
                    const p1 = if (v < tp.time_in_mesh_cap) v else tp.time_in_mesh_cap;
                    topic_score += p1 * tp.time_in_mesh_weight;
                },
                .inactive => {},
            }

            const p2 = if (ts.first_message_deliveries < tp.first_message_deliveries_cap)
                ts.first_message_deliveries
            else
                tp.first_message_deliveries_cap;
            topic_score += p2 * tp.first_message_deliveries_weight;

            if (ts.mesh_message_deliveries_active and
                ts.mesh_message_deliveries < tp.mesh_message_deliveries_threshold and
                tp.mesh_message_deliveries_weight != 0)
            {
                const deficit = tp.mesh_message_deliveries_threshold - ts.mesh_message_deliveries;
                topic_score += deficit * deficit * tp.mesh_message_deliveries_weight;
            }

            topic_score += ts.mesh_failure_penalty * tp.mesh_failure_penalty_weight;

            const p4 = ts.invalid_message_deliveries * ts.invalid_message_deliveries;
            topic_score += p4 * tp.invalid_message_deliveries_weight;

            s += topic_score * tp.topic_weight;
        }

        if (self.params.topic_score_cap > 0 and s > self.params.topic_score_cap) {
            s = self.params.topic_score_cap;
        }

        s += peer.application_score * self.params.app_specific_weight;

        var ip_it = peer.known_ips.iterator();
        while (ip_it.next()) |ip_entry| {
            const ip = ip_entry.key_ptr.*;
            if (self.params.ip_colocation_factor_whitelist.contains(ip)) continue;
            const set = self.peer_ips.get(ip) orelse continue;
            const count: f64 = @floatFromInt(set.count());
            if (count > self.params.ip_colocation_factor_threshold and
                self.params.ip_colocation_factor_weight != 0)
            {
                const surplus = count - self.params.ip_colocation_factor_threshold;
                s += surplus * surplus * self.params.ip_colocation_factor_weight;
            }
        }

        if (peer.behaviour_penalty > self.params.behaviour_penalty_threshold) {
            const excess = peer.behaviour_penalty - self.params.behaviour_penalty_threshold;
            s += excess * excess * self.params.behaviour_penalty_weight;
        }

        if (peer.slow_peer_penalty > self.params.slow_peer_threshold) {
            const excess = peer.slow_peer_penalty - self.params.slow_peer_threshold;
            s += excess * self.params.slow_peer_weight;
        }

        return s;
    }
};

// --- Tests ---

const testing = std.testing;

fn testPeerId(seed: u64) !PeerId {
    var key_bytes: [32]u8 = undefined;
    std.mem.writeInt(u64, key_bytes[0..8], seed, .little);
    @memset(key_bytes[8..], 0);
    var pub_key = @import("peer_id").keys.PublicKey{
        .type = .ED25519,
        .data = &key_bytes,
    };
    return try PeerId.fromPublicKey(testing.allocator, &pub_key);
}

test "score returns 0 for unknown peer" {
    var ps = PeerScore.init(testing.allocator, .{}, .{});
    defer ps.deinit();
    const peer = try testPeerId(1);
    try testing.expectEqual(@as(f64, 0), ps.score(peer));
}

test "score is 0 when peer has no scored topics" {
    var ps = PeerScore.init(testing.allocator, .{}, .{});
    defer ps.deinit();
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    try testing.expectEqual(@as(f64, 0), ps.score(peer));
}

test "P1 time-in-mesh accumulates and is capped" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{}, .{});
    defer ps.deinit();
    try ps.setTopicParams("t", .{
        .topic_weight = 1.0,
        .time_in_mesh_weight = 2.0,
        .time_in_mesh_quantum_ms = 10,
        .time_in_mesh_cap = 5.0,
        .first_message_deliveries_weight = 0,
        .mesh_message_deliveries_weight = 0,
        .mesh_failure_penalty_weight = 0,
        .invalid_message_deliveries_weight = 0,
        .invalid_message_deliveries_decay = 0.5,
    });
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    try ps.graft(peer, "t", 0);
    ps.refreshScores(30);
    try testing.expectApproxEqAbs(@as(f64, 6.0), ps.score(peer), 1e-9);
    ps.refreshScores(1_000_000);
    try testing.expectApproxEqAbs(@as(f64, 10.0), ps.score(peer), 1e-9);
}

test "P2 first deliveries cap then decay" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{}, .{});
    defer ps.deinit();
    try ps.setTopicParams("t", .{
        .topic_weight = 1.0,
        .time_in_mesh_weight = 0,
        .first_message_deliveries_weight = 1.0,
        .first_message_deliveries_decay = 0.5,
        .first_message_deliveries_cap = 5.0,
        .mesh_message_deliveries_weight = 0,
        .mesh_failure_penalty_weight = 0,
        .invalid_message_deliveries_weight = 0,
        .invalid_message_deliveries_decay = 0.3,
    });
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    var i: usize = 0;
    while (i < 10) : (i += 1) try ps.markFirstMessageDelivery(peer, "t");
    try testing.expectApproxEqAbs(@as(f64, 5.0), ps.score(peer), 1e-9);
    ps.refreshScores(1000);
    try testing.expectApproxEqAbs(@as(f64, 2.5), ps.score(peer), 1e-9);
}

test "P3 mesh delivery deficit is deficit-squared, only when active and below threshold" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{}, .{});
    defer ps.deinit();
    try ps.setTopicParams("t", .{
        .topic_weight = 1.0,
        .time_in_mesh_weight = 0,
        .first_message_deliveries_weight = 0,
        .mesh_message_deliveries_weight = -3.0,
        .mesh_message_deliveries_decay = 0.5,
        .mesh_message_deliveries_cap = 100.0,
        .mesh_message_deliveries_threshold = 4.0,
        .mesh_message_deliveries_activation_ms = 1000,
        .mesh_failure_penalty_weight = 0,
        .invalid_message_deliveries_weight = 0,
        .invalid_message_deliveries_decay = 0.3,
    });
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    try ps.graft(peer, "t", 0);

    try testing.expectApproxEqAbs(@as(f64, 0), ps.score(peer), 1e-9);

    ps.refreshScores(2000);
    try testing.expectApproxEqAbs(@as(f64, -48.0), ps.score(peer), 1e-9);

    try ps.markFirstMessageDelivery(peer, "t");
    try ps.markFirstMessageDelivery(peer, "t");
    try testing.expectApproxEqAbs(@as(f64, -12.0), ps.score(peer), 1e-9);
}

test "P3b mesh failure penalty applies on prune with deficit" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{}, .{});
    defer ps.deinit();
    try ps.setTopicParams("t", .{
        .topic_weight = 1.0,
        .time_in_mesh_weight = 0,
        .first_message_deliveries_weight = 0,
        .mesh_message_deliveries_weight = 0,
        .mesh_message_deliveries_decay = 0.5,
        .mesh_message_deliveries_cap = 100.0,
        .mesh_message_deliveries_threshold = 6.0,
        .mesh_message_deliveries_activation_ms = 1000,
        .mesh_failure_penalty_weight = -2.0,
        .mesh_failure_penalty_decay = 0.5,
        .invalid_message_deliveries_weight = 0,
        .invalid_message_deliveries_decay = 0.3,
    });
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    try ps.graft(peer, "t", 0);
    ps.refreshScores(2000);

    try ps.prune(peer, "t");
    try testing.expectApproxEqAbs(@as(f64, -72.0), ps.score(peer), 1e-9);
}

test "P4 invalid messages are squared" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{}, .{});
    defer ps.deinit();
    try ps.setTopicParams("t", .{
        .topic_weight = 1.0,
        .time_in_mesh_weight = 0,
        .first_message_deliveries_weight = 0,
        .mesh_message_deliveries_weight = 0,
        .mesh_failure_penalty_weight = 0,
        .invalid_message_deliveries_weight = -2.0,
        .invalid_message_deliveries_decay = 0.5,
    });
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    try ps.markInvalidMessageDelivery(peer, "t");
    try ps.markInvalidMessageDelivery(peer, "t");
    try ps.markInvalidMessageDelivery(peer, "t");
    try testing.expectApproxEqAbs(@as(f64, -18.0), ps.score(peer), 1e-9);
}

test "P5 application score multiplied by app weight" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{ .app_specific_weight = 3.0 }, .{});
    defer ps.deinit();
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    try testing.expect(ps.setApplicationScore(peer, -4.0));
    try testing.expectApproxEqAbs(@as(f64, -12.0), ps.score(peer), 1e-9);
}

test "P7 behaviour penalty is excess squared above threshold" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{
        .behaviour_penalty_weight = -2.0,
        .behaviour_penalty_threshold = 2.0,
        .behaviour_penalty_decay = 0.5,
    }, .{});
    defer ps.deinit();
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    ps.addPenalty(peer, 5);
    try testing.expectApproxEqAbs(@as(f64, -18.0), ps.score(peer), 1e-9);
}

test "topic_score_cap clamps positive topic contributions" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{ .topic_score_cap = 10.0 }, .{});
    defer ps.deinit();
    try ps.setTopicParams("t", .{
        .topic_weight = 1.0,
        .time_in_mesh_weight = 0,
        .first_message_deliveries_weight = 1.0,
        .first_message_deliveries_decay = 0.5,
        .first_message_deliveries_cap = 1000.0,
        .mesh_message_deliveries_weight = 0,
        .mesh_failure_penalty_weight = 0,
        .invalid_message_deliveries_weight = 0,
        .invalid_message_deliveries_decay = 0.3,
    });
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    var i: usize = 0;
    while (i < 100) : (i += 1) try ps.markFirstMessageDelivery(peer, "t");
    try testing.expectApproxEqAbs(@as(f64, 10.0), ps.score(peer), 1e-9);
}

test "decay_to_zero floors small counters" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{ .decay_to_zero = 0.1 }, .{});
    defer ps.deinit();
    try ps.setTopicParams("t", .{
        .topic_weight = 1.0,
        .time_in_mesh_weight = 0,
        .first_message_deliveries_weight = 1.0,
        .first_message_deliveries_decay = 0.01,
        .first_message_deliveries_cap = 100.0,
        .mesh_message_deliveries_weight = 0,
        .mesh_failure_penalty_weight = 0,
        .invalid_message_deliveries_weight = 0,
        .invalid_message_deliveries_decay = 0.3,
    });
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    try ps.markFirstMessageDelivery(peer, "t");
    ps.refreshScores(1000);
    try testing.expectApproxEqAbs(@as(f64, 0), ps.score(peer), 1e-9);
}

test "scoreParameterDecay solves factor^n = decay_to_zero" {
    const f = scoreParameterDecay(60_000);
    const expected = std.math.pow(f64, DEFAULT_DECAY_TO_ZERO, 1.0 / 60.0);
    try testing.expectApproxEqAbs(expected, f, 1e-12);
}

test "P6 IP colocation: no penalty at or below threshold" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{
        .ip_colocation_factor_weight = -5.0,
        .ip_colocation_factor_threshold = 2.0,
    }, .{});
    defer ps.deinit();
    const peer = try testPeerId(1);
    try ps.addPeer(peer);
    try ps.addIp(peer, "10.0.0.1");
    try testing.expectEqual(@as(f64, 0), ps.score(peer));
}

test "P6 IP colocation: penalty above threshold scales with surplus²" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{
        .ip_colocation_factor_weight = -5.0,
        .ip_colocation_factor_threshold = 2.0,
    }, .{});
    defer ps.deinit();
    const a = try testPeerId(1);
    const b = try testPeerId(2);
    const c = try testPeerId(3);
    const d = try testPeerId(4);
    try ps.addPeer(a);
    try ps.addPeer(b);
    try ps.addPeer(c);
    try ps.addPeer(d);
    try ps.addIp(a, "10.0.0.1");
    try ps.addIp(b, "10.0.0.1");
    try ps.addIp(c, "10.0.0.1");
    try ps.addIp(d, "10.0.0.1");
    try testing.expectApproxEqAbs(@as(f64, -20.0), ps.score(a), 1e-9);
    try testing.expectApproxEqAbs(@as(f64, -20.0), ps.score(b), 1e-9);
    try testing.expectApproxEqAbs(@as(f64, -20.0), ps.score(c), 1e-9);
    try testing.expectApproxEqAbs(@as(f64, -20.0), ps.score(d), 1e-9);
}

test "P6 IP colocation: whitelist suppresses penalty" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{
        .ip_colocation_factor_weight = -5.0,
        .ip_colocation_factor_threshold = 1.0,
    }, .{});
    defer ps.deinit();
    try ps.addIpToWhitelist("10.0.0.1");
    const a = try testPeerId(1);
    const b = try testPeerId(2);
    const c = try testPeerId(3);
    try ps.addPeer(a);
    try ps.addPeer(b);
    try ps.addPeer(c);
    try ps.addIp(a, "10.0.0.1");
    try ps.addIp(b, "10.0.0.1");
    try ps.addIp(c, "10.0.0.1");
    try testing.expectEqual(@as(f64, 0), ps.score(a));
    try testing.expectEqual(@as(f64, 0), ps.score(b));
    try testing.expectEqual(@as(f64, 0), ps.score(c));
}

test "P6 IP colocation: removeIp reverses penalty" {
    const allocator = testing.allocator;
    var ps = PeerScore.init(allocator, .{
        .ip_colocation_factor_weight = -5.0,
        .ip_colocation_factor_threshold = 1.0,
    }, .{});
    defer ps.deinit();
    const a = try testPeerId(1);
    const b = try testPeerId(2);
    const c = try testPeerId(3);
    try ps.addPeer(a);
    try ps.addPeer(b);
    try ps.addPeer(c);
    try ps.addIp(a, "10.0.0.1");
    try ps.addIp(b, "10.0.0.1");
    try ps.addIp(c, "10.0.0.1");
    try testing.expectApproxEqAbs(@as(f64, -20.0), ps.score(a), 1e-9);

    ps.removeIp(b, "10.0.0.1");
    ps.removeIp(c, "10.0.0.1");
    try testing.expectEqual(@as(f64, 0), ps.score(a));
}

test "thresholds validate" {
    const ok: PeerScoreThresholds = .{};
    try testing.expect(ok.validate() == null);

    const bad_gossip: PeerScoreThresholds = .{ .gossip_threshold = 1.0 };
    try testing.expect(bad_gossip.validate() != null);

    const bad_publish: PeerScoreThresholds = .{ .publish_threshold = -5.0, .gossip_threshold = -10.0 };
    try testing.expect(bad_publish.validate() != null);

    const bad_px: PeerScoreThresholds = .{ .accept_px_threshold = -1.0 };
    try testing.expect(bad_px.validate() != null);
}
