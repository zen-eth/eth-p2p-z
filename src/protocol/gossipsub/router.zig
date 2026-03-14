const std = @import("std");
const rpc = @import("../../proto/rpc.proto.zig");
const mcache_mod = @import("mcache.zig");
const codec = @import("codec.zig");
const config_mod = @import("config.zig");

const MessageCache = mcache_mod.MessageCache;
const Config = config_mod.Config;
const Event = config_mod.Event;
const SignaturePolicy = config_mod.SignaturePolicy;
const PeerScoreParams = config_mod.PeerScoreParams;
const TopicScoreParams = config_mod.TopicScoreParams;

const log = std.log.scoped(.gossipsub);

/// A comptime-generic GossipSub v1.1/v1.2/v1.3 router.
///
/// The `Handler` type provides the transport integration. It must implement:
///
/// ```zig
/// const Handler = struct {
///     /// Send raw RPC bytes to a peer. Return true if sent.
///     fn sendRpc(self: *Handler, peer: []const u8, data: []const u8) bool;
///     /// Get a random u64 for shuffling.
///     fn randomU64(self: *Handler) u64;
///     /// Get the current monotonic time in milliseconds.
///     fn currentTimeMs(self: *Handler) u64;
///     /// (optional) Check if a peer is still connected.
///     fn isPeerConnected(self: *Handler, peer: []const u8) bool;
///     /// (optional) Check if the connection to a peer is outbound.
///     fn isOutbound(self: *Handler, peer: []const u8) bool;
///     /// (optional) Get the protocol ID negotiated with a peer.
///     fn peerProtocol(self: *Handler, peer: []const u8) ?[]const u8;
///     /// (optional) Get the IP address of a peer (for IP colocation scoring).
///     fn peerIP(self: *Handler, peer: []const u8) ?[]const u8;
/// };
/// ```
pub fn Router(comptime Handler: type) type {
    comptime {
        if (!@hasDecl(Handler, "sendRpc")) @compileError("Handler must implement sendRpc");
        if (!@hasDecl(Handler, "randomU64")) @compileError("Handler must implement randomU64");
        if (!@hasDecl(Handler, "currentTimeMs")) @compileError("Handler must implement currentTimeMs");
    }

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        config: Config,
        handler: *Handler,

        // Topic subscription state: topic -> set of peers subscribed
        topics: std.StringHashMap(PeerSet),
        // Our own subscriptions
        subscriptions: std.StringHashMap(void),

        // Mesh overlay: topic -> set of mesh peers
        mesh: std.StringHashMap(PeerSet),
        // Fanout: topic -> set of peers for non-subscribed publish
        fanout: std.StringHashMap(PeerSet),
        // Last publish time per fanout topic (for TTL expiry)
        fanout_last_pub: std.StringHashMap(u64),

        // Message cache
        mcache: MessageCache,
        // Dedup seen cache: message_id -> void
        seen: std.StringHashMap(void),

        // Per-heartbeat rate limiting
        peer_ihave_count: std.StringHashMap(u32),
        peer_iasked: std.StringHashMap(u32),

        // v1.1 PRUNE backoff: peer -> earliest regraft time (ms)
        backoff: std.StringHashMap(u64),
        // v1.1 Outbound mesh quota tracking: topic -> number of outbound peers in mesh
        // (used to ensure at least D_out outbound peers)

        // Pending control messages to piggyback
        pending_graft: std.StringHashMap(std.ArrayList([]const u8)),
        pending_prune: std.StringHashMap(std.ArrayList([]const u8)),

        // v1.2: Per-peer IDONTWANT tracking: peer -> set of message IDs to skip forwarding
        peer_idontwant: std.StringHashMap(std.StringHashMap(void)),

        // v1.3: Per-peer extension declarations
        peer_extensions: std.StringHashMap(config_mod.Extensions),
        // v1.3: Set of peers that have already received our extensions (first-message-per-stream)
        extensions_sent: std.StringHashMap(void),

        // v1.1: Peer scoring
        peer_scores: std.StringHashMap(PeerScore),
        score_params: PeerScoreParams,
        topic_score_params: std.StringHashMap(TopicScoreParams),

        // Events buffer
        events: std.ArrayList(Event),

        // Heartbeat counter
        heartbeat_ticks: u64,

        const PeerSet = std.StringHashMap(void);

        /// v1.1: Per-peer score tracking.
        const PeerScore = struct {
            /// Per-topic scoring data.
            topic_scores: std.StringHashMap(TopicScore),
            /// Behaviour penalty counter (protocol violations).
            behaviour_penalty: f64,
            /// When this peer's score was last updated (ms).
            last_update_ms: u64,
            /// When this peer first connected (ms).
            connected_at_ms: u64,

            fn init(allocator: std.mem.Allocator, now: u64) PeerScore {
                return .{
                    .topic_scores = std.StringHashMap(TopicScore).init(allocator),
                    .behaviour_penalty = 0,
                    .last_update_ms = now,
                    .connected_at_ms = now,
                };
            }

            fn deinit(self: *PeerScore) void {
                self.topic_scores.deinit();
            }

            fn score(self: *const PeerScore, score_params: PeerScoreParams, topic_params: *const std.StringHashMap(TopicScoreParams)) f64 {
                var topic_total: f64 = 0;
                var iter = self.topic_scores.iterator();
                while (iter.next()) |entry| {
                    const tp = topic_params.get(entry.key_ptr.*) orelse TopicScoreParams{};
                    topic_total += entry.value_ptr.compute(&tp) * tp.topic_weight;
                }
                topic_total = @min(topic_total, score_params.topic_score_cap);

                const behaviour = self.behaviour_penalty * self.behaviour_penalty * score_params.behaviour_penalty_weight;
                return topic_total + behaviour;
            }
        };

        /// Per-topic score counters for a peer.
        const TopicScore = struct {
            /// Time spent in mesh (ms).
            time_in_mesh_ms: u64 = 0,
            /// First message deliveries (unique messages this peer delivered first).
            first_message_deliveries: f64 = 0,
            /// Mesh message deliveries (messages received while in mesh).
            mesh_message_deliveries: f64 = 0,
            /// Mesh failure penalty counter.
            mesh_failure_penalty: f64 = 0,
            /// Invalid message deliveries counter.
            invalid_message_deliveries: f64 = 0,
            /// Whether this peer is currently in mesh for this topic.
            in_mesh: bool = false,
            /// When the peer joined the mesh for this topic (ms).
            mesh_joined_ms: u64 = 0,

            fn compute(self: *const TopicScore, params: *const TopicScoreParams) f64 {
                // P1: time in mesh
                const time_quanta: f64 = if (params.time_in_mesh_quantum_ms > 0)
                    @as(f64, @floatFromInt(self.time_in_mesh_ms)) / @as(f64, @floatFromInt(params.time_in_mesh_quantum_ms))
                else
                    0;
                const p1 = @min(time_quanta, params.time_in_mesh_cap) * params.time_in_mesh_weight;

                // P2: first message deliveries
                const p2 = @min(self.first_message_deliveries, params.first_message_deliveries_cap) * params.first_message_deliveries_weight;

                // P3: mesh message delivery rate
                var p3: f64 = 0;
                if (params.mesh_message_deliveries_weight < 0) {
                    const deficit = params.mesh_message_deliveries_threshold - self.mesh_message_deliveries;
                    if (deficit > 0) {
                        p3 = deficit * deficit * params.mesh_message_deliveries_weight;
                    }
                }

                // P3b: mesh failure penalty
                const p3b = self.mesh_failure_penalty * params.mesh_failure_penalty_weight;

                // P4: invalid messages
                const p4 = self.invalid_message_deliveries * self.invalid_message_deliveries * params.invalid_message_deliveries_weight;

                return p1 + p2 + p3 + p3b + p4;
            }
        };

        /// v1.1 default backoff duration: 1 minute.
        const default_prune_backoff_ms: u64 = 60_000;
        /// v1.1 minimum outbound peers in mesh.
        const d_out: u32 = 2;

        pub fn init(
            allocator: std.mem.Allocator,
            config: Config,
            handler: *Handler,
        ) error{ OutOfMemory, HistoryLengthExceeded }!Self {
            return initWithScoring(allocator, config, handler, .{}, null);
        }

        /// Initialize with peer scoring enabled.
        pub fn initWithScoring(
            allocator: std.mem.Allocator,
            config: Config,
            handler: *Handler,
            score_params: PeerScoreParams,
            topic_params: ?std.StringHashMap(TopicScoreParams),
        ) error{ OutOfMemory, HistoryLengthExceeded }!Self {
            var mc = mcache_mod.MessageCache.init(
                allocator,
                config.history_gossip,
                config.history_length,
                config.msg_id_fn,
            ) catch |err| switch (err) {
                error.HistoryLengthExceeded => return error.HistoryLengthExceeded,
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.OutOfMemory,
            };
            errdefer mc.deinit();

            return Self{
                .allocator = allocator,
                .config = config,
                .handler = handler,
                .topics = std.StringHashMap(PeerSet).init(allocator),
                .subscriptions = std.StringHashMap(void).init(allocator),
                .mesh = std.StringHashMap(PeerSet).init(allocator),
                .fanout = std.StringHashMap(PeerSet).init(allocator),
                .fanout_last_pub = std.StringHashMap(u64).init(allocator),
                .mcache = mc,
                .seen = std.StringHashMap(void).init(allocator),
                .peer_ihave_count = std.StringHashMap(u32).init(allocator),
                .peer_iasked = std.StringHashMap(u32).init(allocator),
                .backoff = std.StringHashMap(u64).init(allocator),
                .pending_graft = std.StringHashMap(std.ArrayList([]const u8)).init(allocator),
                .pending_prune = std.StringHashMap(std.ArrayList([]const u8)).init(allocator),
                .peer_idontwant = std.StringHashMap(std.StringHashMap(void)).init(allocator),
                .peer_extensions = std.StringHashMap(config_mod.Extensions).init(allocator),
                .extensions_sent = std.StringHashMap(void).init(allocator),
                .peer_scores = std.StringHashMap(PeerScore).init(allocator),
                .score_params = score_params,
                .topic_score_params = topic_params orelse std.StringHashMap(TopicScoreParams).init(allocator),
                .events = .empty,
                .heartbeat_ticks = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            self.events.deinit(self.allocator);

            // Peer scoring
            {
                var iter = self.peer_scores.iterator();
                while (iter.next()) |entry| {
                    entry.value_ptr.deinit();
                }
                self.peer_scores.deinit();
            }
            self.topic_score_params.deinit();

            // v1.3 extensions
            self.extensions_sent.deinit();
            self.peer_extensions.deinit();

            // v1.2 per-peer IDONTWANT
            {
                var iter = self.peer_idontwant.iterator();
                while (iter.next()) |entry| {
                    var inner = entry.value_ptr.*;
                    var key_iter = inner.keyIterator();
                    while (key_iter.next()) |key| {
                        self.allocator.free(key.*);
                    }
                    inner.deinit();
                }
                self.peer_idontwant.deinit();
            }

            deinitPeerMap(&self.pending_prune, self.allocator);
            deinitPeerMap(&self.pending_graft, self.allocator);
            self.backoff.deinit();
            self.peer_iasked.deinit();
            self.peer_ihave_count.deinit();

            deinitKeyedMap(&self.seen, self.allocator);

            self.mcache.deinit();

            deinitKeyedMap2(&self.fanout_last_pub, self.allocator);
            deinitPeerSetMap(&self.fanout, self.allocator);
            deinitPeerSetMap(&self.mesh, self.allocator);
            deinitKeyedMap(&self.subscriptions, self.allocator);
            deinitPeerSetMap(&self.topics, self.allocator);

            self.* = undefined;
        }

        // --- Public API ---

        /// Subscribe to a topic. Joins the mesh and sends GRAFT to mesh peers.
        pub fn subscribe(self: *Self, topic: []const u8) !void {
            const gop = try self.subscriptions.getOrPut(topic);
            if (gop.found_existing) return;
            gop.key_ptr.* = try self.allocator.dupe(u8, topic);

            try self.joinMesh(topic);
        }

        /// Unsubscribe from a topic. Leaves the mesh and sends PRUNE to mesh peers.
        pub fn unsubscribe(self: *Self, topic: []const u8) !void {
            const kv = self.subscriptions.fetchRemove(topic) orelse return;
            self.allocator.free(kv.key);
            try self.leaveMesh(topic);
        }

        /// Publish a message to a topic.
        /// Returns the number of peers the message was sent to.
        pub fn publish(self: *Self, topic: []const u8, data: []const u8) !u32 {
            const msg = rpc.Message{
                .from = null,
                .data = data,
                .seqno = null,
                .topic = topic,
                .signature = null,
                .key = null,
            };

            const mid = try self.config.msg_id_fn(self.allocator, &msg);
            defer self.allocator.free(mid);

            // Dedup
            if (self.seen.contains(mid)) return 0;

            const mid_owned = try self.allocator.dupe(u8, mid);
            try self.seen.put(mid_owned, {});

            // Store in mcache, ignore duplicate or missing topic
            self.mcache.put(&msg) catch {};

            // Select peers and send
            const peers = try self.selectPeersToPublish(topic);
            defer self.allocator.free(peers);

            var rpc_msg = rpc.RPC{};
            var pub_msgs = [_]?rpc.Message{msg};
            rpc_msg.publish = &pub_msgs;

            const frame = codec.encodeRpc(self.allocator, &rpc_msg) catch return 0;
            defer self.allocator.free(frame);

            // v1.2: send IDONTWANT to mesh peers for large messages
            if (data.len >= self.config.idontwant_min_message_size) {
                self.sendIDontWant(topic, mid) catch {};
            }

            var sent_count: u32 = 0;
            for (peers) |peer| {
                if (self.handler.sendRpc(peer, frame)) {
                    sent_count += 1;
                }
            }

            // Self delivery
            if (self.config.emit_self and self.subscriptions.contains(topic)) {
                try self.events.append(self.allocator, .{ .message = .{
                    .topic = topic,
                    .data = data,
                    .from = null,
                    .seqno = null,
                } });
            }

            // Update fanout timestamp if not subscribed
            if (!self.subscriptions.contains(topic)) {
                if (self.fanout_last_pub.getPtr(topic)) |ts| {
                    ts.* = self.handler.currentTimeMs();
                }
            }

            return sent_count;
        }

        /// Handle an incoming RPC from a peer.
        /// The `rpc_bytes` should be the protobuf payload (without varint length prefix).
        pub fn handleRpc(self: *Self, from_peer: []const u8, rpc_bytes: []const u8) !void {
            var reader = rpc.RPCReader.init(rpc_bytes) catch return;

            // Handle subscriptions
            while (reader.subscriptionsNext()) |sub| {
                const topic_id = sub.getTopicid();
                const is_subscribe = sub.getSubscribe();
                try self.handleSubscription(from_peer, topic_id, is_subscribe);
            }

            // Handle published messages
            while (reader.publishNext()) |msg_reader| {
                try self.handleIncomingMessage(from_peer, &msg_reader);
            }

            // Handle control messages
            var control = reader.getControl() catch return;
            if (control.sourceBytes().len > 0) {
                try self.handleControl(from_peer, &control);
            }
        }

        /// Called when a peer connects. Adds the peer to topic tracking.
        pub fn addPeer(self: *Self, peer_id: []const u8) !void {
            // Initialize scoring for this peer
            if (!self.peer_scores.contains(peer_id)) {
                try self.peer_scores.put(peer_id, PeerScore.init(self.allocator, self.handler.currentTimeMs()));
            }

            // v1.3: Send extensions on first contact
            self.maybeSendExtensions(peer_id);
        }

        /// Called when a peer disconnects. Removes from all topic/mesh/fanout state.
        pub fn removePeer(self: *Self, peer_id: []const u8) void {
            // Remove from all topic peer sets
            var topics_iter = self.topics.iterator();
            while (topics_iter.next()) |entry| {
                if (entry.value_ptr.fetchRemove(peer_id)) |kv| {
                    self.allocator.free(kv.key);
                }
            }

            // Remove from all mesh peer sets
            var mesh_iter = self.mesh.iterator();
            while (mesh_iter.next()) |entry| {
                if (entry.value_ptr.fetchRemove(peer_id)) |kv| {
                    self.allocator.free(kv.key);
                }
            }

            // Remove from all fanout peer sets
            var fanout_iter = self.fanout.iterator();
            while (fanout_iter.next()) |entry| {
                if (entry.value_ptr.fetchRemove(peer_id)) |kv| {
                    self.allocator.free(kv.key);
                }
            }

            // Remove backoff
            _ = self.backoff.remove(peer_id);

            // Remove IDONTWANT tracking
            if (self.peer_idontwant.fetchRemove(peer_id)) |kv| {
                var inner = kv.value;
                var key_iter = inner.keyIterator();
                while (key_iter.next()) |key| {
                    self.allocator.free(key.*);
                }
                inner.deinit();
            }

            // Remove extensions
            _ = self.peer_extensions.remove(peer_id);
            _ = self.extensions_sent.remove(peer_id);

            // Remove scoring (keep score data for retain_score_ms - simplified: remove immediately)
            if (self.peer_scores.fetchRemove(peer_id)) |kv| {
                var ps = kv.value;
                ps.deinit();
            }
        }

        /// Drain accumulated events. Caller owns the returned slice.
        pub fn drainEvents(self: *Self) ![]Event {
            return self.events.toOwnedSlice(self.allocator);
        }

        /// Execute one heartbeat tick. Should be called periodically.
        pub fn heartbeat(self: *Self) !void {
            self.heartbeat_ticks += 1;

            // Reset per-heartbeat rate limit counters
            self.peer_ihave_count.clearRetainingCapacity();
            self.peer_iasked.clearRetainingCapacity();

            // Collect topics to process (avoid iterator invalidation)
            var topic_list: std.ArrayList([]const u8) = .empty;
            defer topic_list.deinit(self.allocator);
            {
                var mesh_iter = self.mesh.keyIterator();
                while (mesh_iter.next()) |key| {
                    try topic_list.append(self.allocator, key.*);
                }
            }

            // Mesh maintenance per topic
            for (topic_list.items) |topic| {
                const mesh_peers = self.mesh.getPtr(topic) orelse continue;

                // v1.1: Prune disconnected peers
                if (@hasDecl(Handler, "isPeerConnected")) {
                    var to_remove: std.ArrayList([]const u8) = .empty;
                    defer to_remove.deinit(self.allocator);
                    {
                        var peer_iter = mesh_peers.keyIterator();
                        while (peer_iter.next()) |peer_key| {
                            if (!self.handler.isPeerConnected(peer_key.*)) {
                                try to_remove.append(self.allocator, peer_key.*);
                            }
                        }
                    }
                    for (to_remove.items) |peer| {
                        if (mesh_peers.fetchRemove(peer)) |kv| {
                            self.allocator.free(kv.key);
                        }
                    }
                }

                const mesh_count: u32 = @intCast(mesh_peers.count());

                // Fill mesh if below D_lo
                if (mesh_count < self.config.mesh_degree_lo) {
                    try self.graftPeers(topic, mesh_peers, self.config.mesh_degree -| mesh_count);
                }

                // Prune mesh if above D_hi (v1.1: prefer pruning non-outbound peers)
                if (mesh_count > self.config.mesh_degree_hi) {
                    try self.prunePeersV11(topic, mesh_peers, mesh_count - self.config.mesh_degree);
                }

                // v1.1: Opportunistic grafting — if mesh has no outbound peers, try to add some
                if (@hasDecl(Handler, "isOutbound")) {
                    var outbound_count: u32 = 0;
                    var peer_iter = mesh_peers.keyIterator();
                    while (peer_iter.next()) |peer_key| {
                        if (self.handler.isOutbound(peer_key.*)) {
                            outbound_count += 1;
                        }
                    }
                    if (outbound_count < d_out and mesh_count < self.config.mesh_degree) {
                        try self.graftOutboundPeers(topic, mesh_peers, d_out -| outbound_count);
                    }
                }

                // Gossip: emit IHAVE to non-mesh peers
                try self.emitGossip(topic, mesh_peers);
            }

            // Fanout expiry and maintenance
            try self.maintainFanout();

            // v1.1: Expire backoff entries
            self.expireBackoffs();

            // v1.1: Decay peer scores
            self.decayScores();

            // v1.2: Clear per-peer IDONTWANT sets (stale entries from previous heartbeat)
            self.clearIDontWant();

            // Flush pending control messages
            try self.flushControl();

            // Shift message cache window
            self.mcache.shift();
        }

        // --- Internal: Subscription handling ---

        fn handleSubscription(self: *Self, peer_id: []const u8, topic: []const u8, is_subscribe: bool) !void {
            if (is_subscribe) {
                const topic_gop = try self.topics.getOrPut(topic);
                if (!topic_gop.found_existing) {
                    topic_gop.key_ptr.* = try self.allocator.dupe(u8, topic);
                    topic_gop.value_ptr.* = PeerSet.init(self.allocator);
                }
                const peer_gop = try topic_gop.value_ptr.getOrPut(peer_id);
                if (!peer_gop.found_existing) {
                    peer_gop.key_ptr.* = try self.allocator.dupe(u8, peer_id);
                }
            } else {
                if (self.topics.getPtr(topic)) |peer_set| {
                    if (peer_set.fetchRemove(peer_id)) |kv| {
                        self.allocator.free(kv.key);
                    }
                }
            }

            try self.events.append(self.allocator, .{ .subscription_changed = .{
                .peer_id = peer_id,
                .topic = topic,
                .action = if (is_subscribe) .subscribe else .unsubscribe,
            } });
        }

        // --- Internal: Message handling ---

        fn handleIncomingMessage(self: *Self, from_peer: []const u8, msg_reader: *const rpc.MessageReader) !void {
            const topic = msg_reader.getTopic();
            if (topic.len == 0) return;

            // Build a Message for ID computation and caching
            const msg = rpc.Message{
                .from = optionalBytes(msg_reader.getFrom()),
                .data = optionalBytes(msg_reader.getData()),
                .seqno = optionalBytes(msg_reader.getSeqno()),
                .topic = topic,
                .signature = optionalBytes(msg_reader.getSignature()),
                .key = optionalBytes(msg_reader.getKey()),
            };

            // Generate message ID
            const mid = self.config.msg_id_fn(self.allocator, &msg) catch return;
            defer self.allocator.free(mid);

            // Dedup check
            if (self.seen.contains(mid)) return;

            const mid_owned = try self.allocator.dupe(u8, mid);
            try self.seen.put(mid_owned, {});

            // Cache the message
            self.mcache.put(&msg) catch {};

            // v1.1: Record first delivery for scoring
            self.recordFirstDelivery(from_peer, topic);
            self.recordMeshDelivery(from_peer, topic);

            // v1.2: send IDONTWANT to mesh peers for large messages
            const data_len = if (msg.data) |d| d.len else 0;
            if (data_len >= self.config.idontwant_min_message_size) {
                self.sendIDontWantExcluding(topic, mid, from_peer) catch {};
            }

            // Emit event if we are subscribed to this topic
            if (self.subscriptions.contains(topic)) {
                try self.events.append(self.allocator, .{ .message = .{
                    .topic = topic,
                    .data = msg.data orelse "",
                    .from = msg.from,
                    .seqno = msg.seqno,
                } });
            }

            // Forward to mesh peers (excluding source)
            self.forwardMessage(from_peer, topic, mid, &msg);
        }

        fn forwardMessage(self: *Self, from_peer: []const u8, topic: []const u8, mid: []const u8, msg: *const rpc.Message) void {
            const mesh_peers = self.mesh.getPtr(topic) orelse return;

            var rpc_msg = rpc.RPC{};
            var pub_msgs = [_]?rpc.Message{msg.*};
            rpc_msg.publish = &pub_msgs;

            const frame = codec.encodeRpc(self.allocator, &rpc_msg) catch return;
            defer self.allocator.free(frame);

            var iter = mesh_peers.iterator();
            while (iter.next()) |entry| {
                const peer = entry.key_ptr.*;
                if (std.mem.eql(u8, peer, from_peer)) continue;
                // v1.2: skip peers that sent IDONTWANT for this message
                if (self.peer_idontwant.getPtr(peer)) |idw| {
                    if (idw.contains(mid)) continue;
                }
                _ = self.handler.sendRpc(peer, frame);
            }
        }

        // --- Internal: Control message handling ---

        fn handleControl(self: *Self, from_peer: []const u8, control: *rpc.ControlMessageReader) !void {
            // IHAVE
            while (control.ihaveNext()) |ihave| {
                try self.handleIHave(from_peer, &ihave);
            }

            // IWANT
            while (control.iwantNext()) |iwant| {
                try self.handleIWant(from_peer, &iwant);
            }

            // GRAFT
            while (control.graftNext()) |graft| {
                try self.handleGraft(from_peer, &graft);
            }

            // PRUNE
            while (control.pruneNext()) |prune| {
                try self.handlePrune(from_peer, &prune);
            }

            // v1.2: IDONTWANT — track which message IDs this peer already has
            while (control.idontwantNext()) |idontwant| {
                try self.handleIDontWant(from_peer, &idontwant);
            }

            // v1.3: Extensions
            const ext_reader = control.getExtensions() catch return;
            if (ext_reader.sourceBytes().len > 0) {
                try self.handleExtensions(from_peer, &ext_reader);
            }
        }

        fn handleIHave(self: *Self, from_peer: []const u8, ihave: *const rpc.ControlIHaveReader) !void {
            // Rate limiting
            const count_gop = try self.peer_ihave_count.getOrPut(from_peer);
            if (!count_gop.found_existing) count_gop.value_ptr.* = 0;
            count_gop.value_ptr.* += 1;
            if (count_gop.value_ptr.* > self.config.max_ihave_messages) return;

            const asked_gop = try self.peer_iasked.getOrPut(from_peer);
            if (!asked_gop.found_existing) asked_gop.value_ptr.* = 0;
            if (asked_gop.value_ptr.* >= self.config.max_ihave_length) return;

            // Collect message IDs we haven't seen
            var iwant_ids: std.ArrayList([]const u8) = .empty;
            defer iwant_ids.deinit(self.allocator);

            var ihave_var = ihave.*;
            while (ihave_var.messageIDsNext()) |mid| {
                if (!self.seen.contains(mid)) {
                    try iwant_ids.append(self.allocator, mid);
                }
            }

            if (iwant_ids.items.len == 0) return;

            // Shuffle to avoid bias, then cap to budget
            shuffleSlice([]const u8, iwant_ids.items, self.handler);

            const budget = self.config.max_ihave_length -| asked_gop.value_ptr.*;
            const to_ask = @min(iwant_ids.items.len, budget);
            asked_gop.value_ptr.* += @intCast(to_ask);

            // Send IWANT
            var optionals: [128]?[]const u8 = undefined;
            const count = @min(to_ask, 128);
            for (0..count) |i| {
                optionals[i] = iwant_ids.items[i];
            }
            const iwant_msg = rpc.ControlIWant{
                .message_i_ds = optionals[0..count],
            };

            var ctrl = rpc.ControlMessage{};
            var iwant_arr = [_]?rpc.ControlIWant{iwant_msg};
            ctrl.iwant = &iwant_arr;

            var rpc_msg = rpc.RPC{};
            rpc_msg.control = ctrl;

            const frame = codec.encodeRpc(self.allocator, &rpc_msg) catch return;
            defer self.allocator.free(frame);
            _ = self.handler.sendRpc(from_peer, frame);
        }

        fn handleIWant(self: *Self, from_peer: []const u8, iwant: *const rpc.ControlIWantReader) !void {
            var iwant_var = iwant.*;
            while (iwant_var.messageIDsNext()) |mid| {
                const result = self.mcache.getForPeer(mid, from_peer) catch continue;
                if (result) |r| {
                    if (r.count > @as(i32, @intCast(self.config.gossip_retransmission))) continue;

                    // Send the message
                    var rpc_msg = rpc.RPC{};
                    var pub_msgs = [_]?rpc.Message{r.msg.*};
                    rpc_msg.publish = &pub_msgs;

                    const frame = codec.encodeRpc(self.allocator, &rpc_msg) catch continue;
                    defer self.allocator.free(frame);
                    _ = self.handler.sendRpc(from_peer, frame);
                }
            }
        }

        fn handleGraft(self: *Self, from_peer: []const u8, graft: *const rpc.ControlGraftReader) !void {
            const topic = graft.getTopicID();
            if (topic.len == 0) return;

            // v1.1: check if we are subscribed to this topic
            if (!self.subscriptions.contains(topic)) {
                // We are not subscribed; respond with PRUNE
                try self.enqueuePrune(from_peer, topic);
                return;
            }

            const mesh_peers = self.mesh.getPtr(topic) orelse return;

            if (mesh_peers.contains(from_peer)) return;

            // v1.1: check backoff timer
            if (self.backoff.get(from_peer)) |until| {
                if (self.handler.currentTimeMs() < until) {
                    // Peer is in backoff; reject with PRUNE
                    try self.enqueuePrune(from_peer, topic);
                    return;
                }
            }

            // v1.1: check peer score before accepting graft
            if (self.peerScore(from_peer) < self.score_params.gossip_threshold) {
                try self.enqueuePrune(from_peer, topic);
                self.addBehaviourPenalty(from_peer, 1);
                return;
            }

            const mesh_count: u32 = @intCast(mesh_peers.count());
            if (mesh_count >= self.config.mesh_degree_hi) {
                // Mesh full; reject with PRUNE
                try self.enqueuePrune(from_peer, topic);
                return;
            }

            // Accept: add to mesh
            const peer_owned = try self.allocator.dupe(u8, from_peer);
            try mesh_peers.put(peer_owned, {});

            // v1.1: Update scoring
            self.scoreGraft(from_peer, topic);

            try self.events.append(self.allocator, .{ .graft = .{
                .peer_id = from_peer,
                .topic = topic,
            } });
        }

        fn handlePrune(self: *Self, from_peer: []const u8, prune: *const rpc.ControlPruneReader) !void {
            const topic = prune.getTopicID();
            if (topic.len == 0) return;

            const mesh_peers = self.mesh.getPtr(topic) orelse return;
            if (mesh_peers.fetchRemove(from_peer)) |kv| {
                self.allocator.free(kv.key);
            }

            // v1.1: Update scoring
            self.scorePrune(from_peer, topic);

            // v1.1: apply backoff from PRUNE message
            const backoff_duration = prune.getBackoff();
            const effective_backoff = if (backoff_duration > 0) backoff_duration * 1000 else default_prune_backoff_ms;
            try self.backoff.put(from_peer, self.handler.currentTimeMs() + effective_backoff);

            try self.events.append(self.allocator, .{ .prune = .{
                .peer_id = from_peer,
                .topic = topic,
            } });
        }

        // --- Internal: v1.2 IDONTWANT handling ---

        fn handleIDontWant(self: *Self, from_peer: []const u8, idontwant: *const rpc.ControlIDontWantReader) !void {
            var idontwant_var = idontwant.*;
            while (idontwant_var.messageIDsNext()) |mid| {
                if (mid.len == 0) continue;
                const gop = try self.peer_idontwant.getOrPut(from_peer);
                if (!gop.found_existing) {
                    gop.value_ptr.* = std.StringHashMap(void).init(self.allocator);
                }
                if (!gop.value_ptr.contains(mid)) {
                    const mid_owned = try self.allocator.dupe(u8, mid);
                    try gop.value_ptr.put(mid_owned, {});
                }
            }
        }

        // --- Internal: v1.3 Extensions handling ---

        fn handleExtensions(self: *Self, from_peer: []const u8, ext: *const rpc.ControlExtensionsReader) !void {
            // Only accept extensions once per peer (per spec)
            if (self.peer_extensions.contains(from_peer)) return;

            const extensions = config_mod.Extensions{
                .partial_messages = ext.getPartialMessages(),
            };
            try self.peer_extensions.put(from_peer, extensions);

            try self.events.append(self.allocator, .{ .peer_extensions = .{
                .peer_id = from_peer,
                .partial_messages = extensions.partial_messages,
            } });
        }

        /// v1.3: Send our extensions declaration to a peer (first message on stream).
        fn maybeSendExtensions(self: *Self, peer: []const u8) void {
            if (self.extensions_sent.contains(peer)) return;

            // Only send if we support v1.3
            if (@hasDecl(Handler, "peerProtocol")) {
                if (self.handler.peerProtocol(peer)) |proto| {
                    if (!std.mem.eql(u8, proto, config_mod.protocol_ids.v1_3)) return;
                } else return;
            } else return;

            const ext = rpc.ControlExtensions{
                .partial_messages = self.config.extensions.partial_messages,
            };
            var ctrl = rpc.ControlMessage{};
            ctrl.extensions = ext;

            var rpc_msg = rpc.RPC{};
            rpc_msg.control = ctrl;

            const frame = codec.encodeRpc(self.allocator, &rpc_msg) catch return;
            defer self.allocator.free(frame);
            if (self.handler.sendRpc(peer, frame)) {
                self.extensions_sent.put(peer, {}) catch {};
            }
        }

        // --- Internal: Peer scoring ---

        /// Get the score for a peer. Returns 0 if peer is unknown.
        pub fn peerScore(self: *Self, peer_id: []const u8) f64 {
            const ps = self.peer_scores.getPtr(peer_id) orelse return 0;
            return ps.score(self.score_params, &self.topic_score_params);
        }

        /// Record a behaviour penalty for a peer (protocol violation).
        pub fn addBehaviourPenalty(self: *Self, peer_id: []const u8, penalty: f64) void {
            if (self.peer_scores.getPtr(peer_id)) |ps| {
                ps.behaviour_penalty += penalty;
            }
        }

        /// Record first message delivery for scoring.
        fn recordFirstDelivery(self: *Self, peer_id: []const u8, topic: []const u8) void {
            const ps = self.peer_scores.getPtr(peer_id) orelse return;
            const gop = ps.topic_scores.getOrPut(topic) catch return;
            if (!gop.found_existing) gop.value_ptr.* = .{};
            gop.value_ptr.first_message_deliveries += 1;
        }

        /// Record mesh message delivery for scoring.
        fn recordMeshDelivery(self: *Self, peer_id: []const u8, topic: []const u8) void {
            const ps = self.peer_scores.getPtr(peer_id) orelse return;
            const gop = ps.topic_scores.getOrPut(topic) catch return;
            if (!gop.found_existing) gop.value_ptr.* = .{};
            if (gop.value_ptr.in_mesh) {
                gop.value_ptr.mesh_message_deliveries += 1;
            }
        }

        /// Record invalid message delivery for scoring.
        pub fn recordInvalidMessage(self: *Self, peer_id: []const u8, topic: []const u8) void {
            const ps = self.peer_scores.getPtr(peer_id) orelse return;
            const gop = ps.topic_scores.getOrPut(topic) catch return;
            if (!gop.found_existing) gop.value_ptr.* = .{};
            gop.value_ptr.invalid_message_deliveries += 1;
        }

        /// Mark a peer as in-mesh for a topic (for scoring).
        fn scoreGraft(self: *Self, peer_id: []const u8, topic: []const u8) void {
            const ps = self.peer_scores.getPtr(peer_id) orelse return;
            const gop = ps.topic_scores.getOrPut(topic) catch return;
            if (!gop.found_existing) gop.value_ptr.* = .{};
            gop.value_ptr.in_mesh = true;
            gop.value_ptr.mesh_joined_ms = self.handler.currentTimeMs();
        }

        /// Mark a peer as out-of-mesh for a topic (for scoring).
        fn scorePrune(self: *Self, peer_id: []const u8, topic: []const u8) void {
            const ps = self.peer_scores.getPtr(peer_id) orelse return;
            if (ps.topic_scores.getPtr(topic)) |ts| {
                ts.in_mesh = false;
            }
        }

        /// Decay score counters during heartbeat.
        fn decayScores(self: *Self) void {
            const now = self.handler.currentTimeMs();
            var iter = self.peer_scores.iterator();
            while (iter.next()) |entry| {
                const ps = entry.value_ptr;
                const elapsed = now -| ps.last_update_ms;
                ps.last_update_ms = now;

                // Decay behaviour penalty
                ps.behaviour_penalty *= self.score_params.behaviour_penalty_decay;
                if (@abs(ps.behaviour_penalty) < self.score_params.decay_to_zero) {
                    ps.behaviour_penalty = 0;
                }

                // Update per-topic scores
                var topic_iter = ps.topic_scores.iterator();
                while (topic_iter.next()) |te| {
                    const ts = te.value_ptr;
                    const tp = self.topic_score_params.get(te.key_ptr.*) orelse TopicScoreParams{};

                    // Update time in mesh
                    if (ts.in_mesh) {
                        ts.time_in_mesh_ms += elapsed;
                    }

                    // Decay first message deliveries
                    ts.first_message_deliveries *= tp.first_message_deliveries_decay;
                    if (ts.first_message_deliveries < self.score_params.decay_to_zero) {
                        ts.first_message_deliveries = 0;
                    }

                    // Decay mesh message deliveries
                    if (tp.mesh_message_deliveries_decay > 0) {
                        ts.mesh_message_deliveries *= tp.mesh_message_deliveries_decay;
                        if (ts.mesh_message_deliveries < self.score_params.decay_to_zero) {
                            ts.mesh_message_deliveries = 0;
                        }
                    }

                    // Decay mesh failure penalty
                    if (tp.mesh_failure_penalty_decay > 0) {
                        ts.mesh_failure_penalty *= tp.mesh_failure_penalty_decay;
                        if (ts.mesh_failure_penalty < self.score_params.decay_to_zero) {
                            ts.mesh_failure_penalty = 0;
                        }
                    }

                    // Decay invalid message deliveries
                    if (tp.invalid_message_deliveries_decay > 0) {
                        ts.invalid_message_deliveries *= tp.invalid_message_deliveries_decay;
                        if (ts.invalid_message_deliveries < self.score_params.decay_to_zero) {
                            ts.invalid_message_deliveries = 0;
                        }
                    }
                }
            }
        }

        // --- Internal: Mesh management ---

        fn joinMesh(self: *Self, topic: []const u8) !void {
            if (self.mesh.contains(topic)) return;

            var mesh_peers = PeerSet.init(self.allocator);
            errdefer {
                var inner_iter = mesh_peers.keyIterator();
                while (inner_iter.next()) |key| {
                    self.allocator.free(key.*);
                }
                mesh_peers.deinit();
            }

            // Seed from fanout if available
            if (self.fanout.getPtr(topic)) |fanout_peers| {
                var fp_iter = fanout_peers.iterator();
                while (fp_iter.next()) |entry| {
                    const peer_copy = try self.allocator.dupe(u8, entry.key_ptr.*);
                    try mesh_peers.put(peer_copy, {});
                }
            }

            // Fill up to D from topic subscribers
            const mesh_count: u32 = @intCast(mesh_peers.count());
            if (mesh_count < self.config.mesh_degree) {
                try self.fillFromTopicPeers(topic, &mesh_peers, self.config.mesh_degree -| mesh_count);
            }

            const topic_owned = try self.allocator.dupe(u8, topic);
            try self.mesh.put(topic_owned, mesh_peers);

            // Clean up fanout
            if (self.fanout.fetchRemove(topic)) |kv| {
                // Free the fanout PeerSet entries (the keys were copied into mesh_peers above)
                var old_ps = kv.value;
                var old_iter = old_ps.keyIterator();
                while (old_iter.next()) |key| {
                    self.allocator.free(key.*);
                }
                old_ps.deinit();
                self.allocator.free(kv.key);
            }
            if (self.fanout_last_pub.fetchRemove(topic)) |kv| {
                self.allocator.free(kv.key);
            }

            // Send GRAFT to all mesh peers
            const mesh_added = self.mesh.getPtr(topic).?;
            var iter = mesh_added.keyIterator();
            while (iter.next()) |peer_key| {
                self.sendGraft(peer_key.*, topic) catch {};
            }
        }

        fn leaveMesh(self: *Self, topic: []const u8) !void {
            const kv = self.mesh.fetchRemove(topic) orelse return;

            // Send PRUNE to all mesh peers
            var peers = kv.value;
            var iter = peers.iterator();
            while (iter.next()) |entry| {
                self.sendPrune(entry.key_ptr.*, topic) catch {};
                self.allocator.free(entry.key_ptr.*);
            }
            peers.deinit();
            self.allocator.free(kv.key);
        }

        fn graftPeers(self: *Self, topic: []const u8, mesh_peers: *PeerSet, needed: u32) !void {
            const candidates = try self.getTopicCandidates(topic, mesh_peers);
            defer self.allocator.free(candidates);

            // v1.1: filter out peers in backoff
            var filtered: std.ArrayList([]const u8) = .empty;
            defer filtered.deinit(self.allocator);
            const now = self.handler.currentTimeMs();
            for (candidates) |peer| {
                const in_backoff = if (self.backoff.get(peer)) |until| now < until else false;
                if (!in_backoff) {
                    try filtered.append(self.allocator, peer);
                }
            }

            shuffleSlice([]const u8, filtered.items, self.handler);

            const to_graft = @min(filtered.items.len, needed);
            for (filtered.items[0..to_graft]) |peer| {
                const peer_owned = try self.allocator.dupe(u8, peer);
                try mesh_peers.put(peer_owned, {});
                try self.enqueueGraft(peer, topic);
            }
        }

        /// v1.1: When pruning excess peers, prefer keeping outbound connections.
        fn prunePeersV11(self: *Self, topic: []const u8, mesh_peers: *PeerSet, excess: u32) !void {
            var peers = try peerSetToSlice(self.allocator, mesh_peers);
            defer self.allocator.free(peers);

            if (@hasDecl(Handler, "isOutbound")) {
                // Partition: inbound first, outbound last (prune inbound first)
                var inbound_end: usize = 0;
                for (0..peers.len) |i| {
                    if (!self.handler.isOutbound(peers[i])) {
                        const tmp = peers[inbound_end];
                        peers[inbound_end] = peers[i];
                        peers[i] = tmp;
                        inbound_end += 1;
                    }
                }
                // Shuffle within the inbound portion for fairness
                shuffleSlice([]const u8, peers[0..inbound_end], self.handler);
            } else {
                shuffleSlice([]const u8, peers, self.handler);
            }

            const to_prune = @min(peers.len, excess);
            for (peers[0..to_prune]) |peer| {
                if (mesh_peers.fetchRemove(peer)) |kv| {
                    self.allocator.free(kv.key);
                }
                try self.enqueuePrune(peer, topic);
            }
        }

        /// v1.1: Opportunistic grafting — try to graft outbound peers specifically.
        fn graftOutboundPeers(self: *Self, topic: []const u8, mesh_peers: *PeerSet, needed: u32) !void {
            if (!@hasDecl(Handler, "isOutbound")) return;

            const candidates = try self.getTopicCandidates(topic, mesh_peers);
            defer self.allocator.free(candidates);

            // Filter to outbound-only peers not in backoff
            var outbound: std.ArrayList([]const u8) = .empty;
            defer outbound.deinit(self.allocator);
            const now = self.handler.currentTimeMs();
            for (candidates) |peer| {
                if (!self.handler.isOutbound(peer)) continue;
                const in_backoff = if (self.backoff.get(peer)) |until| now < until else false;
                if (!in_backoff) {
                    try outbound.append(self.allocator, peer);
                }
            }

            shuffleSlice([]const u8, outbound.items, self.handler);

            const to_graft = @min(outbound.items.len, needed);
            for (outbound.items[0..to_graft]) |peer| {
                const peer_owned = try self.allocator.dupe(u8, peer);
                try mesh_peers.put(peer_owned, {});
                try self.enqueueGraft(peer, topic);
            }
        }

        fn fillFromTopicPeers(self: *Self, topic: []const u8, mesh_peers: *PeerSet, needed: u32) !void {
            var candidates = try self.getTopicCandidates(topic, mesh_peers);
            defer self.allocator.free(candidates);

            shuffleSlice([]const u8, candidates, self.handler);

            const to_add = @min(candidates.len, needed);
            for (candidates[0..to_add]) |peer| {
                const peer_owned = try self.allocator.dupe(u8, peer);
                try mesh_peers.put(peer_owned, {});
            }
        }

        fn getTopicCandidates(self: *Self, topic: []const u8, exclude: *PeerSet) ![][]const u8 {
            const topic_peers = self.topics.getPtr(topic) orelse return try self.allocator.alloc([]const u8, 0);

            var result: std.ArrayList([]const u8) = .empty;
            errdefer result.deinit(self.allocator);

            var iter = topic_peers.iterator();
            while (iter.next()) |entry| {
                if (!exclude.contains(entry.key_ptr.*)) {
                    try result.append(self.allocator, entry.key_ptr.*);
                }
            }

            return result.toOwnedSlice(self.allocator);
        }

        fn selectPeersToPublish(self: *Self, topic: []const u8) ![][]const u8 {
            if (self.config.flood_publish) {
                return self.getAllTopicPeers(topic);
            }

            if (self.mesh.getPtr(topic)) |mesh_peers| {
                return peerSetToSlice(self.allocator, mesh_peers);
            }

            // Use fanout
            const fanout_gop = try self.fanout.getOrPut(topic);
            if (!fanout_gop.found_existing) {
                fanout_gop.key_ptr.* = try self.allocator.dupe(u8, topic);
                fanout_gop.value_ptr.* = PeerSet.init(self.allocator);
                try self.fillFromTopicPeers(topic, fanout_gop.value_ptr, self.config.mesh_degree);

                const ts_key = try self.allocator.dupe(u8, topic);
                try self.fanout_last_pub.put(ts_key, self.handler.currentTimeMs());
            }

            return peerSetToSlice(self.allocator, fanout_gop.value_ptr);
        }

        fn getAllTopicPeers(self: *Self, topic: []const u8) ![][]const u8 {
            const topic_peers = self.topics.getPtr(topic) orelse return try self.allocator.alloc([]const u8, 0);
            return peerSetToSlice(self.allocator, topic_peers);
        }

        // --- Internal: Gossip emission ---

        fn emitGossip(self: *Self, topic: []const u8, mesh_peers: *PeerSet) !void {
            const mids = self.mcache.getGossipIDs(topic) catch return;
            defer self.allocator.free(mids);

            if (mids.len == 0) return;

            var candidates = try self.getTopicCandidates(topic, mesh_peers);
            defer self.allocator.free(candidates);

            if (candidates.len == 0) return;

            shuffleSlice([]const u8, candidates, self.handler);

            const factor_count: u32 = @intFromFloat(@ceil(@as(f32, @floatFromInt(candidates.len)) * self.config.gossip_factor));
            const gossip_count = @max(self.config.mesh_degree_lazy, factor_count);
            const to_gossip = @min(candidates.len, gossip_count);

            for (candidates[0..to_gossip]) |peer| {
                self.sendIHave(peer, topic, mids) catch {};
            }
        }

        // --- Internal: v1.2 IDONTWANT ---

        fn sendIDontWant(self: *Self, topic: []const u8, mid: []const u8) !void {
            const mesh_peers = self.mesh.getPtr(topic) orelse return;

            var optionals = [_]?[]const u8{mid};
            const idontwant_msg = rpc.ControlIDontWant{
                .message_i_ds = &optionals,
            };

            var ctrl = rpc.ControlMessage{};
            var idontwant_arr = [_]?rpc.ControlIDontWant{idontwant_msg};
            ctrl.idontwant = &idontwant_arr;

            var rpc_msg = rpc.RPC{};
            rpc_msg.control = ctrl;

            const frame = codec.encodeRpc(self.allocator, &rpc_msg) catch return;
            defer self.allocator.free(frame);

            var iter = mesh_peers.keyIterator();
            while (iter.next()) |peer_key| {
                _ = self.handler.sendRpc(peer_key.*, frame);
            }
        }

        fn sendIDontWantExcluding(self: *Self, topic: []const u8, mid: []const u8, exclude_peer: []const u8) !void {
            const mesh_peers = self.mesh.getPtr(topic) orelse return;

            var optionals = [_]?[]const u8{mid};
            const idontwant_msg = rpc.ControlIDontWant{
                .message_i_ds = &optionals,
            };

            var ctrl = rpc.ControlMessage{};
            var idontwant_arr = [_]?rpc.ControlIDontWant{idontwant_msg};
            ctrl.idontwant = &idontwant_arr;

            var rpc_msg = rpc.RPC{};
            rpc_msg.control = ctrl;

            const frame = codec.encodeRpc(self.allocator, &rpc_msg) catch return;
            defer self.allocator.free(frame);

            var iter = mesh_peers.keyIterator();
            while (iter.next()) |peer_key| {
                if (std.mem.eql(u8, peer_key.*, exclude_peer)) continue;
                // v1.2+: only send to peers that support v1.2 or later
                if (@hasDecl(Handler, "peerProtocol")) {
                    if (self.handler.peerProtocol(peer_key.*)) |proto| {
                        if (!peerSupportsIDontWant(proto)) continue;
                    } else continue;
                }
                _ = self.handler.sendRpc(peer_key.*, frame);
            }
        }

        // --- Internal: Fanout maintenance ---

        fn maintainFanout(self: *Self) !void {
            const now = self.handler.currentTimeMs();

            // Expire old fanout topics
            var expired: std.ArrayList([]const u8) = .empty;
            defer expired.deinit(self.allocator);

            var fp_iter = self.fanout_last_pub.iterator();
            while (fp_iter.next()) |entry| {
                if (now -| entry.value_ptr.* > self.config.fanout_ttl_ms) {
                    try expired.append(self.allocator, entry.key_ptr.*);
                }
            }

            for (expired.items) |topic| {
                if (self.fanout.fetchRemove(topic)) |kv| {
                    var ps = kv.value;
                    var inner_iter = ps.keyIterator();
                    while (inner_iter.next()) |key| {
                        self.allocator.free(key.*);
                    }
                    ps.deinit();
                    self.allocator.free(kv.key);
                }
                if (self.fanout_last_pub.fetchRemove(topic)) |kv| {
                    self.allocator.free(kv.key);
                }
            }

            // Replenish remaining fanout topics
            var topic_list: std.ArrayList([]const u8) = .empty;
            defer topic_list.deinit(self.allocator);
            {
                var fanout_iter = self.fanout.keyIterator();
                while (fanout_iter.next()) |key| {
                    try topic_list.append(self.allocator, key.*);
                }
            }
            for (topic_list.items) |topic| {
                const fanout_peers = self.fanout.getPtr(topic) orelse continue;
                const count: u32 = @intCast(fanout_peers.count());
                if (count < self.config.mesh_degree) {
                    try self.fillFromTopicPeers(topic, fanout_peers, self.config.mesh_degree -| count);
                }
            }
        }

        // --- Internal: Backoff management ---

        fn expireBackoffs(self: *Self) void {
            const now = self.handler.currentTimeMs();
            var to_remove: std.ArrayList([]const u8) = .empty;
            defer to_remove.deinit(self.allocator);

            var iter = self.backoff.iterator();
            while (iter.next()) |entry| {
                if (now >= entry.value_ptr.*) {
                    to_remove.append(self.allocator, entry.key_ptr.*) catch continue;
                }
            }

            for (to_remove.items) |peer| {
                _ = self.backoff.remove(peer);
            }
        }

        /// v1.2: Clear per-peer IDONTWANT sets during heartbeat.
        fn clearIDontWant(self: *Self) void {
            var iter = self.peer_idontwant.iterator();
            while (iter.next()) |entry| {
                var inner_iter = entry.value_ptr.keyIterator();
                while (inner_iter.next()) |key| {
                    self.allocator.free(key.*);
                }
                entry.value_ptr.clearRetainingCapacity();
            }
        }

        // --- Internal: Control message sending ---

        fn enqueueGraft(self: *Self, peer: []const u8, topic: []const u8) !void {
            const gop = try self.pending_graft.getOrPut(peer);
            if (!gop.found_existing) {
                gop.value_ptr.* = .empty;
            }
            const topic_copy = try self.allocator.dupe(u8, topic);
            try gop.value_ptr.append(self.allocator, topic_copy);
        }

        fn enqueuePrune(self: *Self, peer: []const u8, topic: []const u8) !void {
            const gop = try self.pending_prune.getOrPut(peer);
            if (!gop.found_existing) {
                gop.value_ptr.* = .empty;
            }
            const topic_copy = try self.allocator.dupe(u8, topic);
            try gop.value_ptr.append(self.allocator, topic_copy);
        }

        fn flushControl(self: *Self) !void {
            // Flush GRAFTs
            var graft_iter = self.pending_graft.iterator();
            while (graft_iter.next()) |entry| {
                for (entry.value_ptr.items) |topic| {
                    self.sendGraft(entry.key_ptr.*, topic) catch {};
                    self.allocator.free(topic);
                }
                entry.value_ptr.deinit(self.allocator);
            }
            self.pending_graft.clearRetainingCapacity();

            // Flush PRUNEs
            var prune_iter = self.pending_prune.iterator();
            while (prune_iter.next()) |entry| {
                for (entry.value_ptr.items) |topic| {
                    self.sendPruneWithBackoff(entry.key_ptr.*, topic) catch {};
                    self.allocator.free(topic);
                }
                entry.value_ptr.deinit(self.allocator);
            }
            self.pending_prune.clearRetainingCapacity();
        }

        fn sendGraft(self: *Self, peer: []const u8, topic: []const u8) !void {
            const graft_msg = rpc.ControlGraft{ .topic_i_d = topic };
            var ctrl = rpc.ControlMessage{};
            var graft_arr = [_]?rpc.ControlGraft{graft_msg};
            ctrl.graft = &graft_arr;

            var rpc_msg = rpc.RPC{};
            rpc_msg.control = ctrl;

            const frame = codec.encodeRpc(self.allocator, &rpc_msg) catch return;
            defer self.allocator.free(frame);
            _ = self.handler.sendRpc(peer, frame);
        }

        /// v1.1: PRUNE with backoff duration (in seconds on the wire).
        fn sendPruneWithBackoff(self: *Self, peer: []const u8, topic: []const u8) !void {
            const prune_msg = rpc.ControlPrune{
                .topic_i_d = topic,
                .backoff = default_prune_backoff_ms / 1000, // wire format is seconds
            };
            var ctrl = rpc.ControlMessage{};
            var prune_arr = [_]?rpc.ControlPrune{prune_msg};
            ctrl.prune = &prune_arr;

            var rpc_msg = rpc.RPC{};
            rpc_msg.control = ctrl;

            const frame = codec.encodeRpc(self.allocator, &rpc_msg) catch return;
            defer self.allocator.free(frame);
            _ = self.handler.sendRpc(peer, frame);
        }

        fn sendPrune(self: *Self, peer: []const u8, topic: []const u8) !void {
            return self.sendPruneWithBackoff(peer, topic);
        }

        fn sendIHave(self: *Self, peer: []const u8, topic: []const u8, mids: [][]const u8) !void {
            var optionals: [256]?[]const u8 = undefined;
            const count = @min(mids.len, 256);
            for (0..count) |i| {
                optionals[i] = mids[i];
            }

            const ihave_msg = rpc.ControlIHave{
                .topic_i_d = topic,
                .message_i_ds = optionals[0..count],
            };
            var ctrl = rpc.ControlMessage{};
            var ihave_arr = [_]?rpc.ControlIHave{ihave_msg};
            ctrl.ihave = &ihave_arr;

            var rpc_msg = rpc.RPC{};
            rpc_msg.control = ctrl;

            const frame = codec.encodeRpc(self.allocator, &rpc_msg) catch return;
            defer self.allocator.free(frame);
            _ = self.handler.sendRpc(peer, frame);
        }

        // --- Helpers ---

        fn optionalBytes(slice: []const u8) ?[]const u8 {
            return if (slice.len > 0) slice else null;
        }

        /// Check if a protocol version supports IDONTWANT (v1.2+).
        fn peerSupportsIDontWant(proto: []const u8) bool {
            return std.mem.eql(u8, proto, config_mod.protocol_ids.v1_2) or
                std.mem.eql(u8, proto, config_mod.protocol_ids.v1_3);
        }

        fn peerSetToSlice(allocator: std.mem.Allocator, peers: *PeerSet) ![][]const u8 {
            var result: std.ArrayList([]const u8) = .empty;
            errdefer result.deinit(allocator);

            var iter = peers.iterator();
            while (iter.next()) |entry| {
                try result.append(allocator, entry.key_ptr.*);
            }

            return result.toOwnedSlice(allocator);
        }

        fn shuffleSlice(comptime T: type, items: []T, handler: *Handler) void {
            if (items.len <= 1) return;
            var i: usize = items.len - 1;
            while (i > 0) : (i -= 1) {
                const j = handler.randomU64() % (i + 1);
                const tmp = items[i];
                items[i] = items[j];
                items[j] = tmp;
            }
        }

        fn deinitPeerSetMap(map: *std.StringHashMap(PeerSet), allocator: std.mem.Allocator) void {
            var iter = map.iterator();
            while (iter.next()) |entry| {
                var inner_iter = entry.value_ptr.keyIterator();
                while (inner_iter.next()) |key| {
                    allocator.free(key.*);
                }
                entry.value_ptr.deinit();
                allocator.free(entry.key_ptr.*);
            }
            map.deinit();
        }

        fn deinitKeyedMap(map: anytype, allocator: std.mem.Allocator) void {
            var iter = map.keyIterator();
            while (iter.next()) |key| {
                allocator.free(key.*);
            }
            map.deinit();
        }

        fn deinitKeyedMap2(map: *std.StringHashMap(u64), allocator: std.mem.Allocator) void {
            var iter = map.keyIterator();
            while (iter.next()) |key| {
                allocator.free(key.*);
            }
            map.deinit();
        }

        fn deinitPeerMap(map: *std.StringHashMap(std.ArrayList([]const u8)), allocator: std.mem.Allocator) void {
            var iter = map.iterator();
            while (iter.next()) |entry| {
                for (entry.value_ptr.items) |topic| {
                    allocator.free(topic);
                }
                entry.value_ptr.deinit(allocator);
            }
            map.deinit();
        }
    };
}

// --- Tests ---

const TestHandler = struct {
    sent: std.ArrayList(SentRpc),
    allocator: std.mem.Allocator,
    time_ms: u64,
    rng_state: u64,
    connected_peers: std.StringHashMap(void),
    outbound_peers: std.StringHashMap(void),
    peer_protocols: std.StringHashMap([]const u8),

    const SentRpc = struct {
        peer: []const u8,
        data: []const u8,
    };

    fn init(allocator: std.mem.Allocator) TestHandler {
        return .{
            .sent = .empty,
            .allocator = allocator,
            .time_ms = 1000,
            .rng_state = 42,
            .connected_peers = std.StringHashMap(void).init(allocator),
            .outbound_peers = std.StringHashMap(void).init(allocator),
            .peer_protocols = std.StringHashMap([]const u8).init(allocator),
        };
    }

    fn deinit(self: *TestHandler) void {
        for (self.sent.items) |item| {
            self.allocator.free(item.data);
        }
        self.sent.deinit(self.allocator);
        self.connected_peers.deinit();
        self.outbound_peers.deinit();
        self.peer_protocols.deinit();
        self.* = undefined;
    }

    pub fn sendRpc(self: *TestHandler, peer: []const u8, data: []const u8) bool {
        const data_copy = self.allocator.dupe(u8, data) catch return false;
        self.sent.append(self.allocator, .{ .peer = peer, .data = data_copy }) catch {
            self.allocator.free(data_copy);
            return false;
        };
        return true;
    }

    pub fn randomU64(self: *TestHandler) u64 {
        self.rng_state ^= self.rng_state << 13;
        self.rng_state ^= self.rng_state >> 7;
        self.rng_state ^= self.rng_state << 17;
        return self.rng_state;
    }

    pub fn currentTimeMs(self: *TestHandler) u64 {
        return self.time_ms;
    }

    pub fn isPeerConnected(self: *TestHandler, peer: []const u8) bool {
        return self.connected_peers.contains(peer);
    }

    pub fn isOutbound(self: *TestHandler, peer: []const u8) bool {
        return self.outbound_peers.contains(peer);
    }

    pub fn peerProtocol(self: *TestHandler, peer: []const u8) ?[]const u8 {
        return self.peer_protocols.get(peer);
    }

    fn clearSent(self: *TestHandler) void {
        for (self.sent.items) |item| {
            self.allocator.free(item.data);
        }
        self.sent.items.len = 0;
    }

    fn markConnected(self: *TestHandler, peer: []const u8) !void {
        try self.connected_peers.put(peer, {});
    }

    fn markOutbound(self: *TestHandler, peer: []const u8) !void {
        try self.outbound_peers.put(peer, {});
    }
};

const TestRouter = Router(TestHandler);

fn addPeerSubscription(router: *TestRouter, peer: []const u8, topic: []const u8) !void {
    var subs = [_]?rpc.RPC.SubOpts{
        .{ .subscribe = true, .topicid = topic },
    };
    var rpc_msg = rpc.RPC{ .subscriptions = &subs };
    const encoded = rpc_msg.encode(router.allocator) catch unreachable;
    defer router.allocator.free(encoded);
    try router.handleRpc(peer, encoded);
}

test "Router init and deinit" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();
}

test "Router subscribe and unsubscribe" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    try router.subscribe("test-topic");
    try std.testing.expect(router.subscriptions.contains("test-topic"));
    try std.testing.expect(router.mesh.contains("test-topic"));

    try router.unsubscribe("test-topic");
    try std.testing.expect(!router.subscriptions.contains("test-topic"));
    try std.testing.expect(!router.mesh.contains("test-topic"));
}

test "Router subscribe is idempotent" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    try router.subscribe("topic-a");
    try router.subscribe("topic-a");
    try std.testing.expectEqual(@as(u32, 1), router.subscriptions.count());
}

test "Router handleRpc processes subscriptions" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    try addPeerSubscription(&router, "peer-1", "topic-a");

    try std.testing.expect(router.topics.contains("topic-a"));
    const peer_set = router.topics.getPtr("topic-a").?;
    try std.testing.expect(peer_set.contains("peer-1"));

    const events = try router.drainEvents();
    defer allocator.free(events);
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expect(events[0] == .subscription_changed);
}

test "Router heartbeat with peers" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    try router.subscribe("topic-a");

    for ([_][]const u8{ "peer-1", "peer-2", "peer-3", "peer-4", "peer-5" }) |peer| {
        try handler.markConnected(peer);
        try addPeerSubscription(&router, peer, "topic-a");
    }

    try router.heartbeat();
    try std.testing.expectEqual(@as(u64, 1), router.heartbeat_ticks);
}

test "Router removePeer cleans up state" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    try addPeerSubscription(&router, "peer-1", "topic-a");
    try std.testing.expect(router.topics.getPtr("topic-a").?.contains("peer-1"));

    router.removePeer("peer-1");
    try std.testing.expect(!router.topics.getPtr("topic-a").?.contains("peer-1"));
}

test "Router v1.1 PRUNE backoff is respected" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    try router.subscribe("topic-a");

    // Add peers
    for ([_][]const u8{ "peer-1", "peer-2", "peer-3" }) |peer| {
        try handler.markConnected(peer);
        try addPeerSubscription(&router, peer, "topic-a");
    }

    // Set backoff for peer-1 (far in the future)
    try router.backoff.put("peer-1", handler.time_ms + 120_000);

    handler.clearSent();
    const events = try router.drainEvents();
    defer allocator.free(events);

    // Heartbeat should NOT graft peer-1 while in backoff
    try router.heartbeat();

    // Advance time past backoff
    handler.time_ms += 130_000;
    router.expireBackoffs();
    try std.testing.expect(!router.backoff.contains("peer-1"));
}

test "Router GRAFT rejects when not subscribed" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    // Don't subscribe, but receive a GRAFT
    const graft_msg = rpc.ControlGraft{ .topic_i_d = "unknown-topic" };
    var ctrl = rpc.ControlMessage{};
    var graft_arr = [_]?rpc.ControlGraft{graft_msg};
    ctrl.graft = &graft_arr;
    var rpc_msg = rpc.RPC{};
    rpc_msg.control = ctrl;

    const encoded = rpc_msg.encode(allocator) catch unreachable;
    defer allocator.free(encoded);
    try router.handleRpc("peer-1", encoded);

    // Should have enqueued a PRUNE response
    try std.testing.expect(router.pending_prune.contains("peer-1"));
}

test "Router memory management" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.testing.expect(false) catch @panic("Memory leak detected!");
        }
    }
    const allocator = gpa.allocator();

    {
        var handler = TestHandler.init(allocator);
        defer handler.deinit();

        var router = try TestRouter.init(allocator, .{}, &handler);
        defer router.deinit();

        try router.subscribe("topic-1");
        try router.subscribe("topic-2");

        for ([_][]const u8{ "peer-a", "peer-b", "peer-c" }) |peer| {
            try handler.markConnected(peer);
            try addPeerSubscription(&router, peer, "topic-1");
            try addPeerSubscription(&router, peer, "topic-2");
        }

        try router.heartbeat();
        try router.heartbeat();

        const events = try router.drainEvents();
        allocator.free(events);

        try router.unsubscribe("topic-1");
        router.removePeer("peer-a");
    }
}

test "Router v1.1 PRUNE with backoff on wire" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    try router.subscribe("topic-a");

    // Send a PRUNE and verify the router applies backoff
    const prune = rpc.ControlPrune{
        .topic_i_d = "topic-a",
        .backoff = 120, // 120 seconds
    };
    var ctrl = rpc.ControlMessage{};
    var prune_arr = [_]?rpc.ControlPrune{prune};
    ctrl.prune = &prune_arr;
    var rpc_msg = rpc.RPC{};
    rpc_msg.control = ctrl;

    // First add peer-1 to mesh manually
    const mesh_peers = router.mesh.getPtr("topic-a").?;
    const peer_owned = try allocator.dupe(u8, "peer-1");
    try mesh_peers.put(peer_owned, {});

    const encoded = rpc_msg.encode(allocator) catch unreachable;
    defer allocator.free(encoded);

    try router.handleRpc("peer-1", encoded);

    // Peer should be removed from mesh
    try std.testing.expect(!mesh_peers.contains("peer-1"));

    // Backoff should be set (120 * 1000 = 120000ms from now)
    const backoff_until = router.backoff.get("peer-1").?;
    try std.testing.expect(backoff_until > handler.time_ms);
}

test "Router v1.2 IDONTWANT skips forwarding" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    try router.subscribe("topic-a");

    // Add mesh peers
    for ([_][]const u8{ "peer-1", "peer-2", "peer-3" }) |peer| {
        try handler.markConnected(peer);
        try addPeerSubscription(&router, peer, "topic-a");
    }

    // Simulate IDONTWANT from peer-2 for a specific message ID
    var optionals = [_]?[]const u8{"msg-id-123"};
    const idontwant_msg = rpc.ControlIDontWant{
        .message_i_ds = &optionals,
    };
    var ctrl = rpc.ControlMessage{};
    var idontwant_arr = [_]?rpc.ControlIDontWant{idontwant_msg};
    ctrl.idontwant = &idontwant_arr;
    var rpc_msg = rpc.RPC{};
    rpc_msg.control = ctrl;

    const encoded = rpc_msg.encode(allocator) catch unreachable;
    defer allocator.free(encoded);
    try router.handleRpc("peer-2", encoded);

    // Verify the IDONTWANT is tracked
    const idw = router.peer_idontwant.getPtr("peer-2").?;
    try std.testing.expect(idw.contains("msg-id-123"));
}

test "Router v1.2 IDONTWANT cleared on heartbeat" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    // Manually add an IDONTWANT entry
    var inner = std.StringHashMap(void).init(allocator);
    const mid_owned = try allocator.dupe(u8, "some-msg");
    try inner.put(mid_owned, {});
    try router.peer_idontwant.put("peer-1", inner);

    try router.heartbeat();

    // IDONTWANT set should be cleared (but map entry remains)
    const idw = router.peer_idontwant.getPtr("peer-1").?;
    try std.testing.expectEqual(@as(u32, 0), idw.count());
}

test "Router v1.1 peer scoring basics" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    // No score for unknown peer
    try std.testing.expectEqual(@as(f64, 0), router.peerScore("unknown"));

    // Add a peer
    try router.addPeer("peer-1");

    // Initial score should be 0 (no deliveries, no penalties)
    try std.testing.expectEqual(@as(f64, 0), router.peerScore("peer-1"));

    // Add a behaviour penalty
    router.addBehaviourPenalty("peer-1", 1);
    // Score should now be negative (penalty^2 * negative weight)
    try std.testing.expect(router.peerScore("peer-1") < 0);
}

test "Router v1.1 score decay" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    try router.addPeer("peer-1");
    router.addBehaviourPenalty("peer-1", 10);

    const score_before = router.peerScore("peer-1");
    try std.testing.expect(score_before < 0);

    // Decay scores
    handler.time_ms += 1000;
    router.decayScores();

    const score_after = router.peerScore("peer-1");
    // Score should have decayed towards zero (less negative)
    try std.testing.expect(score_after > score_before);
}

test "Router v1.3 extensions tracking" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    // Simulate receiving an extensions control message
    const ext = rpc.ControlExtensions{
        .partial_messages = true,
    };
    var ctrl = rpc.ControlMessage{};
    ctrl.extensions = ext;
    var rpc_msg = rpc.RPC{};
    rpc_msg.control = ctrl;

    const encoded = rpc_msg.encode(allocator) catch unreachable;
    defer allocator.free(encoded);
    try router.handleRpc("peer-1", encoded);

    // Extensions should be tracked
    const peer_ext = router.peer_extensions.get("peer-1").?;
    try std.testing.expect(peer_ext.partial_messages);

    // Event should have been emitted
    const events = try router.drainEvents();
    defer allocator.free(events);
    try std.testing.expect(events.len > 0);
    try std.testing.expect(events[0] == .peer_extensions);
}

test "Router v1.3 extensions only accepted once" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var router = try TestRouter.init(allocator, .{}, &handler);
    defer router.deinit();

    // First extensions message
    const ext1 = rpc.ControlExtensions{ .partial_messages = true };
    var ctrl1 = rpc.ControlMessage{};
    ctrl1.extensions = ext1;
    var rpc_msg1 = rpc.RPC{};
    rpc_msg1.control = ctrl1;

    const encoded1 = rpc_msg1.encode(allocator) catch unreachable;
    defer allocator.free(encoded1);
    try router.handleRpc("peer-1", encoded1);

    // Second extensions message (should be ignored)
    const ext2 = rpc.ControlExtensions{ .partial_messages = false };
    var ctrl2 = rpc.ControlMessage{};
    ctrl2.extensions = ext2;
    var rpc_msg2 = rpc.RPC{};
    rpc_msg2.control = ctrl2;

    const encoded2 = rpc_msg2.encode(allocator) catch unreachable;
    defer allocator.free(encoded2);
    try router.handleRpc("peer-1", encoded2);

    // First value should still be in effect
    const peer_ext = router.peer_extensions.get("peer-1").?;
    try std.testing.expect(peer_ext.partial_messages);
}

test "Router v1.1 GRAFT rejected for low-score peer" {
    const allocator = std.testing.allocator;
    var handler = TestHandler.init(allocator);
    defer handler.deinit();

    var score_params = PeerScoreParams{};
    score_params.gossip_threshold = -100;
    score_params.behaviour_penalty_weight = -1;

    var router = try TestRouter.initWithScoring(allocator, .{}, &handler, score_params, null);
    defer router.deinit();

    try router.subscribe("topic-a");
    try router.addPeer("peer-1");

    // Give peer-1 a very bad score
    router.addBehaviourPenalty("peer-1", 100);
    const score = router.peerScore("peer-1");
    try std.testing.expect(score < score_params.gossip_threshold);

    // Send GRAFT from peer-1 — should be rejected
    const graft_msg = rpc.ControlGraft{ .topic_i_d = "topic-a" };
    var ctrl = rpc.ControlMessage{};
    var graft_arr = [_]?rpc.ControlGraft{graft_msg};
    ctrl.graft = &graft_arr;
    var rpc_msg = rpc.RPC{};
    rpc_msg.control = ctrl;

    const encoded = rpc_msg.encode(allocator) catch unreachable;
    defer allocator.free(encoded);
    try router.handleRpc("peer-1", encoded);

    // peer-1 should NOT be in mesh
    const mesh_peers = router.mesh.getPtr("topic-a").?;
    try std.testing.expect(!mesh_peers.contains("peer-1"));

    // Should have enqueued a PRUNE response
    try std.testing.expect(router.pending_prune.contains("peer-1"));
}

test "Router memory management with v1.2/v1.3 features" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.testing.expect(false) catch @panic("Memory leak detected!");
        }
    }
    const allocator = gpa.allocator();

    {
        var handler = TestHandler.init(allocator);
        defer handler.deinit();

        var router = try TestRouter.init(allocator, .{}, &handler);
        defer router.deinit();

        try router.subscribe("topic-1");

        for ([_][]const u8{ "peer-a", "peer-b" }) |peer| {
            try handler.markConnected(peer);
            try router.addPeer(peer);
            try addPeerSubscription(&router, peer, "topic-1");
        }

        // Simulate IDONTWANT
        var optionals = [_]?[]const u8{"msg-42"};
        const idontwant_msg = rpc.ControlIDontWant{ .message_i_ds = &optionals };
        var ctrl = rpc.ControlMessage{};
        var idontwant_arr = [_]?rpc.ControlIDontWant{idontwant_msg};
        ctrl.idontwant = &idontwant_arr;
        var rpc_msg = rpc.RPC{};
        rpc_msg.control = ctrl;

        const encoded = rpc_msg.encode(allocator) catch unreachable;
        defer allocator.free(encoded);
        try router.handleRpc("peer-a", encoded);

        // Simulate extensions
        const ext = rpc.ControlExtensions{ .partial_messages = true };
        var ctrl2 = rpc.ControlMessage{};
        ctrl2.extensions = ext;
        var rpc_msg2 = rpc.RPC{};
        rpc_msg2.control = ctrl2;

        const encoded2 = rpc_msg2.encode(allocator) catch unreachable;
        defer allocator.free(encoded2);
        try router.handleRpc("peer-b", encoded2);

        try router.heartbeat();

        const events = try router.drainEvents();
        allocator.free(events);

        router.removePeer("peer-a");
        router.removePeer("peer-b");
    }
}
