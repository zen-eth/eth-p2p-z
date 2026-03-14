const std = @import("std");
const mcache = @import("mcache.zig");

/// Gossipsub protocol identifiers.
pub const protocol_ids = struct {
    pub const v1_0 = "/meshsub/1.0.0";
    pub const v1_1 = "/meshsub/1.1.0";
    pub const v1_2 = "/meshsub/1.2.0";
    pub const v1_3 = "/meshsub/1.3.0";
};

/// All supported protocol IDs in negotiation order (newest first).
pub const supported_protocols: []const []const u8 = &.{
    protocol_ids.v1_3,
    protocol_ids.v1_2,
    protocol_ids.v1_1,
    protocol_ids.v1_0,
};

/// Signature verification policy.
pub const SignaturePolicy = enum {
    /// Messages must be signed. Unsigned messages are rejected.
    strict_sign,
    /// Messages must NOT be signed. Signed messages are rejected.
    strict_no_sign,
};

/// Publish message signing policy.
pub const PublishPolicy = enum {
    /// Sign outgoing messages with the node's key.
    signing,
    /// Publish without signing.
    anonymous,
};

/// Gossipsub router configuration.
///
/// Default values follow the GossipSub v1.1 spec recommendation.
pub const Config = struct {
    /// Target mesh degree (number of peers per topic mesh).
    mesh_degree: u32 = 6,
    /// Lower bound for mesh degree. Heartbeat fills below this.
    mesh_degree_lo: u32 = 4,
    /// Upper bound for mesh degree. Heartbeat prunes above this.
    mesh_degree_hi: u32 = 12,
    /// Lazy push degree: number of peers to gossip IHAVE to.
    mesh_degree_lazy: u32 = 6,

    /// Heartbeat interval in milliseconds.
    heartbeat_interval_ms: u64 = 1_000,
    /// How long fanout state is maintained for topics we are not subscribed to (ms).
    fanout_ttl_ms: u64 = 60_000,

    /// Fraction of mesh peers to gossip to (if mesh is large).
    gossip_factor: f32 = 0.25,
    /// Flood-publish: send to all mesh peers rather than a random subset.
    flood_publish: bool = true,

    /// Maximum number of IHAVE messages to accept from a peer per heartbeat.
    max_ihave_messages: u32 = 10,
    /// Maximum number of message IDs in a single IHAVE.
    max_ihave_length: u32 = 5000,
    /// Maximum retransmission count for IWANT fulfillment.
    gossip_retransmission: u32 = 3,

    /// Number of history windows to keep (total sliding window depth).
    history_length: u32 = 5,
    /// Number of history windows included in gossip (IHAVE).
    history_gossip: u32 = 3,

    /// Message-seen cache TTL in seconds.
    seen_ttl_seconds: u32 = 120,

    /// Minimum message size to trigger IDontWant.
    idontwant_min_message_size: u32 = 1024,

    /// Whether we emit messages to ourselves when publishing.
    emit_self: bool = false,

    /// Signature policy for incoming messages.
    signature_policy: SignaturePolicy = .strict_sign,
    /// Publishing policy for outgoing messages.
    publish_policy: PublishPolicy = .signing,

    /// Custom message ID function. Defaults to `from || seqno`.
    msg_id_fn: mcache.MessageIdFn = mcache.defaultMsgId,

    /// v1.3: Extensions we support (sent on first stream message).
    extensions: Extensions = .{},
};

/// v1.3: Extension capabilities declared on stream open.
pub const Extensions = struct {
    /// Whether this node supports the partial messages extension.
    partial_messages: bool = false,
};

/// v1.1: Peer scoring parameters.
/// Controls how peer scores are computed and applied for mesh management.
pub const PeerScoreParams = struct {
    /// Weight of topic-specific scores in the overall score.
    topic_score_cap: f64 = 3200.0,
    /// IP colocation factor weight. Penalizes peers sharing an IP.
    ip_colocation_factor_weight: f64 = -1.0,
    /// IP colocation factor threshold. Only applies penalty above this count.
    ip_colocation_factor_threshold: u32 = 3,
    /// Behaviour penalty weight for protocol violations.
    behaviour_penalty_weight: f64 = -16.0,
    /// Behaviour penalty decay per heartbeat (multiplicative).
    behaviour_penalty_decay: f64 = 0.99,
    /// Score below which a peer is considered for pruning.
    gossip_threshold: f64 = -4000.0,
    /// Score below which a peer is disconnected.
    graylist_threshold: f64 = -16000.0,
    /// Decay interval in milliseconds (how often decay is applied).
    decay_interval_ms: u64 = 1_000,
    /// Decay to zero threshold. Scores below this are snapped to zero.
    decay_to_zero: f64 = 0.01,
    /// How long to retain score data after peer disconnects (ms).
    retain_score_ms: u64 = 3_600_000,
};

/// v1.1: Per-topic scoring parameters.
pub const TopicScoreParams = struct {
    /// Weight of this topic's score in the peer's overall score.
    topic_weight: f64 = 1.0,

    // P1: Time in mesh
    /// Weight for time-in-mesh score component.
    time_in_mesh_weight: f64 = 0.0,
    /// Quantum for time-in-mesh (ms per unit).
    time_in_mesh_quantum_ms: u64 = 1_000,
    /// Cap on time-in-mesh score.
    time_in_mesh_cap: f64 = 3600.0,

    // P2: First message deliveries
    /// Weight for first-message-delivery score component.
    first_message_deliveries_weight: f64 = 0.0,
    /// Cap on first-message-delivery score.
    first_message_deliveries_cap: f64 = 1000.0,
    /// Decay per heartbeat for first-message-delivery counter.
    first_message_deliveries_decay: f64 = 0.99,

    // P3: Mesh message delivery rate
    /// Weight for mesh-message-delivery-rate component.
    mesh_message_deliveries_weight: f64 = 0.0,
    /// Threshold below which this penalty applies.
    mesh_message_deliveries_threshold: f64 = 0.0,
    /// Cap on the counter.
    mesh_message_deliveries_cap: f64 = 0.0,
    /// Decay per heartbeat for mesh-message-delivery counter.
    mesh_message_deliveries_decay: f64 = 0.0,
    /// Activation window: penalty only applies after this duration in mesh (ms).
    mesh_message_deliveries_activation_ms: u64 = 0,

    // P3b: Mesh failure penalty
    /// Weight for mesh-failure-penalty component (triggered on prune with low delivery).
    mesh_failure_penalty_weight: f64 = 0.0,
    /// Decay per heartbeat for the failure penalty counter.
    mesh_failure_penalty_decay: f64 = 0.0,

    // P4: Invalid messages
    /// Weight for invalid-message penalty.
    invalid_message_deliveries_weight: f64 = 0.0,
    /// Decay per heartbeat for invalid-message counter.
    invalid_message_deliveries_decay: f64 = 0.0,
};

/// Subscription change direction.
pub const SubscriptionAction = enum {
    subscribe,
    unsubscribe,
};

/// An event emitted by the gossipsub router.
pub const Event = union(enum) {
    /// A validated message was received on a subscribed topic.
    message: MessageEvent,
    /// A peer changed its topic subscription.
    subscription_changed: SubscriptionChangedEvent,
    /// A peer was grafted into a topic mesh.
    graft: GraftEvent,
    /// A peer was pruned from a topic mesh.
    prune: PruneEvent,
    /// A peer's score dropped below the gossip threshold.
    score_below_threshold: ScoreEvent,
    /// v1.3: A peer declared its supported extensions.
    peer_extensions: PeerExtensionsEvent,
};

pub const MessageEvent = struct {
    /// The topic the message was published to.
    topic: []const u8,
    /// The raw message data.
    data: []const u8,
    /// The sender's peer ID (if available from the message).
    from: ?[]const u8,
    /// The message sequence number (if available).
    seqno: ?[]const u8,
};

pub const SubscriptionChangedEvent = struct {
    peer_id: []const u8,
    topic: []const u8,
    action: SubscriptionAction,
};

pub const GraftEvent = struct {
    peer_id: []const u8,
    topic: []const u8,
};

pub const PruneEvent = struct {
    peer_id: []const u8,
    topic: []const u8,
};

pub const ScoreEvent = struct {
    peer_id: []const u8,
    score: f64,
};

pub const PeerExtensionsEvent = struct {
    peer_id: []const u8,
    partial_messages: bool,
};

// --- Tests ---

test "Config defaults" {
    const cfg = Config{};
    try std.testing.expectEqual(@as(u32, 6), cfg.mesh_degree);
    try std.testing.expectEqual(@as(u32, 4), cfg.mesh_degree_lo);
    try std.testing.expectEqual(@as(u32, 12), cfg.mesh_degree_hi);
    try std.testing.expectEqual(@as(u32, 6), cfg.mesh_degree_lazy);
    try std.testing.expectEqual(@as(u64, 1_000), cfg.heartbeat_interval_ms);
    try std.testing.expectEqual(@as(u64, 60_000), cfg.fanout_ttl_ms);
    try std.testing.expect(cfg.flood_publish);
    try std.testing.expect(!cfg.emit_self);
    try std.testing.expectEqual(SignaturePolicy.strict_sign, cfg.signature_policy);
    try std.testing.expectEqual(PublishPolicy.signing, cfg.publish_policy);
}

test "Config custom values" {
    const cfg = Config{
        .mesh_degree = 8,
        .mesh_degree_lo = 6,
        .mesh_degree_hi = 16,
        .heartbeat_interval_ms = 500,
        .flood_publish = false,
        .signature_policy = .strict_no_sign,
        .publish_policy = .anonymous,
    };
    try std.testing.expectEqual(@as(u32, 8), cfg.mesh_degree);
    try std.testing.expectEqual(@as(u64, 500), cfg.heartbeat_interval_ms);
    try std.testing.expect(!cfg.flood_publish);
    try std.testing.expectEqual(SignaturePolicy.strict_no_sign, cfg.signature_policy);
    try std.testing.expectEqual(PublishPolicy.anonymous, cfg.publish_policy);
}

test "Event union tag" {
    const event: Event = .{ .message = .{
        .topic = "test",
        .data = "hello",
        .from = null,
        .seqno = null,
    } };
    try std.testing.expect(event == .message);
}

test "protocol_ids are valid strings" {
    try std.testing.expect(std.mem.startsWith(u8, protocol_ids.v1_0, "/meshsub/"));
    try std.testing.expect(std.mem.startsWith(u8, protocol_ids.v1_1, "/meshsub/"));
    try std.testing.expect(std.mem.startsWith(u8, protocol_ids.v1_2, "/meshsub/"));
    try std.testing.expect(std.mem.startsWith(u8, protocol_ids.v1_3, "/meshsub/"));
}

test "Extensions defaults" {
    const ext = Extensions{};
    try std.testing.expect(!ext.partial_messages);
}

test "PeerScoreParams defaults" {
    const params = PeerScoreParams{};
    try std.testing.expect(params.gossip_threshold < 0);
    try std.testing.expect(params.graylist_threshold < params.gossip_threshold);
    try std.testing.expect(params.behaviour_penalty_decay > 0 and params.behaviour_penalty_decay < 1.0);
}

test "TopicScoreParams defaults" {
    const params = TopicScoreParams{};
    try std.testing.expectEqual(@as(f64, 1.0), params.topic_weight);
    try std.testing.expectEqual(@as(f64, 0.0), params.time_in_mesh_weight);
}
