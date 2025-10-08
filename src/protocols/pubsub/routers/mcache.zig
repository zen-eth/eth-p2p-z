const std = @import("std");
const Allocator = std.mem.Allocator;
const rpc = @import("../../../proto/rpc.proto.zig");
const PeerId = @import("peer_id").PeerId;
const pubsub = @import("../pubsub.zig");
const testing = std.testing;

/// This is a message cache implementation follow the
/// [spec](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.0.md#message-cache)
/// It is ported from the [Go implementation](https://github.com/libp2p/go-libp2p-pubsub/blob/abb8f8a2cd5aee610e16de66d63cd539a353e166/mcache.go)
///
/// MessageCache implements a sliding window cache for PubSub messages following the GossipSub spec.
///
/// The cache maintains messages in a sliding window structure where:
/// - Messages are stored with their unique IDs for fast lookup
/// - A sliding window tracks message age for garbage collection
/// - Per-peer transmission counts prevent duplicate sends
///
/// ## Architecture Overview:
/// ```
/// ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
/// │  Message Store  │    │  History Windows │    │ Peer Transmissions │
/// │                 │    │                  │    │                     │
/// │ msgID -> Message│    │ [W0][W1][W2][W3] │    │ msgID -> peerCounts │
/// │                 │    │                  │    │                     │
/// └─────────────────┘    └──────────────────┘    └─────────────────────┘
///          │                       │                         │
///          └───────────────────────┼─────────────────────────┘
///                                  │
///                            References only
/// ```
///
/// ## Sliding Window Mechanism:
/// ```
/// Time →  [Current]  [Recent]  [Older]  [Oldest]
///         Window 0   Window 1  Window 2  Window 3
///            │          │         │         │
///            │          │         │         └─ Messages aged out (deleted)
///            │          │         └─ Old messages (not gossiped)
///            │          └─ Recent messages (included in gossip)
///            └─ New messages (included in gossip)
///
/// New message arrives:
///   1. Add to msgs map
///   2. Add reference to Window 0
///   3. Window 0 is always current
///
/// shift() called (periodically):
///   1. Create new empty Window 0
///   2. Shift all windows right: W0→W1, W1→W2, W2→W3
///   3. Delete oldest window (W3) and cleanup messages
/// ```
///
/// ## Message Lifecycle:
/// ```
/// put(msg) → [W0] ──shift()──→ [W1] ──shift()──→ [W2] ──shift()──→ [deleted]
///              │                 │                 │                    │
///              │                 │                 │                    │
///           fresh msg        recent msg         old msg            cleaned up
///           (gossiped)       (gossiped)       (not gossiped)      (freed memory)
/// ```
///
/// ## Gossip Selection:
/// Only messages in the first `gossip` windows are included in gossip.
/// For example, with gossip=2 and history_size=4:
/// ```
/// [W0][W1] | [W2][W3]
///  ↑────↑  |  ↑────↑
/// Gossiped | Not gossiped
/// ```
///
/// ## Memory Management:
/// - `msgs`: Owns message data (cloned on put)
/// - `peertx`: Owns peer ID strings (cloned on first access)
/// - `history`: Contains references to message IDs (not owned)
/// - All memory is properly cleaned up during shift() and deinit()
///
/// ## Thread Safety:
/// This implementation is NOT thread-safe. External synchronization required.
///
/// ## Example Usage:
/// ```zig
/// var cache = try MessageCache.init(allocator, 3, 5, null, MessageCache.defaultMsgId);
/// defer cache.deinit();
///
/// // Store message
/// try cache.put(&my_message);
///
/// // Get gossip candidates for a topic
/// const gossip_ids = try cache.getGossipIDs("my_topic");
/// defer allocator.free(gossip_ids);
///
/// // Track transmission to peer
/// const result = try cache.getForPeer(message_id, "peer_123");
///
/// // Age out old messages (call periodically)
/// cache.shift();
/// ```
pub const MessageCache = struct {
    allocator: Allocator,
    // Map message ID -> Message
    msgs: std.StringHashMap(rpc.Message),
    // Map message ID -> peer ID -> transmission count
    peertx: std.StringHashMap(PeerTransmissionMap),
    // Sliding window history
    history: std.ArrayList(?std.ArrayList(CacheEntry)),
    // Number of windows to include in gossip
    gossip: usize,

    msg_id_fn: pubsub.MessageIdFn,

    const Error = error{
        DuplicateMessage,
        MissingTopic,
        HistoryLengthExceeded,
        BothFromAndSeqNoNull,
    };

    const PeerTransmissionMap = std.AutoHashMap(PeerId, i32);

    const CacheEntry = struct {
        mid: []const u8,
        topic: []const u8,
    };

    const Self = @This();

    pub fn init(allocator: Allocator, gossip: usize, history_size: usize, msg_id_fn: pubsub.MessageIdFn) !Self {
        if (gossip > history_size) {
            return error.HistoryLengthExceeded;
        }

        var cache = Self{
            .allocator = allocator,
            .msgs = std.StringHashMap(rpc.Message).init(allocator),
            .peertx = std.StringHashMap(PeerTransmissionMap).init(allocator),
            .history = std.ArrayList(?std.ArrayList(CacheEntry)).init(allocator),
            .gossip = gossip,
            .msg_id_fn = msg_id_fn,
        };

        try cache.history.resize(history_size);

        @memset(cache.history.items, null);
        // Initialize history window 0
        cache.history.items[0] = std.ArrayList(CacheEntry).init(allocator);

        return cache;
    }

    pub fn deinit(self: *Self) void {
        // Clean up messages
        var msgs_iter = self.msgs.iterator();
        while (msgs_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*); // Free owned message ID string
            self.freeMessage(&entry.value_ptr.*);
            // self.allocator.destroy(entry.value_ptr.*); // Free message
        }
        self.msgs.deinit();

        // Clean up peer transmissions
        var peertx_iter = self.peertx.iterator();
        while (peertx_iter.next()) |entry| {
            // key is used as same as msgs, so it does not need to be freed
            var peer_map = entry.value_ptr.*;
            peer_map.deinit();
        }
        self.peertx.deinit();

        // Clean up history
        for (self.history.items) |window| {
            if (window) |*entry| {
                entry.deinit();
            }
        }
        self.history.deinit();
    }

    pub fn put(self: *Self, msg: *const rpc.Message) !void {
        const mid = try self.msg_id_fn(self.allocator, msg);
        errdefer self.allocator.free(mid);
        const gop = try self.msgs.getOrPut(mid);
        if (gop.found_existing) {
            return error.DuplicateMessage; // Already exists, do not overwrite
        }
        const cloned_msg = try self.cloneMessage(msg);
        gop.value_ptr.* = cloned_msg;
        gop.key_ptr.* = mid;

        // Add to history[0] (current window)
        const entry: CacheEntry = .{
            .mid = mid,
            .topic = cloned_msg.topic orelse return error.MissingTopic,
        };
        try self.history.items[0].?.append(entry);
    }

    pub fn putWithId(self: *Self, mid: []const u8, msg: *const rpc.Message) !void {
        const gop = try self.msgs.getOrPut(mid);
        if (gop.found_existing) {
            return error.DuplicateMessage; // Already exists, do not overwrite
        }
        const cloned_msg = try self.cloneMessage(msg);
        errdefer self.freeMessage(&cloned_msg);

        const cloned_key = try self.allocator.dupe(u8, mid);
        errdefer self.allocator.free(cloned_key);
        gop.value_ptr.* = cloned_msg;
        gop.key_ptr.* = cloned_key;
        errdefer _ = self.msgs.remove(gop.key_ptr.*);

        // Add to history[0] (current window)
        const entry: CacheEntry = .{
            .mid = mid,
            .topic = cloned_msg.topic orelse return error.MissingTopic,
        };
        try self.history.items[0].?.append(entry);
    }

    pub fn get(self: *Self, mid: []const u8) ?*rpc.Message {
        return self.msgs.getPtr(mid);
    }

    pub fn getForPeer(self: *Self, mid: []const u8, peer_id: PeerId) !?struct { msg: *rpc.Message, count: i32 } {
        const msg = self.msgs.getPtr(mid) orelse return null;

        const tx_result = try self.peertx.getOrPut(self.msgs.getKey(mid).?);
        if (!tx_result.found_existing) {
            tx_result.value_ptr.* = PeerTransmissionMap.init(self.allocator);
        }

        // Increment transmission count for this peer
        const peer_result = try tx_result.value_ptr.getOrPut(peer_id);
        if (!peer_result.found_existing) {
            peer_result.key_ptr.* = peer_id;
            peer_result.value_ptr.* = 0;
        }
        peer_result.value_ptr.* += 1;

        return .{ .msg = msg, .count = peer_result.value_ptr.* };
    }

    pub fn getGossipIDs(self: *Self, topic: []const u8) ![][]const u8 {
        var mids = std.ArrayList([]const u8).init(self.allocator);

        // Iterate through gossip windows (first 'gossip' windows)
        for (0..self.gossip) |i| {
            if (self.history.items[i]) |*window| {
                for (window.items) |entry| {
                    if (std.mem.eql(u8, entry.topic, topic)) {
                        try mids.append(entry.mid);
                    }
                }
            }
        }

        return mids.toOwnedSlice();
    }

    pub fn shift(self: *Self) void {
        const history_len = self.history.items.len;
        if (history_len == 0) return;

        if (self.history.items[history_len - 1]) |*last_window| {
            for (last_window.items) |entry| {
                if (self.msgs.fetchRemove(entry.mid)) |*kv| {
                    self.allocator.free(kv.key);
                    self.freeMessage(&kv.value);
                }

                if (self.peertx.fetchRemove(entry.mid)) |*kv| {
                    // key is used as same as msgs, so it does not need to be freed
                    var peer_map = kv.value;
                    peer_map.deinit();
                }
            }
            last_window.deinit();
        }

        // Shift history windows right
        if (history_len > 1) {
            std.mem.copyBackwards(?std.ArrayList(CacheEntry), self.history.items[1..], self.history.items[0 .. history_len - 1]);
        }

        self.history.items[0] = std.ArrayList(CacheEntry).init(self.allocator);
    }

    fn freeMessage(self: *Self, msg: *const rpc.Message) void {
        if (msg.from) |from| self.allocator.free(from);
        if (msg.seqno) |seqno| self.allocator.free(seqno);
        if (msg.topic) |topic| self.allocator.free(topic);
        if (msg.data) |data| self.allocator.free(data);
        if (msg.signature) |sig| self.allocator.free(sig);
        if (msg.key) |key| self.allocator.free(key);
    }

    fn cloneMessage(self: *Self, msg: *const rpc.Message) !rpc.Message {
        return rpc.Message{
            .from = if (msg.from) |from| try self.allocator.dupe(u8, from) else null,
            .seqno = if (msg.seqno) |seqno| try self.allocator.dupe(u8, seqno) else null,
            .topic = if (msg.topic) |topic| try self.allocator.dupe(u8, topic) else null,
            .data = if (msg.data) |data| try self.allocator.dupe(u8, data) else null,
            .signature = if (msg.signature) |sig| try self.allocator.dupe(u8, sig) else null,
            .key = if (msg.key) |key| try self.allocator.dupe(u8, key) else null,
        };
    }
};

fn createTestMessage(allocator: std.mem.Allocator, from: []const u8, seqno: []const u8, topic: []const u8, data: []const u8) !*rpc.Message {
    const msg = try allocator.create(rpc.Message);
    msg.* = rpc.Message{
        .from = try allocator.dupe(u8, from),
        .seqno = try allocator.dupe(u8, seqno),
        .topic = try allocator.dupe(u8, topic),
        .data = try allocator.dupe(u8, data),
        .signature = null,
        .key = null,
    };
    return msg;
}

fn freeTestMessage(allocator: std.mem.Allocator, msg: *rpc.Message) void {
    if (msg.from) |from| allocator.free(from);
    if (msg.seqno) |seqno| allocator.free(seqno);
    if (msg.topic) |topic| allocator.free(topic);
    if (msg.data) |data| allocator.free(data);
    if (msg.signature) |sig| allocator.free(sig);
    if (msg.key) |key| allocator.free(key);
}

fn makeTestMessage(allocator: std.mem.Allocator, n: usize) !*rpc.Message {
    var seqno_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &seqno_bytes, @intCast(n), .big);

    const data_str = try std.fmt.allocPrint(allocator, "{d}", .{n});
    defer allocator.free(data_str);

    return createTestMessage(allocator, "test", &seqno_bytes, "test", data_str);
}

test "MessageCache.init basic initialization" {
    const allocator = testing.allocator;

    var cache = try MessageCache.init(allocator, 3, // gossip
        5, // history_size
        pubsub.defaultMsgId);
    defer cache.deinit();

    try testing.expect(cache.gossip == 3);
    try testing.expect(cache.history.items.len == 5);
    try testing.expect(cache.msgs.count() == 0);
    try testing.expect(cache.peertx.count() == 0);
}

test "MessageCache.init invalid parameters" {
    const allocator = testing.allocator;

    // gossip > history_size should fail
    const result = MessageCache.init(allocator, 6, // gossip
        5, // history_size
        pubsub.defaultMsgId);

    try testing.expectError(error.HistoryLengthExceeded, result);
}

test "MessageCache.defaultMsgId" {
    const allocator = testing.allocator;

    const msg = try createTestMessage(allocator, "peer123", "seq456", "test-topic", "hello");
    defer allocator.destroy(msg);
    defer freeTestMessage(allocator, msg);

    const result = try pubsub.defaultMsgId(allocator, msg);
    defer allocator.free(result);
    try testing.expectEqualSlices(u8, "peer123seq456", result);
}

test "MessageCache.defaultMsgId invalid parameters" {
    const allocator = testing.allocator;

    // Message with both from and seqno as null
    const msg = try allocator.create(rpc.Message);
    defer allocator.destroy(msg);
    msg.* = rpc.Message{
        .from = null,
        .seqno = null,
        .topic = try allocator.dupe(u8, "test-topic"),
        .data = try allocator.dupe(u8, "hello"),
        .signature = null,
        .key = null,
    };
    defer allocator.free(msg.topic.?);
    defer allocator.free(msg.data.?);

    const result = pubsub.defaultMsgId(allocator, msg);

    try testing.expectError(error.BothFromAndSeqNoNull, result);
}

test "MessageCache.put and get" {
    const allocator = testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, pubsub.defaultMsgId);
    defer cache.deinit();

    const msg = try createTestMessage(allocator, "peer123", "seq456", "test-topic", "hello");
    defer allocator.destroy(msg);
    defer freeTestMessage(allocator, msg);
    try cache.put(msg);

    try testing.expect(cache.msgs.count() == 1);
    try testing.expect(cache.history.items[0].?.items.len == 1);

    const expected_mid = try pubsub.defaultMsgId(allocator, msg);
    defer allocator.free(expected_mid);
    const retrieved_msg = cache.get(expected_mid);
    try testing.expect(retrieved_msg != null);
    try testing.expectEqualSlices(u8, msg.data.?, retrieved_msg.?.data.?);
}

test "MessageCache.put duplicate message" {
    const allocator = testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, pubsub.defaultMsgId);
    defer cache.deinit();

    const msg1 = try createTestMessage(allocator, "peer123", "seq456", "test-topic", "hello");
    defer allocator.destroy(msg1);
    defer freeTestMessage(allocator, msg1);

    const msg2 = try createTestMessage(allocator, "peer123", "seq456", "test-topic", "world"); // Same ID
    defer allocator.destroy(msg2);
    defer freeTestMessage(allocator, msg2);
    // Put first message
    try cache.put(msg1);

    // Put second message with same ID
    const result = cache.put(msg2);
    try testing.expectError(error.DuplicateMessage, result);

    // Should still only have one message
    try testing.expect(cache.msgs.count() == 1);
    try testing.expect(cache.history.items[0].?.items.len == 1);

    const mid = try pubsub.defaultMsgId(allocator, msg1);
    defer allocator.free(mid);
    const retrieved_msg = cache.get(mid).?;
    try testing.expectEqualSlices(u8, "hello", retrieved_msg.data.?);
}

test "MessageCache.getForPeer" {
    const allocator = testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, pubsub.defaultMsgId);
    defer cache.deinit();

    const msg = try createTestMessage(allocator, "peer123", "seq456", "test-topic", "hello");
    defer allocator.destroy(msg);
    defer freeTestMessage(allocator, msg);
    try cache.put(msg);

    const mid = try pubsub.defaultMsgId(allocator, msg);
    defer allocator.free(mid);

    const peer_a = try PeerId.random();
    // First call for peer
    const result1 = try cache.getForPeer(mid, peer_a);
    try testing.expect(result1 != null);
    try testing.expect(result1.?.count == 1);

    // Second call for same peer
    const result2 = try cache.getForPeer(mid, peer_a);
    try testing.expect(result2 != null);
    try testing.expect(result2.?.count == 2);

    // First call for different peer
    const peer_b = try PeerId.random();
    const result3 = try cache.getForPeer(mid, peer_b);
    try testing.expect(result3 != null);
    try testing.expect(result3.?.count == 1);
}

test "MessageCache.getForPeer non-existent message" {
    const allocator = testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, pubsub.defaultMsgId);
    defer cache.deinit();

    const peer_a = try PeerId.random();
    const result = try cache.getForPeer("non-existent-id", peer_a);
    try testing.expect(result == null);
}

test "MessageCache.getGossipIDs" {
    const allocator = testing.allocator;

    var cache = try MessageCache.init(allocator, 2, // gossip = 2 windows
        5, pubsub.defaultMsgId);
    defer cache.deinit();

    // Add messages to different topics
    const msg1 = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "data1");
    defer allocator.destroy(msg1);
    defer freeTestMessage(allocator, msg1);
    const msg2 = try createTestMessage(allocator, "peer2", "seq2", "topic-b", "data2");
    defer allocator.destroy(msg2);
    defer freeTestMessage(allocator, msg2);
    const msg3 = try createTestMessage(allocator, "peer3", "seq3", "topic-a", "data3");
    defer allocator.destroy(msg3);
    defer freeTestMessage(allocator, msg3);

    try cache.put(msg1);
    try cache.put(msg2);
    try cache.put(msg3);

    // Get gossip IDs for topic-a
    const gossip_ids = try cache.getGossipIDs("topic-a");
    defer allocator.free(gossip_ids);

    try testing.expect(gossip_ids.len == 2); // msg1 and msg3
}

test "MessageCache.getGossipIDs empty topic" {
    const allocator = testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, pubsub.defaultMsgId);
    defer cache.deinit();

    const msg = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "data1");
    defer allocator.destroy(msg);
    defer freeTestMessage(allocator, msg);
    try cache.put(msg);

    // Get gossip IDs for non-existent topic
    const gossip_ids = try cache.getGossipIDs("topic-b");
    defer allocator.free(gossip_ids);

    try testing.expect(gossip_ids.len == 0);
}

test "MessageCache.shift" {
    const allocator = testing.allocator;

    var cache = try MessageCache.init(allocator, 2, 3, // 3 windows total
        pubsub.defaultMsgId);
    defer cache.deinit();

    const msg = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "data1");
    defer allocator.destroy(msg);
    defer freeTestMessage(allocator, msg);
    try cache.put(msg);

    try testing.expect(cache.msgs.count() == 1);
    try testing.expect(cache.history.items[0].?.items.len == 1);

    // Shift - message should move from window 0 to window 1
    cache.shift();

    // Window 0 should be empty, window 1 should have the message
    try testing.expect(cache.history.items[0].?.items.len == 0);
    try testing.expect(cache.history.items[1].?.items.len == 1);
    try testing.expect(cache.msgs.count() == 1); // Message still exists

    // Shift again - message moves to window 2
    cache.shift();
    try testing.expect(cache.history.items[0].?.items.len == 0);
    try testing.expect(cache.history.items[1].?.items.len == 0);
    try testing.expect(cache.history.items[2].?.items.len == 1);
    try testing.expect(cache.msgs.count() == 1); // Message still exists

    // Shift again - message should be deleted (falls off the end)
    cache.shift();
    try testing.expect(cache.history.items[0].?.items.len == 0);
    try testing.expect(cache.history.items[1].?.items.len == 0);
    try testing.expect(cache.history.items[2].?.items.len == 0);
    try testing.expect(cache.msgs.count() == 0); // Message deleted
}

test "MessageCache.shift empty cache" {
    const allocator = testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, pubsub.defaultMsgId);
    defer cache.deinit();

    // Shift empty cache should not crash
    cache.shift();

    try testing.expect(cache.msgs.count() == 0);
    try testing.expect(cache.history.items[0].?.items.len == 0);
}

test "MessageCache memory management" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.testing.expect(false) catch @panic("Memory leak detected!");
        }
    }
    const allocator = gpa.allocator();

    {
        var cache = try MessageCache.init(allocator, 2, 3, pubsub.defaultMsgId);
        defer cache.deinit();

        for (0..10) |i| {
            const from = try std.fmt.allocPrint(allocator, "peer{d}", .{i});
            defer allocator.free(from);
            const seqno = try std.fmt.allocPrint(allocator, "seq{d}", .{i});
            defer allocator.free(seqno);
            const topic = try std.fmt.allocPrint(allocator, "topic{d}", .{i % 3});
            defer allocator.free(topic);
            const data = try std.fmt.allocPrint(allocator, "data{d}", .{i});
            defer allocator.free(data);

            const msg = try createTestMessage(allocator, from, seqno, topic, data);
            defer allocator.destroy(msg);
            defer freeTestMessage(allocator, msg);
            try cache.put(msg);
        }

        for (0..5) |_| {
            cache.shift();
        }

        const test_msg = try createTestMessage(allocator, "test", "test", "test", "test");
        defer allocator.destroy(test_msg);
        defer freeTestMessage(allocator, test_msg);
        try cache.put(test_msg);

        const mid = try pubsub.defaultMsgId(allocator, test_msg);
        defer allocator.free(mid);

        const peer1 = try PeerId.random();
        const peer2 = try PeerId.random();
        _ = try cache.getForPeer(mid, peer1);
        _ = try cache.getForPeer(mid, peer2);
    }
}

test "MessageCache comprehensive test" {
    const allocator = testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, pubsub.defaultMsgId);
    defer cache.deinit();

    // Create 60 test messages
    var msgs: [60]*rpc.Message = undefined;
    for (0..60) |i| {
        msgs[i] = try makeTestMessage(allocator, i);
    }
    defer {
        for (msgs) |msg| {
            freeTestMessage(allocator, msg);
            allocator.destroy(msg);
        }
    }

    // Put first 10 messages
    for (0..10) |i| {
        try cache.put(msgs[i]);
    }

    // Verify first 10 messages are in cache
    for (0..10) |i| {
        const mid = try pubsub.defaultMsgId(allocator, msgs[i]);
        defer allocator.free(mid);

        const m = cache.get(mid);
        try testing.expect(m != null);
        try testing.expectEqualSlices(u8, msgs[i].data.?, m.?.data.?);
    }

    // Check gossip IDs for first 10 messages
    {
        const gids = try cache.getGossipIDs("test");
        defer allocator.free(gids);
        try testing.expect(gids.len == 10);

        for (0..10) |i| {
            const mid = try pubsub.defaultMsgId(allocator, msgs[i]);
            defer allocator.free(mid);
            try testing.expectEqualSlices(u8, mid, gids[i]);
        }
    }

    // Shift and add next 10 messages
    cache.shift();
    for (10..20) |i| {
        try cache.put(msgs[i]);
    }

    // Verify all 20 messages are in cache
    for (0..20) |i| {
        const mid = try pubsub.defaultMsgId(allocator, msgs[i]);
        defer allocator.free(mid);

        const m = cache.get(mid);
        try testing.expect(m != null);
        try testing.expectEqualSlices(u8, msgs[i].data.?, m.?.data.?);
    }

    // Check gossip IDs for 20 messages
    {
        const gids = try cache.getGossipIDs("test");
        defer allocator.free(gids);
        try testing.expect(gids.len == 20);

        // First 10 messages should be in positions 10-19 (they shifted to window 1)
        for (0..10) |i| {
            const mid = try pubsub.defaultMsgId(allocator, msgs[i]);
            defer allocator.free(mid);
            try testing.expectEqualSlices(u8, mid, gids[10 + i]);
        }

        // Next 10 messages should be in positions 0-9 (they're in window 0)
        for (10..20) |i| {
            const mid = try pubsub.defaultMsgId(allocator, msgs[i]);
            defer allocator.free(mid);
            try testing.expectEqualSlices(u8, mid, gids[i - 10]);
        }
    }

    // Add more messages with shifts
    cache.shift();
    for (20..30) |i| {
        try cache.put(msgs[i]);
    }

    cache.shift();
    for (30..40) |i| {
        try cache.put(msgs[i]);
    }

    cache.shift();
    for (40..50) |i| {
        try cache.put(msgs[i]);
    }

    cache.shift();
    for (50..60) |i| {
        try cache.put(msgs[i]);
    }

    // Should have 50 messages (last 5 windows worth)
    try testing.expect(cache.msgs.count() == 50);

    // First 10 messages should be gone
    for (0..10) |i| {
        const mid = try pubsub.defaultMsgId(allocator, msgs[i]);
        defer allocator.free(mid);

        const m = cache.get(mid);
        try testing.expect(m == null);
    }

    // Messages 10-59 should still be in cache
    for (10..60) |i| {
        const mid = try pubsub.defaultMsgId(allocator, msgs[i]);
        defer allocator.free(mid);

        const m = cache.get(mid);
        try testing.expect(m != null);
        try testing.expectEqualSlices(u8, msgs[i].data.?, m.?.data.?);
    }

    // Check final gossip IDs (should be 30 - first 3 windows)
    {
        const gids = try cache.getGossipIDs("test");
        defer allocator.free(gids);
        try testing.expect(gids.len == 30);

        // Window 0: messages 50-59
        for (0..10) |i| {
            const mid = try pubsub.defaultMsgId(allocator, msgs[50 + i]);
            defer allocator.free(mid);
            try testing.expectEqualSlices(u8, mid, gids[i]);
        }

        // Window 1: messages 40-49
        for (10..20) |i| {
            const mid = try pubsub.defaultMsgId(allocator, msgs[30 + i]);
            defer allocator.free(mid);
            try testing.expectEqualSlices(u8, mid, gids[i]);
        }

        // Window 2: messages 20-29
        for (20..30) |i| {
            const mid = try pubsub.defaultMsgId(allocator, msgs[10 + i]);
            defer allocator.free(mid);
            try testing.expectEqualSlices(u8, mid, gids[i]);
        }
    }
}
