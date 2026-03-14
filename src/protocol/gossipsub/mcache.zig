const std = @import("std");
const rpc = @import("../../proto/rpc.proto.zig");

/// Function type for computing message IDs from a Message.
pub const MessageIdFn = *const fn (allocator: std.mem.Allocator, message: *const rpc.Message) anyerror![]const u8;

/// Default message ID: concatenate `from` and `seqno` fields.
pub fn defaultMsgId(allocator: std.mem.Allocator, msg: *const rpc.Message) error{ BothFromAndSeqNoNull, OutOfMemory }![]const u8 {
    if (msg.from == null and msg.seqno == null) {
        return error.BothFromAndSeqNoNull;
    }
    return std.mem.concat(allocator, u8, &.{ msg.from orelse "", msg.seqno orelse "" });
}

/// Sliding-window message cache for GossipSub.
///
/// Messages are stored in a map for fast lookup and tracked in a sliding
/// window for age-based eviction. Per-peer transmission counts prevent
/// duplicate sends during IWANT fulfillment.
///
/// The first `gossip_window_count` windows are included in IHAVE gossip.
/// On each `shift()`, windows slide right and the oldest window is evicted.
pub const MessageCache = struct {
    allocator: std.mem.Allocator,
    msgs: std.StringHashMap(rpc.Message),
    peertx: std.StringHashMap(PeerTransmissionMap),
    history: std.ArrayList(?std.ArrayList(CacheEntry)),
    gossip_window_count: u32,
    msg_id_fn: MessageIdFn,

    /// Maps a peer identifier (arbitrary bytes) to its transmission count.
    const PeerTransmissionMap = std.StringHashMap(i32);

    const CacheEntry = struct {
        mid: []const u8,
        topic: []const u8,
    };

    pub const Error = error{
        DuplicateMessage,
        MissingTopic,
        HistoryLengthExceeded,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        gossip_window_count: u32,
        history_size: u32,
        msg_id_fn: MessageIdFn,
    ) (Error || error{OutOfMemory})!MessageCache {
        if (gossip_window_count > history_size) {
            return Error.HistoryLengthExceeded;
        }

        var history: std.ArrayList(?std.ArrayList(CacheEntry)) = .empty;
        errdefer history.deinit(allocator);

        try history.resize(allocator, history_size);
        @memset(history.items, null);
        history.items[0] = .empty;

        return MessageCache{
            .allocator = allocator,
            .msgs = std.StringHashMap(rpc.Message).init(allocator),
            .peertx = std.StringHashMap(PeerTransmissionMap).init(allocator),
            .history = history,
            .gossip_window_count = gossip_window_count,
            .msg_id_fn = msg_id_fn,
        };
    }

    pub fn deinit(self: *MessageCache) void {
        // Clean up messages (reverse of: msgs populated in put/putWithId)
        var msgs_iter = self.msgs.iterator();
        while (msgs_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            freeMessage(self.allocator, &entry.value_ptr.*);
        }
        self.msgs.deinit();

        // Clean up peer transmission maps (reverse of: peertx populated in getForPeer)
        var peertx_iter = self.peertx.iterator();
        while (peertx_iter.next()) |entry| {
            var peer_map = entry.value_ptr.*;
            var key_iter = peer_map.keyIterator();
            while (key_iter.next()) |key| {
                self.allocator.free(key.*);
            }
            peer_map.deinit();
        }
        self.peertx.deinit();

        // Clean up history windows (reverse of: history allocated in init)
        for (self.history.items) |maybe_window| {
            if (maybe_window) |w| {
                var window = w;
                window.deinit(self.allocator);
            }
        }
        self.history.deinit(self.allocator);

        self.* = undefined;
    }

    /// Store a message. Generates the message ID via `msg_id_fn`.
    /// Returns `DuplicateMessage` if the ID already exists.
    pub fn put(self: *MessageCache, msg: *const rpc.Message) !void {
        const mid = try self.msg_id_fn(self.allocator, msg);

        const gop = self.msgs.getOrPut(mid) catch {
            self.allocator.free(mid);
            return error.OutOfMemory;
        };
        if (gop.found_existing) {
            self.allocator.free(mid);
            return Error.DuplicateMessage;
        }

        const cloned_msg = cloneMessage(self.allocator, msg) catch {
            _ = self.msgs.fetchRemove(mid);
            self.allocator.free(mid);
            return error.OutOfMemory;
        };

        gop.value_ptr.* = cloned_msg;
        gop.key_ptr.* = mid;

        const topic = cloned_msg.topic orelse {
            _ = self.msgs.fetchRemove(mid);
            freeMessage(self.allocator, &cloned_msg);
            self.allocator.free(mid);
            return Error.MissingTopic;
        };

        self.history.items[0].?.append(self.allocator, .{
            .mid = mid,
            .topic = topic,
        }) catch {
            _ = self.msgs.fetchRemove(mid);
            freeMessage(self.allocator, &cloned_msg);
            self.allocator.free(mid);
            return error.OutOfMemory;
        };
    }

    /// Store a message with a pre-computed ID.
    pub fn putWithId(self: *MessageCache, mid: []const u8, msg: *const rpc.Message) !void {
        const gop = try self.msgs.getOrPut(mid);
        if (gop.found_existing) {
            return Error.DuplicateMessage;
        }

        const cloned_msg = cloneMessage(self.allocator, msg) catch {
            _ = self.msgs.fetchRemove(mid);
            return error.OutOfMemory;
        };

        const cloned_key = self.allocator.dupe(u8, mid) catch {
            _ = self.msgs.fetchRemove(mid);
            freeMessage(self.allocator, &cloned_msg);
            return error.OutOfMemory;
        };

        gop.value_ptr.* = cloned_msg;
        gop.key_ptr.* = cloned_key;

        const topic = cloned_msg.topic orelse {
            _ = self.msgs.fetchRemove(cloned_key);
            freeMessage(self.allocator, &cloned_msg);
            self.allocator.free(cloned_key);
            return Error.MissingTopic;
        };

        self.history.items[0].?.append(self.allocator, .{
            .mid = cloned_key,
            .topic = topic,
        }) catch {
            _ = self.msgs.fetchRemove(cloned_key);
            freeMessage(self.allocator, &cloned_msg);
            self.allocator.free(cloned_key);
            return error.OutOfMemory;
        };
    }

    /// Look up a message by ID.
    pub fn get(self: *MessageCache, mid: []const u8) ?*rpc.Message {
        return self.msgs.getPtr(mid);
    }

    /// Look up a message and track per-peer transmission count.
    /// Returns null if the message is not found.
    pub fn getForPeer(self: *MessageCache, mid: []const u8, peer_id: []const u8) !?struct { msg: *rpc.Message, count: i32 } {
        const msg = self.msgs.getPtr(mid) orelse return null;

        const tx_result = try self.peertx.getOrPut(self.msgs.getKey(mid).?);
        if (!tx_result.found_existing) {
            tx_result.value_ptr.* = PeerTransmissionMap.init(self.allocator);
        }

        const peer_result = try tx_result.value_ptr.getOrPut(peer_id);
        if (!peer_result.found_existing) {
            peer_result.key_ptr.* = try self.allocator.dupe(u8, peer_id);
            peer_result.value_ptr.* = 0;
        }
        peer_result.value_ptr.* += 1;

        return .{ .msg = msg, .count = peer_result.value_ptr.* };
    }

    /// Return message IDs from the gossip windows for a given topic.
    /// Caller owns the returned slice.
    pub fn getGossipIDs(self: *MessageCache, topic: []const u8) ![][]const u8 {
        var mids: std.ArrayList([]const u8) = .empty;
        errdefer mids.deinit(self.allocator);

        for (0..self.gossip_window_count) |i| {
            if (self.history.items[i]) |*window| {
                for (window.items) |entry| {
                    if (std.mem.eql(u8, entry.topic, topic)) {
                        try mids.append(self.allocator, entry.mid);
                    }
                }
            }
        }

        return mids.toOwnedSlice(self.allocator);
    }

    /// Slide the window: evict the oldest window and create a new current one.
    pub fn shift(self: *MessageCache) void {
        const history_len = self.history.items.len;
        if (history_len == 0) return;

        if (self.history.items[history_len - 1]) |*last_window| {
            for (last_window.items) |entry| {
                if (self.msgs.fetchRemove(entry.mid)) |*kv| {
                    self.allocator.free(kv.key);
                    freeMessage(self.allocator, &kv.value);
                }

                if (self.peertx.fetchRemove(entry.mid)) |*kv| {
                    var peer_map = kv.value;
                    var key_iter = peer_map.keyIterator();
                    while (key_iter.next()) |key| {
                        self.allocator.free(key.*);
                    }
                    peer_map.deinit();
                }
            }
            last_window.deinit(self.allocator);
        }

        if (history_len > 1) {
            std.mem.copyBackwards(
                ?std.ArrayList(CacheEntry),
                self.history.items[1..],
                self.history.items[0 .. history_len - 1],
            );
        }

        self.history.items[0] = .empty;
    }
};

fn freeMessage(allocator: std.mem.Allocator, msg: *const rpc.Message) void {
    if (msg.from) |from| allocator.free(from);
    if (msg.seqno) |seqno| allocator.free(seqno);
    if (msg.topic) |topic| allocator.free(topic);
    if (msg.data) |data| allocator.free(data);
    if (msg.signature) |sig| allocator.free(sig);
    if (msg.key) |key| allocator.free(key);
}

fn cloneMessage(allocator: std.mem.Allocator, msg: *const rpc.Message) error{OutOfMemory}!rpc.Message {
    const from = if (msg.from) |f| try allocator.dupe(u8, f) else null;
    errdefer if (from) |f| allocator.free(f);

    const seqno = if (msg.seqno) |s| try allocator.dupe(u8, s) else null;
    errdefer if (seqno) |s| allocator.free(s);

    const topic = if (msg.topic) |t| try allocator.dupe(u8, t) else null;
    errdefer if (topic) |t| allocator.free(t);

    const data = if (msg.data) |d| try allocator.dupe(u8, d) else null;
    errdefer if (data) |d| allocator.free(d);

    const signature = if (msg.signature) |s| try allocator.dupe(u8, s) else null;
    errdefer if (signature) |s| allocator.free(s);

    const key = if (msg.key) |k| try allocator.dupe(u8, k) else null;

    return rpc.Message{
        .from = from,
        .seqno = seqno,
        .topic = topic,
        .data = data,
        .signature = signature,
        .key = key,
    };
}

// --- Tests ---

fn createTestMessage(
    allocator: std.mem.Allocator,
    from: []const u8,
    seqno: []const u8,
    topic: []const u8,
    data: []const u8,
) error{OutOfMemory}!rpc.Message {
    const from_d = try allocator.dupe(u8, from);
    errdefer allocator.free(from_d);

    const seqno_d = try allocator.dupe(u8, seqno);
    errdefer allocator.free(seqno_d);

    const topic_d = try allocator.dupe(u8, topic);
    errdefer allocator.free(topic_d);

    const data_d = try allocator.dupe(u8, data);

    return rpc.Message{
        .from = from_d,
        .seqno = seqno_d,
        .topic = topic_d,
        .data = data_d,
        .signature = null,
        .key = null,
    };
}

fn freeTestMessage(allocator: std.mem.Allocator, msg: *const rpc.Message) void {
    freeMessage(allocator, msg);
}

fn makeTestMessage(allocator: std.mem.Allocator, n: usize) !rpc.Message {
    var seqno_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &seqno_bytes, @intCast(n), .big);

    const data_str = try std.fmt.allocPrint(allocator, "{d}", .{n});
    defer allocator.free(data_str);

    return createTestMessage(allocator, "test", &seqno_bytes, "test", data_str);
}

test "MessageCache init basic" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, defaultMsgId);
    defer cache.deinit();

    try std.testing.expectEqual(@as(u32, 3), cache.gossip_window_count);
    try std.testing.expectEqual(@as(usize, 5), cache.history.items.len);
    try std.testing.expectEqual(@as(u32, 0), cache.msgs.count());
}

test "MessageCache init rejects gossip > history" {
    const allocator = std.testing.allocator;
    const result = MessageCache.init(allocator, 6, 5, defaultMsgId);
    try std.testing.expectError(MessageCache.Error.HistoryLengthExceeded, result);
}

test "defaultMsgId concatenates from and seqno" {
    const allocator = std.testing.allocator;

    var msg = rpc.Message{
        .from = "peer123",
        .seqno = "seq456",
        .topic = "test",
        .data = null,
        .signature = null,
        .key = null,
    };

    const result = try defaultMsgId(allocator, &msg);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("peer123seq456", result);
}

test "defaultMsgId rejects null from and seqno" {
    const allocator = std.testing.allocator;

    var msg = rpc.Message{
        .from = null,
        .seqno = null,
        .topic = "test",
        .data = null,
        .signature = null,
        .key = null,
    };

    const result = defaultMsgId(allocator, &msg);
    try std.testing.expectError(error.BothFromAndSeqNoNull, result);
}

test "MessageCache put and get" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, defaultMsgId);
    defer cache.deinit();

    var msg = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "hello");
    defer freeTestMessage(allocator, &msg);

    try cache.put(&msg);
    try std.testing.expectEqual(@as(u32, 1), cache.msgs.count());

    const mid = try defaultMsgId(allocator, &msg);
    defer allocator.free(mid);
    const retrieved = cache.get(mid);
    try std.testing.expect(retrieved != null);
    try std.testing.expectEqualStrings("hello", retrieved.?.data.?);
}

test "MessageCache rejects duplicate" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, defaultMsgId);
    defer cache.deinit();

    var msg1 = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "hello");
    defer freeTestMessage(allocator, &msg1);
    var msg2 = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "world");
    defer freeTestMessage(allocator, &msg2);

    try cache.put(&msg1);
    const result = cache.put(&msg2);
    try std.testing.expectError(MessageCache.Error.DuplicateMessage, result);
    try std.testing.expectEqual(@as(u32, 1), cache.msgs.count());
}

test "MessageCache getForPeer tracks transmission count" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, defaultMsgId);
    defer cache.deinit();

    var msg = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "hello");
    defer freeTestMessage(allocator, &msg);
    try cache.put(&msg);

    const mid = try defaultMsgId(allocator, &msg);
    defer allocator.free(mid);

    const r1 = try cache.getForPeer(mid, "peerA");
    try std.testing.expect(r1 != null);
    try std.testing.expectEqual(@as(i32, 1), r1.?.count);

    const r2 = try cache.getForPeer(mid, "peerA");
    try std.testing.expectEqual(@as(i32, 2), r2.?.count);

    const r3 = try cache.getForPeer(mid, "peerB");
    try std.testing.expectEqual(@as(i32, 1), r3.?.count);
}

test "MessageCache getForPeer returns null for missing message" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, defaultMsgId);
    defer cache.deinit();

    const result = try cache.getForPeer("nonexistent", "peerA");
    try std.testing.expect(result == null);
}

test "MessageCache getGossipIDs filters by topic" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 2, 5, defaultMsgId);
    defer cache.deinit();

    var msg1 = try createTestMessage(allocator, "p1", "s1", "topic-a", "d1");
    defer freeTestMessage(allocator, &msg1);
    var msg2 = try createTestMessage(allocator, "p2", "s2", "topic-b", "d2");
    defer freeTestMessage(allocator, &msg2);
    var msg3 = try createTestMessage(allocator, "p3", "s3", "topic-a", "d3");
    defer freeTestMessage(allocator, &msg3);

    try cache.put(&msg1);
    try cache.put(&msg2);
    try cache.put(&msg3);

    const ids = try cache.getGossipIDs("topic-a");
    defer allocator.free(ids);
    try std.testing.expectEqual(@as(usize, 2), ids.len);
}

test "MessageCache shift evicts oldest window" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 2, 3, defaultMsgId);
    defer cache.deinit();

    var msg = try createTestMessage(allocator, "p1", "s1", "topic-a", "d1");
    defer freeTestMessage(allocator, &msg);
    try cache.put(&msg);

    try std.testing.expectEqual(@as(u32, 1), cache.msgs.count());

    cache.shift();
    try std.testing.expectEqual(@as(u32, 1), cache.msgs.count());

    cache.shift();
    try std.testing.expectEqual(@as(u32, 1), cache.msgs.count());

    cache.shift();
    try std.testing.expectEqual(@as(u32, 0), cache.msgs.count());
}

test "MessageCache shift empty cache" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, defaultMsgId);
    defer cache.deinit();

    cache.shift();
    try std.testing.expectEqual(@as(u32, 0), cache.msgs.count());
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
        var cache = try MessageCache.init(allocator, 2, 3, defaultMsgId);
        defer cache.deinit();

        for (0..10) |i| {
            var msg = try makeTestMessage(allocator, i);
            defer freeTestMessage(allocator, &msg);
            try cache.put(&msg);
        }

        for (0..5) |_| {
            cache.shift();
        }

        var test_msg = try createTestMessage(allocator, "test", "test", "test", "test");
        defer freeTestMessage(allocator, &test_msg);
        try cache.put(&test_msg);

        const mid = try defaultMsgId(allocator, &test_msg);
        defer allocator.free(mid);

        _ = try cache.getForPeer(mid, "peer1");
        _ = try cache.getForPeer(mid, "peer2");
    }
}
