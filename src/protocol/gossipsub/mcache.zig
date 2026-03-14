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

/// A message stored in the cache with a single contiguous backing buffer.
/// All slices in `message` point into `backing`. Freeing `backing` releases
/// all field data in one allocation.
const StoredMessage = struct {
    message: rpc.Message,
    backing: []u8,
};

/// Sliding-window message cache for GossipSub.
///
/// Messages are stored in a map for fast lookup and tracked in a sliding
/// window for age-based eviction. Per-peer transmission counts prevent
/// duplicate sends during IWANT fulfillment.
///
/// The first `gossip_window_count` windows are included in IHAVE gossip.
/// On each `shift()`, windows slide right and the oldest window is evicted.
///
/// ## Allocation strategy
///
/// Each stored message uses a single contiguous backing buffer for all field
/// data (from, seqno, topic, data, signature, key). This reduces allocations
/// per `put()` from 7 to 2 (message ID + backing buffer) and makes `shift()`
/// eviction cheaper with only 2 frees per message.
pub const MessageCache = struct {
    allocator: std.mem.Allocator,
    msgs: std.StringHashMap(StoredMessage),
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
            .msgs = std.StringHashMap(StoredMessage).init(allocator),
            .peertx = std.StringHashMap(PeerTransmissionMap).init(allocator),
            .history = history,
            .gossip_window_count = gossip_window_count,
            .msg_id_fn = msg_id_fn,
        };
    }

    pub fn deinit(self: *MessageCache) void {
        var msgs_iter = self.msgs.iterator();
        while (msgs_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.backing);
        }
        self.msgs.deinit();

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

        const stored = cloneMessage(self.allocator, msg) catch {
            _ = self.msgs.fetchRemove(mid);
            self.allocator.free(mid);
            return error.OutOfMemory;
        };

        gop.value_ptr.* = stored;
        gop.key_ptr.* = mid;

        const topic = stored.message.topic orelse {
            _ = self.msgs.fetchRemove(mid);
            self.allocator.free(stored.backing);
            self.allocator.free(mid);
            return Error.MissingTopic;
        };

        self.history.items[0].?.append(self.allocator, .{
            .mid = mid,
            .topic = topic,
        }) catch {
            _ = self.msgs.fetchRemove(mid);
            self.allocator.free(stored.backing);
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

        const stored = cloneMessage(self.allocator, msg) catch {
            _ = self.msgs.fetchRemove(mid);
            return error.OutOfMemory;
        };

        const cloned_key = self.allocator.dupe(u8, mid) catch {
            _ = self.msgs.fetchRemove(mid);
            self.allocator.free(stored.backing);
            return error.OutOfMemory;
        };

        gop.value_ptr.* = stored;
        gop.key_ptr.* = cloned_key;

        const topic = stored.message.topic orelse {
            _ = self.msgs.fetchRemove(cloned_key);
            self.allocator.free(stored.backing);
            self.allocator.free(cloned_key);
            return Error.MissingTopic;
        };

        self.history.items[0].?.append(self.allocator, .{
            .mid = cloned_key,
            .topic = topic,
        }) catch {
            _ = self.msgs.fetchRemove(cloned_key);
            self.allocator.free(stored.backing);
            self.allocator.free(cloned_key);
            return error.OutOfMemory;
        };
    }

    /// Look up a message by ID.
    pub fn get(self: *MessageCache, mid: []const u8) ?*rpc.Message {
        const stored = self.msgs.getPtr(mid) orelse return null;
        return &stored.message;
    }

    /// Look up a message and track per-peer transmission count.
    /// Returns null if the message is not found.
    pub fn getForPeer(self: *MessageCache, mid: []const u8, peer_id: []const u8) !?struct { msg: *rpc.Message, count: i32 } {
        const stored = self.msgs.getPtr(mid) orelse return null;

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

        return .{ .msg = &stored.message, .count = peer_result.value_ptr.* };
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
                    self.allocator.free(kv.value.backing);
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

/// Clone a message into a single contiguous backing buffer.
/// Returns a StoredMessage where all slice fields point into the backing buffer.
/// Only 1 allocation for all field data.
fn cloneMessage(allocator: std.mem.Allocator, msg: *const rpc.Message) error{OutOfMemory}!StoredMessage {
    // Calculate total size needed
    var total_len: usize = 0;
    if (msg.from) |f| total_len += f.len;
    if (msg.seqno) |s| total_len += s.len;
    if (msg.topic) |t| total_len += t.len;
    if (msg.data) |d| total_len += d.len;
    if (msg.signature) |s| total_len += s.len;
    if (msg.key) |k| total_len += k.len;

    const backing = try allocator.alloc(u8, total_len);
    var offset: usize = 0;

    const from = if (msg.from) |f| copyField(backing, &offset, f) else null;
    const seqno = if (msg.seqno) |s| copyField(backing, &offset, s) else null;
    const topic = if (msg.topic) |t| copyField(backing, &offset, t) else null;
    const data = if (msg.data) |d| copyField(backing, &offset, d) else null;
    const signature = if (msg.signature) |s| copyField(backing, &offset, s) else null;
    const key = if (msg.key) |k| copyField(backing, &offset, k) else null;

    std.debug.assert(offset == total_len);

    return .{
        .message = .{
            .from = from,
            .seqno = seqno,
            .topic = topic,
            .data = data,
            .signature = signature,
            .key = key,
        },
        .backing = backing,
    };
}

/// Copy a field into the backing buffer at the current offset, returning a slice into it.
inline fn copyField(backing: []u8, offset: *usize, source: []const u8) []const u8 {
    const start = offset.*;
    @memcpy(backing[start..][0..source.len], source);
    offset.* = start + source.len;
    return backing[start..][0..source.len];
}

// --- Tests ---

fn createTestMessage(
    allocator: std.mem.Allocator,
    from: []const u8,
    seqno: []const u8,
    topic: []const u8,
    data: []const u8,
) error{OutOfMemory}!StoredMessage {
    return cloneMessage(allocator, &.{
        .from = from,
        .seqno = seqno,
        .topic = topic,
        .data = data,
        .signature = null,
        .key = null,
    });
}

fn freeTestMessage(allocator: std.mem.Allocator, stored: *const StoredMessage) void {
    allocator.free(stored.backing);
}

fn makeTestMessage(allocator: std.mem.Allocator, n: usize) !StoredMessage {
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

    var stored = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "hello");
    defer freeTestMessage(allocator, &stored);

    try cache.put(&stored.message);
    try std.testing.expectEqual(@as(u32, 1), cache.msgs.count());

    const mid = try defaultMsgId(allocator, &stored.message);
    defer allocator.free(mid);
    const retrieved = cache.get(mid);
    try std.testing.expect(retrieved != null);
    try std.testing.expectEqualStrings("hello", retrieved.?.data.?);
}

test "MessageCache rejects duplicate" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, defaultMsgId);
    defer cache.deinit();

    var s1 = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "hello");
    defer freeTestMessage(allocator, &s1);
    var s2 = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "world");
    defer freeTestMessage(allocator, &s2);

    try cache.put(&s1.message);
    const result = cache.put(&s2.message);
    try std.testing.expectError(MessageCache.Error.DuplicateMessage, result);
    try std.testing.expectEqual(@as(u32, 1), cache.msgs.count());
}

test "MessageCache getForPeer tracks transmission count" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 3, 5, defaultMsgId);
    defer cache.deinit();

    var stored = try createTestMessage(allocator, "peer1", "seq1", "topic-a", "hello");
    defer freeTestMessage(allocator, &stored);
    try cache.put(&stored.message);

    const mid = try defaultMsgId(allocator, &stored.message);
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

    var s1 = try createTestMessage(allocator, "p1", "s1", "topic-a", "d1");
    defer freeTestMessage(allocator, &s1);
    var s2 = try createTestMessage(allocator, "p2", "s2", "topic-b", "d2");
    defer freeTestMessage(allocator, &s2);
    var s3 = try createTestMessage(allocator, "p3", "s3", "topic-a", "d3");
    defer freeTestMessage(allocator, &s3);

    try cache.put(&s1.message);
    try cache.put(&s2.message);
    try cache.put(&s3.message);

    const ids = try cache.getGossipIDs("topic-a");
    defer allocator.free(ids);
    try std.testing.expectEqual(@as(usize, 2), ids.len);
}

test "MessageCache shift evicts oldest window" {
    const allocator = std.testing.allocator;

    var cache = try MessageCache.init(allocator, 2, 3, defaultMsgId);
    defer cache.deinit();

    var stored = try createTestMessage(allocator, "p1", "s1", "topic-a", "d1");
    defer freeTestMessage(allocator, &stored);
    try cache.put(&stored.message);

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
            var stored = try makeTestMessage(allocator, i);
            defer freeTestMessage(allocator, &stored);
            try cache.put(&stored.message);
        }

        for (0..5) |_| {
            cache.shift();
        }

        var test_stored = try createTestMessage(allocator, "test", "test", "test", "test");
        defer freeTestMessage(allocator, &test_stored);
        try cache.put(&test_stored.message);

        const mid = try defaultMsgId(allocator, &test_stored.message);
        defer allocator.free(mid);

        _ = try cache.getForPeer(mid, "peer1");
        _ = try cache.getForPeer(mid, "peer2");
    }
}

test "cloneMessage uses single backing buffer" {
    const allocator = std.testing.allocator;

    const stored = try cloneMessage(allocator, &.{
        .from = "alice",
        .seqno = "0001",
        .topic = "chat",
        .data = "hello world",
        .signature = "sig",
        .key = "pubkey",
    });
    defer allocator.free(stored.backing);

    // All fields should point into the single backing buffer
    const backing_start = @intFromPtr(stored.backing.ptr);
    const backing_end = backing_start + stored.backing.len;

    inline for (.{ stored.message.from, stored.message.seqno, stored.message.topic, stored.message.data, stored.message.signature, stored.message.key }) |maybe_field| {
        if (maybe_field) |field| {
            const field_start = @intFromPtr(field.ptr);
            try std.testing.expect(field_start >= backing_start);
            try std.testing.expect(field_start + field.len <= backing_end);
        }
    }

    // Verify content
    try std.testing.expectEqualStrings("alice", stored.message.from.?);
    try std.testing.expectEqualStrings("0001", stored.message.seqno.?);
    try std.testing.expectEqualStrings("chat", stored.message.topic.?);
    try std.testing.expectEqualStrings("hello world", stored.message.data.?);
    try std.testing.expectEqualStrings("sig", stored.message.signature.?);
    try std.testing.expectEqualStrings("pubkey", stored.message.key.?);

    // Total backing = 5 + 4 + 4 + 11 + 3 + 6 = 33 bytes, 1 allocation
    try std.testing.expectEqual(@as(usize, 33), stored.backing.len);
}
