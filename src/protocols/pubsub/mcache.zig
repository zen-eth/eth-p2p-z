//! The gossipsub message cache: a windowed store of recently-published and
//! -forwarded messages, keyed by message-id, so the router can answer an IWANT
//! (hand back the full message) and advertise recent ids via IHAVE.
//!
//! Structure (the go-libp2p MessageCache model): a ring of `history_length`
//! windows, newest at the front. Each `put` lands in the newest window. Each
//! heartbeat `shift` drops the oldest window — releasing every message it held —
//! and opens a fresh empty newest window, so a message lives for about
//! `history_length` heartbeats. The newest `gossip_length` windows are the
//! "gossipable" ones whose ids `getGossipIDs` returns for IHAVE.
//!
//! Each stored message reuses the refcounted `OutboundFrame` that the router
//! already framed once to forward it (a single-message publish RPC frame): the
//! cache holds one extra reference, so answering an IWANT is just retain + push —
//! no second copy of the (up-to-1 MiB) payload. An `id → frame` index gives O(1)
//! `get`. A message-id is stored at most once (a duplicate `put` is a no-op), so
//! it lives in exactly one window and eviction removes it cleanly. (The index
//! maps id → frame rather than id → entry on purpose: the per-window entry lists
//! reallocate as they grow, so a pointer into them would dangle — the frame
//! pointer is stable, and eviction looks an entry's id up by value, not by
//! pointer.)

const std = @import("std");
const peer_io = @import("peer_io.zig");

/// Number of heartbeat windows retained. A message stays cached for about this
/// many heartbeats before the sliding window evicts it. (go-libp2p default.)
const history_length = 5;

/// Number of newest windows whose message-ids are advertised via IHAVE. Must be
/// <= history_length. (go-libp2p default.)
const gossip_length = 3;

comptime {
    std.debug.assert(gossip_length <= history_length);
}

/// The window count, for tests that drive `history_length` heartbeat shifts and
/// want to track this module's source of truth rather than hard-code a number.
pub fn historyLengthForTest() u64 {
    return history_length;
}

/// One cached message. Owns its `id` and `topic` byte copies and holds exactly
/// one reference on `frame`; the entry's eviction (or the cache's deinit) frees
/// the two copies and releases the frame exactly once.
const Entry = struct {
    id: []u8,
    topic: []u8,
    frame: *peer_io.OutboundFrame,
};

pub const MessageCache = struct {
    allocator: std.mem.Allocator,
    /// Ring of windows, newest first: `windows[0]` is the current window every
    /// `put` lands in; `windows[history_length - 1]` is the oldest, dropped by
    /// the next `shift`.
    windows: [history_length]std.ArrayListUnmanaged(Entry),
    /// id → the stored frame, for O(1) `get` and duplicate detection. The keys
    /// alias the owning entry's `id` slice (freed once, via the entry, on
    /// evict/deinit), so the map never owns its keys. Frame pointers are stable,
    /// unlike pointers into the resizable per-window entry lists.
    index: std.StringHashMapUnmanaged(*peer_io.OutboundFrame),

    pub fn init(allocator: std.mem.Allocator) MessageCache {
        return .{
            .allocator = allocator,
            .windows = [_]std.ArrayListUnmanaged(Entry){.empty} ** history_length,
            .index = .empty,
        };
    }

    /// Release every entry's frame, free every id/topic copy, and free the
    /// window/index storage.
    pub fn deinit(self: *MessageCache) void {
        for (&self.windows) |*window| {
            for (window.items) |*entry| self.freeEntry(entry);
            window.deinit(self.allocator);
        }
        self.index.deinit(self.allocator);
        self.* = undefined;
    }

    /// Insert a message into the current (newest) window, taking copies of `id`
    /// and `topic` and retaining `frame` (the cache then holds one reference).
    /// Duplicate `id` is a no-op: the message is NOT stored or retained twice, so
    /// it stays in exactly one window. On allocation failure the message is
    /// simply not cached (and `frame` is not retained), which only costs the
    /// ability to later serve it via IWANT — safe, and the alternative (failing
    /// the forward) is worse.
    pub fn put(self: *MessageCache, id: []const u8, topic: []const u8, frame: *peer_io.OutboundFrame) !void {
        if (self.index.contains(id)) return;

        const id_copy = try self.allocator.dupe(u8, id);
        errdefer self.allocator.free(id_copy);
        const topic_copy = try self.allocator.dupe(u8, topic);
        errdefer self.allocator.free(topic_copy);

        // Index the id BEFORE appending: the index key aliases `id_copy`, and the
        // entry owns it, so on an index-insert failure we undo via the errdefers
        // (freeing the copies) without ever having an orphaned window entry. The
        // value is the stable frame pointer (never a pointer into the resizable
        // window list).
        try self.index.put(self.allocator, id_copy, frame);
        errdefer _ = self.index.remove(id_copy);

        const window = &self.windows[0];
        try window.append(self.allocator, .{ .id = id_copy, .topic = topic_copy, .frame = frame });

        // All bookkeeping committed: the cache now owns the copies and takes its
        // reference on the frame.
        frame.retain();
    }

    /// The stored frame for `id`, or null if absent. NOT retained — a caller that
    /// keeps it (e.g. to push onto a peer queue for an IWANT reply) must `retain`
    /// it first.
    pub fn get(self: *MessageCache, id: []const u8) ?*peer_io.OutboundFrame {
        return self.index.get(id);
    }

    /// The message-ids in the newest `gossip_length` windows whose entry topic
    /// matches `topic`. The returned slice is allocated with `allocator` and owned
    /// by the caller; each id within it is BORROWED from the cache and stays valid
    /// only until a `shift` evicts the window holding it.
    pub fn getGossipIDs(self: *MessageCache, allocator: std.mem.Allocator, topic: []const u8) ![][]const u8 {
        var ids: std.ArrayListUnmanaged([]const u8) = .empty;
        errdefer ids.deinit(allocator);
        const gossipable = @min(gossip_length, history_length);
        for (self.windows[0..gossipable]) |window| {
            for (window.items) |entry| {
                if (std.mem.eql(u8, entry.topic, topic)) {
                    try ids.append(allocator, entry.id);
                }
            }
        }
        return ids.toOwnedSlice(allocator);
    }

    /// Slide the window by one heartbeat: drop the oldest window (releasing each
    /// of its entries' frames, freeing the id/topic copies, and removing the ids
    /// from the index), shift the rest one slot toward older, and open a fresh
    /// empty newest window. Because an id lives in exactly one window, eviction
    /// removes it from the index cleanly.
    pub fn shift(self: *MessageCache) void {
        // The oldest window is evicted. Release/free its entries and drop them
        // from the index, then reuse its (emptied) storage as the new newest
        // window so no allocation is needed to re-open one.
        var oldest = self.windows[history_length - 1];
        for (oldest.items) |*entry| {
            _ = self.index.remove(entry.id);
            self.freeEntry(entry);
        }
        oldest.clearRetainingCapacity();

        // Rotate the ring: every window ages by one slot.
        var i: usize = history_length - 1;
        while (i > 0) : (i -= 1) self.windows[i] = self.windows[i - 1];
        self.windows[0] = oldest;
    }

    /// Release an entry's frame reference and free its owned id/topic copies.
    /// Does NOT touch the index (callers remove the index key first when needed).
    fn freeEntry(self: *MessageCache, entry: *Entry) void {
        entry.frame.release();
        self.allocator.free(entry.id);
        self.allocator.free(entry.topic);
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Build a single-byte publish-shaped frame with one reference, owned by the
/// caller. The cache `put` retains it; the caller releases its own reference
/// afterward, leaving the cache holding exactly one (so testing.allocator
/// catches a missed release as a leak and a double-release as a double-free).
fn testFrame(allocator: std.mem.Allocator, byte: u8) !*peer_io.OutboundFrame {
    const bytes = try allocator.alloc(u8, 1);
    errdefer allocator.free(bytes);
    bytes[0] = byte;
    const ids = try allocator.alloc([]u8, 0);
    errdefer allocator.free(ids);
    return peer_io.OutboundFrame.create(allocator, bytes, ids, 1);
}

test "put/get round-trip and miss returns null" {
    const allocator = std.testing.allocator;
    var cache = MessageCache.init(allocator);
    defer cache.deinit();

    const frame = try testFrame(allocator, 0x42);
    try cache.put("id-1", "topic-a", frame);
    frame.release(); // the cache retained; drop our builder reference.

    const got = cache.get("id-1") orelse return error.MissingEntry;
    try std.testing.expectEqual(@as(u8, 0x42), got.bytes[0]);
    try std.testing.expect(cache.get("nope") == null);
}

test "put dedups a repeated id: stored once, retained once" {
    const allocator = std.testing.allocator;
    var cache = MessageCache.init(allocator);
    defer cache.deinit();

    const frame = try testFrame(allocator, 0x01);
    // Put the SAME id twice. The second is a no-op: no second store, no second
    // retain. After dropping our builder reference the cache holds exactly one,
    // so the eventual deinit frees the frame once (a double-retain would leak;
    // testing.allocator would flag it).
    try cache.put("dup", "t", frame);
    try cache.put("dup", "t", frame);
    frame.release();

    // Refcount is back to 1 (the cache's single reference).
    try std.testing.expectEqual(@as(usize, 1), frame.refs.load(.monotonic));
    try std.testing.expect(cache.get("dup") != null);
}

test "getGossipIDs returns only the requested topic's ids from the gossipable windows" {
    const allocator = std.testing.allocator;
    var cache = MessageCache.init(allocator);
    defer cache.deinit();

    // Window 0: a1 (topic a), b1 (topic b).
    const fa1 = try testFrame(allocator, 1);
    try cache.put("a1", "a", fa1);
    fa1.release();
    const fb1 = try testFrame(allocator, 2);
    try cache.put("b1", "b", fb1);
    fb1.release();

    // Shift to a new window, then add a2 (topic a) there.
    cache.shift();
    const fa2 = try testFrame(allocator, 3);
    try cache.put("a2", "a", fa2);
    fa2.release();

    const ids_a = try cache.getGossipIDs(allocator, "a");
    defer allocator.free(ids_a);
    // Both topic-"a" ids, no topic-"b" id.
    try std.testing.expectEqual(@as(usize, 2), ids_a.len);
    var saw_a1 = false;
    var saw_a2 = false;
    for (ids_a) |id| {
        if (std.mem.eql(u8, id, "a1")) saw_a1 = true;
        if (std.mem.eql(u8, id, "a2")) saw_a2 = true;
        try std.testing.expect(!std.mem.eql(u8, id, "b1"));
    }
    try std.testing.expect(saw_a1 and saw_a2);

    const ids_b = try cache.getGossipIDs(allocator, "b");
    defer allocator.free(ids_b);
    try std.testing.expectEqual(@as(usize, 1), ids_b.len);
    try std.testing.expectEqualSlices(u8, "b1", ids_b[0]);
}

test "getGossipIDs excludes windows beyond the gossipable horizon" {
    const allocator = std.testing.allocator;
    var cache = MessageCache.init(allocator);
    defer cache.deinit();

    // Put one id, then shift it back to the (gossip_length)-th window — one past
    // the gossipable horizon — without yet evicting it (it survives until
    // history_length shifts). It must still be retrievable via get but absent
    // from getGossipIDs.
    const frame = try testFrame(allocator, 7);
    try cache.put("old", "t", frame);
    frame.release();

    var i: usize = 0;
    while (i < gossip_length) : (i += 1) cache.shift();

    // Still cached (history_length > gossip_length so it is not evicted yet).
    try std.testing.expect(cache.get("old") != null);
    // But no longer gossipable.
    const ids = try cache.getGossipIDs(allocator, "t");
    defer allocator.free(ids);
    try std.testing.expectEqual(@as(usize, 0), ids.len);
}

test "shift evicts after history_length windows: frame released, id/topic freed" {
    const allocator = std.testing.allocator;
    var cache = MessageCache.init(allocator);
    defer cache.deinit();

    const frame = try testFrame(allocator, 9);
    try cache.put("ev", "t", frame);
    // Keep a builder reference of our own so we can observe the cache's release
    // on eviction without freeing the frame from under the assertion.
    frame.retain();
    frame.release(); // drop the original builder reference; cache + this test hold it now.
    try std.testing.expectEqual(@as(usize, 2), frame.refs.load(.monotonic));

    // Present before eviction.
    try std.testing.expect(cache.get("ev") != null);

    // Shift exactly history_length times: the put landed in window 0, so after
    // history_length shifts it has aged out of the ring and been evicted.
    var i: usize = 0;
    while (i < history_length) : (i += 1) cache.shift();

    // Evicted: gone from the index, the cache released its reference (only our
    // test reference remains), and it no longer appears in getGossipIDs.
    try std.testing.expect(cache.get("ev") == null);
    try std.testing.expectEqual(@as(usize, 1), frame.refs.load(.monotonic));
    const ids = try cache.getGossipIDs(allocator, "t");
    defer allocator.free(ids);
    try std.testing.expectEqual(@as(usize, 0), ids.len);

    frame.release(); // drop our test reference; frame frees here, no leak.
}

test "deinit with entries present in multiple windows frees everything" {
    const allocator = std.testing.allocator;
    var cache = MessageCache.init(allocator);
    defer cache.deinit();

    // Spread entries across several windows; deinit must release every frame and
    // free every id/topic (testing.allocator confirms no leak / double-free).
    const f1 = try testFrame(allocator, 1);
    try cache.put("w0a", "t", f1);
    f1.release();
    const f2 = try testFrame(allocator, 2);
    try cache.put("w0b", "t2", f2);
    f2.release();

    cache.shift();
    const f3 = try testFrame(allocator, 3);
    try cache.put("w1a", "t", f3);
    f3.release();

    cache.shift();
    const f4 = try testFrame(allocator, 4);
    try cache.put("w2a", "t2", f4);
    f4.release();
    // No explicit cleanup: the `defer cache.deinit()` above must free it all.
}
