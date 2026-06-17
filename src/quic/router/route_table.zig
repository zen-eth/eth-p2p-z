//! The endpoint's CID → per-connection packet-inbox routing table, shared
//! between the router's recv fiber (one lookup per datagram) and the writers
//! that manage CID lifetimes (connection actors, dialer, server accept,
//! teardown).
//!
//! A plain mutex-protected map written DIRECTLY by its writers (the quinn
//! model), not actor-owned state: CID routing is not quiche state, so it needs
//! no single-writer ordering. Direct writes keep the recv loop a single
//! persistent fiber and make a CID routable the instant `quiche_conn_new_scid`
//! returns, before the NEW_CONNECTION_ID frame reaches the peer.
//!
//! Locking: `deliver` enqueues UNDER the lock — the channel's internal mutexes
//! nest inside the table lock and never the reverse, so no inversion. All via
//! `lockUncancelable`: these run on teardown paths where a cancellation point
//! would re-introduce one-shot-cancel fragility.
//!
//! Lifetime: embedded by value in `EndpointCore`, retained for life by every
//! connection actor (refcounted), so a writer can never deref a freed table.
//! Each entry holds one channel retain (taken in `map`, dropped in
//! `unmap`/`clear`); `clear` in core teardown backstops entries registered
//! after the recv loop exited.

const std = @import("std");
const packet_route = @import("../io/packet_route.zig");
const cid_mod = @import("../connection/cid.zig");

pub const CidKey = cid_mod.CidKey;
const IncomingPacketChannel = packet_route.IncomingPacketChannel;
const RoutedPacket = packet_route.RoutedPacket;

pub const RouteTable = struct {
    mutex: std.Io.Mutex = .init,
    entries: std.AutoHashMap(CidKey, *IncomingPacketChannel),

    pub const DeliverResult = enum { queued, dropped, no_route };
    /// `mapped` = NEW entry inserted (one new channel retain held);
    /// `already_mapped` = cid already maps to the SAME channel (idempotent, NO
    /// new retain — callers must not bump entry gauges); `failed` = cid belongs
    /// to a DIFFERENT channel, or the insert hit OOM.
    pub const MapResult = enum { mapped, already_mapped, failed };
    pub const MapExistingResult = enum { mapped, already_mapped, unknown_existing, failed };

    pub fn init(allocator: std.mem.Allocator) RouteTable {
        return .{ .entries = std.AutoHashMap(CidKey, *IncomingPacketChannel).init(allocator) };
    }

    /// Release every held channel retain and free the map. Only for core
    /// teardown, after the recv loop has exited; concurrent writers must be
    /// impossible by then (every actor retain on core has been released).
    pub fn deinit(table: *RouteTable, io: std.Io) void {
        _ = table.clear(io);
        table.entries.deinit();
    }

    /// Route one packet into the owning connection's inbox. `no_route` leaves
    /// the packet untouched (caller may try accept or drop); `dropped` means the
    /// inbox was full (loss-like, QUIC retransmits) and the packet was consumed.
    pub fn deliver(table: *RouteTable, io: std.Io, cid: CidKey, packet: *RoutedPacket) DeliverResult {
        table.mutex.lockUncancelable(io);
        defer table.mutex.unlock(io);
        const channel = table.entries.get(cid) orelse return .no_route;
        return switch (channel.sender().enqueue(io, packet)) {
            .queued => .queued,
            .dropped => .dropped,
        };
    }

    /// Insert `cid → channel` (see `MapResult` for the retain/gauge contract).
    pub fn map(table: *RouteTable, io: std.Io, cid: CidKey, channel: *IncomingPacketChannel) MapResult {
        table.mutex.lockUncancelable(io);
        defer table.mutex.unlock(io);
        return table.mapLocked(cid, channel);
    }

    /// Map `new_cid` to whatever channel currently owns `existing_cid` — the
    /// actor's new-source-CID path. One atomic lookup+insert under the lock.
    pub fn mapFromExisting(table: *RouteTable, io: std.Io, existing_cid: CidKey, new_cid: CidKey) MapExistingResult {
        table.mutex.lockUncancelable(io);
        defer table.mutex.unlock(io);
        const channel = table.entries.get(existing_cid) orelse return .unknown_existing;
        return switch (table.mapLocked(new_cid, channel)) {
            .mapped => .mapped,
            .already_mapped => .already_mapped,
            .failed => .failed,
        };
    }

    /// Remove `cid`, releasing the entry's channel retain. Returns whether an
    /// entry was removed (a second unmap of the same CID is a harmless no-op,
    /// so the loop-exit `clear` and an actor's own teardown unmap can overlap).
    pub fn unmap(table: *RouteTable, io: std.Io, cid: CidKey) bool {
        table.mutex.lockUncancelable(io);
        defer table.mutex.unlock(io);
        if (table.entries.fetchRemove(cid)) |kv| {
            kv.value.release();
            return true;
        }
        return false;
    }

    /// All-or-nothing registration of a fresh connection's CIDs. Returns the
    /// count of NEWLY inserted entries (the caller's gauge delta), or null on
    /// failure — rolling back only the entries THIS call inserted (an already
    /// same-channel cid is left untouched). At most 64 cids per call.
    pub fn registerMany(table: *RouteTable, io: std.Io, cids: []const CidKey, channel: *IncomingPacketChannel) ?usize {
        std.debug.assert(cids.len <= 64);
        table.mutex.lockUncancelable(io);
        defer table.mutex.unlock(io);
        var new_mask: u64 = 0;
        var newly: usize = 0;
        for (cids, 0..) |cid, i| {
            switch (table.mapLocked(cid, channel)) {
                .mapped => {
                    new_mask |= @as(u64, 1) << @intCast(i);
                    newly += 1;
                },
                .already_mapped => {},
                .failed => {
                    for (cids[0..i], 0..) |done, j| {
                        if ((new_mask >> @intCast(j)) & 1 == 0) continue;
                        if (table.entries.fetchRemove(done)) |kv| kv.value.release();
                    }
                    return null;
                },
            }
        }
        return newly;
    }

    /// Drop every route, releasing all held retains. Returns how many entries
    /// were removed (for the caller's gauge accounting).
    pub fn clear(table: *RouteTable, io: std.Io) usize {
        table.mutex.lockUncancelable(io);
        defer table.mutex.unlock(io);
        const removed = table.entries.count();
        var values = table.entries.valueIterator();
        while (values.next()) |channel| channel.*.release();
        table.entries.clearRetainingCapacity();
        return removed;
    }

    pub fn count(table: *RouteTable, io: std.Io) usize {
        table.mutex.lockUncancelable(io);
        defer table.mutex.unlock(io);
        return table.entries.count();
    }

    fn mapLocked(table: *RouteTable, cid: CidKey, channel: *IncomingPacketChannel) MapResult {
        if (table.entries.get(cid)) |existing| {
            return if (existing == channel) .already_mapped else .failed;
        }
        channel.retain();
        table.entries.putNoClobber(cid, channel) catch {
            channel.release();
            return .failed;
        };
        return .mapped;
    }
};

test "route table: map/deliver/unmap with retain accounting" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var table = RouteTable.init(allocator);
    defer table.deinit(io);

    const channel = try IncomingPacketChannel.init(allocator, io, 4096, 4);
    defer channel.release(); // the test's own ref; table retains its own
    defer channel.close(io);

    const cid = CidKey.init(&[_]u8{ 1, 2, 3, 4 }) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(RouteTable.MapResult.mapped, table.map(io, cid, channel));
    // Idempotent re-map of the same pair reports already_mapped (no new retain).
    try std.testing.expectEqual(RouteTable.MapResult.already_mapped, table.map(io, cid, channel));
    try std.testing.expectEqual(@as(usize, 1), table.count(io));

    const from: std.Io.net.IpAddress = .{ .ip4 = .loopback(9000) };
    const to: std.Io.net.IpAddress = .{ .ip4 = .loopback(9001) };
    var packet = (try RoutedPacket.init(allocator, "hello", from, to)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(RouteTable.DeliverResult.queued, table.deliver(io, cid, &packet));

    const other = CidKey.init(&[_]u8{ 9, 9, 9, 9 }) orelse return error.TestUnexpectedResult;
    var stray = (try RoutedPacket.init(allocator, "stray", from, to)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(RouteTable.DeliverResult.no_route, table.deliver(io, other, &stray));
    stray.deinit();

    var out: RoutedPacket = undefined;
    try std.testing.expect(channel.receiver().tryRecv(io, &out));
    try std.testing.expectEqualSlices(u8, "hello", out.constBytes());
    out.deinit();

    try std.testing.expect(table.unmap(io, cid));
    try std.testing.expect(!table.unmap(io, cid)); // second unmap: no-op
    try std.testing.expectEqual(@as(usize, 0), table.count(io));
}

test "route table: mapFromExisting follows the owning channel; registerMany rolls back" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var table = RouteTable.init(allocator);
    defer table.deinit(io);

    const a = try IncomingPacketChannel.init(allocator, io, 4096, 4);
    defer a.release();
    defer a.close(io);
    const b = try IncomingPacketChannel.init(allocator, io, 4096, 4);
    defer b.release();
    defer b.close(io);

    const cid1 = CidKey.init(&[_]u8{1}) orelse return error.TestUnexpectedResult;
    const cid2 = CidKey.init(&[_]u8{2}) orelse return error.TestUnexpectedResult;
    const cid3 = CidKey.init(&[_]u8{3}) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqual(RouteTable.MapResult.mapped, table.map(io, cid1, a));
    try std.testing.expectEqual(RouteTable.MapExistingResult.mapped, table.mapFromExisting(io, cid1, cid2));
    try std.testing.expectEqual(RouteTable.MapExistingResult.already_mapped, table.mapFromExisting(io, cid1, cid2));
    try std.testing.expectEqual(RouteTable.MapExistingResult.unknown_existing, table.mapFromExisting(io, cid3, cid3));

    // registerMany with a collision (cid2 is owned by `a`) commits nothing new.
    try std.testing.expectEqual(@as(?usize, null), table.registerMany(io, &[_]CidKey{ cid3, cid2 }, b));
    try std.testing.expectEqual(@as(usize, 2), table.count(io));
    try std.testing.expectEqual(RouteTable.DeliverResult.no_route, blk: {
        var p = (try RoutedPacket.init(allocator, "x", .{ .ip4 = .loopback(1) }, .{ .ip4 = .loopback(2) })) orelse return error.TestUnexpectedResult;
        const r = table.deliver(io, cid3, &p);
        if (r != .queued) p.deinit();
        break :blk r;
    });

    try std.testing.expectEqual(@as(?usize, 1), table.registerMany(io, &[_]CidKey{cid3}, b));
    try std.testing.expectEqual(@as(usize, 3), table.count(io));
    try std.testing.expectEqual(@as(usize, 3), table.clear(io));
    try std.testing.expectEqual(@as(usize, 0), table.count(io));
}
