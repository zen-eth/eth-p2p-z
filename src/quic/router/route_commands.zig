const std = @import("std");
const channel = @import("../io/channel.zig");
const packet_route = @import("../io/packet_route.zig");
const cid_mod = @import("../connection/cid.zig");

pub const CidKey = cid_mod.CidKey;

/// Request issued when a freshly-routed connection needs its source CIDs
/// mapped to the router's per-connection packet inbox. The request does not
/// transfer a route retain; `router.mapRoute` retains each successful map and
/// releases those retains when the CID is unmapped or the router map is cleared.
pub const RegisterRouteRequest = struct {
    allocator: std.mem.Allocator,
    cids: std.ArrayList(CidKey),
    route: *packet_route.IncomingPacketChannel,
    ack: std.Io.Event = .unset,
    success: bool = false,

    pub fn destroy(req: *RegisterRouteRequest) void {
        req.cids.deinit(req.allocator);
        req.allocator.destroy(req);
    }
};

pub const Command = union(enum) {
    map_cid: struct {
        existing_cid: CidKey,
        new_cid: CidKey,
    },
    unmap_cid: CidKey,
    register_route: *RegisterRouteRequest,
};

/// Drop hook wired into the channel: producers parking on `req.ack` would
/// otherwise be orphaned when the channel discards a queued request (close,
/// shutdown, or release path).
fn dropCommand(slot: *Command, io: std.Io) void {
    switch (slot.*) {
        .register_route => |req| {
            req.success = false;
            req.ack.set(io);
        },
        .map_cid, .unmap_cid => {},
    }
}

pub const Queue = channel.Unbounded(Command, dropCommand);

test "discardQueued fails and acks register_route requests" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const state = try Queue.State.init(allocator, io);
    defer state.release();

    const route = try packet_route.IncomingPacketChannel.init(allocator, io, 1024, 1);
    defer {
        route.close(io);
        route.release();
    }
    const req = try allocator.create(RegisterRouteRequest);
    req.* = .{
        .allocator = allocator,
        .cids = .empty,
        .route = route,
    };
    defer req.destroy();
    try req.cids.append(allocator, CidKey.init(&[_]u8{ 1, 2, 3 }) orelse return error.TestUnexpectedResult);

    try std.testing.expect(try state.sender().trySend(io, .{ .register_route = req }));
    state.close(io);
    state.discardQueued(io);

    try std.testing.expect(req.ack.isSet());
    try std.testing.expect(!req.success);
}
