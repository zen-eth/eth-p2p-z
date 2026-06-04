//! The public gossipsub service. `Gossipsub` constructs and owns a single-fiber
//! `Router`, registers it on a `*Switch` (as the `/meshsub/1.1.0` inbound service
//! and as the peer connect/disconnect observer), and exposes the lifecycle.
//!
//! The Switch's peer-event callbacks fire on whichever fiber called
//! dial/accept/deinit, OUTSIDE the Switch's connection lock. They must only post
//! to the router inbox and return — which is exactly what the callbacks here do,
//! keeping all router-state mutation on the single router fiber.
//!
//! This is wired on the root import path (root.zig), NOT through protocols.zig:
//! the router imports switch.zig and switch.zig imports protocols.zig, so routing
//! gossipsub through the protocols chain would risk an import cycle.

const std = @import("std");
const swarm = @import("../../switch.zig");
const pubsub = @import("pubsub.zig");
const router_mod = @import("router.zig");
const PeerId = @import("peer_id").PeerId;

const Switch = swarm.Switch;
const SwitchConnection = swarm.SwitchConnection;
const Router = router_mod.Router;

/// The gossipsub service handle. Owns the router; borrows the Switch (which must
/// outlive this). Construct one per Switch before any dial/accept so its
/// peer-event callback is registered when connections come up.
pub const Gossipsub = struct {
    allocator: std.mem.Allocator,
    router: *Router,

    /// Construct the router, start its fiber, register the inbound service and
    /// the peer-event callback on the Switch. On any failure the router is torn
    /// down so nothing leaks.
    pub fn init(allocator: std.mem.Allocator, io: std.Io, sw: *Switch) !*Gossipsub {
        const router = try Router.create(allocator, io, sw);
        errdefer router.destroy();

        try router.start();

        const service = try router.inboundService();
        var service_owned = true;
        // If addProtocolService fails it does NOT take ownership, so free the
        // service object ourselves on that path.
        errdefer if (service_owned) service.deinit();
        try sw.addProtocolService(pubsub.protocol_id, service);
        service_owned = false;

        sw.setPeerEventCallback(.{
            .ctx = router,
            .on_connected = onConnected,
            .on_disconnected = onDisconnected,
        });

        const self = try allocator.create(Gossipsub);
        self.* = .{ .allocator = allocator, .router = router };
        return self;
    }

    /// Shut the router down (tears down every peer, joins its fiber) and free.
    /// Call before deiniting the Switch: the Switch cancels the inbound handler
    /// fibers on connection teardown, and the router's peer state borrows the
    /// Switch connections, so the router must stop while the Switch is still live.
    pub fn deinit(self: *Gossipsub) void {
        self.router.destroy();
        self.allocator.destroy(self);
    }

    /// Number of peers the router currently tracks. Used by tests to observe the
    /// per-peer I/O lifecycle.
    pub fn peerCount(self: *const Gossipsub) usize {
        return self.router.peerCount();
    }

    // The Switch peer-event callbacks: ctx is the *Router. They only post to the
    // router inbox (non-blocking, dropping on a closed/full inbox) and return.

    fn onConnected(ctx: *anyopaque, peer: PeerId, conn: *SwitchConnection, remote_addr: std.Io.net.IpAddress) void {
        const router: *Router = @ptrCast(@alignCast(ctx));
        router.inbox.putOne(router.io, .{ .peer_connected = .{
            .peer = peer,
            .conn = conn,
            .remote_addr = remote_addr,
        } }) catch {};
    }

    fn onDisconnected(ctx: *anyopaque, peer: PeerId) void {
        const router: *Router = @ptrCast(@alignCast(ctx));
        router.inbox.putOne(router.io, .{ .peer_disconnected = .{ .peer = peer } }) catch {};
    }
};
