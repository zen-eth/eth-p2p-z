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
    ///
    /// PRECONDITION (closes the router↔Switch lifetime hole): before this runs,
    /// every connection on this Switch that uses gossipsub must already be torn
    /// down (or be torn down with no inbound `/meshsub` RPC in flight), OR this
    /// Gossipsub must be deinited before any such connection outlives it.
    ///
    /// The Switch holds three references that point at the router we are about to
    /// free: (a) the peer-event callback's `ctx`, (b) the registered `/meshsub`
    /// inbound-service instance's `router` field, and (c) any live Switch-owned
    /// inbound handler fiber holding `*Router`. We close all three holes here:
    ///   - First `clearPeerEventCallback`: after this, no connect/disconnect can
    ///     post into the router, so freeing it cannot be raced by a peer event.
    ///     (Per its contract this assumes no concurrent connect/disconnect is in
    ///     flight — the precondition above.)
    ///   - Then `router.destroy`: it tears down every peer (closing each peer's
    ///     sink/stream while the connections are still alive) and frees.
    /// The inbound-handler hole (b)/(c) is closed by the Switch's own teardown
    /// property: inbound handler fibers are Switch-owned and the Switch cancels
    /// them on connection teardown, so once a connection is closed no handler can
    /// post to the router. Given the precondition (connections torn down first),
    /// no handler survives to touch the freed router; the dangling service-object
    /// `router` field is likewise only reached via `openInbound` on a new inbound
    /// stream, which cannot arrive once the connections are gone.
    pub fn deinit(self: *Gossipsub) void {
        self.router.sw.clearPeerEventCallback();
        self.router.destroy();
        self.allocator.destroy(self);
    }

    /// Number of peers the router currently tracks. Used by tests to observe the
    /// per-peer I/O lifecycle.
    pub fn peerCount(self: *const Gossipsub) usize {
        return self.router.peerCount();
    }

    // The Switch peer-event callbacks: ctx is the *Router. Each posts one Command
    // to the router inbox and returns. `putOne` blocks only if the inbox is full
    // (it never drops): the inbox is sized large enough that a full inbox is not
    // expected in practice, so the post is effectively non-blocking, but the
    // blocking variant is the safe choice — dropping a `peer_disconnected` would
    // leak a peer. The callbacks must stay cheap (just this post); on a closed
    // inbox the post fails and is swallowed (the router is shutting down anyway).

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
