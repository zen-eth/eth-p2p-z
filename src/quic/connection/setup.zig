//! Allocates the setup-time pieces of a connection (SharedState +
//! ConnectionActor), wires them together, and hands them back to the caller as
//! a `PendingConnection` ready to register its CIDs with the router and start
//! its handshake fiber.
//!
//! The lifetime story:
//!   - `SharedState` starts at refs=2 (one for the handle, one for the
//!     actor). Both sides drop their ref independently.
//!   - `ConnectionActor` is allocated on the heap. Its main loop releases
//!     actor-owned logical resources; after `spawn`, the `Connection` handle
//!     owns the actor pointer and frees the allocation once spawned futures
//!     have stopped.
//!   - `IncomingPacketChannel` is refcounted: actor holds one ref; the
//!     router holds additional refs once `mapRoute` has run.
//!   - `PendingConnection` owns the future handle ref until `spawn` creates
//!     the user-facing `Connection`.
//!
//! `PendingConnection` holds the actor + handle ref between construction and
//! `spawn`. Misuse after consume is caught in debug builds via asserts on the
//! `consumed` flag.

const std = @import("std");
const quiche = @import("quiche").c;

const conn_handle = @import("handle.zig");
const connection_actor = @import("actor.zig");
const cid_mod = @import("cid.zig");
const conn_limits = @import("limits.zig");
const conn_stats = @import("stats.zig");
const io_time = @import("../io/time.zig");
const packet_route = @import("../io/packet_route.zig");
const shared_state_mod = @import("shared_state.zig");
const transport_mod = @import("../io/transport.zig");

const Allocator = std.mem.Allocator;

pub const Connection = conn_handle.Connection;
pub const ConnectionActor = connection_actor.ConnectionActor;
pub const SharedState = shared_state_mod.SharedState;
pub const NetworkTransport = transport_mod.NetworkTransport;
pub const RoutedPacket = packet_route.RoutedPacket;
pub const PacketEnqueueResult = packet_route.EnqueueResult;
pub const IncomingPacketChannel = packet_route.IncomingPacketChannel;
pub const CidKey = cid_mod.CidKey;

/// Per-connection setup tuning shared by `endpoint/raw.zig` (which builds the
/// quiche conn) and `connection/setup.zig` (which wires the actor + handle).
pub const ActorParams = struct {
    transport: NetworkTransport,
    control_queue_len: usize,
    stream_accept_queue_len: usize,
    recv_datagram_slots: usize,
    recv_datagram_slot_size: usize,
    inbound_packet_ring_bytes: usize,
    inbound_packet_queue_len: usize,
    stream_inbound_queue_bytes: usize,
    stream_outbound_queue_bytes: usize,
    stream_inbound_quantum_bytes: usize,
    stream_outbound_quantum_bytes: usize,
    outbound_pending_queue_len: usize,
    /// Keep-alive period in ns (0 = disabled). See `config.TransportOptions.keep_alive_period_ms`.
    keep_alive_period_ns: u64 = 0,
};

pub const max_outbound_batch_size = connection_actor.max_outbound_batch_size;
pub const max_flush_packet_len = connection_actor.max_flush_packet_len;

pub const Params = struct {
    allocator: Allocator,
    io: std.Io,
    conn: *quiche.quiche_conn,
    actor: ActorParams,
    pending_accept_cap: usize = 0,
};

/// What the router needs to register a freshly-built connection's source
/// CIDs to its packet inbox. `cids` borrows from the actor's registry —
/// valid only until the next mutation (effectively, until `spawn`).
pub const RouteRegistration = struct {
    cids: []const CidKey,
    channel: *IncomingPacketChannel,
};

/// Bundle held between `createPending` and `spawn`. Carries the actor and
/// future handle ref so callers can run the configure phase (CID registration,
/// optional Initial-packet enqueue) before the actor's fiber starts. After
/// `spawn`, the bundle is consumed and the caller has just `*Connection`.
pub const PendingConnection = struct {
    allocator: Allocator,
    actor: *ConnectionActor,
    shared: *SharedState,
    consumed: bool = false,

    /// Idempotent. Cleans up actor + pending handle ref if `spawn` has not
    /// consumed the bundle. Safe to `defer` at construction.
    pub fn deinit(self: *PendingConnection) void {
        if (self.consumed) return;
        self.consumed = true;
        self.actor.destroyPrespawn();
        self.shared.release();
    }

    /// Curated view of what the router needs to map this connection's CIDs
    /// to its packet inbox. The returned slice borrows from `actor.cids`;
    /// drain it before calling `spawn` (which lets the actor's fiber start
    /// mutating the registry).
    pub fn routeRegistration(self: *const PendingConnection) RouteRegistration {
        std.debug.assert(!self.consumed);
        return .{
            .cids = self.actor.cids.slice(),
            .channel = self.actor.incoming_packets.?,
        };
    }

    /// Server-only: feed the Initial packet that triggered this accept into
    /// the actor's inbox. Must run before `spawn` so the packet is the first
    /// thing the actor's main loop sees.
    pub fn enqueueInboundPacket(self: *PendingConnection, packet: *RoutedPacket) PacketEnqueueResult {
        std.debug.assert(!self.consumed);
        return self.actor.enqueueInboundPacket(packet);
    }

    /// Spawn the handshake fiber and return the user-facing `*Connection`.
    /// On success this PendingConnection is marked consumed (`deinit`
    /// becomes a no-op). On failure self is unchanged and the caller's
    /// deferred `deinit` will clean up.
    pub fn spawn(self: *PendingConnection, io: std.Io, timeout: std.Io.Timeout) (std.Io.ConcurrentError || Allocator.Error)!*Connection {
        std.debug.assert(!self.consumed);
        self.actor.handshake_started_mono_ns = io_time.monotonicNs(io);
        const deadline_ns = io_time.timeoutDeadlineNs(io, timeout);
        try self.actor.spawn(.handshaking, deadline_ns);
        const connection = conn_handle.createSpawned(self.allocator, self.shared, self.actor) catch |err| {
            self.actor.destroySpawned();
            self.shared.release();
            self.consumed = true;
            return err;
        };
        self.consumed = true;
        return connection;
    }
};

/// Builds the SharedState/Actor pair from a freshly-created `quiche_conn`,
/// wires them together, and registers the actor's initial source CIDs (the
/// ones quiche minted from the long-header SCID/DCID).
/// Returns a PendingConnection ready for route registration and `spawn`.
pub fn createPending(params: Params) Allocator.Error!PendingConnection {
    var conn_owned = false;
    errdefer if (!conn_owned) quiche.quiche_conn_free(params.conn);

    const w = params.actor;
    const initial_stats = conn_stats.ConnectionStats.fromQuiche(params.conn, 0);

    const shared = try SharedState.create(params.allocator, params.io, .{
        .inbox_capacity = w.control_queue_len,
        .accept_capacity = w.stream_accept_queue_len,
        .outbound_pending_capacity = w.outbound_pending_queue_len,
        .datagram_pool_slots = w.recv_datagram_slots,
        .datagram_pool_slot_size = w.recv_datagram_slot_size,
        .datagram_queue_capacity = w.recv_datagram_slots,
        .initial_stats = initial_stats,
        .initial_max_datagram_payload = conn_limits.maxDatagramPayload(params.conn),
    });
    var shared_handle_owned = true;
    var shared_actor_owned = true;
    errdefer {
        if (shared_handle_owned) shared.release();
        if (shared_actor_owned) shared.release();
    }

    const actor = try ConnectionActor.create(.{
        .allocator = params.allocator,
        .io = params.io,
        .conn = params.conn,
        .transport = w.transport,
        .shared = shared,
        .pending_accept_cap = if (params.pending_accept_cap == 0) @max(@as(usize, 32), w.stream_accept_queue_len) else params.pending_accept_cap,
        .stream_inbound_queue_bytes = w.stream_inbound_queue_bytes,
        .stream_outbound_queue_bytes = w.stream_outbound_queue_bytes,
        .stream_inbound_quantum_bytes = w.stream_inbound_quantum_bytes,
        .stream_outbound_quantum_bytes = w.stream_outbound_quantum_bytes,
        .keep_alive_period_ns = w.keep_alive_period_ns,
    });
    conn_owned = true;
    var actor_owned = true;
    errdefer if (actor_owned) actor.destroyPrespawn();
    shared_actor_owned = false;

    try actor.initPacketInbox(w.inbound_packet_ring_bytes, w.inbound_packet_queue_len);
    actor.attachIncomingPacketWaitSet();
    _ = try actor.registerInitialSourceIds();

    shared_handle_owned = false;
    actor_owned = false;

    return .{ .allocator = params.allocator, .actor = actor, .shared = shared };
}
