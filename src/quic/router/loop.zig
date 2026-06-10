const std = @import("std");
const quiche = @import("quiche").c;
const config = @import("../config.zig");
const connection_state = @import("../connection/setup.zig");
const connection_mod = @import("../connection/mod.zig");
const cid_mod = @import("../connection/cid.zig");
const cid_gen = @import("../connection/cid_gen.zig");
const endpoint_core = @import("../endpoint/core.zig");
const endpoint_raw = @import("../endpoint/raw.zig");
const socket_control = @import("../io/socket_control.zig");
const io_time = @import("../io/time.zig");
const packet_route = @import("../io/packet_route.zig");
const transport_mod = @import("../io/transport.zig");
const accept_queue = @import("accept_queue.zig");
const route_table_mod = @import("route_table.zig");
const retry_token = @import("retry_token.zig");

const retry_token_max_age_ns: u64 = 60 * std.time.ns_per_s;

const CidKey = cid_mod.CidKey;
const Connection = connection_mod.Connection;
const IncomingPacketChannel = packet_route.IncomingPacketChannel;
const RoutedPacket = packet_route.RoutedPacket;
const RouteTable = route_table_mod.RouteTable;
pub const SlabPool = packet_route.SlabPool;
pub const AcceptChannel = accept_queue.Channel;
pub const ListenError = error{AlreadyBound} || std.Io.net.IpAddress.BindError || socket_control.ConfigureError || std.mem.Allocator.Error || std.Io.ConcurrentError;
const packet_buf_len = packet_route.max_udp_payload_len;
const local_conn_id_len: usize = cid_mod.local_cid_len;

const quiche_packet_type_initial: u8 = 1;
// RFC 9000 §14.1: clients pad their first Initial to at least 1200 bytes; servers
// must drop shorter Initials. We use the same threshold to gate any reply that is
// reflectable (e.g., Version Negotiation) so we never amplify a short/spoofed input.
const min_initial_packet_len: usize = 1200;

// Scratch for the token quiche_header_info() extracts from an incoming Initial.
// Sized well above our minted retry tokens (retry_token.max_token_len) so an
// oversized or foreign token does not fail header parsing before we can route.
const header_info_token_buf_len: usize = 256;

// Output buffer for a Version Negotiation packet — sized to a full datagram (a
// VN packet itself is small, but this is the scratch we hand quiche).
const version_negotiation_buf_len: usize = 1500;

pub const Context = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    options: config.Options,
    core: *endpoint_core.EndpointCore,
    socket_slot: *?*transport_mod.SharedUdpSocket,
    accept_queue_slot: *?*AcceptChannel.State,
    /// Combined "in-flight admissions": held by reservations issued from
    /// `acceptInitial` and by accepted-but-undelivered connections sitting in
    /// `accept_queue_slot`. Set to `connection_accept_queue_len` on `bind`.
    accept_available: *std.atomic.Value(usize),
    /// Slot for the router's main socket-reader fiber. Owned by the
    /// endpoint handle. Filled on `bind`, cleared (after `await`) by
    /// `closeListener`. We use a `Future` (not a `Group`) for the main
    /// router loop because the surrounding endpoint memory is freed
    /// shortly after `closeListener` returns; `Future.await` blocks until
    /// the runtime has fully torn down the fiber, eliminating the race the
    /// old `Group`-based scheme was vulnerable to. The loop is stopped via
    /// the `stopping` flag (not cancellation — see that field).
    router_future_slot: *?std.Io.Future(RouterLoopError!void),
    /// Group for short-lived "handshake waiter" tasks spawned when the router
    /// promotes an Initial packet to a Connection. Lives alongside the main
    /// router future; cancellation is initiated from `closeListener` after the
    /// main router loop exits.
    handshake_waiters: *std.Io.Group,
    /// Cooperative teardown flag. `closeListener` sets it and wakes the recv
    /// fiber with a zero-length loopback datagram; the loop observes the flag
    /// and returns. A persistent flag, not `Future.cancel` (which is one-shot
    /// and unreliable as the sole teardown signal — see `closeListener`).
    stopping: *std.atomic.Value(bool),
    /// Slot for the recv fiber's slab pool (zero-copy packet views). Filled on
    /// `bind` (when `recv_slab_slots > 0`), endpoint reference dropped by
    /// `closeListener`; in-flight views keep their slabs (and the pool) alive
    /// past that. Null = every received packet is heap-copied.
    slab_pool_slot: *?*packet_route.SlabPool,
    raw: endpoint_raw.Context,

    pub fn addStat(ctx: Context, comptime field: []const u8, value: u64) void {
        ctx.core.addStat(field, value);
    }

    pub fn subStat(ctx: Context, comptime field: []const u8, value: u64) void {
        ctx.core.subStat(field, value);
    }

    pub fn setStat(ctx: Context, comptime field: []const u8, value: u64) void {
        ctx.core.setStat(field, value);
    }

    pub fn sourceControlEnabled(ctx: Context, from: std.Io.net.IpAddress) bool {
        const socket = ctx.socket_slot.* orelse return false;
        return socket.sourceControlEnabled(from);
    }
};

pub const RouterLoopError = std.Io.Cancelable || std.Io.ConcurrentError;

pub fn bind(ep: Context, addr: std.Io.net.IpAddress) ListenError!std.Io.net.IpAddress {
    // Endpoints accept exactly one bound listener. Re-binding silently would orphan
    // any in-flight actors and accepted-but-unyielded connections; force the caller
    // to close the previous listener explicitly via QuicEndpoint.deinit.
    if (ep.socket_slot.* != null) return error.AlreadyBound;
    const io = ep.io;
    var bind_addr = addr;
    // std.Io's `ip6_only` is applied inverted by the backends: setting it yields
    // IPV6_V6ONLY=0 (dual-stack), the opposite of the field's name, but consistently
    // across Threaded/Kqueue/Uring. We want dual-stack for IPv6 binds so peers can
    // arrive over IPv4-mapped addresses.
    const bind_options: std.Io.net.IpAddress.BindOptions = .{
        .mode = .dgram,
        .ip6_only = bind_addr == .ip6,
    };
    const socket = try std.Io.net.IpAddress.bind(&bind_addr, io, bind_options);
    var socket_owned = true;
    errdefer if (socket_owned) socket.close(io);
    const caps = try socket_control.configureUdpSocket(&socket, .{
        .enable_udp_gro = ep.options.endpoint.enable_udp_gro,
        .enable_pktinfo = ep.options.endpoint.enable_pktinfo,
        .enable_orig_dst = ep.options.endpoint.enable_orig_dst,
        .enable_rx_timestamps = ep.options.endpoint.enable_rx_timestamps,
        .socket_mark = ep.options.endpoint.socket_mark,
        .shadow_compatible = ep.options.endpoint.shadow_compatible,
    });
    const shared_socket = try transport_mod.SharedUdpSocket.init(ep.allocator, io, socket, caps);
    socket_owned = false;
    errdefer shared_socket.release();
    ep.socket_slot.* = shared_socket;
    errdefer ep.socket_slot.* = null;
    const accept_capacity = ep.options.endpoint.connection_accept_queue_len;
    ep.accept_queue_slot.* = try AcceptChannel.State.init(ep.allocator, io, accept_capacity, 0);
    ep.accept_available.store(accept_capacity, .release);
    errdefer {
        if (ep.accept_queue_slot.*) |state| {
            state.close(io);
            state.discardQueued(io);
            state.release();
            ep.accept_queue_slot.* = null;
        }
        ep.accept_available.store(0, .release);
    }
    ep.setStat("cid_map_entries", 0);
    if (ep.options.endpoint.recv_slab_slots > 0) {
        ep.slab_pool_slot.* = try packet_route.SlabPool.init(
            ep.allocator,
            ep.options.endpoint.recv_slab_slots,
            ep.options.endpoint.recv_slab_slot_bytes,
        );
    }
    errdefer if (ep.slab_pool_slot.*) |pool| {
        pool.release();
        ep.slab_pool_slot.* = null;
    };
    // Clear any stop signal from a previous listener before (re-)spawning.
    ep.stopping.store(false, .release);
    ep.router_future_slot.* = try std.Io.concurrent(io, routerSocketLoop, .{ep});
    return shared_socket.address();
}

pub const AcceptError = error{ListenerClosed} || std.Io.Cancelable;

pub fn accept(ep: Context) AcceptError!*Connection {
    const io = ep.io;
    // A null slot means the listener was never bound or already fully torn
    // down; a closed-but-present channel means `stopAccepting`/`closeListener`
    // asked accepting to stop. Both surface as `ListenerClosed` so a caller's
    // accept loop can tell "stop accepting" apart from cancellation of its own
    // fiber (`Canceled`).
    const ch = ep.accept_queue_slot.* orelse return error.ListenerClosed;
    const conn = ch.receiver().recv(io) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        error.Closed => return error.ListenerClosed,
    };
    _ = ep.accept_available.fetchAdd(1, .acq_rel);
    return conn;
}

/// Unblock a fiber parked in `accept` for graceful shutdown WITHOUT tearing the
/// listener down. Closes the accept channel so a blocked `recv` returns (and
/// `accept` reports `ListenerClosed`); the router fiber, socket, and CID map
/// stay live until `closeListener` runs. Idempotent: the channel's `close` is
/// guarded by its own `closed` flag, and `closeListener` calling `close` again
/// is a harmless no-op — no double-free, no use-after-free. Safe on an unbound
/// endpoint (null slot is a no-op).
pub fn stopAccepting(ep: Context) void {
    if (ep.accept_queue_slot.*) |state| state.close(ep.io);
}

pub fn closeListener(ep: Context) void {
    const io = ep.io;
    // Stop the router cooperatively: set the persistent flag, then WAKE the
    // recv fiber out of its blocking `recvmsg` with a zero-length datagram sent
    // to the socket's own (loopback-substituted) address — the loop drops
    // empty datagrams and re-checks `stopping` at the top of every iteration.
    // The flag is the truth, the datagram is just the waker: this is the same
    // persistent-signal doctrine the old route-command-channel close
    // implemented, without the per-packet Select that channel forced. If the
    // wake send fails (practically impossible on loopback), fall back to the
    // one-shot `cancel` as a backstop — the recv is a single cancellation
    // point, and the loop maps `Canceled` back to a `stopping` re-check.
    ep.stopping.store(true, .release);
    if (ep.router_future_slot.*) |*future| {
        if (!sendWakeDatagram(ep)) {
            // Backstop only. cancel is idempotent and caches the result, so the
            // await below re-reads it without double-consuming the future.
            future.cancel(io) catch {};
        }
        // Exits cleanly (void) on `stopping`; surface, don't swallow, any error.
        future.await(io) catch |err|
            std.log.warn("router teardown: loop exited with error {}", .{err});
        ep.router_future_slot.* = null;
    }
    // Then drain any handshake waiters spawned by the router accept path. These are
    // self-clearing on success and tracked in a Group; `cancel` blocks
    // until each finishes its post-call cleanup.
    ep.handshake_waiters.cancel(io);
    // Drop the endpoint's pool reference; slabs pinned by in-flight packet
    // views (sitting in connection inboxes) free themselves on last release.
    if (ep.slab_pool_slot.*) |pool| {
        pool.release();
        ep.slab_pool_slot.* = null;
    }
    if (ep.accept_queue_slot.*) |state| {
        state.close(io);
        state.discardQueued(io);
        state.release();
        ep.accept_queue_slot.* = null;
    }
    ep.accept_available.store(0, .release);
    if (ep.socket_slot.*) |socket| {
        socket.release();
        ep.socket_slot.* = null;
    }
}

pub fn localAddr(ep: Context) ?std.Io.net.IpAddress {
    const socket = ep.socket_slot.* orelse return null;
    return socket.address();
}

fn routerSocketLoop(ep: Context) RouterLoopError!void {
    const io = ep.io;
    // Routes die with the listener: release every held channel retain on exit.
    // (Actors unmapping their own CIDs afterwards is a harmless no-op overlap;
    // core.release runs a backstop clear for anything registered later.)
    defer {
        const removed = ep.core.route_table.clear(io);
        if (removed > 0) ep.subStat("cid_map_entries", @intCast(removed));
    }

    // ONE persistent fiber owning the blocking recv — no per-datagram Select,
    // no task spawns, no cancel/join. Route-table updates no longer pass
    // through this loop at all (writers mutate the shared table directly), so
    // the only wake this loop needs is a datagram: real traffic, or
    // closeListener's zero-length loopback wake.
    var recv_slab = RecvSlab{ .pool = ep.slab_pool_slot.* };
    defer recv_slab.retire();
    while (true) {
        if (ep.stopping.load(.acquire)) break;
        var packet = (receiveRouterPacket(ep, &recv_slab, .none) catch |err| switch (err) {
            // The teardown backstop (`closeListener` cancels only if its wake
            // datagram could not be sent) — or a stray cancel; either way the
            // loop-top `stopping` check decides.
            error.Canceled => continue,
            error.Timeout => unreachable, // .none timeout never fires
        }) orelse continue;
        defer packet.deinit();
        processRouterPacket(ep, &packet) catch |err| switch (err) {
            error.Canceled => continue,
        };
    }
}

/// The recv fiber's bump cursor over the current receive slab: datagrams land
/// in the slab's tail and become zero-copy packet views; the slab is retired
/// to a fresh one when the tail can no longer hold a maximum-size datagram.
/// Single-fiber state — only the recv loop touches it.
const RecvSlab = struct {
    pool: ?*packet_route.SlabPool,
    current: ?*packet_route.SlabPool.Slab = null,
    fill: usize = 0,

    /// The writable tail (≥ one max datagram), or null when the pool is
    /// absent/exhausted and the caller must fall back to a heap copy.
    fn tail(rs: *RecvSlab) ?[]u8 {
        const pool = rs.pool orelse return null;
        if (rs.current) |slab| {
            if (slab.buf.len - rs.fill >= packet_buf_len) return slab.buf[rs.fill..];
            slab.release(); // drop the cursor hold; views keep it pinned
            rs.current = null;
        }
        rs.current = pool.acquire() orelse return null;
        rs.fill = 0;
        return rs.current.?.buf;
    }

    /// Bump past `len` bytes the kernel just filled at the tail's front.
    fn consume(rs: *RecvSlab, len: usize) void {
        rs.fill += len;
    }

    fn retire(rs: *RecvSlab) void {
        if (rs.current) |slab| {
            slab.release();
            rs.current = null;
        }
    }
};

/// Wake the recv fiber out of its blocking `recvmsg` for teardown: send a
/// zero-length datagram to the bound socket (loopback-substituted when bound
/// to a wildcard address). Zero-length UDP datagrams are valid and the loop
/// drops empty payloads, so the wake is invisible to connections. Returns
/// false if the send failed and the caller must fall back to `cancel`.
fn sendWakeDatagram(ep: Context) bool {
    const socket = ep.socket_slot.* orelse return false;
    var dest = wakeAddress(socket.address());
    var messages = [_]std.Io.net.OutgoingMessage{.{
        .address = &dest,
        .data_ptr = "".ptr,
        .data_len = 0,
    }};
    socket.sendMany(ep.io, messages[0..], .{}) catch return false;
    return true;
}

/// The address the teardown wake datagram is sent to: the bound address with a
/// wildcard host replaced by loopback (a datagram to 0.0.0.0/:: is not
/// reliably deliverable; loopback to the bound port always is).
fn wakeAddress(bound: std.Io.net.IpAddress) std.Io.net.IpAddress {
    switch (bound) {
        .ip4 => |a| {
            if (std.mem.allEqual(u8, &a.bytes, 0)) return .{ .ip4 = .loopback(a.port) };
            return bound;
        },
        .ip6 => |a| {
            if (std.mem.allEqual(u8, &a.bytes, 0)) {
                var loop6 = a;
                loop6.bytes = [_]u8{0} ** 15 ++ [_]u8{1};
                return .{ .ip6 = loop6 };
            }
            return bound;
        },
    }
}

/// Narrow capability handed to the dialer: just enough to register a
/// freshly-dialed connection's CIDs in the shared route table. The dialer
/// never needs the listener-side bits in `Context` (accept queue, router
/// future, handshake waiters). Registration is a direct, synchronous table
/// write — no command round-trip, no ack event: a CID is routable before this
/// returns, so the dialer's first packet can never race its own registration.
pub const RouteRegistrar = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    core: *endpoint_core.EndpointCore,

    pub const RegisterError = error{ RegistrationFailed, OutOfMemory };
    pub const Registration = struct {
        registrar: RouteRegistrar,
        cids: std.ArrayList(CidKey) = .empty,
        active: bool = true,

        pub fn deinit(registration: *Registration) void {
            if (registration.active) {
                const reg = registration.registrar;
                for (registration.cids.items) |cid| {
                    if (reg.core.route_table.unmap(reg.io, cid)) {
                        reg.core.subStat("cid_map_entries", 1);
                    }
                }
            }
            registration.disarm();
        }

        pub fn disarm(registration: *Registration) void {
            registration.active = false;
            registration.cids.deinit(registration.registrar.allocator);
            registration.cids = .empty;
        }
    };

    /// Register the dialed connection's initial source CIDs (all-or-nothing).
    /// Caller retains ownership of `route`; on success the table holds one
    /// retain per mapped CID. The returned registration must be disarmed after
    /// the connection is fully handed to its actor; otherwise `deinit` rolls
    /// the mapping back.
    pub fn register(reg: RouteRegistrar, cids: []const CidKey, route: *IncomingPacketChannel) RegisterError!Registration {
        var owned_cids: std.ArrayList(CidKey) = .empty;
        errdefer owned_cids.deinit(reg.allocator);
        try owned_cids.appendSlice(reg.allocator, cids);

        const newly = reg.core.route_table.registerMany(reg.io, cids, route) orelse
            return error.RegistrationFailed;
        reg.core.addStat("cid_map_entries", @intCast(newly));
        return .{ .registrar = reg, .cids = owned_cids };
    }
};

/// Insert `cid → channel` into the shared route table, with the endpoint's
/// gauge accounting. Used by the server accept path; the dialer goes through
/// `RouteRegistrar.register` and actors write the table directly.
pub fn mapRoute(ep: Context, cid: CidKey, channel: *IncomingPacketChannel) bool {
    switch (ep.core.route_table.map(ep.io, cid, channel)) {
        .mapped => {
            ep.addStat("cid_map_entries", 1);
            return true;
        },
        .already_mapped => return true,
        .failed => {
            ep.addStat("cid_map_command_drops", 1);
            return false;
        },
    }
}

fn unmapRoute(ep: Context, cid: CidKey) void {
    if (ep.core.route_table.unmap(ep.io, cid)) ep.subStat("cid_map_entries", 1);
}

fn receiveRouterPacket(ep: Context, rs: *RecvSlab, timeout: std.Io.Timeout) (error{Timeout} || std.Io.Cancelable)!?RoutedPacket {
    const io = ep.io;
    const socket = ep.socket_slot.* orelse return error.Canceled;
    const local_addr = socket.address();
    // Receive straight into the current slab's tail (the packet then becomes a
    // zero-copy VIEW); when the pool is dry, degrade to a stack buffer + the
    // old per-packet heap copy.
    var fallback_buf: [packet_buf_len]u8 = undefined;
    const slab_tail = rs.tail();
    const buf: []u8 = if (slab_tail) |t| t[0..packet_buf_len] else fallback_buf[0..];
    // Shadow mode: a plain `recvmsg` with NO control buffer. zio maps an empty
    // control slice to `msg_control = null`/`msg_controllen = 0`, so the kernel
    // sees a bare datagram receive — the only form the Shadow simulator
    // supports. Wakeup/teardown is the loopback wake datagram, so dropping the
    // timed-control receive does not change the loop's liveness.
    const msg = if (ep.options.endpoint.shadow_compatible)
        socket.receive(io, buf) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            else => {
                ep.addStat("router_recv_errors", 1);
                return null;
            },
        }
    else recv: {
        var control: [socket_control.recv_control_buffer_len]u8 align(socket_control.control_buffer_align) = undefined;
        break :recv socket.receiveWithControlTimeout(io, buf, &control, timeout) catch |err| switch (err) {
            error.Timeout => return error.Timeout,
            error.Canceled => return error.Canceled,
            else => {
                ep.addStat("router_recv_errors", 1);
                return null;
            },
        };
    };
    const meta = socket_control.parseIncomingControl(&msg, local_addr) catch |err| switch (err) {
        error.PayloadTruncated => {
            ep.addStat("router_payload_truncated", 1);
            ep.addStat("router_packet_drops", 1);
            return null;
        },
        error.ControlTruncated => {
            ep.addStat("router_control_truncated", 1);
            ep.addStat("router_packet_drops", 1);
            return null;
        },
    };
    const packet_meta = RoutedPacket.Meta{
        .rx_mono_ns = rxMonoNs(io),
        .rx_system_ns = meta.rx_system_ns,
        .gro_segment_size = meta.gro_segment_size,
    };
    if (slab_tail != null) {
        // `msg.data` is the kernel-filled front of the slab tail: wrap it as a
        // view (one slab retain) and bump the cursor past it. Zero copies.
        const slab = rs.current.?;
        const packet = RoutedPacket.initView(slab, msg.data, msg.from, controlDestination(meta, local_addr), packet_meta) orelse {
            ep.addStat("router_packet_drops", 1);
            return null;
        };
        rs.consume(msg.data.len);
        return packet;
    }
    return (RoutedPacket.initWithMeta(ep.allocator, msg.data, msg.from, controlDestination(meta, local_addr), packet_meta) catch {
        ep.addStat("router_packet_drops", 1);
        return null;
    }) orelse {
        ep.addStat("router_packet_drops", 1);
        return null;
    };
}

// The local address a packet was actually delivered to, used as the source for
// our reply. Precedence: the kernel's original destination (IP_ORIGDSTADDR /
// IPV6_ORIGDSTADDR — survives transparent-proxy / dual-stack rewrites) first, then
// IP_PKTINFO's destination, and finally the socket's bound address when neither
// control message is present.
fn controlDestination(meta: socket_control.ParsedControl, fallback_to: std.Io.net.IpAddress) std.Io.net.IpAddress {
    return meta.orig_dst_to orelse meta.pktinfo_to orelse fallback_to;
}

fn processRouterPacket(ep: Context, packet: *RoutedPacket) std.Io.Cancelable!void {
    if (packet.gro_segment_size) |segment_size| {
        if (segment_size > 0 and packet.data.len > segment_size) {
            return processRouterGroPacket(ep, packet, segment_size);
        }
    }
    return processRouterSinglePacket(ep, packet);
}

fn processRouterGroPacket(ep: Context, packet: *const RoutedPacket, segment_size: u16) std.Io.Cancelable!void {
    var offset: usize = 0;
    while (offset < packet.data.len) {
        const end = @min(offset + @as(usize, segment_size), packet.data.len);
        const segment_meta = RoutedPacket.Meta{
            .rx_mono_ns = packet.rx_mono_ns,
            .rx_system_ns = packet.rx_system_ns,
        };
        // A slab-backed super-datagram splits into sibling VIEWS of the same
        // bytes (one retain each, zero copies); the heap-copy fallback dupes
        // per segment as before.
        var segment = if (packet.slab) |slab|
            (RoutedPacket.initView(slab, packet.data[offset..end], packet.from, packet.to, segment_meta) orelse {
                ep.addStat("router_packet_drops", 1);
                return;
            })
        else
            ((RoutedPacket.initWithMeta(
                ep.allocator,
                packet.data[offset..end],
                packet.from,
                packet.to,
                segment_meta,
            ) catch {
                ep.addStat("router_packet_drops", 1);
                return;
            }) orelse {
                ep.addStat("router_packet_drops", 1);
                return;
            });
        errdefer segment.deinit();
        try processRouterSinglePacket(ep, &segment);
        segment.deinit();
        offset = end;
    }
}

fn processRouterSinglePacket(ep: Context, packet: *RoutedPacket) std.Io.Cancelable!void {
    const data = packet.constBytes();
    const from = packet.from;

    ep.addStat("router_packets_recv", 1);

    if (data.len == 0) {
        ep.addStat("router_rejected_initial_packets", 1);
        return;
    }

    if ((data[0] & 0x80) == 0) {
        routeShortHeader(ep, packet);
        return;
    }

    var version: u32 = 0;
    var packet_type: u8 = 0;
    var scid: [quiche.QUICHE_MAX_CONN_ID_LEN]u8 = undefined;
    var scid_len: usize = scid.len;
    var dcid: [quiche.QUICHE_MAX_CONN_ID_LEN]u8 = undefined;
    var dcid_len: usize = dcid.len;
    var token: [header_info_token_buf_len]u8 = undefined;
    var token_len: usize = token.len;
    const rc = quiche.quiche_header_info(
        data.ptr,
        data.len,
        local_conn_id_len,
        &version,
        &packet_type,
        &scid,
        &scid_len,
        &dcid,
        &dcid_len,
        &token,
        &token_len,
    );
    if (rc < 0) {
        ep.addStat("router_rejected_initial_packets", 1);
        return;
    }

    // Try to deliver to an existing connection regardless of long-header type.
    // Handshake/0-RTT/retransmitted packets after the first round-trip carry the
    // server-chosen DCID and must reach the same connection that issued it.
    if (CidKey.init(dcid[0..dcid_len])) |known_dcid| {
        if (routeKnownDcid(ep, known_dcid, packet)) return;
    }

    if (!quiche.quiche_version_is_supported(version)) {
        // RFC 9000 §6.1: never emit Version Negotiation in response to a packet
        // whose version field is zero (those are themselves VN packets and would
        // create a reflection loop). Also require the input to be at least the
        // minimum Initial size so we don't amplify short spoofed datagrams.
        if (version == 0) {
            ep.addStat("router_rejected_initial_packets", 1);
            return;
        }
        if (data.len < min_initial_packet_len) {
            ep.addStat("router_rejected_initial_packets", 1);
            return;
        }
        var out: [version_negotiation_buf_len]u8 = undefined;
        const written = quiche.quiche_negotiate_version(&scid, scid_len, &dcid, dcid_len, &out, out.len);
        if (written > 0) {
            const socket = ep.socket_slot.* orelse return error.Canceled;
            sendRouterDatagram(ep, socket, packet.to, &from, out[0..@intCast(written)]) catch {
                ep.addStat("router_version_negotiation_send_failures", 1);
                return;
            };
            ep.addStat("router_version_negotiation_sent", 1);
        }
        return;
    }

    // Only Initial packets without a matching CID start a fresh connection. Other
    // long-header types (Handshake, 0-RTT) without a known DCID are stragglers that
    // belong to a connection we no longer have state for; drop them.
    if (packet_type != quiche_packet_type_initial) {
        ep.addStat("router_rejected_initial_packets", 1);
        return;
    }

    // Servers must drop Initial packets shorter than the spec's amplification
    // limit (RFC 9000 §14.1). This prevents an attacker who guesses a CID from
    // forcing us to allocate a quiche connection on a single-byte spoofed packet.
    if (data.len < min_initial_packet_len) {
        ep.addStat("router_rejected_initial_packets", 1);
        return;
    }

    // RFC 9000 §8.1.2: address validation. The first Initial from a peer
    // arrives without a token; respond with RETRY containing a freshly-minted
    // token bound to the peer's IP and the original DCID. The retried Initial
    // carries the token and we authenticate it before allocating any quiche
    // state — short-circuiting amplification attacks from spoofed sources.
    if (token_len == 0) {
        sendRetry(ep, packet.to, &from, scid[0..scid_len], dcid[0..dcid_len], version);
        return;
    }

    const retry = ep.core.retry_tokens.validate(
        token[0..token_len],
        from,
        io_time.monotonicNs(ep.io),
        retry_token_max_age_ns,
    ) orelse {
        ep.addStat("router_retry_token_invalid", 1);
        ep.addStat("router_rejected_initial_packets", 1);
        return;
    };
    if (retry.retry_scid.len != local_conn_id_len or !std.mem.eql(u8, retry.retry_scid.slice(), dcid[0..dcid_len])) {
        ep.addStat("router_retry_token_invalid", 1);
        ep.addStat("router_rejected_initial_packets", 1);
        return;
    }
    ep.addStat("router_retry_token_validated", 1);

    try acceptInitial(ep, packet, dcid[0..dcid_len], retry.original_dcid.slice());
}

// ----- server accept path -------------------------------------------------

fn acceptInitial(
    ep: Context,
    initial_packet: *RoutedPacket,
    initial_dcid: []const u8,
    original_dcid: []const u8,
) std.Io.Cancelable!void {
    const from = initial_packet.from;
    const local_addr = initial_packet.to;
    const accept_channel = ep.accept_queue_slot.* orelse return error.Canceled;
    var accept_permit = accept_queue.tryReserve(accept_channel, ep.accept_available) orelse {
        ep.addStat("accept_backlog_full", 1);
        ep.addStat("router_rejected_initial_packets", 1);
        return;
    };
    defer accept_permit.cancel();

    const socket = ep.socket_slot.* orelse return error.Canceled;
    startServerConnection(ep, socket, local_addr, from, initial_dcid, original_dcid, initial_packet, &accept_permit);
}

fn startServerConnection(
    ep: Context,
    socket: *transport_mod.SharedUdpSocket,
    local_addr: std.Io.net.IpAddress,
    peer_addr: std.Io.net.IpAddress,
    initial_dcid: []const u8,
    original_dcid: []const u8,
    initial_packet: *RoutedPacket,
    accept_permit: *accept_queue.Permit,
) void {
    const io = ep.io;
    const options = ep.options;
    var pending = endpoint_raw.createPendingConnection(ep.raw, local_addr, peer_addr, true, initial_dcid.ptr, initial_dcid.len, original_dcid.ptr, original_dcid.len, .{
        .transport = .{
            .io = io,
            .socket = socket,
            .local = local_addr,
            .peer = peer_addr,
            .outbound_batch_size = options.actor.outbound_batch_size,
            .core = ep.core,
            .route_table = &ep.core.route_table,
        },
        .control_queue_len = options.actor.control_queue_len,
        .stream_accept_queue_len = options.actor.stream_accept_queue_len,
        .recv_datagram_slots = options.actor.recv_datagram_slots,
        .recv_datagram_slot_size = options.transport.max_recv_udp_payload_size,
        .inbound_packet_ring_bytes = options.actor.inbound_packet_ring_bytes,
        .inbound_packet_queue_len = options.actor.inbound_packet_queue_len,
        .stream_inbound_queue_bytes = options.actor.stream_inbound_queue_bytes,
        .stream_outbound_queue_bytes = options.actor.stream_outbound_queue_bytes,
        .stream_inbound_quantum_bytes = options.actor.stream_inbound_quantum_bytes,
        .stream_outbound_quantum_bytes = options.actor.stream_outbound_quantum_bytes,
        .outbound_pending_queue_len = options.actor.outbound_pending_queue_len,
        .keep_alive_period_ns = options.transport.keep_alive_period_ms * std.time.ns_per_ms,
    }) catch {
        ep.addStat("router_packet_drops", 1);
        return;
    };
    ep.addStat("connections_started", 1);
    defer pending.deinit();

    // RETRY validation already pinned the connection's SCID (= retried
    // Initial's DCID = the new_scid we picked when we sent the Retry packet).
    // The actor's CID registry was populated at construction; we just need
    // to map each registered CID into the router's cid_map.
    const reg = pending.routeRegistration();
    var mapped_routes: usize = 0;
    var routes_committed = false;
    defer if (!routes_committed) {
        for (reg.cids[0..mapped_routes]) |cid| unmapRoute(ep, cid);
    };
    for (reg.cids) |cid| {
        if (!mapRoute(ep, cid, reg.channel)) {
            ep.addStat("router_packet_drops", 1);
            return;
        }
        mapped_routes += 1;
    }
    switch (pending.enqueueInboundPacket(initial_packet)) {
        .queued => ep.addStat("router_packets_dispatched", 1),
        .dropped => {
            ep.addStat("router_packet_drops", 1);
            return;
        },
    }
    ep.addStat("active_actors", 1);
    const conn = pending.spawn(io, io_time.receiveTimeout(@intCast(options.endpoint.handshake_timeout_ns))) catch {
        ep.addStat("failed_handshakes", 1);
        return;
    };

    var waiter_permit = accept_permit.take();
    ep.handshake_waiters.concurrent(io, serverHandshakeWaiter, .{ ep, conn, waiter_permit }) catch {
        waiter_permit.cancel();
        conn.deinit();
        ep.addStat("failed_handshakes", 1);
        return;
    };
    routes_committed = true;
}

fn serverHandshakeWaiter(ep: Context, conn: *connection_mod.Connection, accept_permit: accept_queue.Permit) std.Io.Cancelable!void {
    const io = ep.io;
    var permit = accept_permit;
    var owns_conn = true;
    defer if (owns_conn) {
        permit.cancel();
        conn.deinit();
    };

    conn.waitHandshake(io) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            ep.addStat("failed_handshakes", 1);
            return;
        },
    };
    _ = conn.remotePublicKey();

    ep.addStat("connections_established", 1);

    std.debug.assert(permit.channel != null and permit.available != null);
    if (permit.publish(io, conn)) {
        owns_conn = false;
        return;
    }
    owns_conn = false;
    ep.addStat("accept_backlog_full", 1);
}

fn sendRetry(
    ep: Context,
    local_addr: std.Io.net.IpAddress,
    peer_addr: *const std.Io.net.IpAddress,
    client_scid: []const u8,
    original_dcid: []const u8,
    version: u32,
) void {
    const io = ep.io;
    const socket = ep.socket_slot.* orelse return;

    // Generate the fresh server-chosen connection ID. The client echoes this
    // back as the DCID of the retried Initial; that DCID becomes the
    // connection's SCID once we accept it, so we do not need to track the
    // value in router state — it round-trips through the peer.
    const new_scid = cid_gen.randomLocalCid() catch {
        ep.addStat("router_retry_send_failures", 1);
        return;
    };

    var token_buf: [retry_token.max_token_len]u8 = undefined;
    const token_len = ep.core.retry_tokens.mint(
        &token_buf,
        peer_addr.*,
        original_dcid,
        &new_scid,
        io_time.monotonicNs(io),
    );

    var out: [packet_buf_len]u8 = undefined;
    const written = quiche.quiche_retry(
        client_scid.ptr,
        client_scid.len,
        original_dcid.ptr,
        original_dcid.len,
        &new_scid,
        new_scid.len,
        &token_buf,
        token_len,
        version,
        &out,
        out.len,
    );
    if (written <= 0) {
        ep.addStat("router_retry_send_failures", 1);
        return;
    }

    sendRouterDatagram(ep, socket, local_addr, peer_addr, out[0..@intCast(written)]) catch {
        ep.addStat("router_retry_send_failures", 1);
        return;
    };
    ep.addStat("router_retry_sent", 1);
}

fn sendRouterDatagram(
    ep: Context,
    socket: *transport_mod.SharedUdpSocket,
    local_addr: std.Io.net.IpAddress,
    peer_addr: *const std.Io.net.IpAddress,
    data: []const u8,
) std.Io.net.Socket.SendError!void {
    var destination = peer_addr.*;
    var message = std.Io.net.OutgoingMessage{
        .address = &destination,
        .data_ptr = data.ptr,
        .data_len = data.len,
    };
    var control: [socket_control.send_control_buffer_len]u8 align(socket_control.control_buffer_align) = undefined;
    if (ep.sourceControlEnabled(local_addr)) {
        message.control = socket_control.encodeOutgoingControl(control[0..], .{
            .caps = socket.caps,
            .from = local_addr,
        });
    }
    var messages = [_]std.Io.net.OutgoingMessage{message};
    return socket.sendMany(ep.io, messages[0..], .{});
}

fn routeShortHeader(
    ep: Context,
    packet: *RoutedPacket,
) void {
    const data = packet.constBytes();
    if (extractShortHeaderDcid(data)) |dcid| {
        if (routeKnownDcid(ep, dcid, packet)) return;
        ep.addStat("router_unknown_dcid_packets", 1);
    } else {
        ep.addStat("router_rejected_initial_packets", 1);
    }
}

fn routeKnownDcid(
    ep: Context,
    dcid: CidKey,
    packet: *RoutedPacket,
) bool {
    switch (ep.core.route_table.deliver(ep.io, dcid, packet)) {
        .queued => {
            ep.addStat("router_packets_dispatched", 1);
            return true;
        },
        .dropped => {
            ep.addStat("router_packet_drops", 1);
            return true;
        },
        .no_route => return false,
    }
}

fn extractShortHeaderDcid(packet: []const u8) ?CidKey {
    if (packet.len < 1 + local_conn_id_len) return null;
    if ((packet[0] & 0x80) != 0) return null;
    return CidKey.init(packet[1..][0..local_conn_id_len]);
}

fn rxMonoNs(io: std.Io) u64 {
    const ns = std.Io.Clock.awake.now(io).toNanoseconds();
    if (ns <= 0) return 0;
    return @intCast(ns);
}

test "router control destination prefers original destination over packet info" {
    const fallback: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 9000 } };
    const pktinfo: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 198, 51, 100, 7 }, .port = 9000 } };
    const original: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 203, 0, 113, 8 }, .port = 4433 } };

    const destination = controlDestination(.{
        .pktinfo_to = pktinfo,
        .orig_dst_to = original,
    }, fallback);
    try std.testing.expectEqualSlices(u8, &original.ip4.bytes, &destination.ip4.bytes);
    try std.testing.expectEqual(original.ip4.port, destination.ip4.port);
}

test "router splits GRO datagrams before CID dispatch" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const core = try endpoint_core.EndpointCore.init(allocator, io);
    defer core.release();

    const route_a = try packet_route.IncomingPacketChannel.init(allocator, io, 4096, 4);
    defer {
        route_a.close(io);
        route_a.release();
    }
    const route_b = try packet_route.IncomingPacketChannel.init(allocator, io, 4096, 4);
    defer {
        route_b.close(io);
        route_b.release();
    }

    var socket_slot: ?*transport_mod.SharedUdpSocket = null;
    var accept_queue_slot: ?*AcceptChannel.State = null;
    var accept_available: std.atomic.Value(usize) = .init(0);
    var router_future_slot: ?std.Io.Future(RouterLoopError!void) = null;
    var handshake_waiters: std.Io.Group = .init;
    var stopping: std.atomic.Value(bool) = .init(false);
    var slab_pool_slot: ?*packet_route.SlabPool = null;
    const ep = Context{
        .allocator = allocator,
        .io = io,
        .options = undefined,
        .core = core,
        .socket_slot = &socket_slot,
        .accept_queue_slot = &accept_queue_slot,
        .accept_available = &accept_available,
        .router_future_slot = &router_future_slot,
        .handshake_waiters = &handshake_waiters,
        .stopping = &stopping,
        .slab_pool_slot = &slab_pool_slot,
        .raw = undefined,
    };

    // Routes live in the shared core.route_table; core.release (deferred above)
    // runs the backstop clear that releases the table's channel retains.
    const cid_a = [_]u8{0x11} ** local_conn_id_len;
    const cid_b = [_]u8{0x22} ** local_conn_id_len;
    try std.testing.expect(mapRoute(ep, CidKey.init(&cid_a).?, route_a));
    try std.testing.expect(mapRoute(ep, CidKey.init(&cid_b).?, route_b));

    const segment_len = 1 + local_conn_id_len + 3;
    var data: [segment_len * 2]u8 = undefined;
    fillShortHeaderSegment(data[0..segment_len], &cid_a, "one");
    fillShortHeaderSegment(data[segment_len..][0..segment_len], &cid_b, "two");

    const from: std.Io.net.IpAddress = .{ .ip4 = .loopback(9000) };
    const to: std.Io.net.IpAddress = .{ .ip4 = .loopback(9001) };
    var packet = (try RoutedPacket.initWithMeta(allocator, &data, from, to, .{
        .gro_segment_size = segment_len,
    })) orelse return error.TestUnexpectedResult;
    defer packet.deinit();

    try processRouterPacket(ep, &packet);

    var out_a: RoutedPacket = undefined;
    try std.testing.expect(route_a.receiver().tryRecv(io, &out_a));
    defer out_a.deinit();
    try std.testing.expectEqual(@as(?u16, null), out_a.gro_segment_size);
    try std.testing.expectEqualSlices(u8, data[0..segment_len], out_a.constBytes());

    var out_b: RoutedPacket = undefined;
    try std.testing.expect(route_b.receiver().tryRecv(io, &out_b));
    defer out_b.deinit();
    try std.testing.expectEqual(@as(?u16, null), out_b.gro_segment_size);
    try std.testing.expectEqualSlices(u8, data[segment_len..][0..segment_len], out_b.constBytes());

    var extra: RoutedPacket = undefined;
    try std.testing.expect(!route_a.receiver().tryRecv(io, &extra));
    try std.testing.expect(!route_b.receiver().tryRecv(io, &extra));
}

fn fillShortHeaderSegment(out: []u8, cid: *const [local_conn_id_len]u8, payload: []const u8) void {
    out[0] = 0x40;
    @memcpy(out[1..][0..local_conn_id_len], cid[0..]);
    @memcpy(out[1 + local_conn_id_len ..][0..payload.len], payload);
}
