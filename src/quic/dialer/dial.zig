const std = @import("std");
const connection_mod = @import("../connection/mod.zig");
const connection_setup = @import("../connection/setup.zig");
const config = @import("../config.zig");
const endpoint_core = @import("../endpoint/core.zig");
const raw = @import("../endpoint/raw.zig");
const io_time = @import("../io/time.zig");
const router = @import("../router/mod.zig");
const transport_mod = @import("../io/transport.zig");

const Connection = connection_mod.Connection;
const PendingConnection = connection_setup.PendingConnection;
const keys = @import("peer_id").keys;

pub const DialOptions = struct {
    timeout: std.Io.Timeout = .none,
    /// If set, the dial fails with `error.PeerIdentityMismatch` when the
    /// peer's verified public key does not match. Compared by key type +
    /// raw bytes (libp2p protobuf payload). Caller retains ownership.
    expected_peer_key: ?*const keys.PublicKey = null,
};
pub const DialError = error{
    AddressFamilyMismatch,
    EndpointNotBound,
    /// Handshake failed for an unattributable reason (catch-all).
    HandshakeFailed,
    /// Handshake deadline elapsed before the connection established.
    HandshakeTimeout,
    /// Peer closed / drained the connection without a specific error code.
    ConnectionClosed,
    /// The peer's libp2p identity (cert / SignedKey) failed verification.
    PeerVerifyFailed,
    /// Peer/we sent a QUIC CRYPTO_ERROR (TLS alert) during the handshake.
    CryptoError,
    /// A QUIC transport error or local I/O fault aborted the handshake.
    TransportError,
    /// The verified peer key did not match `DialOptions.expected_peer_key`.
    PeerIdentityMismatch,
    RouteRegistrationFailed,
} || std.Io.Cancelable || std.Io.Timeout.Error || std.Io.UnexpectedError || std.Io.ConcurrentError || std.mem.Allocator.Error;

/// Map the connection's structured handshake-failure reason to a `DialError`,
/// so callers learn *why* the handshake failed instead of an opaque
/// `HandshakeFailed`.
fn dialErrorFromFailure(failure: connection_mod.HandshakeFailure) DialError {
    return switch (failure.kind) {
        .timeout => error.HandshakeTimeout,
        .peer_verify_failed => error.PeerVerifyFailed,
        .crypto_error => error.CryptoError,
        .transport_error => error.TransportError,
        .closed => error.ConnectionClosed,
        .none, .unknown => error.HandshakeFailed,
    };
}

test "dialErrorFromFailure maps each handshake-failure kind to a distinct DialError" {
    const HF = connection_mod.HandshakeFailure;
    try std.testing.expectEqual(DialError.HandshakeTimeout, dialErrorFromFailure(HF{ .kind = .timeout }));
    try std.testing.expectEqual(DialError.PeerVerifyFailed, dialErrorFromFailure(HF{ .kind = .peer_verify_failed }));
    try std.testing.expectEqual(DialError.CryptoError, dialErrorFromFailure(HF{ .kind = .crypto_error }));
    try std.testing.expectEqual(DialError.TransportError, dialErrorFromFailure(HF{ .kind = .transport_error }));
    try std.testing.expectEqual(DialError.ConnectionClosed, dialErrorFromFailure(HF{ .kind = .closed }));
    try std.testing.expectEqual(DialError.HandshakeFailed, dialErrorFromFailure(HF{ .kind = .unknown }));
    try std.testing.expectEqual(DialError.HandshakeFailed, dialErrorFromFailure(HF{ .kind = .none }));
}

pub const Context = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    options: config.Options,
    core: *endpoint_core.EndpointCore,
    raw: raw.Context,
    /// Shared endpoint socket. Required: callers must `bind()` the endpoint
    /// before dialing. Inbound packets reach the connection via the router's
    /// CID map.
    shared_socket: ?*transport_mod.SharedUdpSocket = null,
    /// Required so we can register the dialed connection's CIDs with the
    /// router fiber.
    route_registrar: ?router.RouteRegistrar = null,

    pub fn addStat(ctx: Context, comptime field: []const u8, value: u64) void {
        ctx.core.addStat(field, value);
    }
};

pub fn dial(ep: Context, addr: std.Io.net.IpAddress, opts: DialOptions) DialError!*Connection {
    const io = ep.io;
    const options = ep.options;

    const socket = ep.shared_socket orelse return error.EndpointNotBound;
    const registrar = ep.route_registrar orelse return error.EndpointNotBound;
    if (std.meta.activeTag(addr) != std.meta.activeTag(socket.address())) {
        return error.AddressFamilyMismatch;
    }
    const local_addr = socket.address();

    var pending = try raw.createPendingConnection(ep.raw, local_addr, addr, false, null, 0, null, 0, .{
        .transport = .{
            .io = io,
            .socket = socket,
            .local = local_addr,
            .peer = addr,
            .outbound_batch_size = options.actor.outbound_batch_size,
            .core = ep.core,
            .route_updates = registrar.route_updates,
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
    });
    ep.addStat("connections_started", 1);
    defer pending.deinit();
    ep.addStat("active_actors", 1);

    var route_registration = try registerDialedRoute(registrar, &pending);
    defer route_registration.deinit();

    const handshake_timeout = if (opts.timeout == .none) io_time.receiveTimeout(@intCast(options.endpoint.handshake_timeout_ns)) else opts.timeout;
    const conn = try pending.spawn(io, handshake_timeout);
    errdefer conn.deinit();
    conn.waitHandshake(io) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        error.ConnectionClosed => {
            ep.addStat("failed_handshakes", 1);
            return error.ConnectionClosed;
        },
        error.HandshakeFailed => {
            ep.addStat("failed_handshakes", 1);
            // Refine the opaque failure into a specific cause from the
            // connection's published stats (TLS alert / transport error /
            // timeout / peer-verify rejection).
            return dialErrorFromFailure(conn.failReason());
        },
    };
    if (opts.expected_peer_key) |expected| {
        const remote_pub_key = conn.remotePublicKey();
        if (!publicKeysEqual(expected, &remote_pub_key)) {
            ep.addStat("failed_handshakes", 1);
            return error.PeerIdentityMismatch;
        }
    }
    ep.addStat("connections_established", 1);
    route_registration.disarm();
    return conn;
}

fn publicKeysEqual(a: *const keys.PublicKey, b: *const keys.PublicKey) bool {
    if (a.type != b.type) return false;
    // Fail closed: a key with no bytes is not a valid identity. Coercing null to
    // an empty slice would let two data-less keys (or null-vs-empty) compare equal
    // and satisfy a pinned `expected_peer_key` against an unverified peer.
    const a_data = a.data orelse return false;
    const b_data = b.data orelse return false;
    return std.mem.eql(u8, a_data, b_data);
}

fn registerDialedRoute(registrar: router.RouteRegistrar, pending: *PendingConnection) DialError!router.RouteRegistrar.Registration {
    const reg = pending.routeRegistration();
    if (reg.cids.len == 0) return error.RouteRegistrationFailed;
    return registrar.register(reg.cids, reg.channel) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.RegistrationFailed => return error.RouteRegistrationFailed,
    };
}
