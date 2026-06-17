const std = @import("std");
const quiche = @import("quiche").c;

pub const ActorError = enum(u8) {
    none,
    address_invalid,
    quiche_send_failed,
    quiche_recv_failed,
    socket_send_failed,
    socket_recv_failed,
    concurrent_failed,
    canceled,
    /// libp2p peer-certificate / SignedKey verification rejected the peer after
    /// the QUIC handshake established (see tls.extractPublicKey).
    peer_verify_failed,
    unknown,

    pub fn fromError(err: anyerror) ActorError {
        return switch (err) {
            error.AddressInvalid => .address_invalid,
            error.QuicheSendFailed => .quiche_send_failed,
            error.QuicheRecvFailed => .quiche_recv_failed,
            error.PeerVerifyFailed => .peer_verify_failed,
            error.Canceled => .canceled,
            error.SocketNotBound,
            error.MessageTooBig,
            error.NetworkUnreachable,
            error.AccessDenied,
            error.SystemResources,
            error.ConnectionRefused,
            error.ConnectionResetByPeer,
            error.WouldBlock,
            error.BrokenPipe,
            error.Unexpected,
            => .socket_send_failed,
            error.OperationAborted,
            error.SocketNotConnected,
            => .socket_recv_failed,
            error.OutOfMemory,
            error.ConcurrentAlreadyStarted,
            => .concurrent_failed,
            else => .unknown,
        };
    }
};

pub const CloseReason = enum(u8) {
    none,
    application_close_requested,
    handshake_failed,
    /// The handshake deadline elapsed before the connection established
    /// (distinct from `idle_timeout`, which is quiche's post-handshake idle).
    handshake_timeout,
    actor_error,
    idle_timeout,
    peer_error,
    local_error,
    draining,
    closed,
};

/// Structured reason a handshake failed, derived from a `ConnectionStats`
/// snapshot by `classifyHandshakeFailure` — lets dialers surface *why* (TLS
/// alert vs transport param vs timeout vs peer-identity) not an opaque fail.
pub const HandshakeFailure = struct {
    kind: Kind = .none,
    /// QUIC CONNECTION_CLOSE error code for `.crypto_error`/`.transport_error`,
    /// else 0. For `.crypto_error` the TLS alert is the low byte, per RFC 9000 §20.1.
    error_code: u64 = 0,
    /// True when the close came from the peer's CONNECTION_CLOSE.
    from_peer: bool = false,

    pub const Kind = enum {
        none,
        /// Handshake deadline elapsed before establishing.
        timeout,
        /// libp2p cert / SignedKey verification rejected the peer's identity.
        peer_verify_failed,
        /// QUIC CRYPTO_ERROR (TLS alert), error code 0x0100-0x01ff.
        crypto_error,
        /// QUIC transport error (0x00-0xff) or a local I/O fault.
        transport_error,
        /// Connection closed/draining without a more specific cause.
        closed,
        /// No attributable cause found in the published stats.
        unknown,
    };
};

/// 0x0100-0x01ff is CRYPTO_ERROR (TLS alert in the low byte), else transport.
fn classifyCloseCode(code: u64, from_peer: bool) HandshakeFailure {
    if (code >= 0x100 and code <= 0x1ff)
        return .{ .kind = .crypto_error, .error_code = code, .from_peer = from_peer };
    return .{ .kind = .transport_error, .error_code = code, .from_peer = from_peer };
}

/// Derive a structured handshake-failure reason from a stats snapshot. Pure (no
/// I/O), so unit-testable without a real handshake. Most-specific cause wins.
pub fn classifyHandshakeFailure(s: ConnectionStats) HandshakeFailure {
    // 1. libp2p peer-identity rejection, recorded by the actor when
    //    tls.extractPublicKey fails on an otherwise-established connection.
    if (s.last_actor_error == .peer_verify_failed) return .{ .kind = .peer_verify_failed };

    // 2. Handshake/idle deadline.
    if (s.close_reason == .handshake_timeout or s.close_reason == .idle_timeout)
        return .{ .kind = .timeout };

    // 3. A QUIC CONNECTION_CLOSE carrying a non-zero error code. The peer's
    //    close takes precedence (it tells us why *they* rejected us). `is_app`
    //    closes are application-level, not handshake faults, so skip them.
    if (!s.peer_close_is_app and s.peer_close_error_code != 0)
        return classifyCloseCode(s.peer_close_error_code, true);
    if (!s.local_close_is_app and s.local_close_error_code != 0)
        return classifyCloseCode(s.local_close_error_code, false);

    // 4. A local I/O / quiche fault surfaced by the actor.
    switch (s.last_actor_error) {
        .quiche_send_failed,
        .quiche_recv_failed,
        .socket_send_failed,
        .socket_recv_failed,
        => return .{ .kind = .transport_error },
        else => {},
    }

    // 5. A plain close / drain with no attributable error code.
    switch (s.close_reason) {
        .draining,
        .closed,
        .application_close_requested,
        .peer_error,
        .local_error,
        => return .{ .kind = .closed },
        else => {},
    }

    // 6. A fatal local actor fault (.actor_error). Steps 1 and 4 already handled
    //    peer_verify_failed / quiche / socket; anything else here (e.g.
    //    .concurrent_failed, .address_invalid) is a local transport fault.
    if (s.close_reason == .actor_error) return .{ .kind = .transport_error };

    return .{ .kind = .unknown };
}

pub const PathStats = struct {
    rtt_ns: u64 = 0,
    min_rtt_ns: u64 = 0,
    max_rtt_ns: u64 = 0,
    rttvar_ns: u64 = 0,
    cwnd: u64 = 0,
    pmtu: u64 = 0,
    delivery_rate: u64 = 0,
    max_bandwidth: u64 = 0,
    total_pto_count: u64 = 0,
    sent_bytes: u64 = 0,
    recv_bytes: u64 = 0,
    lost_bytes: u64 = 0,
    stream_retrans_bytes: u64 = 0,
};

pub const ConnectionStats = struct {
    opened_streams_local: u64 = 0,
    opened_streams_remote: u64 = 0,
    open_streams: u64 = 0,
    pending_accept_streams: u64 = 0,
    streams_collected_after_fin: u64 = 0,
    pending_accept_overflow: u64 = 0,
    inbound_stream_queue_full: u64 = 0,
    outbound_stream_queue_full: u64 = 0,
    outbound_ready_queue_drops: u64 = 0,
    cid_map_command_failures: u64 = 0,
    control_inbox_drops: u64 = 0,
    actor_loop_iterations: u64 = 0,
    actor_deadline_checks: u64 = 0,
    actor_deadlines_processed: u64 = 0,
    actor_deadline_late_ns_max: u64 = 0,
    actor_ready_inbound_packets: u64 = 0,
    actor_ready_control_commands: u64 = 0,
    actor_ready_stream_inbound: u64 = 0,
    actor_ready_stream_outbound: u64 = 0,
    actor_ready_accepted_stream_pop: u64 = 0,
    actor_ready_shutdown: u64 = 0,
    handshake_duration_ns: u64 = 0,
    write_batches: u64 = 0,
    write_packets: u64 = 0,
    /// GSO sends that coalesced >= 2 packets into one UDP_SEGMENT, and the
    /// packets they carried; write_packets still counts these (not a subset).
    write_gso_groups: u64 = 0,
    write_gso_segments: u64 = 0,
    write_bytes: u64 = 0,
    write_errors: u64 = 0,
    close_flush_errors: u64 = 0,
    stream_close_errors: u64 = 0,
    pacing_deferred: u64 = 0,
    send_on_path_calls: u64 = 0,
    send_on_path_done: u64 = 0,
    packets_recv: u64 = 0,
    packets_sent: u64 = 0,
    packets_lost: u64 = 0,
    packets_spurious_lost: u64 = 0,
    packets_retrans: u64 = 0,
    bytes_recv: u64 = 0,
    bytes_sent: u64 = 0,
    bytes_acked: u64 = 0,
    bytes_lost: u64 = 0,
    stream_retrans_bytes: u64 = 0,
    datagrams_recv: u64 = 0,
    datagrams_sent: u64 = 0,
    datagrams_dropped: u64 = 0,
    datagrams_oversized: u64 = 0,
    datagrams_pool_full: u64 = 0,
    datagrams_queue_full: u64 = 0,
    paths_count: u64 = 0,
    path_events_drained: u64 = 0,
    actor_fatal_errors: u64 = 0,
    last_actor_error: ActorError = .none,
    close_reason: CloseReason = .none,
    local_close_is_app: bool = false,
    local_close_error_code: u64 = 0,
    peer_close_is_app: bool = false,
    peer_close_error_code: u64 = 0,
    reset_stream_count_local: u64 = 0,
    reset_stream_count_remote: u64 = 0,
    stopped_stream_count_local: u64 = 0,
    stopped_stream_count_remote: u64 = 0,
    data_blocked_sent_count: u64 = 0,
    stream_data_blocked_sent_count: u64 = 0,
    data_blocked_recv_count: u64 = 0,
    stream_data_blocked_recv_count: u64 = 0,
    active_path: ?PathStats = null,
    last_updated_mono_ns: u64 = 0,

    /// Overwrite the quiche-sourced subset of `out` in place. Actor-tracked
    /// fields are not touched; safe to call repeatedly on a running snapshot.
    pub fn applyQuicheFields(out: *ConnectionStats, conn: *const quiche.quiche_conn, now_mono_ns: u64) void {
        var raw: quiche.quiche_stats = undefined;
        quiche.quiche_conn_stats(conn, &raw);

        out.packets_recv = raw.recv;
        out.packets_sent = raw.sent;
        out.packets_lost = raw.lost;
        out.packets_spurious_lost = raw.spurious_lost;
        out.packets_retrans = raw.retrans;
        out.bytes_recv = raw.recv_bytes;
        out.bytes_sent = raw.sent_bytes;
        out.bytes_acked = raw.acked_bytes;
        out.bytes_lost = raw.lost_bytes;
        out.stream_retrans_bytes = raw.stream_retrans_bytes;
        out.datagrams_recv = raw.dgram_recv;
        out.datagrams_sent = raw.dgram_sent;
        out.paths_count = raw.paths_count;
        out.reset_stream_count_local = raw.reset_stream_count_local;
        out.reset_stream_count_remote = raw.reset_stream_count_remote;
        out.stopped_stream_count_local = raw.stopped_stream_count_local;
        out.stopped_stream_count_remote = raw.stopped_stream_count_remote;
        out.data_blocked_sent_count = raw.data_blocked_sent_count;
        out.stream_data_blocked_sent_count = raw.stream_data_blocked_sent_count;
        out.data_blocked_recv_count = raw.data_blocked_recv_count;
        out.stream_data_blocked_recv_count = raw.stream_data_blocked_recv_count;
        out.last_updated_mono_ns = now_mono_ns;

        out.active_path = null;
        var i: usize = 0;
        while (i < raw.paths_count) : (i += 1) {
            var path: quiche.quiche_path_stats = undefined;
            if (quiche.quiche_conn_path_stats(conn, i, &path) == 0 and path.active) {
                out.active_path = .{
                    .rtt_ns = path.rtt,
                    .min_rtt_ns = path.min_rtt,
                    .max_rtt_ns = path.max_rtt,
                    .rttvar_ns = path.rttvar,
                    .cwnd = path.cwnd,
                    .pmtu = path.pmtu,
                    .delivery_rate = path.delivery_rate,
                    .max_bandwidth = path.max_bandwidth,
                    .total_pto_count = path.total_pto_count,
                    .sent_bytes = path.sent_bytes,
                    .recv_bytes = path.recv_bytes,
                    .lost_bytes = path.lost_bytes,
                    .stream_retrans_bytes = path.stream_retrans_bytes,
                };
                break;
            }
        }
    }

    /// Construct a fresh ConnectionStats with quiche-sourced fields populated
    /// and everything else at default. Used for initial-state snapshots.
    pub fn fromQuiche(conn: *const quiche.quiche_conn, now_mono_ns: u64) ConnectionStats {
        var out: ConnectionStats = .{};
        applyQuicheFields(&out, conn, now_mono_ns);
        return out;
    }
};

pub const Publisher = struct {
    lock: std.Io.Mutex = .init,
    published: ConnectionStats = .{},

    pub fn init(initial: ConnectionStats) Publisher {
        return .{ .published = initial };
    }

    /// Logically a const read (callers hold the connection by const ref) but
    /// must take the lock — interior mutability. `@constCast` is safe: it never
    /// escapes and the `Publisher` is always heap-allocated and mutable.
    pub fn load(p: *const Publisher, io: std.Io) ConnectionStats {
        const mutable: *Publisher = @constCast(p);
        mutable.lock.lockUncancelable(io);
        defer mutable.lock.unlock(io);
        return mutable.published;
    }

    pub fn publish(p: *Publisher, io: std.Io, stats: ConnectionStats) void {
        p.lock.lockUncancelable(io);
        defer p.lock.unlock(io);
        p.published = stats;
    }
};

const testing = std.testing;

test "classifyHandshakeFailure: peer-identity rejection wins over a close code" {
    // Even with a peer close code present, an actor-recorded verify failure is
    // the real cause and must take precedence.
    const f = classifyHandshakeFailure(.{
        .last_actor_error = .peer_verify_failed,
        .peer_close_error_code = 0x12a,
    });
    try testing.expectEqual(HandshakeFailure.Kind.peer_verify_failed, f.kind);
}

test "classifyHandshakeFailure: handshake and idle deadlines map to timeout" {
    try testing.expectEqual(HandshakeFailure.Kind.timeout, classifyHandshakeFailure(.{ .close_reason = .handshake_timeout }).kind);
    try testing.expectEqual(HandshakeFailure.Kind.timeout, classifyHandshakeFailure(.{ .close_reason = .idle_timeout }).kind);
}

test "classifyHandshakeFailure: peer CRYPTO_ERROR decodes to crypto_error + TLS alert byte" {
    // 0x12a = CRYPTO_ERROR base (0x100) + TLS alert 0x2a (certificate_unknown).
    const f = classifyHandshakeFailure(.{ .peer_close_error_code = 0x12a });
    try testing.expectEqual(HandshakeFailure.Kind.crypto_error, f.kind);
    try testing.expectEqual(@as(u64, 0x12a), f.error_code);
    try testing.expect(f.from_peer);
    try testing.expectEqual(@as(u8, 0x2a), @as(u8, @truncate(f.error_code & 0xff)));
}

test "classifyHandshakeFailure: transport-parameter error maps to transport_error" {
    // 0x8 = TRANSPORT_PARAMETER_ERROR, surfaced via our local close.
    const f = classifyHandshakeFailure(.{ .local_close_error_code = 0x8 });
    try testing.expectEqual(HandshakeFailure.Kind.transport_error, f.kind);
    try testing.expectEqual(@as(u64, 0x8), f.error_code);
    try testing.expect(!f.from_peer);
}

test "classifyHandshakeFailure: application close codes are not handshake faults" {
    const f = classifyHandshakeFailure(.{ .peer_close_is_app = true, .peer_close_error_code = 0x42 });
    try testing.expectEqual(HandshakeFailure.Kind.unknown, f.kind);
}

test "classifyHandshakeFailure: local I/O fault maps to transport_error" {
    try testing.expectEqual(HandshakeFailure.Kind.transport_error, classifyHandshakeFailure(.{ .last_actor_error = .quiche_recv_failed }).kind);
}

test "classifyHandshakeFailure: a plain drain/close maps to closed" {
    try testing.expectEqual(HandshakeFailure.Kind.closed, classifyHandshakeFailure(.{ .close_reason = .draining }).kind);
}

test "classifyHandshakeFailure: a fatal local actor fault maps to transport_error" {
    // .actor_error faults other than step-1/step-4 cases (e.g. .concurrent_failed,
    // .address_invalid) are local transport faults, not opaque unknowns.
    try testing.expectEqual(HandshakeFailure.Kind.transport_error, classifyHandshakeFailure(.{ .close_reason = .actor_error, .last_actor_error = .concurrent_failed }).kind);
    try testing.expectEqual(HandshakeFailure.Kind.transport_error, classifyHandshakeFailure(.{ .close_reason = .actor_error, .last_actor_error = .address_invalid }).kind);
}

test "classifyHandshakeFailure: nothing attributable maps to unknown" {
    try testing.expectEqual(HandshakeFailure.Kind.unknown, classifyHandshakeFailure(.{}).kind);
}
