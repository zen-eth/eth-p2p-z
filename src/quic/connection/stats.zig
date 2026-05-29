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
    unknown,

    pub fn fromError(err: anyerror) ActorError {
        return switch (err) {
            error.AddressInvalid => .address_invalid,
            error.QuicheSendFailed => .quiche_send_failed,
            error.QuicheRecvFailed => .quiche_recv_failed,
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
    actor_error,
    idle_timeout,
    peer_error,
    local_error,
    draining,
    closed,
};

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

    /// Reads the latest published snapshot. Logically a const read, but it must
    /// take the lock — classic interior mutability, so we keep the `*const`
    /// receiver (callers hold the connection by const ref) and `@constCast` only
    /// to acquire/release the mutex. Safe: the cast never escapes and the
    /// underlying `Publisher` is always heap-allocated and mutable.
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
