const std = @import("std");
const quiche = @import("quiche").c;
const ssl = @import("ssl").c;
const connection_actor = @import("connection/actor.zig");
const packet_route = @import("io/packet_route.zig");
const tls = @import("../security/tls.zig");

pub const ConfigError = error{ QuicheConfigFailed, InvalidOptions };
pub const default_handshake_timeout_ns: u64 = 10 * std.time.ns_per_s;

pub const CongestionControl = enum {
    reno,
    cubic,
    bbr2,

    fn toQuiche(cc: CongestionControl) c_uint {
        return switch (cc) {
            .reno => quiche.QUICHE_CC_RENO,
            .cubic => quiche.QUICHE_CC_CUBIC,
            .bbr2 => quiche.QUICHE_CC_BBR2_GCONGESTION,
        };
    }
};

/// QUIC transport-parameter and quiche-config knobs that are visible to the
/// peer (idle timeout, flow control, congestion control, etc.). Splitting
/// these out from the local-only actor/endpoint settings keeps two distinct
/// concerns separate: things the peer negotiates against us, vs. things only
/// our own IO loop sees.
pub const TransportOptions = struct {
    max_idle_timeout_ms: u64 = 30_000,
    initial_max_data: u64 = 1024 * 1024,
    initial_max_stream_data_bidi_local: u64 = 256 * 1024,
    initial_max_stream_data_bidi_remote: u64 = 256 * 1024,
    initial_max_stream_data_uni: u64 = 128 * 1024,
    initial_max_streams_bidi: u64 = 100,
    /// libp2p does not currently use unidirectional streams; expose the knob
    /// anyway so future protocols (HTTP/3-style control streams, etc.) can
    /// enable them without a forked build.
    initial_max_streams_uni: u64 = 0,
    max_recv_udp_payload_size: usize = 1350,
    max_send_udp_payload_size: usize = 1350,
    active_connection_id_limit: u64 = 2,
    /// Must stay true until direct socket readers support rebinding the peer
    /// address on quiche path migration.
    disable_active_migration: bool = true,
    enable_pacing: bool = true,
    /// RFC 9000 §8.1: the server MUST NOT send more than `factor *` bytes
    /// before validating the client's address. Quiche defaults to 3 already;
    /// surface it here so operators can tighten or relax for tunnels.
    max_amplification_factor: u64 = 3,
    /// Maximum delay before sending an ACK frame, in milliseconds. Lower
    /// values reduce loss-recovery latency at the cost of more ACK traffic.
    max_ack_delay_ms: u64 = 25,
    /// Congestion control algorithm. tokio-quiche defaults to BBR2; we use
    /// CUBIC because libp2p sees a wider range of network conditions and
    /// CUBIC is friendlier to non-BBR peers.
    congestion_control: CongestionControl = .cubic,
    /// HyStart++ (RFC 9406) — exits slow-start earlier on RTT growth.
    enable_hystart: bool = true,
    /// Maximum size of the connection-level receive flow control window
    /// (auto-tuned upwards from `initial_max_data`). Caps memory usage on
    /// long-lived high-BDP connections.
    max_connection_window: u64 = 24 * 1024 * 1024,
    /// Maximum size of a per-stream receive flow control window (auto-tuned
    /// upwards from `initial_max_stream_data_*`).
    max_stream_window: u64 = 16 * 1024 * 1024,
    /// Initial congestion window in packets. RFC 9002 recommends 10.
    initial_congestion_window_packets: usize = 10,
    /// Maximum send rate in bytes per second when pacing is enabled. 0
    /// disables the cap (quiche pacer chooses based on cwnd/RTT).
    max_pacing_rate_bps: u64 = 0,
    /// When true, retired DCIDs are not reused for outgoing path probes,
    /// which forces fresh CIDs after migration. Default matches quiche.
    disable_dcid_reuse: bool = false,
    /// Delegate certificate-chain verification to quiche/BoringSSL. libp2p
    /// peer identity verification still runs after the TLS handshake.
    verify_peer: bool = false,
    /// Send QUIC GREASE values. Matches tokio-quiche/quiche default behavior.
    grease: bool = true,
    /// Enable TLS key logging for local diagnostics.
    log_keys: bool = false,
    /// Enable QUIC 0-RTT support in quiche. Application code must still decide
    /// whether replayable data is acceptable before using it.
    enable_early_data: bool = false,
    /// ACK delay exponent transport parameter. RFC 9000 default is 3.
    ack_delay_exponent: u64 = 3,
    /// Optional stateless reset token. Server-only in effect: quiche reads this
    /// from the config only for `is_server` connections (`let reset_token = if
    /// is_server { ... } else { None }`), so on our dual-role endpoint's shared
    /// config it is applied to accepted connections and ignored for outbound dials.
    stateless_reset_token: ?[16]u8 = null,
    /// Quiche CUBIC idle restart behavior toggle exposed for parity.
    enable_cubic_idle_restart_fix: bool = false,
};

/// Per-connection actor tuning: inbound packet routing buffers, outbound
/// batching, per-stream queues, and datagram pools. These never reach the
/// wire — they only shape how our own actor handles packets it has already
/// received or queued for send.
///
/// The queue-length knobs here ARE the cross-fiber backpressure budget — the
/// bounded credit between producer and consumer fibers. They are validated
/// non-zero and must stay >= 1 (model_critical; see docs/quiche-refactor-plan.md §0).
pub const ActorOptions = struct {
    /// Maximum queued inbound UDP payload bytes per connection. Packet metadata
    /// and preallocated queue slots are bounded separately by queue length.
    inbound_packet_ring_bytes: usize = 384 * 1024,
    inbound_packet_queue_len: usize = packet_route.default_packet_queue_len,
    outbound_batch_size: usize = 32,
    control_queue_len: usize = 256,
    stream_inbound_queue_bytes: usize = 48 * 1024,
    stream_outbound_queue_bytes: usize = 48 * 1024,
    stream_inbound_quantum_bytes: usize = 32 * 1024,
    stream_outbound_quantum_bytes: usize = 32 * 1024,
    stream_accept_queue_len: usize = 64,
    recv_datagram_slots: usize = 48,
    send_datagram_queue_len: usize = 48,
    /// Capacity of the per-connection MPSC queue that handles use to signal
    /// "this stream has new outbound bytes" to the actor. Sized to cover the
    /// expected concurrent-stream count; on overflow the actor falls back to
    /// a full stream scan, which is correct but slower. Must be ≥ 1.
    outbound_pending_queue_len: usize = 256,
};

/// Endpoint/router-level knobs: the listener accept backlog and the
/// per-connection handshake deadline.
pub const EndpointOptions = struct {
    connection_accept_queue_len: usize = 64,
    handshake_timeout_ns: u64 = default_handshake_timeout_ns,
    enable_udp_gro: bool = true,
    /// Opt-in because packet-info rewrites quiche's local path address on
    /// wildcard sockets; transparent-proxy users can enable it together with
    /// source-address send control.
    enable_pktinfo: bool = false,
    /// Opt-in for transparent proxy deployments where the original
    /// destination address is meaningful to quiche path selection.
    enable_orig_dst: bool = false,
    enable_rx_timestamps: bool = true,
    socket_mark: ?u32 = null,
};

pub const Options = struct {
    transport: TransportOptions = .{},
    actor: ActorOptions = .{},
    endpoint: EndpointOptions = .{},
};

/// Returns `opts` unchanged on success. Consumers receive an `Options` whose
/// invariants have been checked; the value still carries the original defaults
/// so further field access reads exactly what the caller (or `.{}`) provided.
pub fn validateOptions(opts: Options) ConfigError!Options {
    try validateTransport(opts.transport);
    try validateActor(opts.actor);
    try validateEndpoint(opts.endpoint);
    return opts;
}

fn validateTransport(opts: TransportOptions) ConfigError!void {
    if (opts.max_recv_udp_payload_size > packet_route.max_udp_payload_len) return error.InvalidOptions;
    if (opts.max_send_udp_payload_size > connection_actor.max_flush_packet_len) return error.InvalidOptions;
    // RFC 9000 §14.1: a QUIC endpoint must support a 1200-byte UDP payload —
    // Initials are padded to >=1200, and smaller ones are dropped by peers (and
    // by our own router `min_initial_packet_len`), so sub-1200 sizes silently
    // break the handshake. (>=1200 also subsumes the non-zero requirement.)
    if (opts.max_recv_udp_payload_size < 1200) return error.InvalidOptions;
    if (opts.max_send_udp_payload_size < 1200) return error.InvalidOptions;
    if (opts.max_amplification_factor == 0) return error.InvalidOptions;
    // RFC 9000 §13.2.1 caps `max_ack_delay` at 2^14 - 1 (16383ms).
    if (opts.max_ack_delay_ms >= (1 << 14)) return error.InvalidOptions;
    // RFC 9000 §18.2 caps ack_delay_exponent at 20.
    if (opts.ack_delay_exponent > 20) return error.InvalidOptions;
    if (opts.initial_congestion_window_packets == 0) return error.InvalidOptions;
    if (!opts.disable_active_migration) return error.InvalidOptions;
    // The receive auto-tuning ceiling cannot be smaller than the starting
    // window or quiche's flow control credit accounting can underflow.
    if (opts.max_connection_window < opts.initial_max_data) return error.InvalidOptions;
    const max_initial_stream = @max(@max(
        opts.initial_max_stream_data_bidi_local,
        opts.initial_max_stream_data_bidi_remote,
    ), opts.initial_max_stream_data_uni);
    if (opts.max_stream_window < max_initial_stream) return error.InvalidOptions;
}

fn validateActor(opts: ActorOptions) ConfigError!void {
    if (opts.outbound_batch_size == 0 or opts.outbound_batch_size > connection_actor.max_outbound_batch_size) return error.InvalidOptions;
    if (opts.inbound_packet_queue_len == 0) return error.InvalidOptions;
    if (opts.inbound_packet_ring_bytes == 0) return error.InvalidOptions;
    if (opts.stream_inbound_queue_bytes == 0 or opts.stream_outbound_queue_bytes == 0) return error.InvalidOptions;
    if (opts.stream_inbound_quantum_bytes == 0 or opts.stream_outbound_quantum_bytes == 0) return error.InvalidOptions;
    if (opts.stream_accept_queue_len == 0) return error.InvalidOptions;
    if (opts.control_queue_len == 0) return error.InvalidOptions;
    if (opts.recv_datagram_slots == 0 or opts.send_datagram_queue_len == 0) return error.InvalidOptions;
    if (opts.outbound_pending_queue_len == 0) return error.InvalidOptions;
}

fn validateEndpoint(opts: EndpointOptions) ConfigError!void {
    if (opts.connection_accept_queue_len == 0) return error.InvalidOptions;
    if (opts.handshake_timeout_ns == 0) return error.InvalidOptions;
}

pub fn buildQuicheConfig(opts: Options) ConfigError!*quiche.quiche_config {
    const t = opts.transport;
    const w = opts.actor;
    const cfg = quiche.quiche_config_new(quiche.QUICHE_PROTOCOL_VERSION) orelse return error.QuicheConfigFailed;
    errdefer quiche.quiche_config_free(cfg);

    // ALPN must be passed in 8-bit-length-prefixed wire format per RFC 7301 §3.1.
    if (quiche.quiche_config_set_application_protos(cfg, tls.ALPN_PROTOS[0..].ptr, tls.ALPN_PROTOS.len) != 0) {
        return error.QuicheConfigFailed;
    }
    quiche.quiche_config_verify_peer(cfg, t.verify_peer);
    quiche.quiche_config_grease(cfg, t.grease);
    if (t.log_keys) quiche.quiche_config_log_keys(cfg);
    if (t.enable_early_data) quiche.quiche_config_enable_early_data(cfg);
    quiche.quiche_config_set_max_idle_timeout(cfg, t.max_idle_timeout_ms);
    quiche.quiche_config_set_max_amplification_factor(cfg, t.max_amplification_factor);
    quiche.quiche_config_set_ack_delay_exponent(cfg, t.ack_delay_exponent);
    quiche.quiche_config_set_max_ack_delay(cfg, t.max_ack_delay_ms);
    quiche.quiche_config_set_initial_max_data(cfg, t.initial_max_data);
    quiche.quiche_config_set_initial_max_stream_data_bidi_local(cfg, t.initial_max_stream_data_bidi_local);
    quiche.quiche_config_set_initial_max_stream_data_bidi_remote(cfg, t.initial_max_stream_data_bidi_remote);
    quiche.quiche_config_set_initial_max_stream_data_uni(cfg, t.initial_max_stream_data_uni);
    quiche.quiche_config_set_initial_max_streams_bidi(cfg, t.initial_max_streams_bidi);
    quiche.quiche_config_set_initial_max_streams_uni(cfg, t.initial_max_streams_uni);
    quiche.quiche_config_set_max_connection_window(cfg, t.max_connection_window);
    quiche.quiche_config_set_max_stream_window(cfg, t.max_stream_window);
    quiche.quiche_config_set_max_recv_udp_payload_size(cfg, t.max_recv_udp_payload_size);
    quiche.quiche_config_set_max_send_udp_payload_size(cfg, t.max_send_udp_payload_size);
    quiche.quiche_config_set_active_connection_id_limit(cfg, t.active_connection_id_limit);
    quiche.quiche_config_set_disable_active_migration(cfg, t.disable_active_migration);
    quiche.quiche_config_set_disable_dcid_reuse(cfg, t.disable_dcid_reuse);
    quiche.quiche_config_set_cc_algorithm(cfg, t.congestion_control.toQuiche());
    quiche.quiche_config_set_initial_congestion_window_packets(cfg, t.initial_congestion_window_packets);
    quiche.quiche_config_enable_hystart(cfg, t.enable_hystart);
    quiche.quiche_config_enable_pacing(cfg, t.enable_pacing);
    quiche.quiche_config_set_enable_cubic_idle_restart_fix(cfg, t.enable_cubic_idle_restart_fix);
    if (t.max_pacing_rate_bps != 0) {
        quiche.quiche_config_set_max_pacing_rate(cfg, t.max_pacing_rate_bps);
    }
    if (t.stateless_reset_token) |token| {
        quiche.quiche_config_set_stateless_reset_token(cfg, token[0..].ptr);
    }
    // Datagrams are always enabled for libp2p (the `true` is intentional, not an
    // Option); only the queue sizes are user-tunable via ActorOptions.
    quiche.quiche_config_enable_dgram(cfg, true, w.recv_datagram_slots, w.send_datagram_queue_len);
    return cfg;
}

test "builds quiche config" {
    const cfg = try buildQuicheConfig(try validateOptions(.{}));
    defer quiche.quiche_config_free(cfg);
}

test "rejects invalid actor options" {
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .outbound_batch_size = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .outbound_batch_size = connection_actor.max_outbound_batch_size + 1 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .inbound_packet_queue_len = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .inbound_packet_ring_bytes = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .stream_inbound_queue_bytes = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .stream_outbound_queue_bytes = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .stream_inbound_quantum_bytes = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .stream_outbound_quantum_bytes = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .stream_accept_queue_len = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .control_queue_len = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .recv_datagram_slots = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .send_datagram_queue_len = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .actor = .{ .outbound_pending_queue_len = 0 } }));
}

test "rejects invalid endpoint options" {
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .endpoint = .{ .connection_accept_queue_len = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .endpoint = .{ .handshake_timeout_ns = 0 } }));
}

test "separates listener backlog from stream accept capacity" {
    const opts = try validateOptions(.{
        .actor = .{ .stream_accept_queue_len = 7 },
        .endpoint = .{ .connection_accept_queue_len = 3 },
    });
    try std.testing.expectEqual(@as(usize, 7), opts.actor.stream_accept_queue_len);
    try std.testing.expectEqual(@as(usize, 3), opts.endpoint.connection_accept_queue_len);
}

test "rejects invalid transport options" {
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .max_recv_udp_payload_size = packet_route.max_udp_payload_len + 1 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .max_send_udp_payload_size = connection_actor.max_flush_packet_len + 1 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .max_recv_udp_payload_size = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .max_send_udp_payload_size = 0 } }));
    // RFC 9000 §14.1: reject just-below the 1200 floor; accept the boundary.
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .max_recv_udp_payload_size = 1199 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .max_send_udp_payload_size = 1199 } }));
    _ = try validateOptions(.{ .transport = .{ .max_recv_udp_payload_size = 1200, .max_send_udp_payload_size = 1200 } });
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .max_amplification_factor = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .max_ack_delay_ms = 1 << 14 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .ack_delay_exponent = 21 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .initial_congestion_window_packets = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .disable_active_migration = false } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .max_connection_window = 0 } }));
    try std.testing.expectError(error.InvalidOptions, validateOptions(.{ .transport = .{ .max_stream_window = 0 } }));
}

test "build quiche config with non-default tier 1+2 setters" {
    const cfg = try buildQuicheConfig(try validateOptions(.{
        .transport = .{
            .max_amplification_factor = 5,
            .max_ack_delay_ms = 50,
            .congestion_control = .bbr2,
            .enable_hystart = false,
            .initial_congestion_window_packets = 32,
            .max_pacing_rate_bps = 100 * 1024 * 1024,
            .disable_dcid_reuse = true,
            .initial_max_streams_uni = 4,
            .verify_peer = false,
            .grease = false,
            .log_keys = true,
            .enable_early_data = true,
            .ack_delay_exponent = 4,
            .stateless_reset_token = [_]u8{0x42} ** 16,
            .enable_cubic_idle_restart_fix = true,
        },
    }));
    defer quiche.quiche_config_free(cfg);
}

test "quiche accepts libp2p configured tls handles" {
    var fixture = try TlsConnFixture.init();
    defer fixture.deinit();
}

test "quiche tls handshake completes in memory" {
    var fixture = try TlsConnFixture.init();
    defer fixture.deinit();

    var i: usize = 0;
    while (i < 256 and !(quiche.quiche_conn_is_established(fixture.client_conn) and quiche.quiche_conn_is_established(fixture.server_conn))) : (i += 1) {
        _ = try pump(fixture.client_conn, fixture.server_conn, &fixture.client_addr, &fixture.server_addr);
        _ = try pump(fixture.server_conn, fixture.client_conn, &fixture.server_addr, &fixture.client_addr);
    }

    try std.testing.expect(quiche.quiche_conn_is_established(fixture.client_conn));
    try std.testing.expect(quiche.quiche_conn_is_established(fixture.server_conn));

    var cert_ptr: [*c]const u8 = null;
    var cert_len: usize = 0;
    quiche.quiche_conn_peer_cert(fixture.client_conn, &cert_ptr, &cert_len);
    try std.testing.expect(cert_ptr != null);
    try std.testing.expect(cert_len > 0);

    cert_ptr = null;
    cert_len = 0;
    quiche.quiche_conn_peer_cert(fixture.server_conn, &cert_ptr, &cert_len);
    try std.testing.expect(cert_ptr != null);
    try std.testing.expect(cert_len > 0);

    const ConnectionStats = @import("connection/mod.zig").ConnectionStats;
    const client_stats = ConnectionStats.fromQuiche(fixture.client_conn, 1);
    const server_stats = ConnectionStats.fromQuiche(fixture.server_conn, 1);
    try std.testing.expect(client_stats.packets_sent > 0);
    try std.testing.expect(client_stats.packets_recv > 0);
    try std.testing.expect(server_stats.packets_sent > 0);
    try std.testing.expect(server_stats.packets_recv > 0);
    try std.testing.expect(client_stats.active_path != null);
    try std.testing.expect(server_stats.active_path != null);
}

test "quiche stream and datagram frames move in memory" {
    var fixture = try TlsConnFixture.init();
    defer fixture.deinit();
    try fixture.handshake();

    const payload = "hello over stream";
    var err_code: u64 = 0;
    const written = quiche.quiche_conn_stream_send(
        fixture.client_conn,
        0,
        payload.ptr,
        payload.len,
        false,
        &err_code,
    );
    try std.testing.expectEqual(@as(isize, payload.len), written);

    _ = try pump(fixture.client_conn, fixture.server_conn, &fixture.client_addr, &fixture.server_addr);

    var recv_buf: [64]u8 = undefined;
    var fin = false;
    const read = quiche.quiche_conn_stream_recv(
        fixture.server_conn,
        0,
        &recv_buf,
        recv_buf.len,
        &fin,
        &err_code,
    );
    try std.testing.expectEqual(@as(isize, payload.len), read);
    try std.testing.expectEqualStrings(payload, recv_buf[0..@intCast(read)]);

    const dgram = "hello datagram";
    try std.testing.expect(quiche.quiche_conn_dgram_max_writable_len(fixture.client_conn) >= dgram.len);
    const dgram_written = quiche.quiche_conn_dgram_send(fixture.client_conn, dgram.ptr, dgram.len);
    try std.testing.expectEqual(@as(isize, dgram.len), dgram_written);
    _ = try pump(fixture.client_conn, fixture.server_conn, &fixture.client_addr, &fixture.server_addr);

    const dgram_front_len = quiche.quiche_conn_dgram_recv_front_len(fixture.server_conn);
    try std.testing.expectEqual(@as(isize, dgram.len), dgram_front_len);
    var dgram_buf: [64]u8 = undefined;
    const dgram_read = quiche.quiche_conn_dgram_recv(fixture.server_conn, &dgram_buf, dgram_buf.len);
    try std.testing.expectEqual(@as(isize, dgram.len), dgram_read);
    try std.testing.expectEqualStrings(dgram, dgram_buf[0..@intCast(dgram_read)]);
}

const TlsConnFixture = struct {
    client_host: @import("../identity.zig").KeyPair,
    server_host: @import("../identity.zig").KeyPair,
    client_tls: @import("../security/tls.zig").Context,
    server_tls: @import("../security/tls.zig").Context,
    cfg: *quiche.quiche_config,
    client_conn: *quiche.quiche_conn,
    server_conn: *quiche.quiche_conn,
    client_addr: std.posix.sockaddr.in,
    server_addr: std.posix.sockaddr.in,

    fn init() !TlsConnFixture {
        const identity = @import("../identity.zig");

        var client_host = try identity.KeyPair.generate(.ED25519);
        errdefer client_host.deinit();
        var server_host = try identity.KeyPair.generate(.ED25519);
        errdefer server_host.deinit();

        var client_tls = try tls.Context.create(
            std.testing.allocator,
            &client_host,
            .ED25519,
            @ptrCast(&client_host),
            identity.signWithKeyPair,
        );
        errdefer client_tls.deinit();

        var server_tls = try tls.Context.create(
            std.testing.allocator,
            &server_host,
            .ED25519,
            @ptrCast(&server_host),
            identity.signWithKeyPair,
        );
        errdefer server_tls.deinit();

        const client_ssl = try client_tls.newSsl();
        errdefer @import("ssl").c.SSL_free(client_ssl);
        const server_ssl = try server_tls.newSsl();
        errdefer @import("ssl").c.SSL_free(server_ssl);

        const cfg = try buildQuicheConfig(try validateOptions(.{}));
        errdefer quiche.quiche_config_free(cfg);

        var client_scid: [quiche.QUICHE_MAX_CONN_ID_LEN]u8 = undefined;
        var server_scid: [quiche.QUICHE_MAX_CONN_ID_LEN]u8 = undefined;
        fillRandom(&client_scid);
        fillRandom(&server_scid);

        var client_addr = sockaddrIn(9000, .{ 127, 0, 0, 1 });
        var server_addr = sockaddrIn(9001, .{ 127, 0, 0, 1 });

        const client_conn = quiche.quiche_conn_new_with_tls(
            &client_scid,
            client_scid.len,
            null,
            0,
            @ptrCast(&client_addr),
            @sizeOf(std.posix.sockaddr.in),
            @ptrCast(&server_addr),
            @sizeOf(std.posix.sockaddr.in),
            cfg,
            client_ssl,
            false,
        ) orelse return error.QuicheClientFailed;
        errdefer quiche.quiche_conn_free(client_conn);

        const server_conn = quiche.quiche_conn_new_with_tls(
            &server_scid,
            server_scid.len,
            null,
            0,
            @ptrCast(&server_addr),
            @sizeOf(std.posix.sockaddr.in),
            @ptrCast(&client_addr),
            @sizeOf(std.posix.sockaddr.in),
            cfg,
            server_ssl,
            true,
        ) orelse return error.QuicheServerFailed;

        return .{
            .client_host = client_host,
            .server_host = server_host,
            .client_tls = client_tls,
            .server_tls = server_tls,
            .cfg = cfg,
            .client_conn = client_conn,
            .server_conn = server_conn,
            .client_addr = client_addr,
            .server_addr = server_addr,
        };
    }

    fn deinit(f: *TlsConnFixture) void {
        quiche.quiche_conn_free(f.server_conn);
        quiche.quiche_conn_free(f.client_conn);
        quiche.quiche_config_free(f.cfg);
        f.server_tls.deinit();
        f.client_tls.deinit();
        f.server_host.deinit();
        f.client_host.deinit();
    }

    fn handshake(f: *TlsConnFixture) !void {
        var i: usize = 0;
        while (i < 256 and !(quiche.quiche_conn_is_established(f.client_conn) and quiche.quiche_conn_is_established(f.server_conn))) : (i += 1) {
            _ = try pump(f.client_conn, f.server_conn, &f.client_addr, &f.server_addr);
            _ = try pump(f.server_conn, f.client_conn, &f.server_addr, &f.client_addr);
        }

        if (!quiche.quiche_conn_is_established(f.client_conn)) return error.HandshakeFailed;
        if (!quiche.quiche_conn_is_established(f.server_conn)) return error.HandshakeFailed;
    }
};

fn pump(
    src: *quiche.quiche_conn,
    dst: *quiche.quiche_conn,
    src_addr: *std.posix.sockaddr.in,
    dst_addr: *std.posix.sockaddr.in,
) !usize {
    var moved: usize = 0;

    while (true) {
        var out: [1500]u8 = undefined;
        var send_info: quiche.quiche_send_info = undefined;
        const sent = quiche.quiche_conn_send(src, &out, out.len, &send_info);
        if (sent == quiche.QUICHE_ERR_DONE) break;
        if (sent < 0) return error.QuicheSendFailed;

        var recv_info = quiche.quiche_recv_info{
            .from = @ptrCast(src_addr),
            .from_len = @sizeOf(std.posix.sockaddr.in),
            .to = @ptrCast(dst_addr),
            .to_len = @sizeOf(std.posix.sockaddr.in),
        };
        const received = quiche.quiche_conn_recv(dst, &out, @intCast(sent), &recv_info);
        if (received < 0 and received != quiche.QUICHE_ERR_DONE) return error.QuicheRecvFailed;
        moved += 1;
    }

    return moved;
}

fn sockaddrIn(port: u16, ip: [4]u8) std.posix.sockaddr.in {
    return .{
        .port = std.mem.nativeToBig(u16, port),
        .addr = @as(u32, ip[0]) |
            (@as(u32, ip[1]) << 8) |
            (@as(u32, ip[2]) << 16) |
            (@as(u32, ip[3]) << 24),
    };
}

fn fillRandom(buf: []u8) void {
    if (ssl.RAND_bytes(buf.ptr, buf.len) != 1) @panic("BoringSSL RAND_bytes failed");
}
