const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;

const ssl = @import("ssl");
const tls = @import("../../security/tls.zig");
const PeerId = @import("peer_id").PeerId;
const keys = @import("peer_id").keys;

const builtin = @import("builtin");

const log = std.log.scoped(.quic_engine);

const lsquic = @cImport({
    @cInclude("lsquic.h");
    @cInclude("lsquic_types.h");
});

/// Global SSL_CTX ex_data index for storing CertVerifyCtx pointer.
/// Initialized once via SSL_CTX_get_ex_new_index on first engine creation.
var g_ssl_ctx_ex_idx: c_int = -1;

// ── Event types communicated via Io.Queue ──────────────────────────────

/// Event pushed when a connection completes handshake.
pub const ConnEvent = struct {
    conn: *QuicConnection,
};

/// Event pushed when a stream becomes available on a connection.
pub const StreamEvent = struct {
    stream: *QuicStream,
};

/// Event pushed when data is available on a stream.
pub const ReadEvent = struct {
    data: []const u8,
    owned_buf: []u8, // allocated buffer, caller must free
};

// ── QuicStream ─────────────────────────────────────────────────────────

/// QUIC stream backed by lsquic. Reads/writes suspend via Io.Queue.
pub const QuicStream = struct {
    allocator: Allocator,
    lsquic_stream: ?*lsquic.lsquic_stream_t,
    conn: *QuicConnection,
    read_queue_buf: [16]ReadEvent,
    read_queue: Io.Queue(ReadEvent),
    closed: bool,
    /// Leftover data from a previous ReadEvent when caller's buffer was too small.
    leftover_buf: ?[]u8 = null,
    leftover_offset: usize = 0,

    pub fn init(allocator: Allocator, ls: *lsquic.lsquic_stream_t, conn: *QuicConnection) !*QuicStream {
        const self = try allocator.create(QuicStream);
        self.* = .{
            .allocator = allocator,
            .lsquic_stream = ls,
            .conn = conn,
            .read_queue_buf = undefined,
            .read_queue = undefined,
            .closed = false,
        };
        self.read_queue = Io.Queue(ReadEvent).init(&self.read_queue_buf);
        lsquic.lsquic_stream_set_ctx(ls, @ptrCast(self));
        return self;
    }

    pub fn read(self: *QuicStream, io: Io, buf: []u8) anyerror!usize {
        if (self.closed) return error.StreamClosed;

        // Serve from leftover data first
        if (self.leftover_buf) |lb| {
            const remaining = lb.len - self.leftover_offset;
            const len = @min(buf.len, remaining);
            @memcpy(buf[0..len], lb[self.leftover_offset..][0..len]);
            if (len == remaining) {
                // Consumed all leftover data, free the buffer
                self.allocator.free(lb);
                self.leftover_buf = null;
                self.leftover_offset = 0;
            } else {
                self.leftover_offset += len;
            }
            return len;
        }

        const event = self.read_queue.getOne(io) catch |err| switch (err) {
            error.Closed => return 0, // EOF — peer closed the stream
            error.Canceled => return error.StreamClosed,
        };
        const len = @min(buf.len, event.data.len);
        @memcpy(buf[0..len], event.data[0..len]);
        if (len < event.data.len) {
            // Partial read — save remaining data for next read call
            self.leftover_buf = event.owned_buf;
            self.leftover_offset = len;
        } else {
            // Consumed everything, free immediately
            self.allocator.free(event.owned_buf);
        }
        return len;
    }

    pub fn write(self: *QuicStream, _: Io, data: []const u8) anyerror!usize {
        if (self.closed) return error.StreamClosed;
        const ls = self.lsquic_stream orelse return error.StreamClosed;
        const written = lsquic.lsquic_stream_write(ls, data.ptr, data.len);
        if (written < 0) return error.WriteFailed;
        _ = lsquic.lsquic_stream_flush(ls);
        return @intCast(written);
    }

    pub fn close(self: *QuicStream, _: Io) void {
        if (self.lsquic_stream) |ls| {
            // Tell lsquic to close the stream. This will trigger onStreamClose
            // which handles cleanup (closing read_queue, freeing leftover, destroying self).
            if (!self.conn.closed) {
                _ = lsquic.lsquic_stream_close(ls);
            }
        }
    }

    pub fn deinit(self: *QuicStream) void {
        // Only for manual cleanup when onStreamClose won't fire (e.g. error paths before lsquic knows about the stream)
        if (self.lsquic_stream) |ls| {
            if (!self.conn.closed) {
                _ = lsquic.lsquic_stream_close(ls);
            }
        }
        if (self.leftover_buf) |lb| {
            self.allocator.free(lb);
        }
        self.allocator.destroy(self);
    }
};

// ── QuicConnection ─────────────────────────────────────────────────────

/// Signal pushed by `onHskDone` to wake up `waitHandshake`.
pub const HandshakeResult = enum { ok, failed };

/// QUIC connection backed by lsquic. Stream accept/open via Io.Queue.
pub const QuicConnection = struct {
    allocator: Allocator,
    lsquic_conn: ?*lsquic.lsquic_conn_t,
    engine: *QuicEngine,
    stream_queue_buf: [16]StreamEvent,
    stream_queue: Io.Queue(StreamEvent),
    outbound_stream_queue_buf: [16]StreamEvent,
    outbound_stream_queue: Io.Queue(StreamEvent),
    hsk_queue_buf: [1]HandshakeResult,
    hsk_queue: Io.Queue(HandshakeResult),
    peer_id: ?PeerId,
    hsk_completed: bool,
    closed: bool,

    pub fn init(allocator: Allocator, lc: ?*lsquic.lsquic_conn_t, engine: *QuicEngine) !*QuicConnection {
        const self = try allocator.create(QuicConnection);
        self.* = .{
            .allocator = allocator,
            .lsquic_conn = lc,
            .engine = engine,
            .stream_queue_buf = undefined,
            .stream_queue = undefined,
            .outbound_stream_queue_buf = undefined,
            .outbound_stream_queue = undefined,
            .hsk_queue_buf = undefined,
            .hsk_queue = undefined,
            .peer_id = null,
            .hsk_completed = false,
            .closed = false,
        };
        self.stream_queue = Io.Queue(StreamEvent).init(&self.stream_queue_buf);
        self.outbound_stream_queue = Io.Queue(StreamEvent).init(&self.outbound_stream_queue_buf);
        self.hsk_queue = Io.Queue(HandshakeResult).init(&self.hsk_queue_buf);
        if (lc) |c| lsquic.lsquic_conn_set_ctx(c, @ptrCast(self));
        return self;
    }

    pub fn openStream(self: *QuicConnection, io: Io) !*QuicStream {
        if (self.closed) return error.ConnectionClosed;
        const lc = self.lsquic_conn orelse return error.ConnectionClosed;
        lsquic.lsquic_conn_make_stream(lc);
        // Let the background timer loop call processEngine to create the stream
        // via onNewStream. Calling processEngine synchronously here can crash
        // inside lsquic's SSL post-handshake processing when the crypto stream
        // has pending events and the SSL session state is still settling.
        // The suspend on outbound_stream_queue.getOne yields to the background loops.
        // Wait for the stream event from onNewStream callback
        const event = self.outbound_stream_queue.getOne(io) catch |err| switch (err) {
            error.Closed => return error.ConnectionClosed,
            error.Canceled => return error.ConnectionClosed,
        };
        return event.stream;
    }

    pub fn acceptStream(self: *QuicConnection, io: Io) !*QuicStream {
        if (self.closed) return error.ConnectionClosed;
        const event = self.stream_queue.getOne(io) catch |err| switch (err) {
            error.Closed => return error.ConnectionClosed,
            error.Canceled => return error.ConnectionClosed,
        };
        return event.stream;
    }

    pub fn close(self: *QuicConnection, io: Io) void {
        if (self.lsquic_conn) |lc| {
            // Clear conn context before closing so lsquic doesn't assert on destroy
            lsquic.lsquic_conn_set_ctx(lc, null);
            lsquic.lsquic_conn_close(lc);
            self.lsquic_conn = null;
        }
        self.closed = true;
        self.stream_queue.close(io);
        self.outbound_stream_queue.close(io);
        self.hsk_queue.close(io);
    }

    pub fn remotePeerId(self: *const QuicConnection) ?PeerId {
        return self.peer_id;
    }

    /// Suspend until the TLS handshake completes (or fails).
    /// Returns the verified remote peer ID on success.
    pub fn waitHandshake(self: *QuicConnection, io: Io) !PeerId {
        // If already completed (e.g. server path), return immediately
        if (self.hsk_completed) {
            return self.peer_id orelse error.HandshakeFailed;
        }
        const result = self.hsk_queue.getOne(io) catch |err| switch (err) {
            error.Closed => return error.HandshakeFailed,
            error.Canceled => return error.HandshakeFailed,
        };
        return switch (result) {
            .ok => self.peer_id orelse error.HandshakeFailed,
            .failed => error.HandshakeFailed,
        };
    }

    pub fn deinit(self: *QuicConnection) void {
        if (self.lsquic_conn) |lc| {
            lsquic.lsquic_conn_set_ctx(lc, null);
            lsquic.lsquic_conn_close(lc);
        }
        self.allocator.destroy(self);
    }
};

// ── CertVerifyCtx ──────────────────────────────────────────────────────

/// Replaces the threadlocal g_peer_cert hack. Stores verified peer info
/// keyed by connection pointer, passed via ea_verify_ctx.
/// Since lsquic callbacks are single-threaded, `last_verified` holds the most
/// recent successful verification result. `onHskDone` consumes it.
pub const CertVerifyCtx = struct {
    allocator: Allocator,
    last_verified: ?VerifiedPeer,

    pub const VerifiedPeer = struct {
        peer_id: PeerId,
        host_pubkey: keys.PublicKey,
    };

    pub fn init(allocator: Allocator) CertVerifyCtx {
        return .{
            .allocator = allocator,
            .last_verified = null,
        };
    }

    pub fn deinit(self: *CertVerifyCtx) void {
        if (self.last_verified) |vp| {
            if (vp.host_pubkey.data) |d| self.allocator.free(d);
        }
    }

    pub fn storeVerified(self: *CertVerifyCtx, peer: VerifiedPeer) void {
        // Free any unconsumed previous result
        if (self.last_verified) |old| {
            if (old.host_pubkey.data) |d| self.allocator.free(d);
        }
        self.last_verified = peer;
    }

    pub fn takeVerified(self: *CertVerifyCtx) ?VerifiedPeer {
        const result = self.last_verified;
        self.last_verified = null;
        return result;
    }
};

// ── QuicEngine ─────────────────────────────────────────────────────────

/// Bridges lsquic's C callback model with Zig's std.Io suspension model.
///
/// The engine owns a UDP socket, the lsquic_engine_t, and manages the
/// lifecycle of connections and streams. It runs two concurrent tasks:
///   1. UDP receive loop: reads datagrams -> feeds to lsquic
///   2. Engine process loop: timer-driven lsquic_engine_process_conns()
///
/// lsquic callbacks (stream_if) push events into per-connection/per-stream
/// Io.Queue instances. Application code suspends on these queues.
pub const QuicEngine = struct {
    allocator: Allocator,
    engine: *lsquic.lsquic_engine_t,
    ssl_ctx: *ssl.SSL_CTX,
    cert_verify_ctx: CertVerifyCtx,
    conn_queue_buf: [16]ConnEvent,
    conn_queue: Io.Queue(ConnEvent),
    socket: ?net.Socket,
    io: ?Io,
    is_server: bool,
    running: bool,
    has_unsent: bool,
    /// Server-side connection pending to be pushed to conn_queue.
    /// Set in onNewConn callback (which runs inside lsquic_engine_process_conns),
    /// consumed by processEngine() after lsquic returns to avoid re-entrancy.
    pending_server_conn: ?*QuicConnection,

    /// Dedicated OS thread running the poll-based event loop.
    /// Replaces Io.Group fiber loops for zero-overhead packet processing.
    engine_thread: ?std.Thread,

    // lsquic callback vtables (must be stable pointers)
    stream_if: lsquic.lsquic_stream_if,

    pub const Config = struct {
        is_server: bool = false,
        alpn: [:0]const u8 = "libp2p",
        max_streams_per_conn: u32 = 100,
        idle_timeout_secs: u32 = 30,
        /// Host identity key for TLS certificate generation.
        /// If provided, a libp2p TLS certificate will be generated and loaded
        /// into the SSL context. Required for completing TLS handshakes.
        host_key: ?*ssl.EVP_PKEY = null,
    };

    pub fn init(allocator: Allocator, config: Config) !*QuicEngine {
        // Initialize lsquic global state (safe to call multiple times)
        if (lsquic.lsquic_global_init(lsquic.LSQUIC_GLOBAL_CLIENT | lsquic.LSQUIC_GLOBAL_SERVER) != 0) {
            return error.LsquicGlobalInitFailed;
        }

        // Enable lsquic internal logging for diagnostics
        const logger_if = lsquic.lsquic_logger_if{
            .log_buf = struct {
                fn cb(_: ?*anyopaque, buf: [*c]const u8, len: usize) callconv(.c) c_int {
                    if (buf != null and len > 0) {
                        log.debug("[lsquic] {s}", .{buf[0..len]});
                    }
                    return 0;
                }
            }.cb,
        };
        lsquic.lsquic_logger_init(&logger_if, null, lsquic.LLTS_HHMMSSMS);
        _ = lsquic.lsquic_set_log_level("warning");

        const self = try allocator.create(QuicEngine);
        errdefer allocator.destroy(self);

        self.allocator = allocator;
        self.cert_verify_ctx = CertVerifyCtx.init(allocator);
        self.conn_queue_buf = undefined;
        self.socket = null;
        self.io = null;
        self.is_server = config.is_server;
        self.running = false;
        self.has_unsent = false;
        self.pending_server_conn = null;
        self.engine_thread = null;

        self.conn_queue = Io.Queue(ConnEvent).init(&self.conn_queue_buf);

        // Build the stream interface callbacks
        self.stream_if = std.mem.zeroes(lsquic.lsquic_stream_if);
        self.stream_if.on_new_conn = onNewConn;
        self.stream_if.on_conn_closed = onConnClosed;
        self.stream_if.on_new_stream = onNewStream;
        self.stream_if.on_read = onRead;
        self.stream_if.on_write = onWrite;
        self.stream_if.on_close = onStreamClose;
        self.stream_if.on_hsk_done = onHskDone;
        self.stream_if.on_conncloseframe_received = onConnCloseFrame;

        // Create SSL context
        self.ssl_ctx = ssl.SSL_CTX_new(ssl.TLS_method()) orelse return error.SslCtxCreateFailed;

        // QUIC requires TLS 1.3 — pin the protocol version.
        // Without this, BoringSSL may negotiate TLS 1.2, causing HKDF digest
        // size mismatches (assertion failure in hkdf_extract_to_secret).
        // Matches lsquic's own SSL_CTX setup in lsquic_enc_sess_ietf.c.
        if (ssl.SSL_CTX_set_min_proto_version(self.ssl_ctx, @intCast(ssl.TLS1_3_VERSION)) == 0)
            return error.SslCtxCreateFailed;
        if (ssl.SSL_CTX_set_max_proto_version(self.ssl_ctx, @intCast(ssl.TLS1_3_VERSION)) == 0)
            return error.SslCtxCreateFailed;

        // Initialize the global SSL_CTX ex_data index on first engine creation
        if (g_ssl_ctx_ex_idx == -1) {
            g_ssl_ctx_ex_idx = ssl.SSL_CTX_get_ex_new_index(0, null, null, null, null);
            if (g_ssl_ctx_ex_idx < 0) return error.SslCtxCreateFailed;
        }

        // Configure ALPN for both client and server
        // ALPN wire format: <len><proto> — e.g. \x06libp2p
        const alpn_wire = [_]u8{ 6, 'l', 'i', 'b', 'p', '2', 'p' };
        if (ssl.SSL_CTX_set_alpn_protos(self.ssl_ctx, &alpn_wire, alpn_wire.len) != 0) {
            return error.AlpnSetupFailed;
        }
        ssl.SSL_CTX_set_alpn_select_cb(self.ssl_ctx, tls.alpnSelectCallbackfn, null);

        // Store CertVerifyCtx in SSL_CTX ex_data so the custom verify callback
        // can access it without threadlocal storage (nim-libp2p approach, lsquic#579).
        if (ssl.SSL_CTX_set_ex_data(self.ssl_ctx, g_ssl_ctx_ex_idx, @ptrCast(&self.cert_verify_ctx)) == 0) {
            return error.SslCtxCreateFailed;
        }

        // Register BoringSSL custom verify callback for mutual TLS.
        // This replaces SSL_CTX_set_verify + threadlocal g_peer_cert approach.
        // Works for both client and server since getSslCtx is called for both sides.
        ssl.SSL_CTX_set_custom_verify(
            self.ssl_ctx,
            ssl.SSL_VERIFY_PEER | ssl.SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
            customVerifyCallback,
        );

        // Load libp2p TLS certificate if host key is provided
        if (config.host_key) |host_key| {
            const subject_key = tls.generateKeyPair(.ECDSA) catch return error.KeyGenFailed;
            defer ssl.EVP_PKEY_free(subject_key);

            var host_pubkey = tls.createProtobufEncodedPublicKey(allocator, host_key) catch
                return error.KeyEncodeFailed;
            defer if (host_pubkey.data) |d| allocator.free(d);

            const cert = tls.buildCert(
                allocator,
                &host_pubkey,
                @as(?*anyopaque, @ptrCast(host_key)),
                tls.signDataWithTlsKey,
                subject_key,
            ) catch return error.CertBuildFailed;
            defer ssl.X509_free(cert);

            // Debug: dump PEM cert for interop analysis
            if (tls.x509ToPem(allocator, cert)) |pem| {
                defer allocator.free(pem);
                log.debug("Generated TLS cert:\n{s}", .{pem});
            } else |_| {}

            if (ssl.SSL_CTX_use_certificate(self.ssl_ctx, cert) <= 0)
                return error.CertLoadFailed;
            if (ssl.SSL_CTX_use_PrivateKey(self.ssl_ctx, subject_key) <= 0)
                return error.KeyLoadFailed;
        }

        // Configure engine settings
        var settings: lsquic.lsquic_engine_settings = undefined;
        lsquic.lsquic_engine_init_settings(&settings, if (config.is_server) lsquic.LSENG_SERVER else 0);
        settings.es_init_max_streams_bidi = config.max_streams_per_conn;
        settings.es_idle_timeout = config.idle_timeout_secs;
        settings.es_versions = (1 << lsquic.LSQVER_I001); // QUIC v1 (RFC 9000) only — avoids version negotiation with peers that don't support v2/drafts
        settings.es_cc_algo = 2; // BBR congestion control (faster ramp-up than default Cubic)
        settings.es_scid_iss_rate = 180; // Disable SCID issuance rate limiting

        // Build engine API
        var engine_api: lsquic.lsquic_engine_api = std.mem.zeroes(lsquic.lsquic_engine_api);
        engine_api.ea_settings = &settings;
        engine_api.ea_stream_if = &self.stream_if;
        engine_api.ea_stream_if_ctx = @ptrCast(self);
        engine_api.ea_packets_out = packetsOut;
        engine_api.ea_packets_out_ctx = @ptrCast(self);
        engine_api.ea_get_ssl_ctx = getSslCtx;
        // ea_verify_cert is not needed: lsquic only calls it via its internal
        // verify_server_cert_callback, which is only installed when lsquic creates
        // its own SSL_CTX (i.e., when ea_get_ssl_ctx is NULL). We provide our own
        // SSL_CTX with SSL_CTX_set_custom_verify registered.
        engine_api.ea_verify_cert = null;
        engine_api.ea_verify_ctx = null;
        engine_api.ea_alpn = config.alpn.ptr;

        const flags: c_uint = if (config.is_server) lsquic.LSENG_SERVER else 0;
        self.engine = lsquic.lsquic_engine_new(flags, &engine_api) orelse
            return error.EngineCreateFailed;

        return self;
    }

    /// Set the Io context. Must be called before using the engine with
    /// lsquic callbacks that need to push into Io.Queues.
    pub fn setIo(self: *QuicEngine, io: Io) void {
        self.io = io;
    }

    pub fn deinit(self: *QuicEngine) void {
        lsquic.lsquic_engine_destroy(self.engine);
        ssl.SSL_CTX_free(self.ssl_ctx);
        self.cert_verify_ctx.deinit();
        self.allocator.destroy(self);
    }

    /// Accept a new QUIC connection (blocks until one arrives).
    pub fn accept(self: *QuicEngine, io: Io) !*QuicConnection {
        log.debug("accept: waiting for incoming connection...", .{});
        const event = self.conn_queue.getOne(io) catch |err| switch (err) {
            error.Closed => return error.EngineStopped,
            error.Canceled => return error.EngineStopped,
        };
        return event.conn;
    }

    /// Connect to a remote address.
    pub fn connect(
        self: *QuicEngine,
        io: Io,
        remote_addr: *const std.c.sockaddr,
        local_addr: *const std.c.sockaddr,
    ) !*QuicConnection {
        _ = io;
        log.debug("connect: initiating connection", .{});

        // Create QuicConnection wrapper first so we can pass it as conn_ctx
        // to lsquic_engine_connect. This prevents onNewConn from creating a
        // duplicate QuicConnection.
        const conn = try QuicConnection.init(self.allocator, null, self);
        errdefer conn.deinit();

        const lc = lsquic.lsquic_engine_connect(
            self.engine,
            lsquic.LSQVER_I001, // QUIC v1 (RFC 9000)
            @ptrCast(local_addr),
            @ptrCast(remote_addr),
            @ptrCast(self), // peer_ctx
            @ptrCast(conn), // conn_ctx — our QuicConnection
            null, // hostname
            0, // base_plpmtu
            null, // sess_resume
            0, // sess_resume_len
            null, // token
            0, // token_len
        ) orelse return error.ConnectFailed;

        // Update the QuicConnection with the actual lsquic_conn_t
        conn.lsquic_conn = lc;
        lsquic.lsquic_conn_set_ctx(lc, @ptrCast(conn));

        // Process the connection to trigger handshake
        lsquic.lsquic_engine_process_conns(self.engine);

        return conn;
    }

    pub fn stop(self: *QuicEngine, io: Io) void {
        self.running = false;
        self.conn_queue.close(io);
        // Wait for the engine thread to finish
        if (self.engine_thread) |t| {
            t.join();
            self.engine_thread = null;
        }
    }

    /// Start the dedicated engine thread running a poll-based event loop.
    /// Must be called after setIo() and bindSocket().
    /// The engine thread handles both UDP receive and timer-driven processing
    /// on a single OS thread, eliminating thread pool scheduling overhead.
    pub fn startBackgroundLoops(self: *QuicEngine, _: Io) void {
        self.running = true;
        self.engine_thread = std.Thread.spawn(.{}, QuicEngine.runEngineThread, .{self}) catch |err| {
            log.err("failed to spawn engine thread: {}", .{err});
            return;
        };
    }

    /// Bind a UDP socket.
    /// The socket is managed by std.Io which handles blocking/non-blocking internally.
    /// For the C callback packetsOut, we use std.c.sendmsg directly which handles EAGAIN.
    pub fn bindSocket(self: *QuicEngine, io: Io, address: *const net.IpAddress) !void {
        const sock = try net.IpAddress.bind(address, io, .{ .mode = .dgram });
        self.socket = sock;
    }

    /// Single-threaded engine loop using poll() for zero-overhead event processing.
    /// Mirrors the old libxev single-threaded architecture: all lsquic calls happen
    /// on this one thread, eliminating thread pool scheduling latency.
    fn runEngineThread(self: *QuicEngine) void {
        log.debug("engine thread started", .{});
        const sock = self.socket orelse {
            log.err("engine thread: no socket bound", .{});
            return;
        };
        const fd = sock.handle;

        // Set socket to non-blocking for poll-based loop
        const current_flags = std.c.fcntl(fd, std.c.F.GETFL);
        _ = std.c.fcntl(fd, std.c.F.SETFL, current_flags | @as(c_int, @bitCast(std.c.O{.NONBLOCK = true})));

        var buf: [65535]u8 = undefined;

        while (self.running) {
            // Determine poll timeout from lsquic timer
            var diff: c_int = 0;
            var timeout_ms: i32 = 1; // default 1ms poll when idle
            if (lsquic.lsquic_engine_earliest_adv_tick(self.engine, &diff) != 0) {
                if (diff <= 0) {
                    timeout_ms = 0; // process immediately
                } else {
                    // Convert microseconds to milliseconds, rounding up
                    timeout_ms = @intCast(@min(@as(i64, @divTrunc(@as(i64, @intCast(diff)) + 999, 1000)), 50));
                }
            }

            // poll() on the UDP socket — wakes on packet arrival OR timeout
            var pollfds = [1]std.c.pollfd{.{
                .fd = fd,
                .events = std.c.POLL.IN,
                .revents = 0,
            }};
            _ = std.c.poll(&pollfds, 1, timeout_ms);

            if (!self.running) break;

            // Read all available packets without blocking
            while (true) {
                var src_addr: std.c.sockaddr.storage = undefined;
                var addr_len: std.c.socklen_t = @sizeOf(std.c.sockaddr.storage);
                const rc = std.c.recvfrom(
                    fd,
                    &buf,
                    buf.len,
                    0, // flags
                    @ptrCast(&src_addr),
                    &addr_len,
                );
                if (rc <= 0) break; // EAGAIN or error — no more packets

                const n: usize = @intCast(rc);
                log.debug("engine thread: received {} bytes", .{n});

                var local_sa = ipAddressToSockaddr(sock.address);
                var peer_sa: SockaddrStorage = undefined;
                @memcpy(
                    @as([*]u8, @ptrCast(&peer_sa))[0..addr_len],
                    @as([*]const u8, @ptrCast(&src_addr))[0..addr_len],
                );

                _ = lsquic.lsquic_engine_packet_in(
                    self.engine,
                    &buf,
                    n,
                    @ptrCast(&local_sa),
                    @ptrCast(&peer_sa),
                    @ptrCast(self),
                    0, // ecn
                );
            }

            // Process connections (send responses, handle timers)
            self.processEngine();
        }

        log.debug("engine thread exiting", .{});
    }

    /// Process lsquic connections and retry unsent packets if needed.
    /// Called only from the engine thread — no re-entrancy guard needed.
    fn processEngine(self: *QuicEngine) void {
        // Retry unsent packets first (socket may now be writable)
        if (self.has_unsent) {
            self.has_unsent = false;
            lsquic.lsquic_engine_send_unsent_packets(self.engine);
        }
        lsquic.lsquic_engine_process_conns(self.engine);

        // Drain pending server connection (set by onNewConn during process_conns).
        // Must happen AFTER process_conns returns to avoid re-entrancy.
        if (self.pending_server_conn) |conn| {
            self.pending_server_conn = null;
            if (self.io) |io| {
                log.debug("processEngine: pushing pending server conn to accept queue", .{});
                self.conn_queue.putOneUncancelable(io, .{ .conn = conn }) catch |err| {
                    log.err("processEngine: failed to push server conn: {}", .{err});
                };
            }
        }
    }

    // ── lsquic C callbacks ─────────────────────────────────────────────

    fn onNewConn(stream_if_ctx: ?*anyopaque, lc: ?*lsquic.lsquic_conn_t) callconv(.c) ?*lsquic.lsquic_conn_ctx_t {
        log.debug("onNewConn called, stream_if_ctx={?*}, lc={?*}", .{ stream_if_ctx, lc });
        const engine: *QuicEngine = @ptrCast(@alignCast(stream_if_ctx));
        const c = lc orelse return null;

        // For client-side connections, connect() already set conn_ctx
        const existing_ctx = lsquic.lsquic_conn_get_ctx(lc);
        if (existing_ctx != null) {
            log.debug("onNewConn: client-side conn, existing ctx={?*}", .{existing_ctx});
            return @ptrCast(existing_ctx);
        }

        // Server-side: create QuicConnection wrapper and set it as the conn context.
        // For IETF QUIC servers, on_new_conn is called when the mini-conn is promoted
        // to a full connection (i.e., handshake is complete). on_hsk_done is client-only.
        const conn = QuicConnection.init(engine.allocator, c, engine) catch |err| {
            log.err("onNewConn: failed to create QuicConnection: {}", .{err});
            return null;
        };
        conn.hsk_completed = true;
        log.debug("onNewConn: server-side conn created, conn={*}", .{conn});

        // Extract peer identity stored by customVerifyCallback via SSL_CTX ex_data
        if (engine.cert_verify_ctx.takeVerified()) |verified| {
            conn.peer_id = verified.peer_id;
            if (verified.host_pubkey.data) |d| conn.allocator.free(d);
            log.debug("onNewConn: server peer_id extracted from custom verify", .{});
        } else {
            log.warn("onNewConn: no verified peer info from custom verify callback", .{});
        }

        // Defer push to conn_queue: we're inside lsquic_engine_process_conns,
        // so we can't call putOneUncancelable (which may trigger re-entrancy).
        // processEngine() will drain this after lsquic returns.
        engine.pending_server_conn = conn;
        log.debug("onNewConn: server conn queued for accept", .{});

        return @ptrCast(conn);
    }

    fn onHskDone(lc: ?*lsquic.lsquic_conn_t, status: c_uint) callconv(.c) void {
        log.debug("onHskDone called, lc={?*}, status={}", .{ lc, status });
        if (status != lsquic.LSQ_HSK_OK and status != lsquic.LSQ_HSK_RESUMED_OK) {
            log.warn("onHskDone: handshake failed with status={}", .{status});
            // Signal failure to waitHandshake
            if (lc) |c| {
                const conn_ctx = lsquic.lsquic_conn_get_ctx(c);
                if (conn_ctx) |ctx| {
                    const conn: *QuicConnection = @ptrCast(@alignCast(ctx));
                    if (conn.engine.io) |io| {
                        conn.hsk_queue.putOneUncancelable(io, .failed) catch {};
                    }
                }
                lsquic.lsquic_conn_close(c);
            }
            return;
        }

        const conn_ctx = lsquic.lsquic_conn_get_ctx(lc);
        if (conn_ctx) |ctx| {
            const conn: *QuicConnection = @ptrCast(@alignCast(ctx));
            conn.hsk_completed = true;

            // Extract peer identity stored by customVerifyCallback via SSL_CTX ex_data
            if (conn.engine.cert_verify_ctx.takeVerified()) |verified| {
                conn.peer_id = verified.peer_id;
                if (verified.host_pubkey.data) |d| conn.allocator.free(d);
                log.debug("onHskDone: peer_id extracted from custom verify", .{});
            } else {
                log.warn("onHskDone: no verified peer info from custom verify callback", .{});
                if (conn.engine.io) |io| {
                    conn.hsk_queue.putOneUncancelable(io, .failed) catch {};
                }
                if (lc) |c| lsquic.lsquic_conn_close(c);
                return;
            }

            // Signal success to waitHandshake
            if (conn.engine.io) |io| {
                conn.hsk_queue.putOneUncancelable(io, .ok) catch {};
            }

            // Note: on_hsk_done is CLIENT-ONLY in lsquic.
            // The client already has its QuicConnection from connect(), so we
            // just update peer_id above — no need to push to conn_queue.
            log.debug("onHskDone: client handshake complete, peer_id set on existing conn", .{});
        } else {
            log.warn("onHskDone: conn_ctx is NULL (server mini-conn?)", .{});
        }
    }

    fn onConnCloseFrame(
        _: ?*lsquic.lsquic_conn_t,
        app_error: c_int,
        error_code: u64,
        reason: ?[*]const u8,
        reason_len: c_int,
    ) callconv(.c) void {
        const reason_str: []const u8 = if (reason) |r|
            r[0..@as(usize, @intCast(reason_len))]
        else
            "(none)";
        log.warn("CONNECTION_CLOSE received: app_error={d}, code=0x{x}, reason=\"{s}\"", .{
            app_error, error_code, reason_str,
        });
    }

    fn onConnClosed(lc: ?*lsquic.lsquic_conn_t) callconv(.c) void {
        // Log close reason from lsquic
        if (lc) |c| {
            var errbuf: [256]u8 = undefined;
            const status = lsquic.lsquic_conn_status(c, &errbuf, errbuf.len);
            const err_msg: []const u8 = blk: {
                const span = std.mem.sliceTo(&errbuf, 0);
                break :blk if (span.len > 0) span else "(none)";
            };
            log.warn("onConnClosed: status={d}, errmsg={s}, lc={?*}", .{ @as(c_int, @intCast(status)), err_msg, lc });
        } else {
            log.debug("onConnClosed called, lc=null", .{});
        }
        const ctx = lsquic.lsquic_conn_get_ctx(lc);
        if (ctx) |raw| {
            const conn: *QuicConnection = @ptrCast(@alignCast(raw));
            conn.closed = true;
            conn.lsquic_conn = null;
            // Clear conn context so lsquic doesn't assert on engine destroy
            if (lc) |c| lsquic.lsquic_conn_set_ctx(c, null);
            // Close both queues using engine's stored io
            if (conn.engine.io) |io| {
                conn.stream_queue.close(io);
                conn.outbound_stream_queue.close(io);
                conn.hsk_queue.close(io);
            }
        }
    }

    fn onNewStream(stream_if_ctx: ?*anyopaque, ls: ?*lsquic.lsquic_stream_t) callconv(.c) ?*lsquic.lsquic_stream_ctx_t {
        log.debug("onNewStream called, ls={?*}", .{ls});
        const engine: *QuicEngine = @ptrCast(@alignCast(stream_if_ctx));
        const s = ls orelse return null;

        // Find the connection this stream belongs to
        const lc = lsquic.lsquic_stream_conn(s);
        const conn_ctx = lsquic.lsquic_conn_get_ctx(lc);
        if (conn_ctx == null) return null;

        const conn: *QuicConnection = @ptrCast(@alignCast(conn_ctx));

        // Create stream wrapper
        const stream = QuicStream.init(engine.allocator, s, conn) catch return null;

        // Want to read from this stream
        _ = lsquic.lsquic_stream_wantread(s, 1);

        // Route stream to the correct queue based on QUIC stream ID parity.
        // RFC 9000: client-initiated bidi = 4n+0, server-initiated bidi = 4n+1.
        // Locally-initiated streams go to outbound_stream_queue (for openStream),
        // remotely-initiated streams go to stream_queue (for acceptStream).
        if (engine.io) |io| {
            const stream_id = lsquic.lsquic_stream_id(s);
            const is_server_initiated = (stream_id % 4 == 1);
            const is_locally_initiated = (engine.is_server == is_server_initiated);

            if (is_locally_initiated) {
                conn.outbound_stream_queue.putOneUncancelable(io, .{ .stream = stream }) catch {};
            } else {
                conn.stream_queue.putOneUncancelable(io, .{ .stream = stream }) catch {};
            }
        }

        return @ptrCast(stream);
    }

    fn onRead(ls: ?*lsquic.lsquic_stream_t, ctx: ?*lsquic.lsquic_stream_ctx_t) callconv(.c) void {
        const raw = ctx orelse return;
        const stream: *QuicStream = @ptrCast(@alignCast(raw));
        const s = ls orelse return;

        var buf: [4096]u8 = undefined;
        const n = lsquic.lsquic_stream_read(s, &buf, buf.len);
        if (n <= 0) {
            // EOF or error - stop reading and close the read queue
            _ = lsquic.lsquic_stream_wantread(s, 0);
            if (stream.conn.engine.io) |io| {
                stream.read_queue.close(io);
            }
            return;
        }

        const len: usize = @intCast(n);
        // Allocate owned copy of the data
        const owned = stream.allocator.alloc(u8, len) catch {
            if (stream.conn.engine.io) |io| {
                stream.read_queue.close(io);
            }
            return;
        };
        @memcpy(owned, buf[0..len]);

        // Push read event
        if (stream.conn.engine.io) |io| {
            stream.read_queue.putOneUncancelable(io, .{
                .data = owned,
                .owned_buf = owned,
            }) catch {
                stream.allocator.free(owned);
            };
        } else {
            stream.allocator.free(owned);
        }
    }

    fn onWrite(ls: ?*lsquic.lsquic_stream_t, _: ?*lsquic.lsquic_stream_ctx_t) callconv(.c) void {
        // We handle writes synchronously in QuicStream.write(), so just
        // disable write notifications.
        if (ls) |s| {
            _ = lsquic.lsquic_stream_wantwrite(s, 0);
        }
    }

    fn onStreamClose(ls: ?*lsquic.lsquic_stream_t, ctx: ?*lsquic.lsquic_stream_ctx_t) callconv(.c) void {
        if (ctx) |raw| {
            const stream: *QuicStream = @ptrCast(@alignCast(raw));
            log.debug("onStreamClose: stream={*}", .{stream});
            stream.lsquic_stream = null;
            stream.closed = true;
            if (stream.conn.engine.io) |io| {
                stream.read_queue.close(io);
            }
            // Clear context so lsquic won't call us again
            if (ls) |s| lsquic.lsquic_stream_set_ctx(s, null);
            // Free leftover read buffer if any
            if (stream.leftover_buf) |lb| {
                stream.allocator.free(lb);
                stream.leftover_buf = null;
            }
            stream.allocator.destroy(stream);
        }
    }

    fn packetsOut(
        packets_out_ctx: ?*anyopaque,
        specs: [*c]const lsquic.lsquic_out_spec,
        count: c_uint,
    ) callconv(.c) c_int {
        log.debug("packetsOut called, count={}", .{count});
        const engine: *QuicEngine = @ptrCast(@alignCast(packets_out_ctx));
        const sock = engine.socket orelse return -1;

        var sent: c_int = 0;
        var i: c_uint = 0;
        while (i < count) : (i += 1) {
            const spec = specs[i];
            // Build msghdr from spec
            var msg: std.c.msghdr_const = std.mem.zeroes(std.c.msghdr_const);
            msg.iov = @ptrCast(spec.iov);
            msg.iovlen = @intCast(spec.iovlen);
            const sa: ?*const std.c.sockaddr = @ptrCast(@alignCast(spec.dest_sa));
            msg.name = sa;
            msg.namelen = if (sa) |s| switch (s.family) {
                std.posix.AF.INET => @sizeOf(std.c.sockaddr.in),
                std.posix.AF.INET6 => @sizeOf(std.c.sockaddr.in6),
                else => 0,
            } else 0;

            const rc = std.c.sendmsg(sock.handle, &msg, 0);
            if (rc < 0) {
                const e: std.c.E = @enumFromInt(std.c._errno().*);
                if (e == .AGAIN or e == .INTR) {
                    // Non-blocking socket would block or interrupted.
                    // Flag unsent packets so the timer loop can retry
                    // via lsquic_engine_send_unsent_packets().
                    engine.has_unsent = true;
                    return sent;
                }
                return -1;
            }
            sent += 1;
        }
        return sent;
    }

    fn getSslCtx(peer_ctx: ?*anyopaque, _: ?*const lsquic.struct_sockaddr) callconv(.c) ?*lsquic.struct_ssl_ctx_st {
        log.debug("getSslCtx called", .{});
        const engine: *QuicEngine = @ptrCast(@alignCast(peer_ctx));
        return @ptrCast(engine.ssl_ctx);
    }

    /// BoringSSL custom verify callback registered via SSL_CTX_set_custom_verify.
    /// Retrieves CertVerifyCtx from SSL_CTX ex_data (no threadlocal needed).
    /// Called for both client and server sides since getSslCtx is called for both.
    fn customVerifyCallback(
        ssl_obj: ?*ssl.SSL,
        _: [*c]u8,
    ) callconv(.c) ssl.enum_ssl_verify_result_t {
        const s = ssl_obj orelse return ssl.ssl_verify_invalid;

        // Get the SSL_CTX from the SSL object
        const ssl_ctx: *ssl.SSL_CTX = ssl.SSL_get_SSL_CTX(s) orelse return ssl.ssl_verify_invalid;

        // Retrieve the CertVerifyCtx from SSL_CTX ex_data
        const raw_ptr = ssl.SSL_CTX_get_ex_data(@ptrCast(ssl_ctx), g_ssl_ctx_ex_idx) orelse
            return ssl.ssl_verify_invalid;
        const ctx: *CertVerifyCtx = @ptrCast(@alignCast(raw_ptr));

        // Get the peer certificate from the SSL connection
        const cert: *ssl.X509 = ssl.SSL_get_peer_certificate(s) orelse {
            log.warn("customVerifyCallback: no peer certificate", .{});
            return ssl.ssl_verify_invalid;
        };
        defer ssl.X509_free(cert);

        // Verify the libp2p certificate extension and extract peer identity
        const info = tls.verifyAndExtractPeerInfo(ctx.allocator, cert) catch |err| {
            log.warn("customVerifyCallback: verifyAndExtractPeerInfo failed: {s}", .{@errorName(err)});
            return ssl.ssl_verify_invalid;
        };

        if (!info.is_valid) {
            log.warn("customVerifyCallback: cert signature verification failed", .{});
            if (info.host_pubkey.data) |d| ctx.allocator.free(d);
            return ssl.ssl_verify_invalid;
        }

        // Store verified peer info for consumption by onNewConn/onHskDone
        ctx.storeVerified(.{
            .peer_id = info.peer_id,
            .host_pubkey = info.host_pubkey,
        });

        log.debug("customVerifyCallback: peer verified successfully", .{});
        return ssl.ssl_verify_ok;
    }
};

// ── Helpers ────────────────────────────────────────────────────────────

/// Convert std.Io.net.IpAddress to a C sockaddr for lsquic interop.
/// Returns a sockaddr_storage-sized union that can be cast to sockaddr*.
const SockaddrStorage = extern struct {
    // Big enough for both sockaddr_in (16 bytes) and sockaddr_in6 (28 bytes).
    // Aligned to 4 to safely cast to *sockaddr (alignment 2 on Linux, 4 on macOS).
    data: [128]u8 align(4) = std.mem.zeroes([128]u8),
};

pub fn ipAddressToSockaddr(addr: net.IpAddress) SockaddrStorage {
    var storage = SockaddrStorage{};
    switch (addr) {
        .ip4 => |a| {
            if (builtin.os.tag.isDarwin()) {
                // macOS sockaddr_in: len(1) + family(1) + port(2) + addr(4) + zero(8)
                storage.data[0] = @sizeOf(std.c.sockaddr.in);
                storage.data[1] = @intCast(std.posix.AF.INET);
            } else {
                // Linux sockaddr_in: family(2) + port(2) + addr(4) + zero(8)
                const family: u16 = @intCast(std.posix.AF.INET);
                @memcpy(storage.data[0..2], std.mem.asBytes(&family));
            }
            const port_be = std.mem.nativeToBig(u16, a.port);
            const offset: usize = if (builtin.os.tag.isDarwin()) 2 else 2;
            @memcpy(storage.data[offset..][0..2], std.mem.asBytes(&port_be));
            @memcpy(storage.data[offset + 2 ..][0..4], &a.bytes);
        },
        .ip6 => |a| {
            if (builtin.os.tag.isDarwin()) {
                storage.data[0] = @sizeOf(std.c.sockaddr.in6);
                storage.data[1] = @intCast(std.posix.AF.INET6);
            } else {
                const family: u16 = @intCast(std.posix.AF.INET6);
                @memcpy(storage.data[0..2], std.mem.asBytes(&family));
            }
            const port_be = std.mem.nativeToBig(u16, a.port);
            const offset: usize = if (builtin.os.tag.isDarwin()) 2 else 2;
            @memcpy(storage.data[offset..][0..2], std.mem.asBytes(&port_be));
            // flowinfo at offset+4 (4 bytes, zeroed)
            // addr at offset+8 (16 bytes)
            @memcpy(storage.data[offset + 6 ..][0..16], &a.bytes);
        },
    }
    return storage;
}

// ── Tests ──────────────────────────────────────────────────────────────

test "CertVerifyCtx store and retrieve" {
    const allocator = std.testing.allocator;
    var ctx = CertVerifyCtx.init(allocator);
    defer ctx.deinit();

    // Create a test VerifiedPeer with dummy data
    const test_key_data = try allocator.dupe(u8, &[_]u8{ 1, 2, 3, 4 });
    const test_peer = CertVerifyCtx.VerifiedPeer{
        .peer_id = std.mem.zeroes(PeerId),
        .host_pubkey = keys.PublicKey{ .type = .ED25519, .data = test_key_data },
    };

    ctx.storeVerified(test_peer);
    const peer = ctx.takeVerified();
    try std.testing.expect(peer != null);
    // Caller owns the taken peer's host_pubkey data
    allocator.free(peer.?.host_pubkey.data.?);
}

test "CertVerifyCtx returns null when empty" {
    const allocator = std.testing.allocator;
    var ctx = CertVerifyCtx.init(allocator);
    defer ctx.deinit();

    const peer = ctx.takeVerified();
    try std.testing.expect(peer == null);
}

test "QuicEngine init and deinit" {
    const allocator = std.testing.allocator;
    const engine = QuicEngine.init(allocator, .{}) catch |err| {
        // If lsquic init fails (e.g., missing SSL setup), skip
        std.log.warn("QuicEngine init failed (expected in unit test): {}", .{err});
        return;
    };
    defer engine.deinit();

    try std.testing.expect(!engine.is_server);
    try std.testing.expect(!engine.running);
    try std.testing.expect(engine.io == null);
}

test "ipAddressToSockaddr converts IPv4 correctly" {
    const addr = net.IpAddress{ .ip4 = .{ .bytes = .{ 192, 168, 1, 42 }, .port = 8080 } };
    const storage = ipAddressToSockaddr(addr);

    if (builtin.os.tag.isDarwin()) {
        // macOS: sin_len(1) + sin_family(1) + sin_port(2) + sin_addr(4)
        try std.testing.expectEqual(@as(u8, @sizeOf(std.c.sockaddr.in)), storage.data[0]);
        try std.testing.expectEqual(@as(u8, @intCast(std.posix.AF.INET)), storage.data[1]);
    } else {
        // Linux: sin_family(2, little-endian) + sin_port(2) + sin_addr(4)
        const family = std.mem.readInt(u16, storage.data[0..2], .little);
        try std.testing.expectEqual(@as(u16, @intCast(std.posix.AF.INET)), family);
    }

    // Port is always at offset 2, big-endian
    const port_be = std.mem.readInt(u16, storage.data[2..4], .big);
    try std.testing.expectEqual(@as(u16, 8080), port_be);

    // IPv4 addr at offset 4
    try std.testing.expectEqual(@as(u8, 192), storage.data[4]);
    try std.testing.expectEqual(@as(u8, 168), storage.data[5]);
    try std.testing.expectEqual(@as(u8, 1), storage.data[6]);
    try std.testing.expectEqual(@as(u8, 42), storage.data[7]);
}
