const lsquic = @cImport({
    @cInclude("lsquic.h");
    @cInclude("lsquic_types.h");
    @cInclude("lsxpack_header.h");
});
const std = @import("std");
const p2p_conn = @import("../../conn.zig");
const xev = @import("xev");
const io_loop = @import("../../thread_event_loop.zig");
const ssl = @import("ssl");
const keys_proto = @import("../../proto/keys.proto.zig");
const tls = @import("../../security/tls.zig");
const Allocator = std.mem.Allocator;
const UDP = xev.UDP;
const posix = std.posix;
const protoMsgHandler = @import("../../proto_handler.zig").AnyProtocolMessageHandler;

const MaxStreamDataBidiRemote = 64 * 1024 * 1024; // 64 MB
const MaxStreamDataBidiLocal = 64 * 1024 * 1024; // 64 MB
const MaxStreamsBidi = 1000;
const IdleTimeoutSeconds = 120;
const HandshakeTimeoutMicroseconds = 10 * std.time.us_per_s; // 10 seconds

const stream_if: lsquic.lsquic_stream_if = lsquic.lsquic_stream_if{
    .on_new_conn = onNewConn,
    .on_conn_closed = onConnClosed,
    .on_hsk_done = onHskDone,
    .on_new_stream = onNewStream,
    .on_read = onRead,
    .on_write = onWrite,
    .on_close = onClose,
};

pub const QuicEngine = struct {
    pub const Error = error{
        InitializationFailed,
        AlreadyConnecting,
    };

    ssl_context: *ssl.SSL_CTX,

    engine: *lsquic.lsquic_engine_t,

    socket: UDP,

    local_address: std.net.Address,

    allocator: Allocator,

    is_initiator: bool,

    read_buffer: [1500]u8, // Typical MTU size for UDP packets

    c_read: xev.Completion,

    read_state: UDP.State,

    transport: *QuicTransport,

    accept_callback: ?*const fn (ctx: ?*anyopaque, res: anyerror!*QuicConnection) void,

    accept_callback_ctx: ?*anyopaque,

    process_timer: xev.Timer,

    c_process_timer: xev.Completion,

    c_process_timer_cancel: xev.Completion,

    connect_ctx: ?QuicConnection.ConnectCtx,

    pub fn init(self: *QuicEngine, allocator: Allocator, socket: UDP, transport: *QuicTransport, is_initiator: bool) !void {
        var flags: c_uint = 0;
        if (!is_initiator) {
            flags |= lsquic.LSENG_SERVER;
        }

        var engine_settings: lsquic.lsquic_engine_settings = undefined;
        lsquic.lsquic_engine_init_settings(&engine_settings, flags);

        engine_settings.es_init_max_stream_data_bidi_remote = MaxStreamDataBidiRemote;
        engine_settings.es_init_max_stream_data_bidi_local = MaxStreamDataBidiLocal;
        engine_settings.es_init_max_streams_bidi = MaxStreamsBidi;
        engine_settings.es_idle_timeout = IdleTimeoutSeconds;
        engine_settings.es_handshake_to = HandshakeTimeoutMicroseconds;

        var err_buf: [100]u8 = undefined;
        if (lsquic.lsquic_engine_check_settings(
            &engine_settings,
            flags,
            &err_buf,
            100,
        ) == 1) {
            std.log.warn("lsquic_engine_check_settings failed: {any}", .{err_buf});
            return error.InitializationFailed;
        }

        const engine_api: lsquic.lsquic_engine_api = .{ .ea_settings = &engine_settings, .ea_stream_if = &stream_if, .ea_stream_if_ctx = self, .ea_packets_out = packetsOut, .ea_packets_out_ctx = self, .ea_get_ssl_ctx = getSslContext };
        const engine = lsquic.lsquic_engine_new(flags, &engine_api);
        if (engine == null) {
            return error.InitializationFailed;
        }

        var local_address: std.net.Address = undefined;
        var local_socklen: posix.socklen_t = @sizeOf(std.net.Address);
        try std.posix.getsockname(socket.fd, &local_address.any, &local_socklen);

        self.* = .{
            .ssl_context = transport.ssl_context,
            .engine = engine.?,
            .allocator = allocator,
            .socket = socket,
            .local_address = local_address,
            .read_buffer = undefined,
            .c_read = undefined,
            .read_state = undefined,
            .transport = transport,
            .is_initiator = is_initiator,
            .accept_callback = null,
            .accept_callback_ctx = null,
            .process_timer = try xev.Timer.init(),
            .c_process_timer = .{},
            .c_process_timer_cancel = .{},
            .connect_ctx = null,
        };
    }

    pub fn doStart(self: *QuicEngine) void {
        self.socket.read(&self.transport.io_event_loop.loop, &self.c_read, &self.read_state, .{ .slice = &self.read_buffer }, QuicEngine, self, readCallback);
        self.processConns();
    }

    pub fn start(self: *QuicEngine) void {
        if (self.transport.io_event_loop.inEventLoopThread()) {
            self.doStart();
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .quic_start = .{ .engine = self } },
            };
            self.transport.io_event_loop.queueMessage(message) catch unreachable;
        }
    }

    pub fn doConnect(self: *QuicEngine, peer_address: std.net.Address, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        self.connect_ctx = .{
            .address = peer_address,
            .callback_ctx = callback_ctx,
            .callback = callback,
        };

        self.doStart();

        _ = lsquic.lsquic_engine_connect(
            self.engine,
            lsquic.N_LSQVER,
            @ptrCast(&self.local_address.any),
            @ptrCast(&peer_address.any),
            self,
            null, // TODO: Check if we should pass conn ctx earlier
            null,
            0,
            null,
            0,
            null,
            0,
        );
        self.processConns();
    }

    pub fn connect(self: *QuicEngine, peer_address: std.net.Address, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        if (self.connect_ctx != null) {
            callback(callback_ctx, error.AlreadyConnecting);
            return;
        }

        if (self.transport.io_event_loop.inEventLoopThread()) {
            self.doConnect(peer_address, callback_ctx, callback);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .quic_connect = .{
                    .engine = self,
                    .peer_address = peer_address,
                    .callback_ctx = callback_ctx,
                    .callback = callback,
                } },
            };

            self.transport.io_event_loop.queueMessage(message) catch unreachable;
        }
    }

    pub fn onAccept(self: *QuicEngine, accept_callback_ctx: ?*anyopaque, accept_callback: *const fn (ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        self.accept_callback = accept_callback;
        self.accept_callback_ctx = accept_callback_ctx;
    }

    pub fn readCallback(
        ctx: ?*QuicEngine,
        _: *xev.Loop,
        _: *xev.Completion,
        _: *xev.UDP.State,
        address: std.net.Address,
        _: xev.UDP,
        b: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        const self = ctx.?;

        const n = r catch |err| {
            switch (err) {
                error.EOF => {},
                else => std.log.warn("UDP read failed with error: {any}. Disarming read.", .{err}),
            }

            return .disarm;
        };

        if (n == 0) {
            return .disarm;
        }

        const result = lsquic.lsquic_engine_packet_in(
            self.engine,
            b.slice.ptr,
            n,
            @ptrCast(&self.local_address.any),
            @ptrCast(&address.any),
            self,
            0,
        );

        if (result < 0) {
            std.log.warn("QUIC engine packet in failed", .{});
            return .disarm;
        }

        self.processConns();

        return .rearm;
    }

    pub fn processConns(self: *QuicEngine) void {
        lsquic.lsquic_engine_process_conns(self.engine);

        var diff_us: c_int = 0;
        if (lsquic.lsquic_engine_earliest_adv_tick(self.engine, &diff_us) > 0) {
            const next_ms = if (diff_us >= lsquic.LSQUIC_DF_CLOCK_GRANULARITY)
                @divFloor(@as(u64, @intCast(diff_us)), std.time.us_per_ms)
            else if (diff_us <= 0) 0 else @divFloor(@as(u64, @intCast(lsquic.LSQUIC_DF_CLOCK_GRANULARITY)), std.time.us_per_ms);
            std.debug.print("QUIC engine processing connections with next_ms {}\n", .{next_ms});
            self.process_timer.reset(&self.transport.io_event_loop.loop, &self.c_process_timer, &self.c_process_timer_cancel, next_ms, QuicEngine, self, processConnsCallback);
        }
    }

    pub fn processConnsCallback(
        ctx: ?*QuicEngine,
        _: *xev.Loop,
        _: *xev.Completion,
        r: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        const engine = ctx.?;

        _ = r catch |err| {
            std.log.warn("QUIC engine process conns timer failed with error: {}", .{err});
            return .disarm;
        };

        engine.processConns();

        return .disarm;
    }

    pub fn getSslContext(
        peer_ctx: ?*anyopaque,
        _: ?*const lsquic.struct_sockaddr,
    ) callconv(.c) ?*lsquic.struct_ssl_ctx_st {
        const self: *QuicEngine = @ptrCast(@alignCast(peer_ctx.?));
        const res: *lsquic.struct_ssl_ctx_st = @ptrCast(@alignCast(self.ssl_context));
        return res;
    }
};

pub const QuicConnection = struct {
    conn: *lsquic.lsquic_conn_t,
    engine: *QuicEngine,
    direction: p2p_conn.Direction,
    new_stream_ctx: ?NewStreamCtx,
    connect_ctx: ?ConnectCtx,
    close_ctx: ?CloseCtx,

    pub const Error = error{
        NewStreamNotFinished,
    };

    pub const ConnectCtx = struct {
        address: std.net.Address,
        callback_ctx: ?*anyopaque,
        callback: *const fn (ctx: ?*anyopaque, res: anyerror!*QuicConnection) void,
    };

    pub const NewStreamCtx = struct {
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, stream: anyerror!*QuicStream) void,
    };

    pub const CloseCtx = struct {
        callback_ctx: ?*anyopaque,
        // This callback is registered at the time of connection connected,
        // it is used that the connection is closed not by the user, but by the engine.
        callback: *const fn (callback_ctx: ?*anyopaque, res: anyerror!*QuicConnection) void,
        ud_callback_ctx: ?*anyopaque,
        // This callback is used to notify the user that the connection is closed.
        ud_callback: ?*const fn (callback_ctx: ?*anyopaque, res: anyerror!*QuicConnection) void,
    };

    pub fn onStream(self: *QuicConnection, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, stream: anyerror!*QuicStream) void) void {
        if (self.new_stream_ctx != null) {
            callback(callback_ctx, error.NewStreamNotFinished);
            return;
        }

        self.new_stream_ctx = .{
            .callback_ctx = callback_ctx,
            .callback = callback,
        };
    }

    pub fn newStream(self: *QuicConnection, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, stream: anyerror!*QuicStream) void) void {
        if (self.engine.transport.io_event_loop.inEventLoopThread()) {
            self.doNewStream(callback_ctx, callback);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .quic_new_stream = .{ .conn = self, .new_stream_ctx = callback_ctx, .new_stream_callback = callback } },
            };
            self.engine.transport.io_event_loop.queueMessage(message) catch unreachable;
        }
    }

    pub fn doNewStream(self: *QuicConnection, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, stream: anyerror!*QuicStream) void) void {
        if (self.new_stream_ctx != null) {
            callback(callback_ctx, error.NewStreamNotFinished);
            return;
        }

        if (lsquic.lsquic_conn_n_pending_streams(self.conn) != 0) {
            // If there are pending streams, we should not create a new one.
            callback(callback_ctx, error.NewStreamNotFinished);
            return;
        }

        self.new_stream_ctx = .{
            .callback_ctx = callback_ctx,
            .callback = callback,
        };
        lsquic.lsquic_conn_make_stream(self.conn);

        self.engine.processConns();
    }

    pub fn close(self: *QuicConnection, callback_ctx: ?*anyopaque, callback: ?*const fn (callback_ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        if (self.engine.transport.io_event_loop.inEventLoopThread()) {
            self.doClose(callback_ctx, callback);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .quic_close_connection = .{ .conn = self, .callback_ctx = callback_ctx, .callback = callback } },
            };
            self.engine.transport.io_event_loop.queueMessage(message) catch unreachable;
        }
    }

    pub fn doClose(self: *QuicConnection, callback_ctx: ?*anyopaque, callback: ?*const fn (callback_ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        self.close_ctx.?.ud_callback_ctx = callback_ctx;
        self.close_ctx.?.ud_callback = callback;
        lsquic.lsquic_conn_close(self.conn);
        self.engine.processConns();
    }
};

pub const QuicStream = struct {
    pub const Error = error{
        StreamClosed,
        ConnectionReset,
        Unexpected,
        WriteFailed,
        ReadFailed,
        EndOfStream,
    };

    const WriteRequest = struct {
        data: std.ArrayList(u8),
        total_written: usize = 0,
        callback_ctx: ?*anyopaque,
        callback: *const fn (ctx: ?*anyopaque, res: anyerror!usize) void,
    };

    stream: *lsquic.lsquic_stream_t,

    conn: *QuicConnection,

    engine: *QuicEngine,

    allocator: Allocator,

    pending_writes: std.ArrayList(WriteRequest),

    active_write: ?WriteRequest,

    proto_msg_handler: protoMsgHandler,

    pub fn init(self: *QuicStream, stream: *lsquic.lsquic_stream_t, conn: *QuicConnection, engine: *QuicEngine, allocator: Allocator) void {
        self.* = .{
            .stream = stream,
            .conn = conn,
            .engine = engine,
            .allocator = allocator,
            .pending_writes = std.ArrayList(WriteRequest).init(allocator),
            .active_write = null,
            .proto_msg_handler = undefined,
        };
    }

    pub fn deinit(self: *QuicStream) void {
        for (self.pending_writes.items) |*req| {
            req.data.deinit();
        }
        self.pending_writes.deinit();

        if (self.active_write) |*req| {
            req.data.deinit();
        }
    }

    pub fn setProtoMsgHandler(self: *QuicStream, handler: protoMsgHandler) void {
        self.proto_msg_handler = handler;
        _ = lsquic.lsquic_stream_wantread(self.stream, 1);
    }

    pub fn write(self: *QuicStream, data: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!usize) void) void {
        var data_copy = std.ArrayList(u8).init(self.allocator);
        data_copy.appendSlice(data) catch |err| {
            callback(callback_ctx, err);
            return;
        };

        const write_req = WriteRequest{
            .data = data_copy,
            .callback_ctx = callback_ctx,
            .callback = callback,
        };

        self.pending_writes.append(write_req) catch |err| {
            data_copy.deinit();
            callback(callback_ctx, err);
            return;
        };

        self.processNextWrite();
    }

    fn processNextWrite(self: *QuicStream) void {
        if (self.active_write != null or self.pending_writes.items.len == 0) {
            return;
        }

        self.active_write = self.pending_writes.orderedRemove(0);

        _ = lsquic.lsquic_stream_wantwrite(self.stream, 1);
    }
};

pub const QuicListener = struct {
    /// The error type returned by the `init` function. Want to remain the underlying error type, so we used `anyerror`.
    pub const ListenError = anyerror;
    /// The QuicEngine that this listener is associated with, if any.
    engine: ?QuicEngine,
    /// The transport that created this listener.
    transport: *QuicTransport,

    accept_callback: *const fn (instance: ?*anyopaque, res: anyerror!*QuicConnection) void,

    accept_callback_ctx: ?*anyopaque = null,

    /// Initialize the listener with the given transport and accept callback.
    pub fn init(self: *QuicListener, transport: *QuicTransport, accept_callback_ctx: ?*anyopaque, accept_callback: *const fn (instance: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        self.* = .{
            .engine = null,
            .transport = transport,
            .accept_callback = accept_callback,
            .accept_callback_ctx = accept_callback_ctx,
        };
    }

    /// Deinitialize the listener.
    pub fn deinit(_: *QuicListener) void {}

    pub fn listen(self: *QuicListener, address: std.net.Address) ListenError!void {
        const socket = try UDP.init(address);
        try socket.bind(address);

        self.engine = undefined;
        const engine_ptr = &self.engine.?;
        try engine_ptr.init(self.transport.allocator, socket, self.transport, false);
        engine_ptr.onAccept(self.accept_callback_ctx, self.accept_callback);
        engine_ptr.start();
        std.debug.print("QUIC listener started on engine: {*}\n", .{&self.engine});
    }
};

pub const QuicTransport = struct {
    pub const DialError = Allocator.Error || xev.ConnectError || error{ AsyncNotifyFailed, AlreadyConnecting, UnsupportedAddressFamily, InitializationFailed };

    ssl_context: *ssl.SSL_CTX,

    io_event_loop: *io_loop.ThreadEventLoop,

    allocator: Allocator,

    dialer_v4: ?QuicEngine,

    dialer_v6: ?QuicEngine,

    host_keypair: *ssl.EVP_PKEY,

    subject_keypair: *ssl.EVP_PKEY,

    cert: *ssl.X509,

    cert_key_type: keys_proto.KeyType,

    pub fn init(self: *QuicTransport, loop: *io_loop.ThreadEventLoop, host_keypair: *ssl.EVP_PKEY, cert_key_type: keys_proto.KeyType, allocator: Allocator) !void {
        const result = lsquic.lsquic_global_init(lsquic.LSQUIC_GLOBAL_CLIENT | lsquic.LSQUIC_GLOBAL_SERVER);
        if (result != 0) {
            return error.InitializationFailed;
        }

        var maybe_subject_key: ?*ssl.EVP_PKEY = null;

        if (cert_key_type == .ECDSA or cert_key_type == .SECP256K1) {
            const curve_nid = switch (cert_key_type) {
                .ECDSA => ssl.NID_X9_62_prime256v1,
                // TODO: SECP256K1 is not supported in BoringSSL
                .SECP256K1 => unreachable,
                else => unreachable,
            };

            var maybe_params: ?*ssl.EVP_PKEY = null;
            {
                const pctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_EC, null) orelse return error.OpenSSLFailed;
                defer ssl.EVP_PKEY_CTX_free(pctx);

                if (ssl.EVP_PKEY_paramgen_init(pctx) <= 0) {
                    return error.OpenSSLFailed;
                }

                if (ssl.EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, curve_nid) <= 0) {
                    return error.OpenSSLFailed;
                }

                if (ssl.EVP_PKEY_paramgen(pctx, &maybe_params) <= 0) {
                    return error.OpenSSLFailed;
                }
            }
            const params = maybe_params orelse return error.OpenSSLFailed;
            defer ssl.EVP_PKEY_free(params);

            {
                const kctx = ssl.EVP_PKEY_CTX_new(params, null) orelse return error.OpenSSLFailed;
                defer ssl.EVP_PKEY_CTX_free(kctx);

                if (ssl.EVP_PKEY_keygen_init(kctx) <= 0) {
                    return error.OpenSSLFailed;
                }

                if (ssl.EVP_PKEY_keygen(kctx, &maybe_subject_key) <= 0) {
                    return error.OpenSSLFailed;
                }
            }
        } else {
            const key_alg_id = switch (cert_key_type) {
                .ED25519 => ssl.EVP_PKEY_ED25519,
                .RSA => ssl.EVP_PKEY_RSA,
                else => unreachable,
            };

            const pctx = ssl.EVP_PKEY_CTX_new_id(key_alg_id, null) orelse return error.OpenSSLFailed;
            defer ssl.EVP_PKEY_CTX_free(pctx);

            if (ssl.EVP_PKEY_keygen_init(pctx) <= 0) {
                return error.OpenSSLFailed;
            }

            if (ssl.EVP_PKEY_keygen(pctx, &maybe_subject_key) <= 0) {
                return error.OpenSSLFailed;
            }
        }

        const subject_key = maybe_subject_key orelse return error.OpenSSLFailed;

        const cert = try tls.buildCert(allocator, host_keypair, subject_key);

        self.* = .{
            .ssl_context = try initSslContext(subject_key, cert),
            .io_event_loop = loop,
            .allocator = allocator,
            .dialer_v4 = null,
            .dialer_v6 = null,
            .host_keypair = host_keypair,
            .cert_key_type = cert_key_type,
            .subject_keypair = subject_key,
            .cert = cert,
        };
    }

    pub fn deinit(self: *QuicTransport) void {
        lsquic.lsquic_global_cleanup();
        ssl.SSL_CTX_free(self.ssl_context);
        ssl.EVP_PKEY_free(self.subject_keypair);
        ssl.X509_free(self.cert);
    }

    // Initiates a QUIC connection to the specified peer address.
    /// If a connection is already in progress, it returns an error.
    /// If the connection is successful, it invokes the callback with the new `QuicConnection`.
    /// If the connection fails, it invokes the callback with an error.
    /// This is not thread-safe and should not be called from multiple threads concurrently.
    /// Queueuing this operation is recommended.
    pub fn dial(self: *QuicTransport, peer_address: std.net.Address, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        var dialer = self.getOrCreateDialer(peer_address) catch |err| {
            callback(callback_ctx, err);
            return;
        };

        dialer.connect(peer_address, callback_ctx, callback);
    }

    pub fn newListener(self: *QuicTransport, accept_callback_ctx: ?*anyopaque, accept_callback: *const fn (ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) QuicListener {
        var listener: QuicListener = undefined;
        listener.init(self, accept_callback_ctx, accept_callback);
        return listener;
    }

    fn getOrCreateDialer(self: *QuicTransport, peer_address: std.net.Address) !*QuicEngine {
        switch (peer_address.any.family) {
            posix.AF.INET => {
                if (self.dialer_v4) |*dialer| {
                    return dialer;
                }
                const bind_addr = try std.net.Address.parseIp4("0.0.0.0", 0);
                const socket = try UDP.init(bind_addr);
                try socket.bind(bind_addr);

                self.dialer_v4 = undefined;
                const engine_ptr = &self.dialer_v4.?;
                try engine_ptr.init(self.allocator, socket, self, true);
                return engine_ptr;
            },
            posix.AF.INET6 => {
                if (self.dialer_v6) |*dialer| {
                    return dialer;
                }
                const bind_addr = try std.net.Address.parseIp6("::", 0);
                const socket = try UDP.init(bind_addr);
                try socket.bind(bind_addr);

                self.dialer_v6 = undefined;
                const engine_ptr = &self.dialer_v6.?;
                try engine_ptr.init(self.allocator, socket, self, true);
                return engine_ptr;
            },
            else => return error.UnsupportedAddressFamily,
        }
    }

    fn initSslContext(subject_key: *ssl.EVP_PKEY, cert: *ssl.X509) !*ssl.SSL_CTX {
        const ssl_ctx = ssl.SSL_CTX_new(ssl.TLS_method()) orelse return error.InitializationFailed;

        if (ssl.SSL_CTX_set_min_proto_version(ssl_ctx, ssl.TLS1_3_VERSION) == 0)
            return error.InitializationFailed;

        if (ssl.SSL_CTX_set_max_proto_version(ssl_ctx, ssl.TLS1_3_VERSION) == 0)
            return error.InitializationFailed;

        if (ssl.SSL_CTX_set_options(ssl_ctx, ssl.SSL_OP_NO_TLSv1 | ssl.SSL_OP_NO_TLSv1_1 | ssl.SSL_OP_NO_TLSv1_2 | ssl.SSL_OP_NO_COMPRESSION | ssl.SSL_OP_NO_SSLv2 | ssl.SSL_OP_NO_SSLv3) == 0)
            return error.InitializationFailed;

        ssl.SSL_CTX_set_verify(ssl_ctx, ssl.SSL_VERIFY_PEER | ssl.SSL_VERIFY_FAIL_IF_NO_PEER_CERT | ssl.SSL_VERIFY_CLIENT_ONCE, tls.libp2pVerifyCallback);
        ssl.SSL_CTX_set_cert_verify_callback(ssl_ctx, tls.libp2pVerifyCallback1, null);
        const signature_algs: []const u16 = &.{ ssl.SSL_SIGN_ED25519, ssl.SSL_SIGN_ECDSA_SECP256R1_SHA256, ssl.SSL_SIGN_RSA_PKCS1_SHA256 };
        if (ssl.SSL_CTX_set_verify_algorithm_prefs(ssl_ctx, signature_algs.ptr, @intCast(signature_algs.len)) == 0)
            @panic("SSL_CTX_set_verify_algorithm_prefs failed\n");

        if (ssl.SSL_CTX_use_PrivateKey(ssl_ctx, subject_key) == 0) {
            @panic("SSL_CTX_use_PrivateKey failed");
        }

        if (ssl.SSL_CTX_use_certificate(ssl_ctx, cert) == 0) {
            @panic("SSL_CTX_use_certificate failed");
        }

        if (ssl.SSL_CTX_set_alpn_protos(ssl_ctx, tls.ALPN_PROTOS.ptr, @intCast(tls.ALPN_PROTOS.len)) != 0) {
            return error.InitializationFailed;
        }

        ssl.SSL_CTX_set_alpn_select_cb(ssl_ctx, tls.alpnSelectCallbackfn, null);

        return ssl_ctx;
    }
};

pub fn packetsOut(
    ctx: ?*anyopaque,
    specs: ?[*]const lsquic.lsquic_out_spec,
    n_specs: u32,
) callconv(.c) i32 {
    var msg: std.posix.msghdr_const = std.mem.zeroes(std.posix.msghdr_const);
    const engine: *QuicEngine = @ptrCast(@alignCast(ctx.?));
    for (specs.?[0..n_specs]) |spec| {
        const dest_sa: ?*const std.posix.sockaddr = @ptrCast(@alignCast(spec.dest_sa));
        if (dest_sa == null) {
            @panic("sendmsgPosix: dest_sa is null");
        }
        msg.name = dest_sa;
        msg.namelen = switch (dest_sa.?.family) {
            std.posix.AF.INET => @sizeOf(std.posix.sockaddr.in),
            std.posix.AF.INET6 => @sizeOf(std.posix.sockaddr.in6),
            else => @panic("Unsupported address family"),
        };

        msg.iov = @ptrCast(spec.iov.?);
        msg.iovlen = @intCast(spec.iovlen);

        if (xev.backend == .epoll or xev.backend == .io_uring) {
            // TODO: try to use libxev's sendmsg function
        }
        const sent_bytes = std.posix.sendmsg(engine.socket.fd, &msg, 0) catch |err| {
            std.debug.panic("sendmsgPosix failed with: {s}", .{@errorName(err)});
        };
        std.debug.print("QUIC sent {} bytes to {}\n", .{ sent_bytes, dest_sa.? });
    }

    return @intCast(n_specs);
}

pub fn onNewConn(ctx: ?*anyopaque, conn: ?*lsquic.lsquic_conn_t) callconv(.c) ?*lsquic.lsquic_conn_ctx_t {
    const engine: *QuicEngine = @ptrCast(@alignCast(ctx.?));
    // TODO: Can it use a pool for connections?
    const lsquic_conn: *QuicConnection = engine.allocator.create(QuicConnection) catch unreachable;
    lsquic_conn.* = .{
        .connect_ctx = engine.connect_ctx,
        .close_ctx = null,
        .new_stream_ctx = null,
        .conn = conn.?,
        .engine = engine,
        .direction = if (engine.is_initiator) p2p_conn.Direction.OUTBOUND else p2p_conn.Direction.INBOUND,
    };
    engine.connect_ctx = null; // Clear the connect context after use
    const conn_ctx: *lsquic.lsquic_conn_ctx_t = @ptrCast(@alignCast(lsquic_conn));
    lsquic.lsquic_conn_set_ctx(conn, conn_ctx);

    if (!engine.is_initiator) {
        onHskDone(conn, lsquic.LSQ_HSK_OK);
    }
    // Handle new connection logic here
    std.debug.print("New connection established: {any}\n", .{conn});
    return conn_ctx;
}

pub fn onHskDone(conn: ?*lsquic.lsquic_conn_t, status: lsquic.enum_lsquic_hsk_status) callconv(.c) void {
    std.debug.print("Handshake done for connection {any} with status {any}\n", .{ conn, status });
    if (status != lsquic.LSQ_HSK_OK and status != lsquic.LSQ_HSK_RESUMED_OK) {
        std.debug.print("Handshake failed for connection {any} with status {any}\n", .{ conn, status });
        _ = lsquic.lsquic_conn_close(conn);
        return;
    } else {
        const lsquic_conn: *QuicConnection = @ptrCast(@alignCast(lsquic.lsquic_conn_get_ctx(conn.?)));
        if (lsquic_conn.direction == p2p_conn.Direction.INBOUND) {
            lsquic_conn.engine.accept_callback.?(
                lsquic_conn.engine.accept_callback_ctx,
                lsquic_conn,
            );
        } else {
            lsquic_conn.connect_ctx.?.callback(lsquic_conn.connect_ctx.?.callback_ctx, lsquic_conn);
        }
    }
}

pub fn onConnClosed(conn: ?*lsquic.lsquic_conn_t) callconv(.c) void {
    const lsquic_conn: *QuicConnection = @ptrCast(@alignCast(lsquic.lsquic_conn_get_ctx(conn.?)));
    if (lsquic_conn.close_ctx) |close_ctx| {
        close_ctx.callback(close_ctx.callback_ctx, lsquic_conn);
        std.debug.print("Connection closed with callback: {any}\n", .{close_ctx.callback});
        if (close_ctx.ud_callback) |ud_callback| {
            ud_callback(close_ctx.ud_callback_ctx, lsquic_conn);
        }
    }
    std.debug.print("Connection closed: {any}\n", .{conn});
    lsquic.lsquic_conn_set_ctx(conn, null);
    lsquic_conn.engine.allocator.destroy(lsquic_conn);
    std.debug.print("Connection closed: {any}\n", .{conn});
}

pub fn onNewStream(ctx: ?*anyopaque, stream: ?*lsquic.lsquic_stream_t) callconv(.c) ?*lsquic.lsquic_stream_ctx_t {
    const engine: *QuicEngine = @ptrCast(@alignCast(ctx.?));
    const conn: *QuicConnection = @ptrCast(@alignCast(lsquic.lsquic_conn_get_ctx(lsquic.lsquic_stream_conn(stream.?))));
    const lsquic_stream: *QuicStream = engine.allocator.create(QuicStream) catch unreachable;
    lsquic_stream.init(stream.?, conn, engine, engine.allocator);

    const stream_ctx: *lsquic.lsquic_stream_ctx_t = @ptrCast(@alignCast(lsquic_stream));
    std.debug.print("New stream established: {any}\n", .{stream});

    conn.new_stream_ctx.?.callback(conn.new_stream_ctx.?.callback_ctx, lsquic_stream);
    conn.new_stream_ctx = null; // Clear the context after invoking the callback
    return stream_ctx;
}

pub fn onRead(
    stream: ?*lsquic.lsquic_stream_t,
    stream_ctx: ?*lsquic.lsquic_stream_ctx_t,
) callconv(.c) void {
    const self: *QuicStream = @ptrCast(@alignCast(stream_ctx.?));
    const s = stream.?;

    var buf: [4096]u8 = undefined;

    while (true) {
        const n_read = lsquic.lsquic_stream_read(s, &buf, buf.len);

        if (n_read > 0) {
            self.proto_msg_handler.onMessage(self, buf[0..@intCast(n_read)]) catch |err| {
                std.log.warn("Proto message handler failed with error: {any}. Closing stream {any}.", .{ err, s });
                _ = lsquic.lsquic_stream_close(s);
                return;
            };
        } else if (n_read == 0) {
            // End of Stream. The remote peer has closed its writing side.
            _ = lsquic.lsquic_stream_close(s);
            return;
        } else {
            // NOTE: Error handling for lsquic_stream_read on Windows platforms is not implemented.
            // On Windows, error codes may differ and additional handling may be required here.
            const err = posix.errno(n_read);
            if (err == posix.E.AGAIN) {
                std.debug.print("lsquic_stream_read returned E.AGAIN, waiting for more data.\n", .{});
                _ = lsquic.lsquic_stream_wantread(s, 1);
                return;
            }

            const fatal_err = switch (err) {
                posix.E.BADF => error.StreamClosed,
                posix.E.CONNRESET => error.ConnectionReset,
                // Only E.AGAIN, E.BADF, and E.CONNRESET are expected here; any other errno is unexpected.
                else => blk: {
                    std.log.warn("Unexpected errno from lsquic_stream_read (expected E.AGAIN, E.BADF, E.CONNRESET): {}", .{@intFromEnum(err)});
                    break :blk error.Unexpected;
                },
            };

            // If the error is the expected E.BADF or E.CONNRESET, the stream should be already closed.
            if (fatal_err == error.Unexpected) {
                _ = lsquic.lsquic_stream_close(s);
            }
            return;
        }
    }
}

pub fn onWrite(
    stream: ?*lsquic.lsquic_stream_t,
    stream_ctx: ?*lsquic.lsquic_stream_ctx_t,
) callconv(.c) void {
    const self: *QuicStream = @ptrCast(@alignCast(stream_ctx.?));

    // Get a pointer to the active request, not a copy.
    if (self.active_write) |*active_req| {
        const n_written = lsquic.lsquic_stream_write(stream.?, active_req.data.items.ptr, active_req.data.items.len);
        if (n_written < 0) {
            // NOTE: Error handling for lsquic_stream_write on Windows platforms is not implemented.
            // On Windows, error codes may differ and additional handling may be required here.
            // If the error is E.AGAIN, we should wait for the next write event.
            const err = posix.errno(n_written);
            if (err == posix.E.AGAIN) {
                std.debug.print("lsquic_stream_write returned E.AGAIN, waiting for more space to write.\n", .{});
                _ = lsquic.lsquic_stream_wantwrite(stream.?, 1);
                return;
            }

            std.log.warn("lsquic_stream_write failed with error: {}", .{@intFromEnum(err)});
            active_req.callback(active_req.callback_ctx, error.WriteFailed);
            active_req.data.deinit();
            self.active_write = null;
            return;
        } else if (n_written == 0) {
            // `lsquic_stream_write` returned 0, it means that you should try writing later.
            _ = lsquic.lsquic_stream_wantwrite(stream.?, 1);
            return;
        } else {
            const written_usize: usize = @intCast(n_written);
            active_req.total_written += written_usize;
            active_req.data.replaceRange(0, written_usize, &.{}) catch unreachable;

            if (active_req.data.items.len == 0) {
                active_req.callback(active_req.callback_ctx, active_req.total_written);
                active_req.data.deinit();
                self.active_write = null;

                if (self.pending_writes.items.len > 0) {
                    self.processNextWrite();
                } else {
                    _ = lsquic.lsquic_stream_wantwrite(stream.?, 0);
                }
            }
        }
    } else {
        _ = lsquic.lsquic_stream_wantwrite(stream.?, 0);
        return;
    }
}

pub fn onClose(
    _: ?*lsquic.lsquic_stream_t,
    stream_ctx: ?*lsquic.lsquic_stream_ctx_t,
) callconv(.c) void {
    const self: *QuicStream = @ptrCast(@alignCast(stream_ctx.?));
    self.proto_msg_handler.onClose(self) catch |err| {
        std.log.warn("Proto message handler failed with error: {any}. Closing stream {any}.", .{ err, self.stream });
    };
    self.deinit();
    self.engine.allocator.destroy(self);
}

test "lsquic transport initialization" {
    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer {
        loop.close();
        loop.deinit();
    }

    const pctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_ED25519, null) orelse return error.OpenSSLFailed;
    if (ssl.EVP_PKEY_keygen_init(pctx) == 0) {
        return error.OpenSSLFailed;
    }
    var maybe_host_key: ?*ssl.EVP_PKEY = null;
    if (ssl.EVP_PKEY_keygen(pctx, &maybe_host_key) == 0) {
        return error.OpenSSLFailed;
    }
    const host_key = maybe_host_key orelse return error.OpenSSLFailed;

    defer ssl.EVP_PKEY_free(host_key);

    var transport: QuicTransport = undefined;
    try transport.init(&loop, host_key, keys_proto.KeyType.ECDSA, std.testing.allocator);

    defer transport.deinit();
}

test "lsquic engine initialization" {
    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer {
        loop.close();
        loop.deinit();
    }

    const pctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_ED25519, null) orelse return error.OpenSSLFailed;
    if (ssl.EVP_PKEY_keygen_init(pctx) == 0) {
        return error.OpenSSLFailed;
    }
    var maybe_host_key: ?*ssl.EVP_PKEY = null;
    if (ssl.EVP_PKEY_keygen(pctx, &maybe_host_key) == 0) {
        return error.OpenSSLFailed;
    }
    const host_key = maybe_host_key orelse return error.OpenSSLFailed;

    defer ssl.EVP_PKEY_free(host_key);

    var transport: QuicTransport = undefined;
    try transport.init(&loop, host_key, keys_proto.KeyType.ED25519, std.testing.allocator);
    defer transport.deinit();

    const addr = try std.net.Address.parseIp4("127.0.0.1", 9999);
    const udp = try UDP.init(addr);
    var engine: QuicEngine = undefined;
    try engine.init(std.testing.allocator, udp, &transport, false);
    defer lsquic.lsquic_engine_destroy(engine.engine);
}

// test "lsquic transport dialing and listening" {
//     var server_loop: io_loop.ThreadEventLoop = undefined;
//     try server_loop.init(std.testing.allocator);
//     defer {
//         server_loop.close();
//         server_loop.deinit();
//     }

//     const server_pctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_ED25519, null) orelse return error.OpenSSLFailed;
//     if (ssl.EVP_PKEY_keygen_init(server_pctx) == 0) {
//         return error.OpenSSLFailed;
//     }
//     var maybe_server_key: ?*ssl.EVP_PKEY = null;
//     if (ssl.EVP_PKEY_keygen(server_pctx, &maybe_server_key) == 0) {
//         return error.OpenSSLFailed;
//     }
//     const server_key = maybe_server_key orelse return error.OpenSSLFailed;

//     defer ssl.EVP_PKEY_free(server_key);

//     var server: QuicTransport = undefined;
//     try server.init(&server_loop, server_key, keys_proto.KeyType.ECDSA, std.testing.allocator);

//     defer server.deinit();

//     var listener = server.newListener(null, struct {
//         pub fn callback(_: ?*anyopaque, res: anyerror!*QuicConnection) void {
//             if (res) |conn| {
//                 std.debug.print("Server accepted QUIC connection successfully: {any}\n", .{conn});
//             } else |err| {
//                 std.debug.print("Server failed to accept QUIC connection: {any}\n", .{err});
//             }
//         }
//     }.callback);

//     const addr = try std.net.Address.parseIp4("127.0.0.1", 9997);

//     try listener.listen(addr);

//     var loop: io_loop.ThreadEventLoop = undefined;
//     try loop.init(std.testing.allocator);
//     defer {
//         loop.close();
//         loop.deinit();
//     }

//     const pctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_ED25519, null) orelse return error.OpenSSLFailed;
//     if (ssl.EVP_PKEY_keygen_init(pctx) == 0) {
//         return error.OpenSSLFailed;
//     }
//     var maybe_host_key: ?*ssl.EVP_PKEY = null;
//     if (ssl.EVP_PKEY_keygen(pctx, &maybe_host_key) == 0) {
//         return error.OpenSSLFailed;
//     }
//     const host_key = maybe_host_key orelse return error.OpenSSLFailed;

//     defer ssl.EVP_PKEY_free(host_key);

//     var transport: QuicTransport = undefined;
//     try transport.init(&loop, host_key, keys_proto.KeyType.ECDSA, std.testing.allocator);

//     defer transport.deinit();
//     transport.dial(addr, null, struct {
//         pub fn callback(_: ?*anyopaque, res: anyerror!*QuicConnection) void {
//             if (res) |conn| {
//                 std.debug.print("Dialed QUIC connection successfully: {any}\n", .{conn});
//             } else |err| {
//                 std.debug.print("Failed to dial QUIC connection: {any}\n", .{err});
//             }
//         }
//     }.callback);

//     std.time.sleep(200 * std.time.ns_per_ms);
// }
