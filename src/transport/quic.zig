const lsquic = @cImport({
    @cInclude("lsquic.h");
    @cInclude("lsquic_types.h");
    @cInclude("lsxpack_header.h");
});
const std = @import("std");
const libp2p = @import("../root.zig");
const p2p_conn = @import("../conn.zig");
const xev = @import("xev");
const io_loop = @import("../thread_event_loop.zig");
const ssl = @import("ssl");
const keys_proto = @import("../proto/keys.proto.zig");
const tls = @import("../security/tls.zig");
const Allocator = std.mem.Allocator;
const UDP = xev.UDP;
const posix = std.posix;
const protoMsgHandler = libp2p.protocols.AnyProtocolMessageHandler;

// Maximum stream data for bidirectional streams
const MaxStreamDataBidiRemote = 64 * 1024 * 1024; // 64 MB
// Maximum stream data for bidirectional streams (local side)
const MaxStreamDataBidiLocal = 64 * 1024 * 1024; // 64 MB
// Maximum number of bidirectional streams
const MaxStreamsBidi = 1000;
// Idle timeout for connections in seconds
const IdleTimeoutSeconds = 120; // 2 minutes
// Handshake timeout in microseconds
const HandshakeTimeoutMicroseconds = 10 * std.time.us_per_s; // 10 seconds

const SignatureAlgs: []const u16 = &.{ ssl.SSL_SIGN_ED25519, ssl.SSL_SIGN_ECDSA_SECP256R1_SHA256, ssl.SSL_SIGN_RSA_PKCS1_SHA256 };

/// Stream interface for lsquic
const stream_if: lsquic.lsquic_stream_if = lsquic.lsquic_stream_if{
    .on_new_conn = onNewConn,
    .on_conn_closed = onConnClosed,
    .on_hsk_done = onHskDone,
    .on_new_stream = onNewStream,
    .on_read = onStreamRead,
    .on_write = onStreamWrite,
    .on_close = onStreamClose,
};

/// QUIC engine that manages QUIC connections and streams.
/// It handles reading from the UDP socket, processing incoming packets, and managing connections.
/// It also provides methods for starting the engine, connecting to peers, and accepting incoming connections.
/// It uses the lsquic library for QUIC protocol handling and integrates with the event loop for asynchronous operations.
/// All the lsquic API calls are scheduled to run in the event loop thread, ensuring thread safety.
/// It assumes that the event loop is running in a single-threaded environment.
/// It supports both client and server modes, allowing it to initiate connections or accept incoming ones.
pub const QuicEngine = struct {
    pub const Error = error{
        InitializationFailed,
        AlreadyConnecting,
    };

    // SSL context for the QUIC engine
    ssl_context: *ssl.SSL_CTX,
    // The lsquic engine instance
    engine: *lsquic.lsquic_engine_t,
    // The UDP socket used for QUIC communication
    socket: UDP,
    // Local address of the QUIC engine
    local_address: std.net.Address,
    // Allocator for memory management
    allocator: Allocator,
    // Indicates whether the engine is in client mode
    is_client_mode: bool,
    // Read buffer for incoming QUIC packets
    // Typical MTU size for UDP packets
    read_buffer: [1500]u8,
    // Completion object for reading from the UDP socket
    c_read: xev.Completion,
    // State for reading from the UDP socket
    read_state: UDP.State,
    // The transport layer that this engine is part of
    // This is used to access the transport's event loop and other properties.
    transport: *QuicTransport,
    // Context for the listen operation
    // This is set when the engine is in server mode and is waiting for incoming connections.
    listen_ctx: ?QuicConnection.ListenCtx,
    // Timer for processing QUIC connections
    process_timer: xev.Timer,
    // Completion object for the process timer
    c_process_timer: xev.Completion,
    // Completion object for canceling the process timer
    c_process_timer_cancel: xev.Completion,
    // Context for connecting to a peer
    // This is used when the engine is in client mode and is initiating a connection to a peer.
    // The functions in the QuicEngine will be called in the same thread as the event loop.
    // It means that no locks are needed for the engine.
    connect_ctx: ?QuicConnection.ConnectCtx,

    pub fn init(self: *QuicEngine, allocator: Allocator, socket: UDP, transport: *QuicTransport, is_client_mode: bool) !void {
        var flags: c_uint = 0;
        if (!is_client_mode) {
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
            .is_client_mode = is_client_mode,
            .listen_ctx = null,
            .process_timer = try xev.Timer.init(),
            .c_process_timer = .{},
            .c_process_timer_cancel = .{},
            .connect_ctx = null,
        };
    }

    /// doStart is a private method that starts the QUIC engine by initiating the read operation on the UDP socket.
    /// It is called from the `start` method to ensure that the read operation is scheduled in the event loop thread.
    pub fn doStart(self: *QuicEngine) void {
        self.socket.read(&self.transport.io_event_loop.loop, &self.c_read, &self.read_state, .{ .slice = &self.read_buffer }, QuicEngine, self, readCallback);
        self.processConns();
    }

    /// Starts the QUIC engine by initiating the read operation on the UDP socket.
    /// This function should be called after the engine is initialized.
    /// It sets up the read callback to handle incoming QUIC packets and starts processing connections.
    /// It is thread-safe and can be called from any thread.
    pub fn start(self: *QuicEngine) void {
        if (self.transport.io_event_loop.inEventLoopThread()) {
            self.doStart();
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .quic_engine_start = .{ .engine = self } },
            };
            self.transport.io_event_loop.queueMessage(message) catch unreachable;
        }
    }

    /// Initiates a QUIC connection to the specified peer address.
    /// If a connection is already in progress, it returns an error.
    /// If the connection is successful, it invokes the callback with the new `QuicConnection`.
    /// If the connection fails, it invokes the callback with an error.
    /// This function is called from the event loop thread to ensure thread safety.
    /// It should not be called directly from other threads.
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
            null,
            null,
            0,
            null,
            0,
            null,
            0,
        );

        self.processConns();
    }

    /// Initiates a QUIC connection to the specified peer address.
    /// If a connection is already in progress, it returns an error.
    /// If the connection is successful, it invokes the callback with the new `QuicConnection`.
    /// If the connection fails, it invokes the callback with an error.
    /// This function is not thread-safe and should not be called from multiple threads concurrently.
    /// Queueuing this operation is recommended.
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

    /// Processes incoming QUIC connections and streams.
    /// This function is called periodically to handle incoming packets and manage connections.
    /// It processes the connections in the lsquic engine and schedules the next processing based on the earliest advertised tick.
    /// It is called from the event loop thread to ensure thread safety.
    /// It should not be called directly from other threads.
    pub fn processConns(self: *QuicEngine) void {
        lsquic.lsquic_engine_process_conns(self.engine);

        var diff_us: c_int = 0;
        if (lsquic.lsquic_engine_earliest_adv_tick(self.engine, &diff_us) > 0) {
            // Calculate the next timer interval in milliseconds
            // If diff_us is negative or less than the clock granularity, we set it to the clock granularity.
            // This ensures that we do not set a timer with a negative or zero interval.
            // The clock granularity is defined in lsquic.h as LSQUIC_DF_CLOCK_GRANULARITY.
            // It is typically set to 1000 microseconds (1 millisecond).
            // This means that the timer will be set to fire at least every 1 millisecond.
            // If the difference is less than the clock granularity, we set the timer to fire at the clock granularity.
            // If the difference is greater than or equal to the clock granularity, we calculate the next timer interval in milliseconds.
            // This is done to ensure that the engine processes connections at a regular interval,
            // which is important for maintaining the performance and responsiveness of the QUIC engine.
            // The timer is used to schedule the next processing of connections,
            // allowing the engine to handle incoming packets and manage connections efficiently.
            const next_ms = if (diff_us >= lsquic.LSQUIC_DF_CLOCK_GRANULARITY)
                @divFloor(@as(u64, @intCast(diff_us)), std.time.us_per_ms)
            else if (diff_us <= 0) 0 else @divFloor(@as(u64, @intCast(lsquic.LSQUIC_DF_CLOCK_GRANULARITY)), std.time.us_per_ms);
            self.process_timer.reset(&self.transport.io_event_loop.loop, &self.c_process_timer, &self.c_process_timer_cancel, next_ms, QuicEngine, self, processConnsCallback);
        }
    }

    /// Callback for processing packets received from the UDP socket.
    /// This function is called when the UDP socket receives data.
    /// It processes the received data by passing it to the lsquic engine for further handling.
    /// This function is not thread-safe and should not be called from multiple threads concurrently.
    fn readCallback(
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

        // For UDP read errors, we log the error and continue to listen.
        const n = r catch |err| {
            std.log.warn("UDP read failed with error: {any}. Continuing to listen.", .{err});
            return .rearm;
        };
        if (n == 0) {
            return .rearm;
        }

        _ = lsquic.lsquic_engine_packet_in(
            self.engine,
            b.slice.ptr,
            n,
            @ptrCast(&self.local_address.any),
            @ptrCast(&address.any),
            self,
            0,
        );

        // If the packet processing failed, we log the error and rearm the read operation.
        // if (result < 0) {
        //     std.log.warn("QUIC engine packet in failed {}\n", .{result});
        //     return .rearm;
        // }

        self.processConns();

        return .rearm;
    }

    /// Callback for processing connections in the QUIC engine.
    /// This function is called periodically to handle incoming packets and manage connections.
    /// It processes the connections in the lsquic engine and schedules the next processing based on the earliest advertised tick.
    fn processConnsCallback(
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

    /// Get the SSL context for a given peer.
    fn getSslContext(
        peer_ctx: ?*anyopaque,
        _: ?*const lsquic.struct_sockaddr,
    ) callconv(.c) ?*lsquic.struct_ssl_ctx_st {
        const self: *QuicEngine = @ptrCast(@alignCast(peer_ctx.?));
        const res: *lsquic.struct_ssl_ctx_st = @ptrCast(@alignCast(self.ssl_context));
        return res;
    }

    /// Callback for when a new QUIC connection is established.
    /// This function is called by the lsquic library when a new connection is created in the server mode.
    /// It creates a new `QuicConnection` instance and invokes the listen callback if set.
    /// This function is not thread-safe and should not be called from multiple threads concurrently.
    fn onListen(self: *QuicEngine, listen_callback_ctx: ?*anyopaque, listen_callback: *const fn (ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        self.listen_ctx = .{
            .callback_ctx = listen_callback_ctx,
            .callback = listen_callback,
        };
    }
};

/// QUIC connection that represents a single QUIC connection.
/// It manages the connection state, streams, and callbacks for new streams.
/// It provides methods for creating new streams, closing the connection, and handling incoming streams.
/// It is associated with a `QuicEngine` and uses the lsquic library for QUIC protocol handling.
pub const QuicConnection = struct {
    conn: *lsquic.lsquic_conn_t,

    engine: *QuicEngine,

    direction: p2p_conn.Direction,
    // Callback context for when a new stream is created in the client mode.
    new_stream_ctx: ?NewStreamCtx,

    connect_ctx: ?ConnectCtx,

    close_ctx: ?CloseCtx,
    // Callback context for when a new stream is created in the server mode.
    on_stream_ctx: ?NewStreamCtx,

    pub const Error = error{
        NewStreamNotFinished,
        AlreadyAccepting,
    };

    pub const ListenCtx = struct {
        callback_ctx: ?*anyopaque,
        callback: *const fn (ctx: ?*anyopaque, res: anyerror!*QuicConnection) void,
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
        callback: ?*const fn (callback_ctx: ?*anyopaque, res: anyerror!*QuicConnection) void,
        active_callback_ctx: ?*anyopaque,
        // This callback is passed by the user when closing the connection,
        // it is called when the connection is closed by the user.
        active_callback: ?*const fn (callback_ctx: ?*anyopaque, res: anyerror!*QuicConnection) void,
    };

    pub fn onStream(self: *QuicConnection, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, stream: anyerror!*QuicStream) void) void {
        if (self.on_stream_ctx != null) {
            callback(callback_ctx, error.AlreadyAccepting);
            return;
        }

        self.on_stream_ctx = .{
            .callback_ctx = callback_ctx,
            .callback = callback,
        };
    }

    /// `lsquic_conn_make_stream` can't be called in the engine callback, it will cause the reentry error.
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

    pub fn close(self: *QuicConnection, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        if (self.engine.transport.io_event_loop.inEventLoopThread()) {
            self.doClose(callback_ctx, callback);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .quic_close_connection = .{ .conn = self, .callback_ctx = callback_ctx, .callback = callback } },
            };
            self.engine.transport.io_event_loop.queueMessage(message) catch unreachable;
        }
    }

    pub fn doClose(self: *QuicConnection, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        if (self.close_ctx) |*close_ctx| {
            // If we are already closing the connection, we just update the callback context and callback.
            close_ctx.active_callback_ctx = callback_ctx;
            close_ctx.active_callback = callback;
        } else {
            self.close_ctx = .{
                .callback_ctx = null,
                .callback = null,
                .active_callback_ctx = callback_ctx,
                .active_callback = callback,
            };
        }
        lsquic.lsquic_conn_close(self.conn);
        self.engine.processConns();
    }
};

/// QUIC stream that represents a single QUIC stream.
/// It manages the stream state, data writing, and reading operations.
/// It provides methods for writing data to the stream, closing the stream, and handling incoming data.
/// It is associated with a `QuicConnection` and uses the lsquic library for QUIC protocol handling.
/// It supports asynchronous write operations with callbacks for completion.
/// It also supports protocol message handling through a `protoMsgHandler`.
pub const QuicStream = struct {
    pub const Error = error{
        StreamClosed,
        ConnectionReset,
        Unexpected,
        WriteFailed,
        ReadFailed,
        EndOfStream,
    };

    /// Represents a write request for the QUIC stream.
    /// It contains the data to be written, the total number of bytes written so far,
    /// a context for the callback, and the callback function itself.
    const WriteRequest = struct {
        data: std.ArrayList(u8),
        total_written: usize = 0,
        callback_ctx: ?*anyopaque,
        callback: *const fn (ctx: ?*anyopaque, res: anyerror!usize) void,
    };

    stream: *lsquic.lsquic_stream_t,

    conn: *QuicConnection,

    pending_writes: std.ArrayList(WriteRequest),

    active_write: ?WriteRequest,

    proto_msg_handler: ?protoMsgHandler,

    proposed_protocols: ?[]const []const u8,

    pub fn init(self: *QuicStream, stream: *lsquic.lsquic_stream_t, conn: *QuicConnection) void {
        self.* = .{
            .stream = stream,
            .conn = conn,
            .pending_writes = std.ArrayList(WriteRequest).init(conn.engine.allocator),
            .active_write = null,
            .proto_msg_handler = null,
            .proposed_protocols = null,
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
        if (self.conn.engine.transport.io_event_loop.inEventLoopThread()) {
            self.doWrite(data, callback_ctx, callback);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .quic_write_stream = .{ .stream = self, .data = data, .callback_ctx = callback_ctx, .callback = callback } },
            };
            self.conn.engine.transport.io_event_loop.queueMessage(message) catch unreachable;
        }
    }

    pub fn close(self: *QuicStream, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!*QuicStream) void) void {
        if (self.conn.engine.transport.io_event_loop.inEventLoopThread()) {
            self.doClose(callback_ctx, callback);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .quic_close_stream = .{ .stream = self, .callback_ctx = callback_ctx, .callback = callback } },
            };
            self.conn.engine.transport.io_event_loop.queueMessage(message) catch unreachable;
        }
    }

    pub fn doClose(self: *QuicStream, _: ?*anyopaque, _: ?*const fn (callback_ctx: ?*anyopaque, res: anyerror!*QuicStream) void) void {
        _ = lsquic.lsquic_stream_close(self.stream);
        self.conn.engine.processConns();
    }

    /// Writes data to the QUIC stream asynchronously.
    /// It appends the data to a list of pending writes and processes the next write operation.
    /// If the write operation fails, it invokes the callback with the error.
    /// If the write operation is successful, it schedules the next write operation.
    /// This function is called from the event loop thread to ensure thread safety.
    /// It should not be called directly from other threads.
    /// Because the data may be eventually written successfully by the QUIC engine `onStreamWrite` callback multiple times,
    /// it queues the write request and processes it asynchronously.
    pub fn doWrite(self: *QuicStream, data: []const u8, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: anyerror!usize) void) void {
        var data_copy = std.ArrayList(u8).init(self.conn.engine.allocator);
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

/// QUIC listener that listens for incoming QUIC connections.
/// It initializes a UDP socket, binds it to the specified address, and starts the QUIC engine.
/// It provides methods for initializing the listener, starting to listen for incoming connections,
/// and accepting new connections.
/// The listener is associated with a `QuicTransport` and uses a callback to notify when a new connection is accepted.
pub const QuicListener = struct {
    /// The error type returned by the `init` function. Want to remain the underlying error type, so we used `anyerror`.
    pub const ListenError = anyerror;
    /// The QuicEngine that this listener is associated with, if any.
    engine: ?QuicEngine,
    /// The transport that created this listener.
    transport: *QuicTransport,
    /// The callback to be invoked when a new connection is accepted.
    listen_callback: *const fn (instance: ?*anyopaque, res: anyerror!*QuicConnection) void,
    /// The context for the listen callback, if any.
    listen_callback_ctx: ?*anyopaque = null,

    /// Initialize the listener with the given transport and listen callback.
    pub fn init(self: *QuicListener, transport: *QuicTransport, listen_callback_ctx: ?*anyopaque, listen_callback: *const fn (instance: ?*anyopaque, res: anyerror!*QuicConnection) void) void {
        self.* = .{
            .engine = null,
            .transport = transport,
            .listen_callback = listen_callback,
            .listen_callback_ctx = listen_callback_ctx,
        };
    }

    /// Deinitialize the listener.
    pub fn deinit(self: *QuicListener) void {
        if (self.engine) |*engine| {
            lsquic.lsquic_engine_destroy(engine.engine);
        }
    }

    /// Starts listening for incoming QUIC connections on the specified address.
    /// It initializes a UDP socket, binds it to the address, and starts the QUIC engine.
    /// If the listener is already started, it returns an error.
    pub fn listen(self: *QuicListener, address: std.net.Address) ListenError!void {
        const socket = try UDP.init(address);
        try socket.bind(address);

        self.engine = undefined;
        const engine_ptr = &self.engine.?;
        try engine_ptr.init(self.transport.allocator, socket, self.transport, false);
        engine_ptr.onListen(self.listen_callback_ctx, self.listen_callback);
        engine_ptr.start();
    }
};

/// QUIC transport that manages QUIC connections and listeners.
/// It provides methods for initializing the transport, dialing peers, and creating listeners.
/// It uses the lsquic library for QUIC protocol handling and integrates with the event loop for asynchronous operations.
/// The transport is responsible for managing the SSL context, key pairs, and certificates used for QUIC connections.
/// It supports both client and server modes, allowing it to initiate connections or accept incoming ones.
/// The transport is not thread-safe and should be used from a single thread, typically the event loop thread.
/// It provides methods for dialing peers, creating listeners, and managing QUIC connections.
pub const QuicTransport = struct {
    pub const DialError = Allocator.Error || xev.ConnectError || error{ AsyncNotifyFailed, AlreadyConnecting, UnsupportedAddressFamily, InitializationFailed };

    ssl_context: *ssl.SSL_CTX,

    io_event_loop: *io_loop.ThreadEventLoop,

    allocator: Allocator,

    dialer_v4: ?QuicEngine,

    dialer_v6: ?QuicEngine,

    host_keypair: *ssl.EVP_PKEY,

    subject_keypair: *ssl.EVP_PKEY,

    subject_cert: *ssl.X509,

    cert_key_type: keys_proto.KeyType,

    pub fn init(self: *QuicTransport, loop: *io_loop.ThreadEventLoop, host_keypair: *ssl.EVP_PKEY, cert_key_type: keys_proto.KeyType, allocator: Allocator) !void {
        const result = lsquic.lsquic_global_init(lsquic.LSQUIC_GLOBAL_CLIENT | lsquic.LSQUIC_GLOBAL_SERVER);
        if (result != 0) {
            return error.InitializationFailed;
        }

        const subject_keypair = try tls.generateKeyPair(cert_key_type);

        const subject_cert = try tls.buildCert(allocator, host_keypair, subject_keypair);

        self.* = .{
            .ssl_context = try initSslContext(subject_keypair, subject_cert),
            .io_event_loop = loop,
            .allocator = allocator,
            .dialer_v4 = null,
            .dialer_v6 = null,
            .host_keypair = host_keypair,
            .cert_key_type = cert_key_type,
            .subject_keypair = subject_keypair,
            .subject_cert = subject_cert,
        };
    }

    pub fn deinit(self: *QuicTransport) void {
        self.io_event_loop.close();

        if (self.dialer_v4) |*dialer| {
            lsquic.lsquic_engine_destroy(dialer.engine);
        }
        if (self.dialer_v6) |*dialer| {
            lsquic.lsquic_engine_destroy(dialer.engine);
        }
        ssl.SSL_CTX_free(self.ssl_context);
        ssl.EVP_PKEY_free(self.subject_keypair);
        ssl.X509_free(self.subject_cert);
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

    /// Creates a new QUIC listener that listens for incoming connections.
    /// The listener is initialized with the provided listen callback and context.
    /// The listener can be used to accept incoming QUIC connections.
    pub fn newListener(self: *QuicTransport, listen_callback_ctx: ?*anyopaque, listen_callback: *const fn (ctx: ?*anyopaque, res: anyerror!*QuicConnection) void) QuicListener {
        var listener: QuicListener = undefined;
        listener.init(self, listen_callback_ctx, listen_callback);
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

        // Limit the protocol versions to TLS 1.3 only.
        // This is required for QUIC to work properly.
        if (ssl.SSL_CTX_set_min_proto_version(ssl_ctx, ssl.TLS1_3_VERSION) == 0)
            return error.InitializationFailed;

        if (ssl.SSL_CTX_set_max_proto_version(ssl_ctx, ssl.TLS1_3_VERSION) == 0)
            return error.InitializationFailed;

        // Disable older protocols and compression.
        if (ssl.SSL_CTX_set_options(ssl_ctx, ssl.SSL_OP_NO_TLSv1 | ssl.SSL_OP_NO_TLSv1_1 | ssl.SSL_OP_NO_TLSv1_2 | ssl.SSL_OP_NO_COMPRESSION | ssl.SSL_OP_NO_SSLv2 | ssl.SSL_OP_NO_SSLv3) == 0)
            return error.InitializationFailed;

        // Set the custom verification callback for the SSL context.
        // This callback is used to verify the peer's certificate.
        // It is set to verify the peer's certificate and fail if no peer certificate is provided.
        // It also sets the callback for certificate verification.
        ssl.SSL_CTX_set_verify(ssl_ctx, ssl.SSL_VERIFY_PEER | ssl.SSL_VERIFY_FAIL_IF_NO_PEER_CERT | ssl.SSL_VERIFY_CLIENT_ONCE, null);
        ssl.SSL_CTX_set_cert_verify_callback(ssl_ctx, tls.libp2pVerifyCallback, null);

        // Set the certificate algorithm preferences for the SSL context.
        if (ssl.SSL_CTX_set_verify_algorithm_prefs(ssl_ctx, SignatureAlgs.ptr, @intCast(SignatureAlgs.len)) == 0)
            @panic("SSL_CTX_set_verify_algorithm_prefs failed\n");

        // Set the SSL context to use the provided subject key and certificate.
        if (ssl.SSL_CTX_use_PrivateKey(ssl_ctx, subject_key) == 0) {
            @panic("SSL_CTX_use_PrivateKey failed");
        }

        if (ssl.SSL_CTX_use_certificate(ssl_ctx, cert) == 0) {
            @panic("SSL_CTX_use_certificate failed");
        }

        // Set the ALPN protocols for the SSL context.
        if (ssl.SSL_CTX_set_alpn_protos(ssl_ctx, tls.ALPN_PROTOS.ptr, @intCast(tls.ALPN_PROTOS.len)) != 0) {
            return error.InitializationFailed;
        }
        // Set the ALPN select callback for the SSL context.
        ssl.SSL_CTX_set_alpn_select_cb(ssl_ctx, tls.alpnSelectCallbackfn, null);

        return ssl_ctx;
    }
};

fn packetsOut(
    ctx: ?*anyopaque,
    specs: ?[*]const lsquic.lsquic_out_spec,
    n_specs: u32,
) callconv(.c) i32 {
    var msg: std.posix.msghdr_const = std.mem.zeroes(std.posix.msghdr_const);
    const engine: *QuicEngine = @ptrCast(@alignCast(ctx.?));
    for (specs.?[0..n_specs]) |spec| {
        const dest_sa: ?*const std.posix.sockaddr = @ptrCast(@alignCast(spec.dest_sa));
        if (dest_sa == null) {
            std.log.warn("sendmsgPosix: dest_sa is null\n", .{});
            return -1;
        }
        msg.name = dest_sa;
        msg.namelen = switch (dest_sa.?.family) {
            std.posix.AF.INET => @sizeOf(std.posix.sockaddr.in),
            std.posix.AF.INET6 => @sizeOf(std.posix.sockaddr.in6),
            else => unreachable,
        };

        msg.iov = @ptrCast(spec.iov.?);
        msg.iovlen = @intCast(spec.iovlen);

        _ = std.posix.sendmsg(engine.socket.fd, &msg, 0) catch |err| {
            std.log.warn("sendmsgPosix failed with: {}", .{err});
            // TODO: Check the error.WouldBlock, it should copy the data to the buffer and use libxev's write function
            return -1;
        };
    }

    return @intCast(n_specs);
}

fn onNewConn(ctx: ?*anyopaque, conn: ?*lsquic.lsquic_conn_t) callconv(.c) ?*lsquic.lsquic_conn_ctx_t {
    const engine: *QuicEngine = @ptrCast(@alignCast(ctx.?));
    // TODO: Can it use a pool for connections?
    const lsquic_conn: *QuicConnection = engine.allocator.create(QuicConnection) catch unreachable;
    lsquic_conn.* = .{
        .connect_ctx = engine.connect_ctx,
        .close_ctx = null,
        .new_stream_ctx = null,
        .on_stream_ctx = null,
        .conn = conn.?,
        .engine = engine,
        .direction = if (engine.is_client_mode) p2p_conn.Direction.OUTBOUND else p2p_conn.Direction.INBOUND,
    };
    engine.connect_ctx = null; // Clear the connect context after use
    const conn_ctx: *lsquic.lsquic_conn_ctx_t = @ptrCast(@alignCast(lsquic_conn));
    lsquic.lsquic_conn_set_ctx(conn, conn_ctx);

    // Server side will not call onHskDone, so we need to call it manually.
    if (!engine.is_client_mode) {
        onHskDone(conn, lsquic.LSQ_HSK_OK);
    }

    return conn_ctx;
}

fn onHskDone(conn: ?*lsquic.lsquic_conn_t, status: lsquic.enum_lsquic_hsk_status) callconv(.c) void {
    if (status != lsquic.LSQ_HSK_OK and status != lsquic.LSQ_HSK_RESUMED_OK) {
        _ = lsquic.lsquic_conn_close(conn);
        return;
    } else {
        const lsquic_conn: *QuicConnection = @ptrCast(@alignCast(lsquic.lsquic_conn_get_ctx(conn.?)));
        if (lsquic_conn.direction == p2p_conn.Direction.INBOUND) {
            lsquic_conn.engine.listen_ctx.?.callback(lsquic_conn.engine.listen_ctx.?.callback_ctx, lsquic_conn);
        } else {
            lsquic_conn.connect_ctx.?.callback(lsquic_conn.connect_ctx.?.callback_ctx, lsquic_conn);
        }
    }
}

pub fn onConnClosed(conn: ?*lsquic.lsquic_conn_t) callconv(.c) void {
    const lsquic_conn: *QuicConnection = @ptrCast(@alignCast(lsquic.lsquic_conn_get_ctx(conn.?)));
    if (lsquic_conn.close_ctx) |close_ctx| {
        if (close_ctx.callback) |callback| {
            callback(close_ctx.callback_ctx, lsquic_conn);
        }
        if (close_ctx.active_callback) |active_callback| {
            active_callback(close_ctx.active_callback_ctx, lsquic_conn);
        }
    }
    lsquic.lsquic_conn_set_ctx(conn, null);
    lsquic_conn.engine.allocator.destroy(lsquic_conn);
}

fn onNewStream(ctx: ?*anyopaque, stream: ?*lsquic.lsquic_stream_t) callconv(.c) ?*lsquic.lsquic_stream_ctx_t {
    const engine: *QuicEngine = @ptrCast(@alignCast(ctx.?));
    const conn: *QuicConnection = @ptrCast(@alignCast(lsquic.lsquic_conn_get_ctx(lsquic.lsquic_stream_conn(stream.?))));
    const lsquic_stream: *QuicStream = engine.allocator.create(QuicStream) catch unreachable;
    lsquic_stream.init(stream.?, conn);

    const stream_ctx: *lsquic.lsquic_stream_ctx_t = @ptrCast(@alignCast(lsquic_stream));
    if (conn.direction == p2p_conn.Direction.INBOUND) {
        if (conn.on_stream_ctx) |on_stream_ctx| {
            on_stream_ctx.callback(on_stream_ctx.callback_ctx, lsquic_stream);
        }
    } else {
        if (conn.new_stream_ctx) |new_stream_ctx| {
            new_stream_ctx.callback(new_stream_ctx.callback_ctx, lsquic_stream);
            conn.new_stream_ctx = null; // Clear the new stream context after use
        }
    }
    return stream_ctx;
}

fn onStreamRead(
    stream: ?*lsquic.lsquic_stream_t,
    stream_ctx: ?*lsquic.lsquic_stream_ctx_t,
) callconv(.c) void {
    const self: *QuicStream = @ptrCast(@alignCast(stream_ctx.?));
    const s = stream.?;

    var buf: [4096]u8 = undefined;

    while (true) {
        const n_read = lsquic.lsquic_stream_read(s, &buf, buf.len);
        if (n_read > 0) {
            self.proto_msg_handler.?.onMessage(self, buf[0..@intCast(n_read)]) catch |err| {
                std.log.warn("Protocol message handler failed with error: {}. ", .{err});
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
                std.log.warn("lsquic_stream_read returned E.AGAIN, waiting for more data.\n", .{});
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

pub fn onStreamWrite(
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
                std.log.warn("lsquic_stream_write returned E.AGAIN, waiting for more space to write.\n", .{});
                _ = lsquic.lsquic_stream_wantwrite(stream.?, 1);
                return;
            }

            std.log.warn("lsquic_stream_write failed with error: {}", .{err});
            active_req.callback(active_req.callback_ctx, error.WriteFailed);
            active_req.data.deinit();
            self.active_write = null;
            return;
        } else if (n_written == 0) {
            // `lsquic_stream_write` returned 0, it means that you should try writing later.
            _ = lsquic.lsquic_stream_wantwrite(stream.?, 1);
            return;
        } else {
            _ = lsquic.lsquic_stream_flush(stream.?);
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
                    // _ = lsquic.lsquic_stream_wantwrite(stream.?, 0);
                }
            }
        }
    } else {
        _ = lsquic.lsquic_stream_wantwrite(stream.?, 0);
        return;
    }
}

fn onStreamClose(
    _: ?*lsquic.lsquic_stream_t,
    stream_ctx: ?*lsquic.lsquic_stream_ctx_t,
) callconv(.c) void {
    if (stream_ctx == null) return;
    const self: *QuicStream = @ptrCast(@alignCast(stream_ctx.?));
    // When protocol message handler function return error in the `onNewStream` callback,
    // we want to close the stream immediately, but there is an error thrown by lsquic in the server mode.
    // In this case, the stream will be closed until the connection closed.
    // TODO: Can we found a good approach?
    if (self.proto_msg_handler) |*proto_msg_handler| {
        proto_msg_handler.onClose(self) catch |err| {
            std.log.warn("Protocol message handler failed with error: {}.", .{err});
        };
    }
    self.deinit();
    self.conn.engine.allocator.destroy(self);
}

test "lsquic transport initialization" {
    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer loop.deinit();

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
    defer loop.deinit();

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

test "lsquic transport dialing and listening" {
    var server_loop: io_loop.ThreadEventLoop = undefined;
    try server_loop.init(std.testing.allocator);
    defer server_loop.deinit();

    const server_pctx = ssl.EVP_PKEY_CTX_new_id(ssl.EVP_PKEY_ED25519, null) orelse return error.OpenSSLFailed;
    if (ssl.EVP_PKEY_keygen_init(server_pctx) == 0) {
        return error.OpenSSLFailed;
    }
    var maybe_server_key: ?*ssl.EVP_PKEY = null;
    if (ssl.EVP_PKEY_keygen(server_pctx, &maybe_server_key) == 0) {
        return error.OpenSSLFailed;
    }
    const server_key = maybe_server_key orelse return error.OpenSSLFailed;

    defer ssl.EVP_PKEY_free(server_key);

    var server: QuicTransport = undefined;
    try server.init(&server_loop, server_key, keys_proto.KeyType.ECDSA, std.testing.allocator);

    defer {
        server.deinit();
    }

    var listener = server.newListener(null, struct {
        pub fn callback(_: ?*anyopaque, res: anyerror!*QuicConnection) void {
            if (res) |conn| {
                std.debug.print("Server accepted QUIC connection successfully: {*}\n", .{conn});
            } else |err| {
                std.debug.print("Server failed to accept QUIC connection: {}\n", .{err});
            }
        }
    }.callback);
    defer listener.deinit();

    const addr = try std.net.Address.parseIp4("127.0.0.1", 9997);

    try listener.listen(addr);

    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer loop.deinit();

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

    defer {
        transport.deinit();
    }

    const DialCtx = struct {
        conn: *QuicConnection,
        notify: std.Thread.ResetEvent,

        const Self = @This();
        fn callback(ctx: ?*anyopaque, res: anyerror!*QuicConnection) void {
            const self: *Self = @ptrCast(@alignCast(ctx.?));
            if (res) |conn| {
                std.debug.print("Dialed QUIC connection successfully: {*}\n", .{conn});
                self.conn = conn;
            } else |err| {
                std.debug.print("Failed to dial QUIC connection: {}\n", .{err});
            }
            self.notify.set();
        }
    };

    var dial_ctx = DialCtx{
        .conn = undefined,
        .notify = .{},
    };
    transport.dial(addr, &dial_ctx, DialCtx.callback);
    dial_ctx.notify.wait();

    dial_ctx.conn.close(null, struct {
        pub fn callback(_: ?*anyopaque, res: anyerror!*QuicConnection) void {
            if (res) |closed_conn| {
                std.debug.print("Closed QUIC connection successfully: {*}\n", .{closed_conn});
            } else |err| {
                std.debug.print("Failed to close QUIC connection: {}\n", .{err});
            }
        }
    }.callback);

    std.time.sleep(std.time.ns_per_s * 1); // Wait for the connection to close
}
