const lsquic = @cImport({
    @cInclude("lsquic.h");
    @cInclude("lsquic_types.h");
    @cInclude("lsxpack_header.h");
});
const std = @import("std");
const p2p_conn = @import("../../conn.zig");
const Allocator = std.mem.Allocator;
const xev = @import("xev");
const UDP = xev.UDP;
const io_loop = @import("../../thread_event_loop.zig");
const ssl = @import("ssl");

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
    ssl_context: *ssl.SSL_CTX,

    engine: *lsquic.lsquic_engine_t,

    socket: UDP,

    socket_address: std.net.Address,

    allocator: Allocator,

    is_initiator: bool,

    read_buffer: [1500]u8, // Typical MTU size for UDP packets

    c_read: xev.Completion,

    read_state: UDP.State,

    transport: *QuicTransport,

    pub fn init(self: *QuicEngine, allocator: Allocator, socket: UDP, socket_address: std.net.Address, transport: *QuicTransport, is_initiator: bool) !void {
        var flags: c_uint = 0;
        if (!is_initiator) {
            flags |= lsquic.LSENG_SERVER;
        }

        var engine_settings: lsquic.lsquic_engine_settings = undefined;
        lsquic.lsquic_engine_init_settings(&engine_settings, flags);

        // TODO: Make the hardcoded values configurable
        engine_settings.es_init_max_stream_data_bidi_remote = 64 * 1024 * 1024; // 64 MB
        engine_settings.es_init_max_stream_data_bidi_local = 64 * 1024 * 1024; // 64 MB
        engine_settings.es_init_max_streams_bidi = 1000; // 1000 streams
        engine_settings.es_idle_timeout = 120; // 120 seconds
        engine_settings.es_handshake_to = 10 * std.time.us_per_s; // 10 seconds

        var err_buf: [100]u8 = undefined;
        if (lsquic.lsquic_engine_check_settings(
            &engine_settings,
            flags,
            &err_buf,
            100,
        ) == 1) {
            @panic("lsquic_engine_check_settings failed " ++ err_buf);
        }

        const engine_api: lsquic.lsquic_engine_api = .{ .ea_settings = &engine_settings, .ea_stream_if = &stream_if, .ea_stream_if_ctx = self, .ea_packets_out = packetsOut, .ea_packets_out_ctx = self, .ea_get_ssl_ctx = getSslContext };
        const engine = lsquic.lsquic_engine_new(flags, &engine_api);
        if (engine == null) {
            return error.InitializationFailed;
        }
        self.* = .{
            .ssl_context = undefined,
            .engine = engine.?,
            .allocator = allocator,
            .socket = socket,
            .socket_address = socket_address,
            .read_buffer = std.mem.zeroes([1500]u8),
            .c_read = .{},
            .read_state = undefined,
            .transport = transport,
            .is_initiator = is_initiator,
        };
    }

    pub fn start(self: *QuicEngine) !void {
        self.socket.read(&self.transport.io_event_loop.loop, &self.c_read, &self.read_state, .{ .slice = &self.read_buffer }, QuicEngine, self, QuicEngine.readCallback);
    }

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
            @ptrCast(&self.socket_address.any),
            @ptrCast(&address.any),
            self,
            0,
        );

        if (result < 0) {
            std.log.warn("QUIC engine packet in failed", .{});
            return .disarm;
        }

        return .rearm;
    }

    fn processConns(self: *QuicEngine) void {
        lsquic.lsquic_engine_process_conns(self.engine);

        var diff: c_int = 0;
        if (lsquic.lsquic_engine_earliest_adv_tick(self.engine, &diff)) {
            // const timer = xev.Timer.init() catch unreachable;
            // const c_timer = self.transport.io_event_loop.completion_pool.create() catch unreachable;
        }
    }

    fn getSslContext(
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
};

pub const QuicStream = struct {
    stream: *lsquic.lsquic_stream_t,
    conn: *QuicConnection,
    engine: *QuicEngine,
};

pub const QuicListener = struct {
    pub const AcceptError = Allocator.Error || xev.AcceptError || error{AsyncNotifyFailed};
    /// The error type returned by the `init` function. Want to remain the underlying error type, so we used `anyerror`.
    pub const ListenError = anyerror;
    /// The address to listen on.
    address: std.net.Address,
    /// The server to accept connections from.
    server: UDP,
    /// The transport that created this listener.
    transport: *QuicTransport,

    accept_callback: ?*const fn (instance: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void = null,

    accept_callback_instance: ?*anyopaque = null,

    /// Initialize the listener with the given address, backlog, and transport.
    pub fn init(self: *QuicListener, address: std.net.Address, transport: *QuicTransport) ListenError!void {
        const server = try UDP.init(address);
        try server.bind(address);
        self.address = address;
        self.server = server;
        self.transport = transport;
    }

    /// Deinitialize the listener.
    pub fn deinit(_: *QuicListener) void {
        // TODO: should we close the server here?
    }

    pub fn accept(_: *QuicListener, _: ?*anyopaque, _: *const fn (instance: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void) void {}
};

pub const QuicTransport = struct {
    pub const DialError = Allocator.Error || xev.ConnectError || error{AsyncNotifyFailed};

    ssl_context: *ssl.SSL_CTX,

    io_event_loop: *io_loop.ThreadEventLoop,

    allocator: Allocator,

    pub fn init(self: *QuicTransport, loop: *io_loop.ThreadEventLoop, allocator: Allocator) !void {
        // Initialize the QUIC transport layer
        const result = lsquic.lsquic_global_init(lsquic.LSQUIC_GLOBAL_CLIENT);
        if (result != 0) {
            return error.InitializationFailed;
        }
        self.* = .{
            .ssl_context = undefined,
            .io_event_loop = loop,
            .allocator = allocator,
        };
    }

    pub fn deinit(_: *QuicTransport) void {
        // Cleanup the QUIC transport layer
        lsquic.lsquic_global_cleanup();
    }

    pub fn dial(_: *QuicTransport, _: std.net.Address, _: ?*anyopaque, _: *const fn (instance: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void) void {}
};

fn packetsOut(
    ctx: ?*anyopaque,
    specs: ?[*]const lsquic.lsquic_out_spec,
    n_specs: u32,
) callconv(.c) i32 {
    var msg: std.posix.msghdr_const = undefined;
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

        _ = std.posix.sendmsg(engine.socket.fd, &msg, 0) catch |err| {
            std.debug.panic("sendmsgPosix failed with: {s}", .{@errorName(err)});
        };
    }

    return @intCast(n_specs);
}

fn onNewConn(ctx: ?*anyopaque, conn: ?*lsquic.lsquic_conn_t) callconv(.c) ?*lsquic.lsquic_conn_ctx_t {
    const engine: *QuicEngine = @ptrCast(@alignCast(ctx.?));
    // TODO: Can it use a pool for connections?
    const lsquic_conn: *QuicConnection = engine.allocator.create(QuicConnection) catch unreachable;
    lsquic_conn.* = .{
        .conn = conn.?,
        .engine = engine,
        .direction = if (engine.is_initiator) p2p_conn.Direction.OUTBOUND else p2p_conn.Direction.INBOUND,
    };
    const conn_ctx: *lsquic.lsquic_conn_ctx_t = @ptrCast(@alignCast(lsquic_conn));
    lsquic.lsquic_conn_set_ctx(conn, conn_ctx);
    if (!engine.is_initiator) {
        onHskDone(conn, lsquic.LSQ_HSK_OK);
    }
    // Handle new connection logic here
    std.debug.print("New connection established: {any}\n", .{conn});
    return conn_ctx;
}

fn onHskDone(conn: ?*lsquic.lsquic_conn_t, status: lsquic.enum_lsquic_hsk_status) callconv(.c) void {
    _ = conn;
    _ = status;
}

fn onConnClosed(conn: ?*lsquic.lsquic_conn_t) callconv(.c) void {
    const lsquic_conn: *QuicConnection = @ptrCast(@alignCast(lsquic.lsquic_conn_get_ctx(conn.?)));
    lsquic.lsquic_conn_set_ctx(conn, null);
    lsquic_conn.engine.allocator.destroy(lsquic_conn);
    std.debug.print("Connection closed: {any}\n", .{conn});
}

fn onNewStream(ctx: ?*anyopaque, stream: ?*lsquic.lsquic_stream_t) callconv(.c) ?*lsquic.lsquic_stream_ctx_t {
    const engine: *QuicEngine = @ptrCast(@alignCast(ctx.?));
    const conn: *QuicConnection = @ptrCast(@alignCast(lsquic.lsquic_conn_get_ctx(lsquic.lsquic_stream_conn(stream.?))));
    const lsquic_stream: *QuicStream = engine.allocator.create(QuicStream) catch unreachable;
    lsquic_stream.* = .{
        .stream = stream.?,
        .conn = conn,
        .engine = engine,
    };
    const stream_ctx: *lsquic.lsquic_stream_ctx_t = @ptrCast(@alignCast(lsquic_stream)); // Handle new stream logic here
    std.debug.print("New stream established: {any}\n", .{stream});
    return stream_ctx;
}

fn onRead(
    stream: ?*lsquic.lsquic_stream_t,
    stream_ctx: ?*lsquic.lsquic_stream_ctx_t,
) callconv(.c) void {
    _ = stream;
    _ = stream_ctx;
}

fn onWrite(
    stream: ?*lsquic.lsquic_stream_t,
    stream_ctx: ?*lsquic.lsquic_stream_ctx_t,
) callconv(.c) void {
    _ = stream;
    _ = stream_ctx;
}

fn onClose(
    stream: ?*lsquic.lsquic_stream_t,
    stream_ctx: ?*lsquic.lsquic_stream_ctx_t,
) callconv(.c) void {
    _ = stream;
    _ = stream_ctx;
}

test "lsquic transport initialization" {
    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer {
        loop.close();
        loop.deinit();
    }
    var transport: QuicTransport = undefined;
    try transport.init(&loop, std.testing.allocator);
    defer transport.deinit();
}

test "lsquic engine initialization" {
    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(std.testing.allocator);
    defer {
        loop.close();
        loop.deinit();
    }
    var transport: QuicTransport = undefined;
    try transport.init(&loop, std.testing.allocator);
    defer transport.deinit();

    const addr = try std.net.Address.parseIp4("127.0.0.1", 9999);
    const udp = try UDP.init(addr);
    var engine: QuicEngine = undefined;
    try engine.init(std.testing.allocator, udp, addr, &transport, false);
    defer lsquic.lsquic_engine_destroy(engine.engine);
}
