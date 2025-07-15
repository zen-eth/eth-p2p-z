const std = @import("std");
const p2p_conn = @import("../../conn.zig");
const c = @cImport({
    @cInclude("lsquic.h");
    @cInclude("lsquic_types.h");
    @cInclude("lsxpack_header.h");
});
const Allocator = std.mem.Allocator;
const xev = @import("xev");
const UDP = xev.UDP;
const io_loop = @import("../../thread_event_loop.zig");
const ssl = @import("ssl");

pub const LsquicEngine = struct {
    ssl_context: *ssl.SSL_CTX, // This will be set later

    engine: *c.lsquic_engine_t,

    allocator: Allocator,

    pub fn init(_: *LsquicEngine, _: Allocator, is_client: bool) !void {
        var flags: c_uint = 0;
        if (!is_client) {
            // For server, we need to set the server flag
            flags |= c.LSENG_SERVER;
        }

        var engine_settings: c.lsquic_engine_settings = undefined;
        c.lsquic_engine_init_settings(&engine_settings, flags);

        // TODO: Make the hardcoded values configurable
        engine_settings.es_init_max_stream_data_bidi_remote = 64 * 1024 * 1024; // 64 MB
        engine_settings.es_init_max_stream_data_bidi_local = 64 * 1024 * 1024; // 64 MB
        engine_settings.es_init_max_streams_bidi = 1000; // 1000 streams
        engine_settings.es_idle_timeout = 120; // 120 seconds
        engine_settings.es_handshake_to = 10 * std.time.us_per_s; // 10 seconds

        var err_buf: [100]u8 = undefined;
        if (c.lsquic_engine_check_settings(
            &engine_settings,
            flags,
            &err_buf,
            100,
        ) == 1) {
            @panic("lsquic_engine_check_settings failed " ++ err_buf);
        }

        // const stream_if: c.lsquic_stream_if = c.lsquic_stream_if{
        //     .on_new_conn = null,
        //     .on_conn_closed = null,
        //     .on_hsk_done = null,
        //     .on_new_stream = null,
        //     .on_read = null,
        //     .on_write = null,
        //     .on_close = null,
        // };
        // const engine_api: c.lsquic_engine_api = .{ .ea_settings = &engine_settings, .ea_stream_if = &stream_if, .ea_stream_if_ctx = self, .ea_packets_out = null, .ea_packets_out_ctx = self, .ea_get_ssl_ctx = &getSslContext };
        // const engine = c.lsquic_engine_new(flags, &engine_api);
        // if (engine == null) {
        //     return error.InitializationFailed;
        // }
        // self.* = .{
        //     .ssl_context = undefined, // This will be set later
        //     .engine = engine.?,
        //     .allocator = allocator,
        // };
    }

    fn getSslContext(
        peer_ctx: ?*anyopaque,
        _: ?*const c.struct_sockaddr,
    ) callconv(.c) ?*c.struct_ssl_ctx_st {
        const self: *LsquicEngine = @ptrCast(@alignCast(peer_ctx.?));
        const res: *c.struct_ssl_ctx_st = @ptrCast(@alignCast(self.ssl_context));
        return res;
    }
};

pub const LsquicConnection = struct {
    conn: *c.lsquic_conn_t,
};

pub const LsquicListener = struct {
    pub const AcceptError = Allocator.Error || xev.AcceptError || error{AsyncNotifyFailed};
    /// The error type returned by the `init` function. Want to remain the underlying error type, so we used `anyerror`.
    pub const ListenError = anyerror;
    /// The address to listen on.
    address: std.net.Address,
    /// The server to accept connections from.
    server: UDP,
    /// The transport that created this listener.
    transport: *LsquicTransport,

    accept_callback: ?*const fn (instance: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void = null,

    accept_callback_instance: ?*anyopaque = null,

    /// Initialize the listener with the given address, backlog, and transport.
    pub fn init(self: *LsquicListener, address: std.net.Address, transport: *LsquicTransport) ListenError!void {
        const server = try UDP.init(address);
        try server.bind(address);

        self.address = address;
        self.server = server;
        self.transport = transport;
    }

    /// Deinitialize the listener.
    pub fn deinit(_: *LsquicListener) void {
        // TODO: should we close the server here?
    }

    pub fn accept(_: *LsquicListener, _: ?*anyopaque, _: *const fn (instance: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void) void {}
};

pub const LsquicTransport = struct {
    pub const DialError = Allocator.Error || xev.ConnectError || error{AsyncNotifyFailed};

    ssl_context: *ssl.SSL_CTX,

    io_event_loop: *io_loop.ThreadEventLoop,

    /// The allocator.
    allocator: Allocator,

    pub fn init() !void {
        // Initialize the QUIC transport layer
        const result = c.lsquic_global_init(c.LSQUIC_GLOBAL_CLIENT);
        if (result != 0) {
            std.debug.print("Failed to initialize lsquic: {}\n", .{result});
            return error.InitializationFailed;
        }
        std.debug.print("lsquic initialized successfully.\n", .{});
    }

    pub fn deinit() void {
        // Cleanup the QUIC transport layer
        c.lsquic_global_cleanup();
        std.debug.print("lsquic cleaned up successfully.\n", .{});
    }

    pub fn dial(_: *LsquicTransport, _: std.net.Address, _: ?*anyopaque, _: *const fn (instance: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void) void {}
};

test "lsquic transport initialization" {
    try LsquicTransport.init();
    defer LsquicTransport.deinit();

    // Additional tests can be added here to verify functionality
}

test "lsquic engine initialization" {
    var engine: LsquicEngine = undefined;
    try LsquicEngine.init(&engine, std.testing.allocator, false);
    // defer c.lsquic_engine_destroy(engine.engine);

    // Additional tests can be added here to verify functionality
}
