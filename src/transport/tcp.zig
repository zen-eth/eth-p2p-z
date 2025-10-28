const std = @import("std");
const xev_backend = @import("../xev_backend.zig");
const xev = xev_backend.xev;
const TCP = xev.TCP;
const Allocator = std.mem.Allocator;
const ThreadPool = xev.ThreadPool;
const ResetEvent = std.Thread.ResetEvent;
const p2p_conn = @import("../conn.zig");
const p2p_transport = @import("../transport.zig");
const io_loop = @import("../thread_event_loop.zig");

/// SocketChannel represents a socket channel. It is used to send and receive messages.
pub const XevSocketChannel = struct {
    pub const WriteError = Allocator.Error || xev.WriteError || error{AsyncNotifyFailed};
    pub const CloseError = Allocator.Error || xev.ShutdownError || xev.CloseError || error{AsyncNotifyFailed};

    /// The underlying socket.
    socket: TCP,

    /// The transport that created this channel.
    transport: *XevTransport,

    /// Whether this channel is the initiator of the connection. If true, this channel is the client. If false, this channel is the server.
    direction: p2p_conn.Direction,
    /// The handler pipeline for this channel. It is used to handle incoming messages and events.
    handler_pipeline: *p2p_conn.HandlerPipeline,
    /// The security session for this channel, if any.
    session: ?p2p_conn.SecuritySession = null,

    // TODO: MAKE TIMEOUTS CONFIGURABLE
    /// The timeout for write operations in milliseconds.
    write_timeout_ms: u64 = 30000,
    /// The timeout for read operations in milliseconds.
    read_timeout_ms: u64 = 30000,

    /// Initialize the channel with the given socket and transport.
    pub fn init(self: *XevSocketChannel, socket: TCP, transport: *XevTransport, direction: p2p_conn.Direction) void {
        self.socket = socket;
        self.transport = transport;
        self.direction = direction;
    }

    /// Write the given buffer to the socket channel asynchronously.
    pub fn write(self: *XevSocketChannel, buffer: []const u8, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void) void {
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            const w_ctx = self.transport.io_event_loop.write_ctx_pool.create() catch unreachable;

            w_ctx.* = .{
                .channel = self,
                .callback_instance = callback_instance,
                .callback = callback,
            };
            self.socket.write(&self.transport.io_event_loop.loop, c, .{ .slice = buffer }, io_loop.WriteCtx, w_ctx, writeCallback);
        } else {
            io_loop.ThreadEventLoop.TcpTasks.queueTcpWrite(self.transport.io_event_loop, self, buffer, self.write_timeout_ms, callback_instance, callback) catch |err| {
                callback(callback_instance, err);
            };
        }
    }

    /// Close the socket channel asynchronously.
    pub fn close(self: *XevSocketChannel, callback_instance: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            const close_ctx = self.transport.io_event_loop.close_ctx_pool.create() catch unreachable;

            close_ctx.* = .{
                .channel = self,
                .callback_instance = callback_instance,
                .callback = callback,
            };
            self.socket.shutdown(&self.transport.io_event_loop.loop, c, io_loop.CloseCtx, close_ctx, shutdownCB);
        } else {
            io_loop.ThreadEventLoop.TcpTasks.queueTcpClose(self.transport.io_event_loop, self, 30000, callback_instance, callback) catch |err| {
                callback(callback_instance, err);
            };
        }
    }

    /// Get the direction of the connection (INBOUND or OUTBOUND).
    pub fn connDirection(self: *XevSocketChannel) p2p_conn.Direction {
        return self.direction;
    }

    /// Get the handler pipeline for this channel. It is used to handle incoming messages and events.
    pub fn handlerPipeline(self: *XevSocketChannel) *p2p_conn.HandlerPipeline {
        return self.handler_pipeline;
    }

    /// Set the security session for this channel. This is used for secure communication.
    pub fn setSession(self: *XevSocketChannel, session: p2p_conn.SecuritySession) void {
        self.session = session;
    }

    /// Get the security session for this channel, if any. This is used for secure communication.
    pub fn getSession(self: *XevSocketChannel) ?p2p_conn.SecuritySession {
        return self.session;
    }

    // --- Static Wrapper Functions ---
    fn vtableGetPipelineFn(instance: *anyopaque) *p2p_conn.HandlerPipeline {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.handlerPipeline();
    }

    fn vtableDirectionFn(instance: *anyopaque) p2p_conn.Direction {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.connDirection();
    }

    fn vtableWriteFn(
        instance: *anyopaque,
        buffer: []const u8,
        callback_instance: ?*anyopaque,
        callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void,
    ) void {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.write(buffer, callback_instance, callback);
    }

    fn vtableCloseFn(
        instance: *anyopaque,
        callback_instance: ?*anyopaque,
        callback: *const fn (instance: ?*anyopaque, res: anyerror!void) void,
    ) void {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.close(callback_instance, callback);
    }

    fn vtableGetSessionFn(instance: *anyopaque) ?p2p_conn.SecuritySession {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.getSession();
    }

    fn vtableSetSessionFn(instance: *anyopaque, session: p2p_conn.SecuritySession) void {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        self.setSession(session);
    }

    // --- Static VTable Instance ---
    const vtable_instance = p2p_conn.ConnVTable{
        .handlerPipelineFn = vtableGetPipelineFn,
        .directionFn = vtableDirectionFn,
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
        .securitySessionFn = vtableGetSessionFn,
        .setSecuritySessionFn = vtableSetSessionFn,
    };

    pub fn any(self: *XevSocketChannel) p2p_conn.AnyConn {
        return .{ .instance = self, .vtable = &vtable_instance };
    }

    pub fn writeCallback(
        instance: ?*io_loop.WriteCtx,
        _: *xev.Loop,
        c: *xev.Completion,
        _: xev.TCP,
        _: xev.WriteBuffer,
        r: xev.WriteError!usize,
    ) xev.CallbackAction {
        const w_ctx = instance.?;
        const transport = w_ctx.channel.transport;
        defer transport.io_event_loop.completion_pool.destroy(c);
        defer transport.io_event_loop.write_ctx_pool.destroy(w_ctx);

        // In general, user defined callbacks should close the channel on error.
        w_ctx.callback(w_ctx.callback_instance, r);

        return .disarm;
    }

    pub fn readCB(
        instance: ?*XevSocketChannel,
        loop: *xev.Loop,
        c: *xev.Completion,
        _: xev.TCP,
        rb: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        const channel = instance.?;
        const transport = channel.transport;

        const read_bytes = r catch |err| switch (err) {
            error.EOF => {
                // Default behavior on EOF is to close the channel in the `TailHandler` of the pipeline.
                channel.handlerPipeline().fireReadComplete();
                transport.io_event_loop.read_buffer_pool.destroy(
                    @alignCast(
                        @as(*[io_loop.BUFFER_SIZE]u8, @ptrFromInt(@intFromPtr(rb.slice.ptr))),
                    ),
                );
                transport.io_event_loop.completion_pool.destroy(c);
                return .disarm;
            },

            else => {
                // On any other error, we log the error and close the channel, also in the `TailHandler`.
                channel.handlerPipeline().fireErrorCaught(err);
                transport.io_event_loop.read_buffer_pool.destroy(
                    @alignCast(
                        @as(*[io_loop.BUFFER_SIZE]u8, @ptrFromInt(@intFromPtr(rb.slice.ptr))),
                    ),
                );
                transport.io_event_loop.completion_pool.destroy(c);
                return .disarm;
            },
        };

        if (read_bytes > 0) {
            channel.handlerPipeline().fireRead(rb.slice[0..read_bytes]) catch {
                transport.io_event_loop.read_buffer_pool.destroy(
                    @alignCast(
                        @as(*[io_loop.BUFFER_SIZE]u8, @ptrFromInt(@intFromPtr(rb.slice.ptr))),
                    ),
                );
                transport.io_event_loop.completion_pool.destroy(c);
                return .disarm;
            };
        }

        if (loop.stopped()) {
            return .disarm;
        }
        return .rearm;
    }

    /// Callback executed when a socket shutdown operation completes.
    /// This initiates a full socket close after the shutdown completes.
    pub fn shutdownCB(
        ud: ?*io_loop.CloseCtx,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.ShutdownError!void,
    ) xev.CallbackAction {
        const close_ud = ud.?;

        _ = r catch |err| {
            close_ud.callback(close_ud.callback_instance, err);

            return .disarm;
        };

        s.close(l, c, io_loop.CloseCtx, ud, closeCB);
        return .disarm;
    }

    /// Callback executed when a socket close operation completes.
    /// This is the final step in the socket cleanup process.
    pub fn closeCB(
        instance: ?*io_loop.CloseCtx,
        _: *xev.Loop,
        c: *xev.Completion,
        _: xev.TCP,
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        const close_ctx = instance.?;
        const channel = close_ctx.channel;
        const transport = channel.transport;
        defer {
            const p = channel.handlerPipeline();
            p.deinit();
            transport.io_event_loop.handler_pipeline_pool.destroy(p);

            transport.allocator.destroy(channel);
            transport.io_event_loop.completion_pool.destroy(c);
            transport.io_event_loop.close_ctx_pool.destroy(close_ctx);
        }

        close_ctx.callback(close_ctx.callback_instance, r);

        _ = r catch |err| {
            std.log.warn("Error closing socket: {}\n", .{err});
            return .disarm;
        };

        channel.handlerPipeline().fireInactive();

        return .disarm;
    }

    fn noopCloseCB(ud: ?*anyopaque, r: anyerror!void) void {
        _ = ud;
        _ = r catch |err| {
            std.log.warn("Error in no-op close callback: {}\n", .{err});
        };
    }
};

/// Listener listens for incoming connections. It is used to accept connections.
pub const XevListener = struct {
    pub const AcceptError = Allocator.Error || xev.AcceptError || error{AsyncNotifyFailed};
    /// The error type returned by the `init` function. Want to remain the underlying error type, so we used `anyerror`.
    pub const ListenError = anyerror;
    /// The address to listen on.
    address: std.net.Address,
    /// The server to accept connections from.
    server: TCP,
    /// The transport that created this listener.
    transport: *XevTransport,

    /// Initialize the listener with the given address, backlog, and transport.
    pub fn init(self: *XevListener, address: std.net.Address, backlog: u31, transport: *XevTransport) ListenError!void {
        const server = try TCP.init(address);
        try server.bind(address);
        try server.listen(backlog);

        self.address = address;
        self.server = server;
        self.transport = transport;
    }

    /// Deinitialize the listener.
    pub fn deinit(_: *XevListener) void {
        // TODO: should we close the server here?
    }

    /// Accept asynchronously accept a connection from the listener. It does not block. If an error occurs, it returns the error.
    pub fn accept(self: *XevListener, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void) void {
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            const accept_ud = self.transport.io_event_loop.accept_ctx_pool.create() catch unreachable;

            accept_ud.* = .{
                .transport = self.transport,
                .callback_instance = callback_instance,
                .callback = callback,
            };
            self.server.accept(&self.transport.io_event_loop.loop, c, io_loop.AcceptCtx, accept_ud, acceptCB);
        } else {
            io_loop.ThreadEventLoop.TcpTasks.queueTcpAccept(self.transport.io_event_loop, self.server, self.transport, 30000, callback_instance, callback) catch |err| {
                callback(callback_instance, err);
            };
        }
    }

    // --- Static Wrapper Function for ListenerVTable ---
    fn vtableAcceptFn(instance: *anyopaque, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!p2p_conn.AnyConn) void) void {
        const self: *XevListener = @ptrCast(@alignCast(instance));
        return self.accept(user_data, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = p2p_transport.ListenerVTable{
        .acceptFn = vtableAcceptFn,
    };

    // --- any() method ---
    pub fn any(self: *XevListener) p2p_transport.AnyListener {
        return .{ .instance = self, .vtable = &vtable_instance };
    }

    pub fn acceptCB(
        instance: ?*io_loop.AcceptCtx,
        l: *xev.Loop,
        c: *xev.Completion,
        r: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const accept_ctx = instance.?;
        const transport = accept_ctx.transport;
        defer transport.io_event_loop.completion_pool.destroy(c);
        defer transport.io_event_loop.accept_ctx_pool.destroy(accept_ctx);

        const socket = r catch |err| {
            accept_ctx.callback(accept_ctx.callback_instance, err);

            return .disarm;
        };

        const channel = transport.allocator.create(XevSocketChannel) catch unreachable;
        channel.init(socket, transport, p2p_conn.Direction.INBOUND);
        const any_rx_conn = channel.any();

        const p = transport.io_event_loop.handler_pipeline_pool.create() catch unreachable;
        p.init(transport.allocator, any_rx_conn) catch |err| {
            const close_ctx = transport.io_event_loop.close_ctx_pool.create() catch unreachable;
            close_ctx.* = .{
                .channel = channel,
                .callback = XevSocketChannel.noopCloseCB,
                .callback_instance = channel,
            };
            socket.shutdown(l, c, io_loop.CloseCtx, close_ctx, XevSocketChannel.shutdownCB);

            accept_ctx.callback(accept_ctx.callback_instance, err);
            return .disarm;
        };
        channel.handler_pipeline = p;

        transport.conn_enhancer.enhanceConn(any_rx_conn) catch |err| {
            const close_ctx = transport.io_event_loop.close_ctx_pool.create() catch unreachable;
            close_ctx.* = .{
                .channel = channel,
                .callback = XevSocketChannel.noopCloseCB,
                .callback_instance = channel,
            };
            socket.shutdown(l, c, io_loop.CloseCtx, close_ctx, XevSocketChannel.shutdownCB);

            accept_ctx.callback(accept_ctx.callback_instance, err);
            return .disarm;
        };

        accept_ctx.callback(accept_ctx.callback_instance, any_rx_conn);
        p.fireActive() catch |err| {
            accept_ctx.callback(accept_ctx.callback_instance, err);
            return .disarm;
        };

        const read_buf = transport.io_event_loop.read_buffer_pool.create() catch unreachable;
        const read_c = transport.io_event_loop.completion_pool.create() catch unreachable;
        socket.read(l, read_c, .{ .slice = read_buf }, XevSocketChannel, channel, XevSocketChannel.readCB);

        return .disarm;
    }
};

/// Transport is a transport that uses the xev library for asynchronous I/O operations.
pub const XevTransport = struct {
    pub const DialError = Allocator.Error || xev.ConnectError || error{AsyncNotifyFailed};

    conn_enhancer: p2p_conn.AnyConnEnhancer,

    io_event_loop: *io_loop.ThreadEventLoop,

    /// The options for the transport.
    options: Options,

    /// The allocator.
    allocator: Allocator,

    /// Options for the transport.
    pub const Options = struct {
        /// The maximum number of pending connections.
        backlog: u31,
    };

    /// Initialize the transport with the given allocator and options.
    pub fn init(self: *XevTransport, conn_enhancer: p2p_conn.AnyConnEnhancer, loop: *io_loop.ThreadEventLoop, allocator: Allocator, opts: Options) !void {
        self.* = .{
            .conn_enhancer = conn_enhancer,
            .io_event_loop = loop,
            .options = opts,
            .allocator = allocator,
        };
    }

    /// Deinitialize the transport.
    pub fn deinit(_: *XevTransport) void {}

    /// Dial connects to the given address and creates a channel for communication. It blocks until the connection is established. If an error occurs, it returns the error.
    pub fn dial(self: *XevTransport, addr: std.net.Address, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void) void {
        if (self.io_event_loop.inEventLoopThread()) {
            var socket = xev.TCP.init(addr) catch unreachable;
            const c = self.io_event_loop.completion_pool.create() catch unreachable;
            // const timer = xev.Timer.init() catch unreachable;
            // const c_timer = self.completion_pool.create() catch unreachable;
            // const connect_timeout = action_data.timeout_ms;
            const connect_ud = self.io_event_loop.connect_ctx_pool.create() catch unreachable;
            connect_ud.* = .{
                .transport = self,
                .callback = callback,
                .callback_instance = callback_instance,
            };
            // const connect_timeout_ud = self.connect_timeout_pool.create() catch unreachable;
            // connect_timeout_ud.* = .{
            //     .future = action_data.future,
            //     .transport = self,
            //     .socket = socket,
            // };
            socket.connect(&self.io_event_loop.loop, c, addr, io_loop.ConnectCtx, connect_ud, connectCB);
        } else {
            io_loop.ThreadEventLoop.TcpTasks.queueTcpConnect(self.io_event_loop, addr, self, 30000, callback_instance, callback) catch |err| {
                callback(callback_instance, err);
            };
        }
    }

    /// Listen listens for incoming connections on the given address. It blocks until the listener is initialized. If an error occurs, it returns the error.
    pub fn listen(self: *XevTransport, addr: std.net.Address) XevListener.ListenError!p2p_transport.AnyListener {
        const l = try self.allocator.create(XevListener);
        try l.init(addr, self.options.backlog, self);
        return l.any();
    }

    // --- Static Wrapper Functions for TransportVTable ---
    fn vtableDialFn(instance: *anyopaque, addr: std.net.Address, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!p2p_conn.AnyConn) void) void {
        const self: *XevTransport = @ptrCast(@alignCast(instance));
        return self.dial(addr, user_data, callback);
    }

    fn vtableListenFn(instance: *anyopaque, addr: std.net.Address) anyerror!p2p_transport.AnyListener {
        const self: *XevTransport = @ptrCast(@alignCast(instance));
        return self.listen(addr);
    }

    // --- Static VTable Instance ---
    const vtable_instance = p2p_transport.TransportVTable{
        .dialFn = vtableDialFn,
        .listenFn = vtableListenFn,
    };

    // --- any() method ---
    pub fn any(self: *XevTransport) p2p_transport.AnyTransport {
        return .{ .instance = self, .vtable = &vtable_instance };
    }

    /// Start starts the transport. It blocks until the transport is stopped.
    /// It is recommended to call this function in a separate thread.
    pub fn start(_: *XevTransport) !void {}

    /// Close closes the transport.
    pub fn close(_: *XevTransport) void {}

    /// Callback executed when a connection attempt completes.
    /// This handles the result of trying to connect to a remote address.
    pub fn connectCB(
        ud: ?*io_loop.ConnectCtx,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.ConnectError!void,
    ) xev.CallbackAction {
        const connect = ud.?;
        const transport = connect.transport;
        defer transport.io_event_loop.completion_pool.destroy(c);
        defer transport.io_event_loop.connect_ctx_pool.destroy(connect);

        _ = r catch |err| {
            connect.callback(connect.callback_instance, err);

            return .disarm;
        };

        const channel = transport.allocator.create(XevSocketChannel) catch unreachable;
        channel.init(socket, transport, p2p_conn.Direction.OUTBOUND);

        const handler_pipeline = transport.io_event_loop.handler_pipeline_pool.create() catch unreachable;
        const associated_conn = channel.any();

        handler_pipeline.init(transport.allocator, associated_conn) catch |err| {
            const close_ctx = transport.io_event_loop.close_ctx_pool.create() catch unreachable;
            close_ctx.* = .{
                .channel = channel,
                .callback = XevSocketChannel.noopCloseCB,
                .callback_instance = channel,
            };
            socket.shutdown(l, c, io_loop.CloseCtx, close_ctx, XevSocketChannel.shutdownCB);

            connect.callback(connect.callback_instance, err);
            return .disarm;
        };
        channel.handler_pipeline = handler_pipeline;

        transport.conn_enhancer.enhanceConn(associated_conn) catch |err| {
            const close_ctx = transport.io_event_loop.close_ctx_pool.create() catch unreachable;
            close_ctx.* = .{
                .channel = channel,
                .callback = XevSocketChannel.noopCloseCB,
                .callback_instance = channel,
            };
            socket.shutdown(l, c, io_loop.CloseCtx, close_ctx, XevSocketChannel.shutdownCB);

            connect.callback(connect.callback_instance, err);
            return .disarm;
        };

        connect.callback(connect.callback_instance, associated_conn);
        handler_pipeline.fireActive() catch |err| {
            connect.callback(connect.callback_instance, err);
            return .disarm;
        };

        const read_buf = transport.io_event_loop.read_buffer_pool.create() catch unreachable;
        const read_c = transport.io_event_loop.completion_pool.create() catch unreachable;
        socket.read(l, read_c, .{ .slice = read_buf }, XevSocketChannel, channel, XevSocketChannel.readCB);

        return .disarm;
    }

    // pub fn connectTimeoutCB(
    //     ud: ?*io_loop.ConnectTimeout,
    //     l: *xev.Loop,
    //     c: *xev.Completion,
    //     r: xev.Timer.RunError!void,
    // ) xev.CallbackAction {
    //     const connect_timeout = ud.?;
    //     const transport = connect_timeout.transport.?;
    //     defer transport.io_event_loop.completion_pool.destroy(c);
    //     defer transport.io_event_loop.connect_timeout_pool.destroy(connect_timeout);

    //     _ = r catch |err| {
    //         connect_timeout.future.rwlock.lock();
    //         if (!connect_timeout.future.isDone()) {
    //             connect_timeout.future.setError(err);
    //             const shutdown_c = transport.io_event_loop.completion_pool.create() catch unreachable;
    //             connect_timeout.socket.shutdown(l, shutdown_c, XevTransport, transport, shutdownCB);
    //         }
    //         connect_timeout.future.rwlock.unlock();

    //         return .disarm;
    //     };

    //     connect_timeout.future.rwlock.lock();
    //     if (!connect_timeout.future.isDone()) {
    //         connect_timeout.future.setError(error.Timeout);
    //         const shutdown_c = transport.io_event_loop.completion_pool.create() catch unreachable;
    //         connect_timeout.socket.shutdown(l, shutdown_c, XevTransport, transport, shutdownCB);
    //     }
    //     connect_timeout.future.rwlock.unlock();

    //     return .disarm;
    // }

};

const MockConnInitiator = struct {
    pub const Error = anyerror;

    const Self = @This();

    pub fn initConnImpl(self: *Self, conn: p2p_conn.AnyConn) Error!void {
        _ = self;
        _ = conn;
        return;
    }

    // Static wrapper function for the VTable
    fn vtableInitConnFn(instance: *anyopaque, conn: p2p_conn.AnyConn) Error!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.initConnImpl(conn);
    }

    // Static VTable instance
    const vtable_instance = p2p_conn.ConnEnhancerVTable{
        .enhanceConnFn = vtableInitConnFn,
    };

    pub fn any(self: *Self) p2p_conn.AnyConnEnhancer {
        return .{
            .instance = self,
            .vtable = &vtable_instance,
        };
    }
};

const MockConnInitiator1 = struct {
    pub const Error = anyerror;
    handler_to_add: ?p2p_conn.AnyConnHandler = null,
    allocator: ?Allocator = null,

    const Self = @This();

    pub fn init(handler: ?p2p_conn.AnyConnHandler, alloc: ?Allocator) Self {
        return .{
            .handler_to_add = handler,
            .allocator = alloc,
        };
    }

    pub fn initConnImpl(self: *Self, conn: p2p_conn.AnyConn) Error!void {
        if (self.handler_to_add) |handler| {
            const pipeline = conn.getPipeline();
            try pipeline.addLast("handler", handler);
        }
        return;
    }

    // Static wrapper function for the VTable
    fn vtableInitConnFn(instance: *anyopaque, conn: p2p_conn.AnyConn) Error!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.initConnImpl(conn);
    }

    // Static VTable instance
    const vtable_instance = p2p_conn.ConnEnhancerVTable{
        .enhanceConnFn = vtableInitConnFn,
    };

    pub fn any(self: *Self) p2p_conn.AnyConnEnhancer {
        return .{
            .instance = self,
            .vtable = &vtable_instance,
        };
    }
};

const ConnHolder = struct {
    channel: p2p_conn.AnyConn,
    ready: std.Thread.ResetEvent = .{},
    err: ?anyerror = null,

    pub fn init(opaque_userdata: ?*anyopaque, accept_result: anyerror!p2p_conn.AnyConn) void {
        const self: *@This() = @ptrCast(@alignCast(opaque_userdata.?));

        const accepted_channel = accept_result catch |err| {
            self.err = err;
            self.ready.set();
            return;
        };

        self.channel = accepted_channel;
        self.ready.set();
    }
};

test "dial connection refused" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    const opts = XevTransport.Options{
        .backlog = 128,
    };

    var mock_initiator = MockConnInitiator{};
    const any_initiator = mock_initiator.any();

    var event_loop: io_loop.ThreadEventLoop = undefined;
    try event_loop.init(allocator);
    defer {
        event_loop.close();
        event_loop.deinit();
    }

    var transport: XevTransport = undefined;
    try transport.init(any_initiator, &event_loop, allocator, opts);
    defer transport.deinit();

    var conn_holder: ConnHolder = .{ .channel = undefined };
    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
    transport.dial(addr, &conn_holder, ConnHolder.init);
    conn_holder.ready.wait();
    try std.testing.expectEqual(error.ConnectionRefused, conn_holder.err.?);

    var conn_holder1: ConnHolder = .{ .channel = undefined };
    transport.dial(addr, &conn_holder1, ConnHolder.init);
    conn_holder1.ready.wait();
    try std.testing.expectEqual(error.ConnectionRefused, conn_holder1.err.?);

    const thread2 = try std.Thread.spawn(.{}, struct {
        fn run(t: *XevTransport, a: std.net.Address) !void {
            var conn_holder2: ConnHolder = .{ .channel = undefined };
            t.dial(a, &conn_holder2, ConnHolder.init);
            conn_holder2.ready.wait();
            try std.testing.expectEqual(error.ConnectionRefused, conn_holder2.err.?);
        }
    }.run, .{ &transport, addr });

    thread2.join();
}

test "dial and accept" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var mock_initiator = MockConnInitiator{};
    const any_initiator = mock_initiator.any();

    var sl: io_loop.ThreadEventLoop = undefined;
    try sl.init(allocator);
    defer {
        sl.close();
        sl.deinit();
    }

    const opts = XevTransport.Options{
        .backlog = 128,
    };

    var transport: XevTransport = undefined;
    try transport.init(any_initiator, &sl, allocator, opts);
    defer transport.deinit();

    const addr = try std.net.Address.parseIp("0.0.0.0", 8082);
    var listener = try transport.listen(addr);

    const accept_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *p2p_transport.AnyListener) !void {
            var accepted_count: usize = 0;
            while (accepted_count < 2) : (accepted_count += 1) {
                var conn_holder: ConnHolder = .{ .channel = undefined };
                l.accept(&conn_holder, ConnHolder.init);
                conn_holder.ready.wait();
                try std.testing.expectEqual(conn_holder.channel.direction(), p2p_conn.Direction.INBOUND);
            }
        }
    }.run, .{&listener});

    var cl: io_loop.ThreadEventLoop = undefined;
    try cl.init(allocator);
    defer {
        cl.close();
        cl.deinit();
    }
    var client: XevTransport = undefined;
    try client.init(any_initiator, &cl, allocator, opts);
    defer client.deinit();

    var conn_holder1: ConnHolder = .{ .channel = undefined };
    client.dial(addr, &conn_holder1, ConnHolder.init);
    conn_holder1.ready.wait();
    try std.testing.expectEqual(conn_holder1.channel.direction(), p2p_conn.Direction.OUTBOUND);

    var conn_holder2: ConnHolder = .{ .channel = undefined };
    client.dial(addr, &conn_holder2, ConnHolder.init);
    conn_holder2.ready.wait();
    try std.testing.expectEqual(conn_holder2.channel.direction(), p2p_conn.Direction.OUTBOUND);

    accept_thread.join();
}

const OnWriteCallback = struct {
    fn callback(
        _: ?*anyopaque,
        r: anyerror!usize,
    ) void {
        if (r) |_| {} else |err| {
            std.debug.print("Test Failure: Expected successful write, but got error: {any}\n", .{err});
            @panic("write callback received an unexpected error");
        }
    }
};

pub const ServerEchoHandler = struct {
    allocator: Allocator,

    received_message: []u8,

    channel: p2p_conn.AnyConn,

    ready: std.Thread.ResetEvent = .{},

    err: ?anyerror = null,

    const Self = @This();

    pub fn create(alloc: Allocator) !*Self {
        const self = try alloc.create(Self);
        const received_message = try alloc.alloc(u8, 1024);
        @memset(received_message, 0);
        self.* = .{ .allocator = alloc, .received_message = received_message, .channel = undefined };
        return self;
    }

    pub fn destroy(self: *Self) void {
        self.allocator.free(self.received_message);
        self.allocator.destroy(self);
    }

    pub fn init(opaque_userdata: ?*anyopaque, accept_result: anyerror!p2p_conn.AnyConn) void {
        const self: *@This() = @ptrCast(@alignCast(opaque_userdata.?));

        const accepted_channel = accept_result catch |err| {
            self.err = err;
            self.ready.set();
            return;
        };

        self.channel = accepted_channel;
        self.ready.set();
    }

    // --- Actual Handler Implementations ---
    pub fn onActiveImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext) !void {
        _ = self;
        try ctx.fireActive();
    }

    pub fn onInactiveImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext) void {
        _ = self;
        ctx.fireInactive();
    }

    pub fn onReadImpl(_: *Self, ctx: *p2p_conn.ConnHandlerContext, msg: []const u8) !void {
        var onWriteCallback = OnWriteCallback{};
        ctx.write(msg, &onWriteCallback, OnWriteCallback.callback);

        try ctx.fireRead(msg);
    }

    pub fn onReadCompleteImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext) void {
        _ = self;
        ctx.fireReadComplete();
    }

    pub fn onErrorCaughtImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext, err: anyerror) void {
        _ = self;
        ctx.fireErrorCaught(err);
    }

    pub fn writeImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        _ = self;
        ctx.write(buffer, user_data, callback);
    }

    pub fn closeImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        _ = self;
        ctx.close(user_data, callback);
    }

    // --- Static Wrapper Functions for HandlerVTable ---
    fn vtableOnActiveFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) !void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return try self.onActiveImpl(ctx);
    }

    fn vtableOnInactiveFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInactiveImpl(ctx);
    }

    fn vtableOnReadFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, msg: []const u8) !void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return try self.onReadImpl(ctx, msg);
    }

    fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onReadCompleteImpl(ctx);
    }

    fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, err: anyerror) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onErrorCaughtImpl(ctx, err);
    }

    fn vtableWriteFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.writeImpl(ctx, buffer, user_data, callback);
    }

    fn vtableCloseFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.closeImpl(ctx, user_data, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = p2p_conn.ConnHandlerVTable{
        .onActiveFn = vtableOnActiveFn,
        .onInactiveFn = vtableOnInactiveFn,
        .onReadFn = vtableOnReadFn,
        .onReadCompleteFn = vtableOnReadCompleteFn,
        .onErrorCaughtFn = vtableOnErrorCaughtFn,
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
    };

    pub fn any(self: *Self) p2p_conn.AnyConnHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

pub const ClientEchoHandler = struct {
    allocator: Allocator,

    received_message: []u8,

    read: std.Thread.ResetEvent = .{},

    channel: p2p_conn.AnyConn,

    ready: std.Thread.ResetEvent = .{},

    err: ?anyerror = null,

    written: usize,

    written_ready: std.Thread.ResetEvent = .{},

    const Self = @This();

    pub fn create(alloc: Allocator) !*Self {
        const self = try alloc.create(Self);
        const received_message = try alloc.alloc(u8, 1024);
        @memset(received_message, 0);
        self.* = .{ .allocator = alloc, .received_message = received_message, .written = 0, .channel = undefined };
        return self;
    }

    pub fn destroy(self: *Self) void {
        self.allocator.free(self.received_message);
        self.allocator.destroy(self);
    }

    pub fn onWrite(ud: ?*anyopaque, r: anyerror!usize) void {
        const self: *Self = @ptrCast(@alignCast(ud.?));

        if (r) |bytes_written| {
            self.written = bytes_written;
        } else |_| {
            self.written = 0;
        }

        self.written_ready.set();
    }

    pub fn init(opaque_userdata: ?*anyopaque, accept_result: anyerror!p2p_conn.AnyConn) void {
        const self: *@This() = @ptrCast(@alignCast(opaque_userdata.?));

        const accepted_channel = accept_result catch |err| {
            self.err = err;
            self.ready.set();
            return;
        };

        self.channel = accepted_channel;
        self.ready.set();
    }

    // --- Actual Handler Implementations ---
    pub fn onActiveImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext) !void {
        _ = self;
        try ctx.fireActive();
    }

    pub fn onInactiveImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext) void {
        _ = self;
        ctx.fireInactive();
    }

    pub fn onReadImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext, msg: []const u8) !void {
        const len = msg.len;
        @memcpy(self.received_message[0..len], msg[0..len]);
        self.read.set();
        try ctx.fireRead(msg);
    }

    pub fn onReadCompleteImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext) void {
        _ = self;
        ctx.fireReadComplete();
    }

    pub fn onErrorCaughtImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext, err: anyerror) void {
        _ = self;

        ctx.fireErrorCaught(err);
    }

    pub fn writeImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        _ = self;
        ctx.write(buffer, user_data, callback);
    }

    pub fn closeImpl(self: *Self, ctx: *p2p_conn.ConnHandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        _ = self;

        ctx.close(user_data, callback);
    }

    // --- Static Wrapper Functions for HandlerVTable ---
    fn vtableOnActiveFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) !void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return try self.onActiveImpl(ctx);
    }

    fn vtableOnInactiveFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInactiveImpl(ctx);
    }

    fn vtableOnReadFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, msg: []const u8) !void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return try self.onReadImpl(ctx, msg);
    }

    fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onReadCompleteImpl(ctx);
    }

    fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, err: anyerror) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onErrorCaughtImpl(ctx, err);
    }

    fn vtableWriteFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.writeImpl(ctx, buffer, user_data, callback);
    }

    fn vtableCloseFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.closeImpl(ctx, user_data, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = p2p_conn.ConnHandlerVTable{
        .onActiveFn = vtableOnActiveFn,
        .onInactiveFn = vtableOnInactiveFn,
        .onReadFn = vtableOnReadFn,
        .onReadCompleteFn = vtableOnReadCompleteFn,
        .onErrorCaughtFn = vtableOnErrorCaughtFn,
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
    };

    pub fn any(self: *Self) p2p_conn.AnyConnHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

test "echo read and write" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const server_handler = try ServerEchoHandler.create(allocator);
    defer server_handler.destroy();

    const client_handler = try ClientEchoHandler.create(allocator);
    defer client_handler.destroy();
    const server_handler_any = server_handler.any();
    const client_handler_any = client_handler.any();

    var mock_client_initiator = MockConnInitiator1.init(client_handler_any, allocator);
    const any_client_initiator = mock_client_initiator.any();

    var mock_server_initiator = MockConnInitiator1.init(server_handler_any, allocator);
    const any_server_initiator = mock_server_initiator.any();

    var sl: io_loop.ThreadEventLoop = undefined;
    try sl.init(allocator);
    defer {
        sl.close();
        sl.deinit();
    }

    const opts = XevTransport.Options{
        .backlog = 128,
    };

    var transport: XevTransport = undefined;
    try transport.init(any_server_initiator, &sl, allocator, opts);
    defer transport.deinit();

    const addr = try std.net.Address.parseIp("0.0.0.0", 8083);
    var listener = try transport.listen(addr);

    const accept_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *p2p_transport.AnyListener, sh: *ServerEchoHandler) !void {
            var accepted_count: usize = 0;
            while (accepted_count < 1) : (accepted_count += 1) {
                l.accept(sh, ServerEchoHandler.init);
                sh.ready.wait();
                try std.testing.expectEqual(sh.channel.direction(), p2p_conn.Direction.INBOUND);
            }
        }
    }.run, .{ &listener, server_handler });

    var cl: io_loop.ThreadEventLoop = undefined;
    try cl.init(allocator);
    defer {
        cl.close();
        cl.deinit();
    }
    var client: XevTransport = undefined;
    try client.init(any_client_initiator, &cl, allocator, opts);
    defer client.deinit();

    client.dial(addr, client_handler, ClientEchoHandler.init);
    client_handler.ready.wait();
    try std.testing.expectEqual(client_handler.channel.direction(), p2p_conn.Direction.OUTBOUND);

    client_handler.channel.write("buf: []const u8", client_handler, ClientEchoHandler.onWrite);
    client_handler.written_ready.wait();
    try std.testing.expect(client_handler.written == 15);

    client_handler.read.wait();
    try std.testing.expectEqualStrings("buf: []const u8", client_handler.received_message[0..client_handler.written]);
    accept_thread.join();
}
