const std = @import("std");
const xev = @import("xev");
const TCP = xev.TCP;
const Allocator = std.mem.Allocator;
const ThreadPool = xev.ThreadPool;
const ResetEvent = std.Thread.ResetEvent;
const p2p_conn = @import("../../conn.zig");
const p2p_transport = @import("../../transport.zig");
const io_loop = @import("../../thread_event_loop.zig");
const Future = @import("../../concurrent/lib.zig").Future;

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

    handler_pipeline: *p2p_conn.HandlerPipeline,

    // TODO: MAKE TIMEOUTS CONFIGURABLE
    write_timeout_ms: u64 = 30000,

    read_timeout_ms: u64 = 30000,

    /// Initialize the channel with the given socket and transport.
    pub fn init(self: *XevSocketChannel, socket: TCP, transport: *XevTransport, direction: p2p_conn.Direction) void {
        self.socket = socket;
        self.transport = transport;
        self.direction = direction;
    }

    /// Write sends the buffer to the other end of the channel. It blocks until the write is complete. If an error occurs, it returns the error.
    pub fn write(self: *XevSocketChannel, msg: []const u8) !usize {
        const f = try self.transport.allocator.create(Future(usize, anyerror));
        std.debug.print("WriteCB called44444 {*}\n", .{f});

        defer self.transport.allocator.destroy(f);
        f.* = .{};
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            const w = self.transport.io_event_loop.write_pool.create() catch unreachable;

            w.* = .{
                .transport = self.transport,
                .future = f,
            };
            std.debug.print("WriteCB called55555 {*}\n", .{w});
            self.socket.write(&self.transport.io_event_loop.loop, c, .{ .slice = msg }, io_loop.Write, w, writeCB);
            std.debug.print("WriteCB called66666 \n", .{});
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .write = .{
                    .channel = self,
                    .buffer = msg,
                    .future = f,
                    .timeout_ms = self.write_timeout_ms,
                } },
            };

            self.transport.io_event_loop.queueMessage(message) catch |err| {
                std.debug.print("Error queuing message: {}\n", .{err});
                return error.AsyncNotifyFailed;
            };
        }

        f.wait();
        if (f.getErr()) |err| {
            std.debug.print("Error writing: {}\n", .{err});
            return err;
        }
        return f.getValue().?;
    }

    pub fn asyncWrite(self: *XevSocketChannel, buffer: []const u8, erased_userdata: ?*anyopaque, wrapped_cb: ?*const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            const w = self.transport.io_event_loop.write_pool.create() catch unreachable;

            w.* = .{
                .transport = self.transport,
                .user_data = erased_userdata,
                .callback = wrapped_cb,
            };
            std.debug.print("WriteCB called55555 {*}\n", .{w});
            self.socket.write(&self.transport.io_event_loop.loop, c, .{ .slice = buffer }, io_loop.Write, w, writeCB);
            std.debug.print("WriteCB called66666 \n", .{});
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .write = .{
                    .channel = self,
                    .buffer = buffer,
                    .user_data = erased_userdata,
                    .callback = wrapped_cb,
                    .timeout_ms = self.write_timeout_ms,
                } },
            };

            self.transport.io_event_loop.queueMessage(message) catch |err| {
                std.debug.print("Error queuing message: {}\n", .{err});
                if (wrapped_cb) |cb| {
                    cb(erased_userdata, err);
                }
            };
        }
    }

    /// Close closes the channel. It blocks until the close is complete.
    pub fn close(self: *XevSocketChannel) !void {
        const f = try self.transport.allocator.create(Future(void, anyerror));

        defer self.transport.allocator.destroy(f);
        f.* = .{};
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            const close_ud = self.transport.io_event_loop.close_pool.create() catch unreachable;

            close_ud.* = .{
                .channel = self,
                .future = f,
            };
            self.socket.shutdown(&self.transport.io_event_loop.loop, c, io_loop.Close, close_ud, shutdownCB);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .close = .{ .channel = self, .future = f, .timeout_ms = 30000 } },
            };

            self.transport.io_event_loop.queueMessage(message) catch |err| {
                std.debug.print("Error queuing message: {}\n", .{err});
                return error.AsyncNotifyFailed;
            };
        }
    }

    pub fn asyncClose(self: *XevSocketChannel, erased_userdata: ?*anyopaque, wrapped_cb: ?*const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            const close_ud = self.transport.io_event_loop.close_pool.create() catch unreachable;

            close_ud.* = .{
                .channel = self,
                .user_data = erased_userdata,
                .callback = wrapped_cb,
            };
            self.socket.shutdown(&self.transport.io_event_loop.loop, c, io_loop.Close, close_ud, shutdownCB);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .close = .{ .channel = self, .user_data = erased_userdata, .callback = wrapped_cb, .timeout_ms = 30000 } },
            };

            self.transport.io_event_loop.queueMessage(message) catch |err| {
                std.debug.print("Error queuing message: {}\n", .{err});
                if (wrapped_cb) |cb| {
                    cb(erased_userdata, err);
                }
            };
        }
    }

    pub fn connDirection(self: *XevSocketChannel) p2p_conn.Direction {
        return self.direction;
    }

    pub fn handlerPipeline(self: *XevSocketChannel) *p2p_conn.HandlerPipeline {
        return self.handler_pipeline;
    }

    // --- Static Wrapper Functions ---
    fn vtableWriteFn(instance: *anyopaque, buffer: []const u8) anyerror!usize {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.write(buffer);
    }
    fn vtableCloseFn(instance: *anyopaque) anyerror!void {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.close();
    }
    fn vtableGetPipelineFn(instance: *anyopaque) *p2p_conn.HandlerPipeline {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.handlerPipeline();
    }
    fn vtableDirectionFn(instance: *anyopaque) p2p_conn.Direction {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.connDirection();
    }
    fn vtableAsyncWriteFn(
        instance: *anyopaque,
        buffer: []const u8,
        erased_userdata: ?*anyopaque,
        wrapped_cb: ?*const fn (ud: ?*anyopaque, r: anyerror!usize) void,
    ) void {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.asyncWrite(buffer, erased_userdata, wrapped_cb);
    }
    fn vtableAsyncCloseFn(
        instance: *anyopaque,
        erased_userdata: ?*anyopaque,
        wrapped_cb: ?*const fn (ud: ?*anyopaque, r: anyerror!void) void,
    ) void {
        const self: *XevSocketChannel = @ptrCast(@alignCast(instance));
        return self.asyncClose(erased_userdata, wrapped_cb);
    }

    // --- Static VTable Instance ---
    const vtable_instance = p2p_conn.RxConnVTable{
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
        .getPipelineFn = vtableGetPipelineFn,
        .directionFn = vtableDirectionFn,
        .asyncWriteFn = vtableAsyncWriteFn,
        .asyncCloseFn = vtableAsyncCloseFn,
    };

    pub fn any(self: *XevSocketChannel) p2p_conn.AnyRxConn {
        return .{ .instance = self, .vtable = &vtable_instance };
    }

    pub fn writeCB(
        ud: ?*io_loop.Write,
        _: *xev.Loop,
        c: *xev.Completion,
        _: xev.TCP,
        _: xev.WriteBuffer,
        r: xev.WriteError!usize,
    ) xev.CallbackAction {
        std.debug.print("WriteCB called\n", .{});
        const w = ud.?;
        const transport = w.transport.?;
        defer transport.io_event_loop.completion_pool.destroy(c);
        defer transport.io_event_loop.write_pool.destroy(w);

        if (w.callback) |cb| {
            cb(w.user_data, r);
        } else if (w.future) |f| {
            const written = r catch |err| {
                std.debug.print("Error writing111111: {}\n", .{err});
                f.setError(err);
                return .disarm;
            };
            std.debug.print("WriteCB called1111\n {}", .{written});

            std.debug.print("WriteCB called1111ff\n {}", .{w.future.?.*});

            f.setValue(written);
            std.debug.print("WriteCB called2222 {*}\n", .{w.future.?});
        }

        return .disarm;
    }

    pub fn readCB(
        ud: ?*XevSocketChannel,
        loop: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        rb: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        const channel = ud.?;
        const transport = channel.transport;

        const read_bytes = r catch |err| switch (err) {
            error.EOF => {
                channel.handlerPipeline().fireReadComplete();
                transport.io_event_loop.read_buffer_pool.destroy(
                    @alignCast(
                        @as(*[io_loop.BUFFER_SIZE]u8, @ptrFromInt(@intFromPtr(rb.slice.ptr))),
                    ),
                );
                const close_ud = transport.io_event_loop.close_pool.create() catch unreachable;
                close_ud.* = .{
                    .channel = channel,
                };
                socket.shutdown(loop, c, io_loop.Close, close_ud, shutdownCB);

                return .disarm;
            },

            else => {
                channel.handlerPipeline().fireErrorCaught(err);
                transport.io_event_loop.read_buffer_pool.destroy(
                    @alignCast(
                        @as(*[io_loop.BUFFER_SIZE]u8, @ptrFromInt(@intFromPtr(rb.slice.ptr))),
                    ),
                );
                const close_ud = transport.io_event_loop.close_pool.create() catch unreachable;
                close_ud.* = .{
                    .channel = channel,
                };
                socket.shutdown(loop, c, io_loop.Close, close_ud, shutdownCB);

                return .disarm;
            },
        };

        if (read_bytes > 0) {
            channel.handlerPipeline().fireRead(rb.slice[0..read_bytes]);
        }

        return .rearm;
    }

    /// Callback executed when a socket shutdown operation completes.
    /// This initiates a full socket close after the shutdown completes.
    pub fn shutdownCB(
        ud: ?*io_loop.Close,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.ShutdownError!void,
    ) xev.CallbackAction {
        const close_ud = ud.?;

        _ = r catch |err| {
            std.log.err("Error shutting down: {}\n", .{err});
            if (close_ud.callback) |cb| {
                cb(close_ud.user_data, err);
            } else {
                close_ud.future.?.setError(err);
            }

            return .disarm;
        };

        s.close(l, c, io_loop.Close, ud, closeCB);
        return .disarm;
    }

    /// Callback executed when a socket close operation completes.
    /// This is the final step in the socket cleanup process.
    pub fn closeCB(
        ud: ?*io_loop.Close,
        _: *xev.Loop,
        c: *xev.Completion,
        _: xev.TCP,
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        const close_ud = ud.?;
        const channel = close_ud.channel;
        const transport = channel.transport;
        defer {
            const p = channel.handlerPipeline();
            p.deinit();
            transport.io_event_loop.handler_pipeline_pool.destroy(p);

            transport.allocator.destroy(channel);
            transport.io_event_loop.completion_pool.destroy(c);
            transport.io_event_loop.close_pool.destroy(close_ud);
        }

        if (close_ud.callback) |cb| {
            cb(close_ud.user_data, r);

            _ = r catch |err| {
                std.debug.print("Error closing: {}\n", .{err});
                return .disarm;
            };

            channel.handlerPipeline().fireInactive();

            return .disarm;
        } else {
            _ = r catch |err| {
                std.debug.print("Error closing: {}\n", .{err});
                close_ud.future.?.setError(err);
                return .disarm;
            };

            channel.handlerPipeline().fireInactive();

            return .disarm;
        }
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
    pub fn accept(self: *XevListener, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!p2p_conn.AnyRxConn) void) void {
        std.debug.print("Accept called\n", .{});
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            const accept_ud = self.transport.io_event_loop.accept_pool.create() catch unreachable;

            accept_ud.* = .{
                .transport = self.transport,
                .user_data = user_data,
                .callback = callback,
            };
            self.server.accept(&self.transport.io_event_loop.loop, c, io_loop.Accept, accept_ud, acceptCB);
        } else {
            std.debug.print("Accept called2222\n", .{});
            const message = io_loop.IOMessage{
                .action = .{ .accept = .{ .server = self.server, .transport = self.transport, .user_data = user_data, .callback = callback, .timeout_ms = 30000 } },
            };

            self.transport.io_event_loop.queueMessage(message) catch |err| {
                std.debug.print("Error queuing message: {}\n", .{err});
                callback(user_data, err);
            };
        }
    }

    // --- Static Wrapper Function for ListenerVTable ---
    fn vtableAcceptFn(instance: *anyopaque, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!p2p_conn.AnyRxConn) void) void {
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
        ud: ?*io_loop.Accept,
        l: *xev.Loop,
        c: *xev.Completion,
        r: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const accept_ud = ud.?;
        std.debug.print("AcceptCB called 1111222{}\n", .{accept_ud.*});
        const transport = accept_ud.transport;
        defer transport.io_event_loop.completion_pool.destroy(c);
        defer transport.io_event_loop.accept_pool.destroy(accept_ud);

        const socket = r catch |err| {
            std.debug.print("Error accepting: {}\n", .{err});
            accept_ud.callback(accept_ud.user_data, err);

            return .disarm;
        };

        const channel = transport.allocator.create(XevSocketChannel) catch unreachable;
        channel.init(socket, transport, p2p_conn.Direction.INBOUND);
        var any_rx_conn = channel.any();

        const p = transport.io_event_loop.handler_pipeline_pool.create() catch unreachable;
        p.init(transport.allocator, any_rx_conn) catch |err| {
            std.debug.print("Error creating handler pipeline: {}\n", .{err});
            transport.allocator.destroy(channel);
            transport.io_event_loop.handler_pipeline_pool.destroy(p);
            accept_ud.callback(accept_ud.user_data, err);
            return .disarm;
        };
        channel.handler_pipeline = p;

        transport.io_event_loop.conn_initializer.initConn(&any_rx_conn) catch |err| {
            std.debug.print("Error initializing connection: {}\n", .{err});
            transport.allocator.destroy(channel);
            transport.io_event_loop.handler_pipeline_pool.destroy(p);
            accept_ud.callback(accept_ud.user_data, err);
            return .disarm;
        };

        p.fireActive();

        const read_buf = transport.io_event_loop.read_buffer_pool.create() catch unreachable;
        const read_c = transport.io_event_loop.completion_pool.create() catch unreachable;
        socket.read(l, read_c, .{ .slice = read_buf }, XevSocketChannel, channel, XevSocketChannel.readCB);
        accept_ud.callback(accept_ud.user_data, any_rx_conn);
        return .disarm;
    }
};

/// Transport is a transport that uses the xev library for asynchronous I/O operations.
pub const XevTransport = struct {
    pub const DialError = Allocator.Error || xev.ConnectError || error{AsyncNotifyFailed};

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
    pub fn init(self: *XevTransport, loop: *io_loop.ThreadEventLoop, allocator: Allocator, opts: Options) !void {
        self.* = .{
            .io_event_loop = loop,
            .options = opts,
            .allocator = allocator,
        };
    }

    /// Deinitialize the transport.
    pub fn deinit(_: *XevTransport) void {}

    /// Dial connects to the given address and creates a channel for communication. It blocks until the connection is established. If an error occurs, it returns the error.
    pub fn dial(self: *XevTransport, addr: std.net.Address, channel: *p2p_conn.AnyRxConn) !void {
        const f = try self.allocator.create(Future(void, anyerror));
        f.* = .{};
        defer self.allocator.destroy(f);
        const message = io_loop.IOMessage{
            .action = .{ .connect = .{
                .address = addr,
                .channel = channel,
                .transport = self,
                .future = f,
                .timeout_ms = 30000,
            } },
        };

        self.io_event_loop.queueMessage(message) catch |err| {
            std.debug.print("Error queuing message: {}\n", .{err});
            return error.AsyncNotifyFailed;
        };

        f.wait();
        if (f.getErr()) |err| {
            return err;
        }
    }

    /// Listen listens for incoming connections on the given address. It blocks until the listener is initialized. If an error occurs, it returns the error.
    pub fn listen(self: *XevTransport, addr: std.net.Address, listener: *p2p_transport.AnyListener) XevListener.ListenError!void {
        const l = try self.allocator.create(XevListener);
        try l.init(addr, self.options.backlog, self);
        listener.* = l.any();
    }

    // --- Static Wrapper Functions for TransportVTable ---
    fn vtableDialFn(instance: *anyopaque, addr: std.net.Address, connection: *p2p_conn.AnyRxConn) anyerror!void {
        const self: *XevTransport = @ptrCast(@alignCast(instance));
        return self.dial(addr, connection);
    }

    fn vtableListenFn(instance: *anyopaque, addr: std.net.Address, listener: *p2p_transport.AnyListener) anyerror!void {
        const self: *XevTransport = @ptrCast(@alignCast(instance));
        return self.listen(addr, listener);
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
        ud: ?*io_loop.Connect,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.ConnectError!void,
    ) xev.CallbackAction {
        const connect = ud.?;
        const transport = connect.transport.?;
        defer transport.io_event_loop.completion_pool.destroy(c);
        defer transport.io_event_loop.connect_pool.destroy(connect);

        _ = r catch |err| {
            std.debug.print("Error connecting: {}\n", .{err});
            connect.future.setError(err);

            return .disarm;
        };

        const channel = transport.allocator.create(XevSocketChannel) catch unreachable;
        channel.init(socket, transport, p2p_conn.Direction.OUTBOUND);

        const p = transport.io_event_loop.handler_pipeline_pool.create() catch unreachable;
        const associated_conn = channel.any();
        connect.channel.* = associated_conn;
        p.init(transport.allocator, associated_conn) catch |err| {
            std.debug.print("Error creating handler pipeline: {}\n", .{err});
            transport.allocator.destroy(channel);
            transport.io_event_loop.handler_pipeline_pool.destroy(p);
            connect.future.setError(err);
            return .disarm;
        };
        channel.handler_pipeline = p;

        transport.io_event_loop.conn_initializer.initConn(connect.channel) catch |err| {
            std.debug.print("Error initializing connection: {}\n", .{err});
            transport.allocator.destroy(channel);
            connect.future.setError(err);
            return .disarm;
        };

        p.fireActive();

        const read_buf = transport.io_event_loop.read_buffer_pool.create() catch unreachable;
        const read_c = transport.io_event_loop.completion_pool.create() catch unreachable;
        socket.read(l, read_c, .{ .slice = read_buf }, XevSocketChannel, channel, XevSocketChannel.readCB);

        connect.future.rwlock.lock();
        if (!connect.future.isDone()) {
            connect.future.setDone();
        }
        connect.future.rwlock.unlock();
        return .disarm;
    }

    pub fn connectTimeoutCB(
        ud: ?*io_loop.ConnectTimeout,
        l: *xev.Loop,
        c: *xev.Completion,
        r: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        const connect_timeout = ud.?;
        const transport = connect_timeout.transport.?;
        defer transport.io_event_loop.completion_pool.destroy(c);
        defer transport.io_event_loop.connect_timeout_pool.destroy(connect_timeout);

        _ = r catch |err| {
            std.debug.print("Error connecting: {}\n", .{err});
            connect_timeout.future.rwlock.lock();
            if (!connect_timeout.future.isDone()) {
                connect_timeout.future.setError(err);
                const shutdown_c = transport.io_event_loop.completion_pool.create() catch unreachable;
                connect_timeout.socket.shutdown(l, shutdown_c, XevTransport, transport, shutdownCB);
            }
            connect_timeout.future.rwlock.unlock();

            return .disarm;
        };

        connect_timeout.future.rwlock.lock();
        if (!connect_timeout.future.isDone()) {
            connect_timeout.future.setError(error.Timeout);
            const shutdown_c = transport.io_event_loop.completion_pool.create() catch unreachable;
            connect_timeout.socket.shutdown(l, shutdown_c, XevTransport, transport, shutdownCB);
        }
        connect_timeout.future.rwlock.unlock();

        return .disarm;
    }

    /// Callback executed when a socket shutdown operation completes.
    /// This initiates a full socket close after the shutdown completes.
    fn shutdownCB(
        ud: ?*XevTransport,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.ShutdownError!void,
    ) xev.CallbackAction {
        _ = r catch |err| {
            std.debug.print("Error shutting down: {}\n", .{err});

            return .disarm;
        };

        const self = ud.?;
        s.close(l, c, XevTransport, self, closeCB);
        return .disarm;
    }

    /// Callback executed when a socket close operation completes.
    /// This is the final step in the socket cleanup process.
    fn closeCB(
        ud: ?*XevTransport,
        _: *xev.Loop,
        c: *xev.Completion,
        _: xev.TCP,
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        const transport = ud.?;
        defer transport.io_event_loop.completion_pool.destroy(c);

        _ = r catch |err| {
            std.debug.print("Error close: {}\n", .{err});

            return .disarm;
        };

        return .disarm;
    }
};

const MockConnInitializer = struct {
    pub const Error = anyerror;

    const Self = @This();

    // No-op implementation
    pub fn initConnImpl(self: *Self, conn: *p2p_conn.AnyRxConn) Error!void {
        _ = self; // Avoid unused self
        _ = conn; // Avoid unused conn
        // std.debug.print("MockConnInitializer.initConn called (no-op).\n", .{});
        return; // Explicitly return void
    }

    // Static wrapper function for the VTable
    fn vtableInitConnFn(instance: *anyopaque, conn: *p2p_conn.AnyRxConn) Error!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.initConnImpl(conn);
    }

    // Static VTable instance
    const vtable_instance = p2p_conn.ConnInitializerVTable{
        .initConnFn = vtableInitConnFn,
    };

    // any() method to return the interface
    pub fn any(self: *Self) p2p_conn.AnyConnInitializer {
        return .{
            .instance = self,
            .vtable = &vtable_instance,
        };
    }
};

const MockConnInitializer1 = struct {
    pub const Error = anyerror;
    handler_to_add: ?p2p_conn.AnyHandler = null, // Field to store the handler
    allocator: ?Allocator = null, // Allocator for pipeline if needed

    const Self = @This();

    // Constructor to set the handler
    pub fn init(handler: ?p2p_conn.AnyHandler, alloc: ?Allocator) Self {
        return .{
            .handler_to_add = handler,
            .allocator = alloc,
        };
    }

    // Implementation
    pub fn initConnImpl(self: *Self, conn: *p2p_conn.AnyRxConn) Error!void {
        std.debug.print("MockConnInitializer.initConn called.\n", .{});

        if (self.handler_to_add) |handler| {
            // If a handler is provided, add it to the pipeline
            std.debug.print("MockConnInitializer.initConn called 111.\n", .{});

            const pipeline = conn.getPipeline();
            std.debug.print("MockConnInitializer.initConn called 222.\n", .{});

            try pipeline.addLast("handler", handler);
        }

        std.debug.print("MockConnInitializer.initConn called end.\n", .{});

        return; // Explicitly return void
    }

    // Static wrapper function for the VTable
    fn vtableInitConnFn(instance: *anyopaque, conn: *p2p_conn.AnyRxConn) Error!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.initConnImpl(conn);
    }

    // Static VTable instance
    const vtable_instance = p2p_conn.ConnInitializerVTable{
        .initConnFn = vtableInitConnFn,
    };

    // any() method to return the interface
    pub fn any(self: *Self) p2p_conn.AnyConnInitializer {
        return .{
            .instance = self,
            .vtable = &vtable_instance,
        };
    }
};

test "dial connection refused" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    const opts = XevTransport.Options{
        .backlog = 128,
    };

    var mock_initializer = MockConnInitializer{};
    var any_initializer = mock_initializer.any();

    var event_loop: io_loop.ThreadEventLoop = undefined;
    try event_loop.init(allocator, &any_initializer);
    defer {
        event_loop.close();
        event_loop.deinit();
    }

    var transport: XevTransport = undefined;
    try transport.init(&event_loop, allocator, opts);
    defer transport.deinit();

    var channel: p2p_conn.AnyRxConn = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel));

    var channel1: p2p_conn.AnyRxConn = undefined;
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel1));

    const thread2 = try std.Thread.spawn(.{}, struct {
        fn run(t: *XevTransport, a: std.net.Address) !void {
            var ch: p2p_conn.AnyRxConn = undefined;
            try std.testing.expectError(error.ConnectionRefused, t.dial(a, &ch));
        }
    }.run, .{ &transport, addr });

    thread2.join();
}

test "dial and accept" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var mock_initializer = MockConnInitializer{};
    var any_initializer = mock_initializer.any();

    var sl: io_loop.ThreadEventLoop = undefined;
    try sl.init(allocator, &any_initializer);
    defer {
        sl.close();
        sl.deinit();
    }

    const opts = XevTransport.Options{
        .backlog = 128,
    };

    var transport: XevTransport = undefined;
    try transport.init(&sl, allocator, opts);
    defer transport.deinit();

    var listener: p2p_transport.AnyListener = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8082);
    try transport.listen(addr, &listener);

    const ConnHolder = struct {
        channel: p2p_conn.AnyRxConn,
        ready: std.Thread.ResetEvent = .{},

        pub fn init(opaque_userdata: ?*anyopaque, accept_result: anyerror!p2p_conn.AnyRxConn) void {
            std.debug.print("ConnHolder.init: Callback called.\n", .{});
            const self: *@This() = @ptrCast(@alignCast(opaque_userdata.?));

            const accepted_channel = accept_result catch |err| {
                std.debug.print("ConnHolder.init: Error from accept operation: {any}\n", .{err});

                return;
            };

            self.channel = accepted_channel;
            self.ready.set();
            std.debug.print("ConnHolder.init: Channel accepted and set. Direction: {any}\n", .{self.channel.direction()});
        }
    };

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
    try cl.init(allocator, &any_initializer);
    defer {
        cl.close();
        cl.deinit();
    }
    var client: XevTransport = undefined;
    try client.init(&cl, allocator, opts);
    defer client.deinit();

    var channel1: p2p_conn.AnyRxConn = undefined;
    try client.dial(addr, &channel1);
    try std.testing.expectEqual(channel1.direction(), p2p_conn.Direction.OUTBOUND);

    var channel2: p2p_conn.AnyRxConn = undefined;
    try client.dial(addr, &channel2);
    try std.testing.expectEqual(channel2.direction(), p2p_conn.Direction.OUTBOUND);

    accept_thread.join();
}

// const ServerEchoHandler = struct {
//     allocator: Allocator,

//     received_message: []u8,

//     const Self = @This();

//     pub fn create(alloc: Allocator) !*Self {
//         const self = try alloc.create(Self);
//         const received_message = try alloc.alloc(u8, 1024);
//         @memset(received_message, 0);
//         self.* = .{ .allocator = alloc, .received_message = received_message };
//         return self;
//     }

//     pub fn destroy(self: *Self) void {
//         self.allocator.free(self.received_message);
//         self.allocator.destroy(self);
//     }

//     // --- Actual Handler Implementations ---
//     pub fn onActiveImpl(self: *Self, ctx: *p2p_conn.HandlerContext) void {
//         _ = self;
//         std.debug.print("ServerEchoHandler ({s}): Connection active.\n", .{ctx.name});
//         ctx.fireActive();
//     }

//     pub fn onInactiveImpl(self: *Self, ctx: *p2p_conn.HandlerContext) void {
//         _ = self;
//         std.debug.print("ServerEchoHandler ({s}): Connection inactive.\n", .{ctx.name});
//         ctx.fireInactive();
//     }

//     pub fn onReadImpl(_: *Self, ctx: *p2p_conn.HandlerContext, msg: []const u8) void {
//         std.debug.print("ServerEchoHandler ({s}): Received message: {s}\n", .{ ctx.name, msg });

//         // Echo the message back
//         std.debug.print("ServerEchoHandler ({s}): Echoing message back.\n", .{ctx.name});
//         ctx.write(msg, null, null); // Propagate write operation

//         std.debug.print("ServerEchoHandler ({s}): Propagating read operation.\n", .{ctx.name});
//         ctx.fireRead(msg); // Propagate read operation
//     }

//     pub fn onReadCompleteImpl(self: *Self, ctx: *p2p_conn.HandlerContext) void {
//         _ = self;
//         std.debug.print("ServerEchoHandler ({s}): Read complete.\n", .{ctx.name});
//         ctx.fireReadComplete(); // Propagate read complete operation
//     }

//     pub fn onErrorCaughtImpl(self: *Self, ctx: *p2p_conn.HandlerContext, err: anyerror) void {
//         _ = self;
//         std.log.err("ServerEchoHandler ({s}): Error caught: {any}\n", .{ ctx.name, err });
//         // Propagate the error
//         ctx.fireErrorCaught(err);
//     }

//     pub fn writeImpl(self: *Self, ctx: *p2p_conn.HandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: ?*const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
//         _ = self;
//         std.debug.print("ServerEchoHandler ({s}): Write called (passing through).\n", .{ctx.name});
//         // Pass the write operation to the next handler in the pipeline (towards the head)
//         ctx.write(buffer, user_data, callback);
//     }

//     pub fn closeImpl(self: *Self, ctx: *p2p_conn.HandlerContext, user_data: ?*anyopaque, callback: ?*const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
//         _ = self;
//         std.debug.print("ServerEchoHandler ({s}): Close called (passing through).\n", .{ctx.name});
//         // Pass the close operation to the next handler in the pipeline (towards the head)
//         ctx.close(user_data, callback);
//     }

//     // --- Static Wrapper Functions for HandlerVTable ---
//     fn vtableOnActiveFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.onActiveImpl(ctx);
//     }
//     fn vtableOnInactiveFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.onInactiveImpl(ctx);
//     }
//     fn vtableOnReadFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, msg: []const u8) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.onReadImpl(ctx, msg);
//     }
//     fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.onReadCompleteImpl(ctx);
//     }
//     fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, err: anyerror) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.onErrorCaughtImpl(ctx, err);
//     }
//     fn vtableWriteFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: ?*const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.writeImpl(ctx, buffer, user_data, callback);
//     }
//     fn vtableCloseFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, user_data: ?*anyopaque, callback: ?*const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.closeImpl(ctx, user_data, callback);
//     }

//     // --- Static VTable Instance ---
//     const vtable_instance = p2p_conn.HandlerVTable{
//         .onActiveFn = vtableOnActiveFn,
//         .onInactiveFn = vtableOnInactiveFn,
//         .onReadFn = vtableOnReadFn,
//         .onReadCompleteFn = vtableOnReadCompleteFn,
//         .onErrorCaughtFn = vtableOnErrorCaughtFn,
//         .writeFn = vtableWriteFn,
//         .closeFn = vtableCloseFn,
//     };

//     // --- any() method ---
//     pub fn any(self: *Self) p2p_conn.AnyHandler {
//         return .{ .instance = self, .vtable = &vtable_instance };
//     }
// };

// const ClientEchoHandler = struct {
//     allocator: Allocator,

//     received_message: []u8,

//     read: std.Thread.ResetEvent = .{},

//     const Self = @This();

//     pub fn create(alloc: Allocator) !*Self {
//         const self = try alloc.create(Self);
//         const received_message = try alloc.alloc(u8, 1024);
//         @memset(received_message, 0);
//         self.* = .{ .allocator = alloc, .received_message = received_message };
//         return self;
//     }

//     pub fn destroy(self: *Self) void {
//         self.allocator.free(self.received_message);
//         self.allocator.destroy(self);
//     }

//     // --- Actual Handler Implementations ---
//     pub fn onActiveImpl(self: *Self, ctx: *p2p_conn.HandlerContext) void {
//         _ = self;
//         std.debug.print("ClientEchoHandler ({s}): Connection active.\n", .{ctx.name});
//         ctx.fireActive();
//     }

//     pub fn onInactiveImpl(self: *Self, ctx: *p2p_conn.HandlerContext) void {
//         _ = self;
//         std.debug.print("ClientEchoHandler ({s}): Connection inactive.\n", .{ctx.name});
//         ctx.fireInactive();
//     }

//     pub fn onReadImpl(self: *Self, ctx: *p2p_conn.HandlerContext, msg: []const u8) void {
//         const len = msg.len;
//         @memcpy(self.received_message[0..len], msg[0..len]);
//         self.read.set();
//         std.debug.print("ClientEchoHandler ({s}): Received message: {s}\n", .{ ctx.name, msg });
//         ctx.fireRead(msg);
//     }

//     pub fn onReadCompleteImpl(self: *Self, ctx: *p2p_conn.HandlerContext) void {
//         _ = self;
//         std.debug.print("ClientEchoHandler ({s}): Read complete.\n", .{ctx.name});
//         ctx.fireReadComplete();
//     }

//     pub fn onErrorCaughtImpl(self: *Self, ctx: *p2p_conn.HandlerContext, err: anyerror) void {
//         _ = self;
//         std.log.err("ClientEchoHandler ({s}): Error caught: {any}\n", .{ ctx.name, err });
//         // Propagate the error
//         ctx.fireErrorCaught(err);
//     }

//     pub fn writeImpl(self: *Self, ctx: *p2p_conn.HandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: ?*const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
//         _ = self;
//         std.debug.print("ClientEchoHandler ({s}): Write called (passing through).\n", .{ctx.name});
//         // Pass the write operation to the next handler in the pipeline (towards the head)
//         ctx.write(buffer, user_data, callback);
//     }
//     pub fn closeImpl(self: *Self, ctx: *p2p_conn.HandlerContext, user_data: ?*anyopaque, callback: ?*const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
//         _ = self;
//         std.debug.print("ClientEchoHandler ({s}): Close called (passing through).\n", .{ctx.name});
//         // Pass the close operation to the next handler in the pipeline (towards the head)
//         ctx.close(user_data, callback);
//     }
//     // --- Static Wrapper Functions for HandlerVTable ---
//     fn vtableOnActiveFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.onActiveImpl(ctx);
//     }
//     fn vtableOnInactiveFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.onInactiveImpl(ctx);
//     }
//     fn vtableOnReadFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, msg: []const u8) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.onReadImpl(ctx, msg);
//     }
//     fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.onReadCompleteImpl(ctx);
//     }
//     fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, err: anyerror) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.onErrorCaughtImpl(ctx, err);
//     }
//     fn vtableWriteFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: ?*const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.writeImpl(ctx, buffer, user_data, callback);
//     }
//     fn vtableCloseFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, user_data: ?*anyopaque, callback: ?*const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
//         const self: *Self = @ptrCast(@alignCast(instance));
//         return self.closeImpl(ctx, user_data, callback);
//     }
//     // --- Static VTable Instance ---
//     const vtable_instance = p2p_conn.HandlerVTable{
//         .onActiveFn = vtableOnActiveFn,
//         .onInactiveFn = vtableOnInactiveFn,
//         .onReadFn = vtableOnReadFn,
//         .onReadCompleteFn = vtableOnReadCompleteFn,
//         .onErrorCaughtFn = vtableOnErrorCaughtFn,
//         .writeFn = vtableWriteFn,
//         .closeFn = vtableCloseFn,
//     };
//     // --- any() method ---
//     pub fn any(self: *Self) p2p_conn.AnyHandler {
//         return .{ .instance = self, .vtable = &vtable_instance };
//     }
// };

// test "echo read and write" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();

//     const server_handler = try ServerEchoHandler.create(allocator);
//     defer server_handler.destroy();

//     const client_handler = try ClientEchoHandler.create(allocator);
//     defer client_handler.destroy();
//     const server_handler_any = server_handler.any();
//     const client_handler_any = client_handler.any();

//     var mock_client_initializer = MockConnInitializer1.init(client_handler_any, allocator);
//     var any_client_initializer = mock_client_initializer.any();

//     var mock_server_initializer = MockConnInitializer1.init(server_handler_any, allocator);
//     var any_server_initializer = mock_server_initializer.any();

//     // var mock_initializer = MockConnInitializer{};
//     // var any_initializer = mock_initializer.any();

//     var sl: io_loop.ThreadEventLoop = undefined;
//     try sl.init(allocator, &any_server_initializer);
//     defer {
//         sl.close();
//         sl.deinit();
//     }

//     const opts = XevTransport.Options{
//         .backlog = 128,
//     };

//     var transport: XevTransport = undefined;
//     try transport.init(&sl, allocator, opts);
//     defer transport.deinit();

//     var listener: p2p_transport.AnyListener = undefined;
//     const addr = try std.net.Address.parseIp("0.0.0.0", 8083);
//     try transport.listen(addr, &listener);

//     const accept_thread = try std.Thread.spawn(.{}, struct {
//         fn run(l: *p2p_transport.AnyListener) !void {
//             var accepted_count: usize = 0;
//             while (accepted_count < 1) : (accepted_count += 1) {
//                 var accepted_channel: p2p_conn.AnyRxConn = undefined;
//                 try l.accept(&accepted_channel);
//                 try std.testing.expectEqual(accepted_channel.direction(), p2p_conn.Direction.INBOUND);
//             }
//         }
//     }.run, .{&listener});

//     var cl: io_loop.ThreadEventLoop = undefined;
//     try cl.init(allocator, &any_client_initializer);
//     defer {
//         cl.close();
//         cl.deinit();
//     }
//     var client: XevTransport = undefined;
//     try client.init(&cl, allocator, opts);
//     defer client.deinit();

//     var channel1: p2p_conn.AnyRxConn = undefined;
//     try client.dial(addr, &channel1);
//     try std.testing.expectEqual(channel1.direction(), p2p_conn.Direction.OUTBOUND);

//     const n = try channel1.write("buf: []const u8");
//     try std.testing.expect(n == 15);

//     client_handler.read.wait();
//     try std.testing.expectEqualStrings("buf: []const u8", client_handler.received_message[0..n]);
//     accept_thread.join();
// }

// test "echo read and write" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();

//     const opts = XevTransport.Options{
//         .backlog = 128,
//     };

//     // const server_handler = try ServerEchoHandler.create(allocator);
//     // defer server_handler.destroy();

//     // const client_handler = try ClientEchoHandler.create(allocator);
//     // defer client_handler.destroy();
//     // const server_handler_any = server_handler.any();
//     // const client_handler_any = client_handler.any();

//     // var mock_client_initializer = MockConnInitializer1.init(client_handler_any, allocator);
//     // var any_client_initializer = mock_client_initializer.any();

//     // var mock_server_initializer = MockConnInitializer1.init(server_handler_any, allocator);
//     // var any_server_initializer = mock_server_initializer.any();

//         var mock_initializer = MockConnInitializer{};
//     var any_initializer = mock_initializer.any();

//     var sl: io_loop.ThreadEventLoop = undefined;
//     try sl.init(allocator, &any_initializer);
//     defer {
//         sl.close();
//         sl.deinit();
//     }

//     var transport: XevTransport = undefined;
//     try transport.init(&sl, allocator, opts);
//     defer transport.deinit();

//     var listener: p2p_transport.AnyListener = undefined;
//     const addr = try std.net.Address.parseIp("0.0.0.0", 8084);
//     try transport.listen(addr, &listener);

//     const accept_thread = try std.Thread.spawn(.{}, struct {
//         fn run(l: *p2p_transport.AnyListener) !void {
//             var accepted_count: usize = 0;
//             while (accepted_count < 2) : (accepted_count += 1) {
//                 var accepted_channel: p2p_conn.AnyRxConn = undefined;
//                 try l.accept(&accepted_channel);
//                 try std.testing.expectEqual(accepted_channel.direction(), p2p_conn.Direction.INBOUND);
//             }
//         }
//     }.run, .{&listener});

//     var cl: io_loop.ThreadEventLoop = undefined;
//     try cl.init(allocator, &any_initializer);
//     defer {
//         cl.close();
//         cl.deinit();
//     }
//     var client: XevTransport = undefined;
//     try client.init(&cl, allocator, opts);
//     defer client.deinit();

//         var channel1: p2p_conn.AnyRxConn = undefined;
//     try client.dial(addr, &channel1);
//     try std.testing.expectEqual(channel1.direction(), p2p_conn.Direction.OUTBOUND);

//     // _ = try channel1.write("buf: []const u8");
//     // const buf = try allocator.alloc(u8, 1024);
//     // defer allocator.free(buf);
//     // const n = try channel1.read(buf);
//     // try std.testing.expectStringStartsWith(buf, "buf: []const u8");
//     // try std.testing.expect(n == 15);

//     accept_thread.join();

// }

// test "generic transport dial and accept" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();

//     // Server setup
//     var server_loop: io_loop.ThreadEventLoop = undefined;
//     try server_loop.init(allocator);
//     defer {
//         server_loop.close();
//         server_loop.deinit();
//     }

//     const opts = XevTransport.Options{
//         .backlog = 128,
//     };

//     var xev_server: XevTransport = undefined;
//     try xev_server.init(&server_loop, allocator, opts);
//     defer xev_server.deinit();

//     // Get the generic transport interface
//     const server = xev_server.toTransport();

//     // Listener setup
//     var xev_listener: XevListener = undefined;
//     const addr = try std.net.Address.parseIp("0.0.0.0", 8083);
//     try server.listen(addr, &xev_listener);
//     const listener = xev_listener.toListener();

//     // Accept thread using the generic interfaces
//     const accept_thread = try std.Thread.spawn(.{}, struct {
//         fn run(l: Listener) !void {
//             var accepted_channel: XevSocketChannel = undefined;
//             try l.accept(&accepted_channel);
//             std.debug.print("Server accepted connection\n", .{});
//             try std.testing.expectEqual(accepted_channel.direction, .INBOUND);
//         }
//     }.run, .{listener});

//     // Client setup
//     var client_loop: io_loop.ThreadEventLoop = undefined;
//     try client_loop.init(allocator);
//     defer {
//         client_loop.close();
//         client_loop.deinit();
//     }

//     var xev_client: XevTransport = undefined;
//     try xev_client.init(&client_loop, allocator, opts);
//     defer xev_client.deinit();

//     // Get the generic transport interface
//     const client = xev_client.toTransport();

//     // Dialing using the generic interface
//     var channel: XevSocketChannel = undefined;
//     try client.dial(addr, &channel);
//     std.debug.print("Client connected\n", .{});
//     try std.testing.expectEqual(channel.direction, .OUTBOUND);

//     accept_thread.join();
// }

// test "generic transport echo read and write" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();

//     const opts = XevTransport.Options{
//         .backlog = 128,
//     };

//     var sl: io_loop.ThreadEventLoop = undefined;
//     try sl.init(allocator);
//     defer {
//         sl.close();
//         sl.deinit();
//     }

//     var server: XevTransport = undefined;
//     try server.init(&sl, allocator, opts);
//     defer server.deinit();

//     // Get the generic transport interface
//     const server_transport = server.toTransport();

//     var listener: XevListener = undefined;
//     const addr = try std.net.Address.parseIp("0.0.0.0", 8084);
//     try server_transport.listen(addr, &listener);

//     const server_listener = listener.toListener();

//     const accept_thread = try std.Thread.spawn(.{}, struct {
//         fn run(l: Listener, alloc: Allocator) !void {
//             var accepted_channel: XevSocketChannel = undefined;
//             try l.accept(&accepted_channel);
//             try std.testing.expectEqual(accepted_channel.direction, .INBOUND);

//             const accepted_conn = accepted_channel.toConn();

//             const buf = try alloc.alloc(u8, 1024);
//             defer alloc.free(buf);
//             const n = try accepted_conn.read(buf);
//             try std.testing.expectStringStartsWith(buf, "buf: []const u8");

//             try std.testing.expect(n == 15);
//             _ = try accepted_conn.write(buf[0..n]);
//         }
//     }.run, .{ server_listener, allocator });

//     var cl: io_loop.ThreadEventLoop = undefined;
//     try cl.init(allocator);
//     defer {
//         cl.close();
//         cl.deinit();
//     }
//     var client: XevTransport = undefined;
//     try client.init(&cl, allocator, opts);
//     defer client.deinit();

//     // Get the generic transport interface
//     const client_transport = client.toTransport();

//     var channel1: XevSocketChannel = undefined;
//     try client_transport.dial(addr, &channel1);
//     try std.testing.expectEqual(channel1.direction, .OUTBOUND);

//     // Write and read using the generic interface
//     const conn = channel1.toConn();
//     _ = try conn.write("buf: []const u8");
//     const buf = try allocator.alloc(u8, 1024);
//     defer allocator.free(buf);
//     const n = try conn.read(buf);
//     try std.testing.expectStringStartsWith(buf, "buf: []const u8");
//     try std.testing.expect(n == 15);

//     accept_thread.join();
//     client.close();
//     server.close();
// }
