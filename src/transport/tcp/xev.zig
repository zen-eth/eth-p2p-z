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
        const f = self.transport.io_event_loop.write_future_pool.create() catch unreachable;
        defer self.transport.io_event_loop.write_future_pool.destroy(f);
        f.* = .{};
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            const w = self.transport.io_event_loop.write_pool.create() catch unreachable;

            w.* = .{
                .transport = self.transport,
                .future = f,
            };
            self.socket.write(&self.transport.io_event_loop.loop, c, .{ .slice = msg }, io_loop.Write, w, writeCB);
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

    /// Close closes the channel. It blocks until the close is complete.
    pub fn close(self: *XevSocketChannel) !void {
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            self.socket.shutdown(&self.transport.io_event_loop.loop, c, XevSocketChannel, self, shutdownCB);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .close = .{ .channel = self, .timeout_ms = 30000 } },
            };

            self.transport.io_event_loop.queueMessage(message) catch |err| {
                std.debug.print("Error queuing message: {}\n", .{err});
                return error.AsyncNotifyFailed;
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

    // --- Static VTable Instance ---
    const vtable_instance = p2p_conn.RxConnVTable{
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
        .getPipelineFn = vtableGetPipelineFn,
        .directionFn = vtableDirectionFn,
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
        const w = ud.?;
        const transport = w.transport.?;
        defer transport.io_event_loop.completion_pool.destroy(c);
        defer transport.io_event_loop.write_pool.destroy(w);

        const written = r catch |err| {
            std.debug.print("Error writing: {}\n", .{err});
            w.future.setError(err);
            return .disarm;
        };

        w.future.setValue(written);

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
                socket.shutdown(loop, c, XevSocketChannel, channel, shutdownCB);

                return .disarm;
            },

            else => {
                channel.handlerPipeline().fireErrorCaught(err);
                transport.io_event_loop.read_buffer_pool.destroy(
                    @alignCast(
                        @as(*[io_loop.BUFFER_SIZE]u8, @ptrFromInt(@intFromPtr(rb.slice.ptr))),
                    ),
                );
                socket.shutdown(loop, c, XevSocketChannel, channel, shutdownCB);

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
        ud: ?*XevSocketChannel,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.ShutdownError!void,
    ) xev.CallbackAction {
        _ = r catch |err| {
            std.log.err("Error shutting down: {}\n", .{err});
        };

        const self = ud.?;
        s.close(l, c, XevSocketChannel, self, closeCB);
        return .disarm;
    }

    /// Callback executed when a socket close operation completes.
    /// This is the final step in the socket cleanup process.
    pub fn closeCB(
        ud: ?*XevSocketChannel,
        _: *xev.Loop,
        c: *xev.Completion,
        _: xev.TCP,
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        const channel = ud.?;
        const transport = channel.transport;
        defer {
            const p = channel.handlerPipeline();
            p.deinit();
            transport.io_event_loop.handler_pipeline_pool.destroy(p);

            transport.allocator.destroy(channel);
            transport.io_event_loop.completion_pool.destroy(c);
        }

        _ = r catch unreachable;

        channel.handlerPipeline().fireInactive();

        return .disarm;
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

    /// Accept accepts a connection from the listener. It blocks until a connection is accepted. If an error occurs, it returns the error.
    pub fn accept(self: *XevListener, channel: *p2p_conn.AnyRxConn) !void {
        const f = try self.transport.allocator.create(Future(void, anyerror));
        defer self.transport.allocator.destroy(f);
        f.* = .{};
        if (self.transport.io_event_loop.inEventLoopThread()) {
            const c = self.transport.io_event_loop.completion_pool.create() catch unreachable;
            const accept_ud = self.transport.io_event_loop.accept_pool.create() catch unreachable;

            accept_ud.* = .{
                .transport = self.transport,
                .future = f,
                .conn = channel,
            };
            self.server.accept(&self.transport.io_event_loop.loop, c, io_loop.Accept, accept_ud, acceptCB);
        } else {
            const message = io_loop.IOMessage{
                .action = .{ .accept = .{ .channel = channel, .server = self.server, .future = f, .transport = self.transport, .timeout_ms = 30000 } },
            };

            self.transport.io_event_loop.queueMessage(message) catch |err| {
                std.debug.print("Error queuing message: {}\n", .{err});
                return error.AsyncNotifyFailed;
            };
        }

        f.wait();
        if (f.getErr()) |err| {
            return err;
        }
    }

    // --- Static Wrapper Function for ListenerVTable ---
    fn vtableAcceptFn(instance: *anyopaque, connection: *p2p_conn.AnyRxConn) anyerror!void {
        const self: *XevListener = @ptrCast(@alignCast(instance));
        return self.accept(connection);
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
        const transport = accept_ud.transport.?;
        defer transport.io_event_loop.completion_pool.destroy(c);
        defer transport.io_event_loop.accept_pool.destroy(accept_ud);

        const socket = r catch |err| {
            std.debug.print("Error accepting: {}\n", .{err});
            accept_ud.future.setError(err);

            return .disarm;
        };

        const channel = transport.allocator.create(XevSocketChannel) catch unreachable;
        channel.init(socket, transport, p2p_conn.Direction.INBOUND);
        const any_rx_conn = channel.any();
        accept_ud.conn.* = any_rx_conn;

        transport.io_event_loop.conn_initializer.initConn(accept_ud.conn) catch |err| {
            std.debug.print("Error initializing connection: {}\n", .{err});
            transport.allocator.destroy(channel);
            accept_ud.future.setError(err);
            return .disarm;
        };

        const p = transport.io_event_loop.handler_pipeline_pool.create() catch unreachable;
        p.init(transport.allocator, any_rx_conn) catch |err| {
            std.debug.print("Error creating handler pipeline: {}\n", .{err});
            transport.allocator.destroy(channel);
            transport.io_event_loop.handler_pipeline_pool.destroy(p);
            accept_ud.future.setError(err);
            return .disarm;
        };
        channel.handler_pipeline = p;

        p.fireActive();

        const read_buf = transport.io_event_loop.read_buffer_pool.create() catch unreachable;
        const read_c = transport.io_event_loop.completion_pool.create() catch unreachable;
        socket.read(l, read_c, .{ .slice = read_buf }, XevSocketChannel, channel, XevSocketChannel.readCB);
        accept_ud.future.setDone();

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
        _: *xev.Loop,
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
        p.init(transport.allocator, associated_conn) catch |err| {
            std.debug.print("Error creating handler pipeline: {}\n", .{err});
            transport.allocator.destroy(channel);
            transport.io_event_loop.handler_pipeline_pool.destroy(p);
            connect.future.setError(err);
            return .disarm;
        };
        channel.handler_pipeline = p;

        connect.channel.* = associated_conn;
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

    const accept_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *p2p_transport.AnyListener) !void {
            var accepted_count: usize = 0;
            while (accepted_count < 2) : (accepted_count += 1) {
                var accepted_channel: p2p_conn.AnyRxConn = undefined;
                try l.accept(&accepted_channel);
                try std.testing.expectEqual(accepted_channel.direction(), p2p_conn.Direction.INBOUND);
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

// test "echo read and write" {
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

//     var listener: XevListener = undefined;
//     const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
//     try server.listen(addr, &listener);

//     const accept_thread = try std.Thread.spawn(.{}, struct {
//         fn run(l: *XevListener, alloc: Allocator) !void {
//             var accepted_channel: XevSocketChannel = undefined;
//             try l.accept(&accepted_channel);
//             try std.testing.expectEqual(accepted_channel.direction, .INBOUND);

//             const buf = try alloc.alloc(u8, 1024);
//             defer alloc.free(buf);
//             const n = try accepted_channel.read(buf);
//             try std.testing.expectStringStartsWith(buf, "buf: []const u8");

//             try std.testing.expect(n == 15);
//             _ = try accepted_channel.write(buf[0..n]);
//         }
//     }.run, .{ &listener, allocator });

//     var cl: io_loop.ThreadEventLoop = undefined;
//     try cl.init(allocator);
//     defer {
//         cl.close();
//         cl.deinit();
//     }
//     var client: XevTransport = undefined;
//     try client.init(&cl, allocator, opts);
//     defer client.deinit();

//     var channel1: XevSocketChannel = undefined;
//     try client.dial(addr, &channel1);
//     try std.testing.expectEqual(channel1.direction, .OUTBOUND);

//     _ = try channel1.write("buf: []const u8");
//     const buf = try allocator.alloc(u8, 1024);
//     defer allocator.free(buf);
//     const n = try channel1.read(buf);
//     try std.testing.expectStringStartsWith(buf, "buf: []const u8");
//     try std.testing.expect(n == 15);

//     accept_thread.join();
//     client.close();
//     server.close();
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
