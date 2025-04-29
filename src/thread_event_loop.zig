const std = @import("std");
const Allocator = std.mem.Allocator;
const xev = @import("xev");
const Intrusive = @import("concurrent/mpsc_queue.zig").Intrusive;
const Future = @import("concurrent/future.zig").Future;
// const Stream = @import("muxer/yamux/stream.zig").Stream;
const conn = @import("conn.zig");
// const Config = @import("muxer/yamux/Config.zig");
// const session = @import("muxer/yamux/session.zig");
const xev_tcp = @import("transport/tcp/xev.zig");
const pipeline = @import("pipeline.zig");
// const yamux_timer = @import("muxer/yamux/timeout_loop.zig");

pub const IOAction = union(enum) {
    // run_open_stream_timer: struct {
    //     stream: *Stream,
    //     timeout_ms: u64,
    //     io_loop: ?*ThreadEventLoop = null,
    // },
    // run_close_stream_timer: struct {
    //     stream: *Stream,
    //     timeout_ms: u64,
    //     io_loop: ?*ThreadEventLoop = null,
    // },
    // cancel_close_stream_timer: struct {
    //     stream: *Stream,
    //     io_loop: ?*ThreadEventLoop = null,
    // },
    connect: struct {
        address: std.net.Address,
        channel: *conn.AnyRxConn,
        future: *Future(void, anyerror),
        transport: ?*xev_tcp.XevTransport = null,
        timeout_ms: u64,
    },
    // accept: struct {
    //     server: xev.TCP,
    //     channel: *xev_tcp.XevSocketChannel,
    //     future: *Future(void, xev_tcp.XevListener.AcceptError),
    //     io_loop: ?*ThreadEventLoop = null,
    //     transport: ?*xev_tcp.XevTransport = null,
    // },
    // write: struct {
    //     buffer: []const u8,
    //     channel: *xev_tcp.XevSocketChannel,
    //     future: *Future(usize, xev_tcp.XevSocketChannel.WriteError),
    //     io_loop: ?*ThreadEventLoop = null,
    // },
    // read: struct {
    //     channel: *xev_tcp.XevSocketChannel,
    //     buffer: []u8,
    //     future: *Future(usize, xev_tcp.XevSocketChannel.ReadError),
    //     io_loop: ?*ThreadEventLoop = null,
    // },
};

pub const ConnectTimeout = struct {
    socket: xev.TCP,
    /// The future for the connection result.
    future: *Future(void, anyerror),
    /// The transport used for the connection.
    transport: ?*xev_tcp.XevTransport = null,
};

pub const Connect = struct {
    /// The future for the connection result.
    future: *Future(void, anyerror),
    /// The transport used for the connection.
    transport: ?*xev_tcp.XevTransport = null,

    channel: *conn.AnyRxConn,
};

/// Represents a message for I/O operations in the event loop.
pub const IOMessage = struct {
    const Self = @This();

    /// Pointer to the next message in the queue.
    next: ?*Self = null,

    /// The action to be performed, represented as a union of possible operations.
    action: IOAction,
};

/// A memory pool for managing `xev.Completion` objects.
const CompletionPool = std.heap.MemoryPool(xev.Completion);
const ConnectPool = std.heap.MemoryPool(Connect);
const ConnectTimeoutPool = std.heap.MemoryPool(ConnectTimeout);
const XevSocketChannelPool = std.heap.MemoryPool(xev_tcp.XevSocketChannel);
const HandlerPipelinePool = std.heap.MemoryPool(pipeline.HandlerPipeline);

/// Represents a thread-based event loop for managing asynchronous I/O operations.
pub const ThreadEventLoop = struct {
    /// The event loop.
    loop: xev.Loop,
    /// The stop notifier.
    stop_notifier: xev.Async,
    /// The async notifier.
    async_notifier: xev.Async,
    /// The async task queue.
    task_queue: *Intrusive(IOMessage),
    /// The completion for stopping the transport.
    c_stop: xev.Completion,
    /// The completion for async I/O operations.
    c_async: xev.Completion,
    /// The thread for the event loop.
    loop_thread: std.Thread,
    /// The allocator.
    allocator: Allocator,
    /// The memory pool for managing completion objects.
    completion_pool: CompletionPool,

    connect_pool: ConnectPool,

    connect_timeout_pool: ConnectTimeoutPool,

    channel_pool: XevSocketChannelPool,

    handler_pipeline_pool: HandlerPipelinePool,

    conn_initializer: conn.AnyConnInitializer,

    loop_thread_id: std.Thread.Id,

    const Self = @This();

    /// Initializes the event loop.
    pub fn init(self: *Self, allocator: Allocator, conn_initializer: conn.AnyConnInitializer) !void {
        var loop = try xev.Loop.init(.{});
        errdefer loop.deinit();

        var stop_notifier = try xev.Async.init();
        errdefer stop_notifier.deinit();

        var async_notifier = try xev.Async.init();
        errdefer async_notifier.deinit();

        var task_queue = try allocator.create(Intrusive(IOMessage));
        task_queue.init();
        errdefer allocator.destroy(task_queue);

        var completion_pool = CompletionPool.init(allocator);
        errdefer completion_pool.deinit();

        var connect_timeout_pool = ConnectTimeoutPool.init(allocator);
        errdefer connect_timeout_pool.deinit();

        var connect_pool = ConnectPool.init(allocator);
        errdefer connect_pool.deinit();

        var channel_pool = XevSocketChannelPool.init(allocator);
        errdefer channel_pool.deinit();

        var handler_pipeline_pool = HandlerPipelinePool.init(allocator);
        errdefer handler_pipeline_pool.deinit();

        self.* = .{
            .loop = loop,
            .stop_notifier = stop_notifier,
            .async_notifier = async_notifier,
            .task_queue = task_queue,
            .c_stop = .{},
            .c_async = .{},
            .loop_thread = undefined,
            .loop_thread_id = undefined,
            .allocator = allocator,
            .completion_pool = completion_pool,
            .connect_pool = connect_pool,
            .connect_timeout_pool = connect_timeout_pool,
            .channel_pool = channel_pool,
            .handler_pipeline_pool = handler_pipeline_pool,
            .conn_initializer = conn_initializer,
        };

        const thread = try std.Thread.spawn(.{}, start, .{self});
        self.loop_thread = thread;
    }

    /// Deinitializes the event loop, releasing all resources.
    pub fn deinit(self: *Self) void {
        self.loop.deinit();
        self.stop_notifier.deinit();
        self.async_notifier.deinit();
        while (self.task_queue.pop()) |node| {
            self.allocator.destroy(node);
        }
        self.allocator.destroy(self.task_queue);
        self.completion_pool.deinit();
        self.connect_pool.deinit();
        self.connect_timeout_pool.deinit();
        self.channel_pool.deinit();
        self.handler_pipeline_pool.deinit();
    }

    /// Starts the event loop.
    pub fn start(self: *Self) !void {
        self.loop_thread_id = std.Thread.getCurrentId();

        self.stop_notifier.wait(&self.loop, &self.c_stop, ThreadEventLoop, self, &stopCallback);

        self.async_notifier.wait(&self.loop, &self.c_async, ThreadEventLoop, self, &asyncCallback);

        try self.loop.run(.until_done);
    }

    /// Stops the event loop and joins the thread.
    pub fn close(self: *Self) void {
        self.stop_notifier.notify() catch |err| {
            std.debug.print("Error notifying stop: {}\n", .{err});
        };

        self.loop_thread.join();
    }

    /// Queues a message for processing in the event loop.
    pub fn queueMessage(
        self: *Self,
        message: IOMessage,
    ) !void {
        const m = try self.allocator.create(IOMessage);
        m.* = message;

        self.task_queue.push(m);

        try self.async_notifier.notify();
    }

    pub fn inEventLoopThread(self: *Self) bool {
        return self.loop_thread_id == std.Thread.getCurrentId();
    }

    /// Callback for handling the stop notifier.
    fn stopCallback(
        _: ?*Self,
        loop: *xev.Loop,
        _: *xev.Completion,
        r: xev.Async.WaitError!void,
    ) xev.CallbackAction {
        r catch |err| {
            std.debug.print("Error in stop callback: {}\n", .{err});
            return .disarm;
        };

        loop.stop();

        return .disarm;
    }

    /// Callback for handling the async notifier.
    fn asyncCallback(
        self_: ?*Self,
        loop: *xev.Loop,
        _: *xev.Completion,
        r: xev.Async.WaitError!void,
    ) xev.CallbackAction {
        r catch |err| {
            std.debug.print("Error in async callback: {}\n", .{err});
            return .disarm;
        };
        const self = self_.?;

        while (self.task_queue.pop()) |m| {
            switch (m.action) {
                // .run_open_stream_timer => |*action_data| {
                //     const timer = xev.Timer.init() catch unreachable;
                //     const c_timer = self.completion_pool.create() catch unreachable;
                //     timer.run(loop, c_timer, action_data.timeout_ms, IOAction, m.action, yamux_timer.TimeoutLoop.runOpenStreamTimerCB);
                // },
                // .run_close_stream_timer => |*action_data| {
                //     const timer = action_data.stream.close_timer.?;
                //     const c_timer = action_data.stream.c_close_timer.?;
                //     timer.run(loop, c_timer, action_data.timeout_ms, IOAction, m.action, yamux_timer.TimeoutLoop.runCloseStreamTimerCB);
                // },
                // .cancel_close_stream_timer => |*action_data| {
                //     const timer = action_data.stream.close_timer.?;
                //     const c_timer = action_data.stream.c_close_timer.?;
                //     const c_cancel_timer = self.completion_pool.create() catch unreachable;
                //     c_cancel_timer.* = .{};
                //     timer.cancel(loop, c_timer, c_cancel_timer, IOAction, m.action, yamux_timer.TimeoutLoop.cancelCloseStreamTimerCB);
                // },
                .connect => |action_data| {
                    const address = action_data.address;
                    var socket = xev.TCP.init(address) catch unreachable;
                    const c = self.completion_pool.create() catch unreachable;
                    const timer = xev.Timer.init() catch unreachable;
                    const c_timer = self.completion_pool.create() catch unreachable;
                    const connect_timeout = action_data.timeout_ms;
                    const connect_ud = self.connect_pool.create() catch unreachable;
                    connect_ud.* = .{
                        .future = action_data.future,
                        .transport = action_data.transport,
                        .channel = action_data.channel,
                    };
                    const connect_timeout_ud = self.connect_timeout_pool.create() catch unreachable;
                    connect_timeout_ud.* = .{
                        .future = action_data.future,
                        .transport = action_data.transport,
                        .socket = socket,
                    };
                    socket.connect(loop, c, address, Connect, connect_ud, xev_tcp.XevTransport.connectCB);
                    timer.run(loop, c_timer, connect_timeout, ConnectTimeout, connect_timeout_ud, xev_tcp.XevTransport.connectTimeoutCB);
                },
                // .accept => {
                //     const server = m.action.accept.server;
                //     const c = self.completion_pool.create() catch unreachable;
                //     m.action.accept.io_loop = self;
                //     server.accept(loop, c, IOMessage, m, acceptCB);
                // },
                // .write => {
                //     const c = self.completion_pool.create() catch unreachable;
                //     const channel = m.action.write.channel;
                //     const buffer = m.action.write.buffer;
                //     m.action.write.io_loop = self;
                //     channel.socket.write(loop, c, .{ .slice = buffer }, IOMessage, m, writeCB);
                // },
                // .read => {
                //     const channel = m.action.read.channel;
                //     const buffer = m.action.read.buffer;
                //     const c = self.completion_pool.create() catch unreachable;
                //     m.action.read.io_loop = self;
                //     channel.socket.read(loop, c, .{ .slice = buffer }, IOMessage, m, readCB);
                // },
            }
        }

        return .rearm;
    }

    /// Callback executed when the open stream timer expires.
    /// This is used to handle timeouts when establishing a new stream connection.
    fn runOpenStreamTimerCB(
        ud: ?*IOAction,
        _: *xev.Loop,
        c: *xev.Completion,
        tr: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        const action = ud.?.run_open_stream_timer;
        defer action.io_loop.?.completion_pool.destroy(c);
        defer action.io_loop.?.allocator.destroy(action);

        tr catch |err| {
            if (err != xev.Timer.RunError.Canceled) {
                std.log.err("Error in timer callback: {}\n", .{err});
            }
            return .disarm;
        };

        if (action.stream.session.isClosed()) {
            return .disarm;
        }

        if (action.stream.establish_completion.isSet()) {
            return .disarm;
        }

        // timer expired
        action.stream.session.close();
        return .disarm;
    }

    /// Callback executed when the close stream timer expires.
    /// This is used to handle timeouts when closing a stream connection.
    fn runCloseStreamTimerCB(
        ud: ?*IOMessage,
        _: *xev.Loop,
        _: *xev.Completion,
        tr: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.run_close_stream_timer;
        defer action.io_loop.?.allocator.destroy(n);

        tr catch |err| {
            if (err != xev.Timer.RunError.Canceled) {
                std.debug.print("Error in timer callback: {}\n", .{err});
            }
            return .disarm;
        };

        // timer expired
        action.stream.closeTimeout();
        return .disarm;
    }

    /// Callback executed when a close stream timer is canceled.
    /// This is used to clean up resources when a timer is canceled before expiry.
    fn cancelCloseStreamTimerCB(
        ud: ?*IOMessage,
        _: *xev.Loop,
        c: *xev.Completion,
        tr: xev.Timer.CancelError!void,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.cancel_close_stream_timer;
        defer action.io_loop.?.completion_pool.destroy(c);
        defer action.io_loop.?.allocator.destroy(n);

        tr catch |err| {
            std.debug.print("Error in timer callback: {}\n", .{err});
            return .disarm;
        };

        return .disarm;
    }

    /// Callback executed when a connection attempt completes.
    /// This handles the result of trying to connect to a remote address.
    fn connectCB(
        ud: ?*IOMessage,
        _: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.ConnectError!void,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.connect;
        defer action.io_loop.?.completion_pool.destroy(c);
        defer action.io_loop.?.allocator.destroy(n);

        _ = r catch |err| {
            std.debug.print("Error connecting: {}\n", .{err});
            action.future.setError(err);

            return .disarm;
        };

        action.channel.init(socket, action.transport.?, conn.Direction.OUTBOUND);
        action.future.setDone();

        return .disarm;
    }

    /// Callback executed when an accept operation completes.
    /// This handles incoming connections on a listening socket.
    fn acceptCB(
        ud: ?*IOMessage,
        _: *xev.Loop,
        c: *xev.Completion,
        r: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.accept;
        defer action.io_loop.?.completion_pool.destroy(c);
        defer action.io_loop.?.allocator.destroy(n);

        const socket = r catch |err| {
            std.debug.print("Error accepting: {}\n", .{err});
            action.future.setError(err);

            return .disarm;
        };

        action.channel.init(socket, action.transport.?, .INBOUND);
        action.future.setDone();

        return .disarm;
    }

    /// Callback executed when a write operation completes.
    /// This handles the result of writing data to a socket.
    fn writeCB(
        ud: ?*IOMessage,
        _: *xev.Loop,
        c: *xev.Completion,
        _: xev.TCP,
        _: xev.WriteBuffer,
        r: xev.WriteError!usize,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.write;
        defer action.io_loop.?.completion_pool.destroy(c);
        defer action.io_loop.?.allocator.destroy(n);

        const written = r catch |err| {
            std.debug.print("Error writing: {}\n", .{err});
            action.future.setError(err);
            return .disarm;
        };

        action.future.setValue(written);

        return .disarm;
    }

    /// Callback executed when a read operation completes.
    /// This handles the result of reading data from a socket.
    /// If EOF or another error is encountered, it initiates a graceful shutdown.
    fn readCB(
        ud: ?*IOMessage,
        loop: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        _: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.read;
        defer action.io_loop.?.completion_pool.destroy(c);
        defer action.io_loop.?.allocator.destroy(n);

        const read = r catch |err| switch (err) {
            error.EOF => {
                const c_eof_error = action.channel.transport.allocator.create(xev.Completion) catch unreachable;
                socket.shutdown(loop, c_eof_error, xev_tcp.XevSocketChannel, action.channel, shutdownCB);
                action.future.setError(err);

                return .disarm;
            },

            else => {
                const c_error = action.channel.transport.allocator.create(xev.Completion) catch unreachable;
                socket.shutdown(loop, c_error, xev_tcp.XevSocketChannel, action.channel, shutdownCB);
                std.log.warn("server read unexpected err={}", .{err});
                action.future.setError(err);

                return .disarm;
            },
        };

        action.future.setValue(read);

        return .disarm;
    }

    /// Callback executed when a socket shutdown operation completes.
    /// This initiates a full socket close after the shutdown completes.
    fn shutdownCB(
        ud: ?*xev_tcp.XevSocketChannel,
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
        s.close(l, c, xev_tcp.XevSocketChannel, self, closeCB);
        return .disarm;
    }

    /// Callback executed when a socket close operation completes.
    /// This is the final step in the socket cleanup process.
    fn closeCB(
        ud: ?*xev_tcp.XevSocketChannel,
        _: *xev.Loop,
        c: *xev.Completion,
        _: xev.TCP,
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        const n = ud.?;
        defer n.transport.allocator.destroy(c);

        _ = r catch |err| {
            std.debug.print("Error close: {}\n", .{err});

            return .disarm;
        };

        return .disarm;
    }
};

// test "TimerManager add timer for stream" {
//     const allocator = std.testing.allocator;
//     var event_loop: ThreadEventLoop = undefined;
//     try event_loop.init(allocator);
//     defer event_loop.deinit();

//     // Create a test session and stream for timer
//     var pipes = try conn.createPipeConnPair();
//     defer {
//         pipes.client.close();
//         pipes.server.close();
//     }

//     const client_conn = pipes.client.conn().any();
//     var config = Config.defaultConfig();

//     var pool: std.Thread.Pool = undefined;
//     try std.Thread.Pool.init(&pool, .{ .allocator = allocator });
//     defer pool.deinit();

//     var test_session: session.Session = undefined;
//     try session.Session.init(allocator, &config, client_conn, &test_session, &pool);
//     defer test_session.deinit();

//     var test_stream: Stream = undefined;
//     try Stream.init(&test_stream, &test_session, 1, .init, allocator);
//     defer test_stream.deinit();

//     const message = IOMessage{
//         .action = .{
//             .run_open_stream_timer = .{
//                 .stream = &test_stream,
//                 .timeout_ms = 100,
//             },
//         },
//     };
//     try event_loop.queueMessage(message);

//     std.time.sleep(200 * std.time.ns_per_ms);

//     try std.testing.expect(test_session.isClosed());

//     event_loop.close();
// }
