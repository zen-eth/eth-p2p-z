const std = @import("std");
const libp2p = @import("root.zig");
const Allocator = std.mem.Allocator;
const xev = libp2p.xev;
const Intrusive = @import("concurrent/mpsc_queue.zig").Intrusive;
const Future = @import("concurrent/future.zig").Future;
const conn = @import("conn.zig");
const xev_tcp = libp2p.transport.tcp;
const quic = libp2p.transport.quic;
const Multiaddr = @import("multiformats").multiaddr.Multiaddr;
const PeerId = @import("peer_id").PeerId;

/// Memory pool for managing completion objects in the event loop.
const CompletionPool = std.heap.MemoryPool(xev.Completion);
/// Memory pool for managing connection objects in the event loop.
const ConnectCtxPool = std.heap.MemoryPool(ConnectCtx);
/// Memory pool for managing connection timeouts in the event loop.
const ConnectTimeoutPool = std.heap.MemoryPool(ConnectTimeout);
/// Memory pool for managing handler pipelines in the event loop.
const HandlerPipelinePool = std.heap.MemoryPool(conn.HandlerPipeline);
/// Memory pools for managing write operations in the event loop.
const WriteCtxPool = std.heap.MemoryPool(WriteCtx);
/// Memory pools for managing accept operations in the event loop.
const AcceptCtxPool = std.heap.MemoryPool(AcceptCtx);
/// Memory pools for managing close operations in the event loop.
const CloseCtxPool = std.heap.MemoryPool(CloseCtx);
/// Memory pool for managing read buffers in the event loop.
const ReadBufferPool = std.heap.MemoryPool([BUFFER_SIZE]u8);

/// The size of the read buffer used for I/O operations in the event loop.
pub const BUFFER_SIZE = 64 * 1024;
/// A no-op context pool for managing callback contexts in the event loop.
pub const NoOpCtxPool = std.heap.MemoryPool(NoOpCallbackCtx);

/// Represents an I/O action that can be performed in the event loop.
pub const IOAction = union(enum) {
    call: struct {
        func: *const fn (loop: *ThreadEventLoop, ctx: ?*anyopaque) void,
        ctx: ?*anyopaque,
        deinit: ?*const fn (loop: *ThreadEventLoop, ctx: ?*anyopaque) void = null,
    },
};

/// Represents a queued message for I/O operations in the event loop.
pub const IOMessage = struct {
    const Self = @This();
    /// Pointer to the next message in the queue.
    next: ?*Self = null,
    /// The action to be performed, represented as a union of possible operations.
    action: IOAction,
};

pub const ConnectTimeout = struct {
    /// The address to connect to.
    socket: xev.TCP,
    /// The future for the connection result.
    future: *Future(void, anyerror),
    /// The transport used for the connection.
    transport: ?*xev_tcp.XevTransport = null,
};

/// Represents the context for a connection operation in the event loop.
pub const ConnectCtx = struct {
    /// The transport used for the connection.
    transport: *xev_tcp.XevTransport,
    /// The instance to be passed to the callback function.
    callback_instance: ?*anyopaque = null,
    /// The callback function to be called when the connection is established.
    callback: *const fn (instance: ?*anyopaque, res: anyerror!conn.AnyConn) void,
};

/// Represents the context for writing data to a socket channel in the event loop.
pub const WriteCtx = struct {
    /// The socket channel to write data to.
    channel: *xev_tcp.XevSocketChannel,
    /// The instance to be passed to the callback function.
    callback_instance: ?*anyopaque = null,
    /// The callback function to be called when the write operation is complete.
    callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void,
};

/// Represents the context for closing a socket channel in the event loop.
pub const CloseCtx = struct {
    /// The socket channel to be closed.
    channel: *xev_tcp.XevSocketChannel,
    /// The instance to be passed to the callback function.
    callback_instance: ?*anyopaque = null,
    /// The callback function to be called when the close operation is complete.
    callback: *const fn (instance: ?*anyopaque, res: anyerror!void) void,
};

/// Represents the context for accepting a new connection in the event loop.
pub const AcceptCtx = struct {
    /// The transport used for the accepted connection.
    transport: *xev_tcp.XevTransport,
    /// The instance to be passed to the callback function.
    callback_instance: ?*anyopaque = null,
    /// The callback function to be called when the accepted connection is established.
    callback: *const fn (instance: ?*anyopaque, res: anyerror!conn.AnyConn) void,
};

/// Represents a no-op callback context used for handling callbacks in the event loop.
pub const NoOpCallbackCtx = struct {
    /// The connection associated with the callback, if any.
    conn: ?conn.AnyConn = null,
    /// The handler context associated with the callback, if any.
    ctx: ?*conn.ConnHandlerContext = null,
};

/// A no-op callback implementation for handling write and close operations in the event loop.
pub const NoOpCallback = struct {
    /// The close callback function that is called when a close operation is complete.
    pub fn closeCallback(instance: ?*anyopaque, res: anyerror!void) void {
        const cb_ctx: *NoOpCallbackCtx = @ptrCast(@alignCast(instance.?));
        defer if (cb_ctx.conn) |any_conn| any_conn.getPipeline().pool_manager.no_op_ctx_pool.destroy(cb_ctx) else if (cb_ctx.ctx) |ctx| ctx.pipeline.pool_manager.no_op_ctx_pool.destroy(cb_ctx);
        if (res) |_| {} else |err| {
            if (cb_ctx.conn) |any_conn| {
                any_conn.getPipeline().fireErrorCaught(err);
            } else if (cb_ctx.ctx) |ctx| {
                ctx.fireErrorCaught(err);
            }
        }
    }

    /// The write callback function that is called when a write operation is complete.
    pub fn writeCallback(instance: ?*anyopaque, res: anyerror!usize) void {
        const cb_ctx: *NoOpCallbackCtx = @ptrCast(@alignCast(instance.?));
        if (res) |_| {
            if (cb_ctx.conn) |any_conn| any_conn.getPipeline().pool_manager.no_op_ctx_pool.destroy(cb_ctx) else if (cb_ctx.ctx) |ctx| ctx.pipeline.pool_manager.no_op_ctx_pool.destroy(cb_ctx);
        } else |err| {
            if (cb_ctx.conn) |any_conn| {
                any_conn.getPipeline().close(instance, NoOpCallback.closeCallback);
            } else if (cb_ctx.ctx) |ctx| {
                ctx.fireErrorCaught(err);
                ctx.close(instance, NoOpCallback.closeCallback);
            }
        }
    }
};

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
    /// The memory pool for managing connection objects.
    connect_ctx_pool: ConnectCtxPool,
    /// The memory pool for managing connection timeouts.
    connect_timeout_pool: ConnectTimeoutPool,
    /// The memory pool for managing handler pipelines.
    handler_pipeline_pool: HandlerPipelinePool,
    /// The memory pool for managing write operations.
    write_ctx_pool: WriteCtxPool,
    /// The memory pool for managing accept operations.
    accept_ctx_pool: AcceptCtxPool,
    /// The memory pool for managing read buffers.
    read_buffer_pool: ReadBufferPool,
    /// The memory pool for managing close operations.
    close_ctx_pool: CloseCtxPool,
    /// The thread ID of the event loop thread.
    loop_thread_id: std.Thread.Id,

    const Self = @This();

    pub const TcpTasks = @import("thread_event_loop/tasks/tcp.zig").extend(Self, ConnectCtx, AcceptCtx, WriteCtx, CloseCtx);
    pub const QuicTasks = @import("thread_event_loop/tasks/quic.zig").extend(Self);
    pub const SwitchTasks = @import("thread_event_loop/tasks/switch.zig").extend(Self);
    pub const PubsubTasks = @import("thread_event_loop/tasks/pubsub.zig").extend(Self);

    /// Initializes the event loop.
    pub fn init(self: *Self, allocator: Allocator) !void {
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

        var connect_ctx_pool = ConnectCtxPool.init(allocator);
        errdefer connect_ctx_pool.deinit();

        var handler_pipeline_pool = HandlerPipelinePool.init(allocator);
        errdefer handler_pipeline_pool.deinit();

        var write_ctx_pool = WriteCtxPool.init(allocator);
        errdefer write_ctx_pool.deinit();

        var accept_ctx_pool = AcceptCtxPool.init(allocator);
        errdefer accept_ctx_pool.deinit();

        var read_buffer_pool = ReadBufferPool.init(allocator);
        errdefer read_buffer_pool.deinit();

        var close_ctx_pool = CloseCtxPool.init(allocator);
        errdefer close_ctx_pool.deinit();

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
            .connect_ctx_pool = connect_ctx_pool,
            .connect_timeout_pool = connect_timeout_pool,
            .handler_pipeline_pool = handler_pipeline_pool,
            .write_ctx_pool = write_ctx_pool,
            .accept_ctx_pool = accept_ctx_pool,
            .read_buffer_pool = read_buffer_pool,
            .close_ctx_pool = close_ctx_pool,
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
        self.connect_ctx_pool.deinit();
        self.connect_timeout_pool.deinit();
        self.handler_pipeline_pool.deinit();
        self.write_ctx_pool.deinit();
        self.accept_ctx_pool.deinit();
        self.read_buffer_pool.deinit();
        self.close_ctx_pool.deinit();
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
        if (self.inEventLoopThread()) {
            self.loop.stop();
            return;
        }

        self.stop_notifier.notify() catch |err| {
            std.log.warn("Error notifying stop: {}\n", .{err});
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

    pub fn queueCall(self: *Self, comptime T: type, ctx: *T, comptime func: *const fn (loop: *Self, ctx: *T) void) !void {
        try self.queueCallWithDeinit(T, ctx, func, null);
    }

    pub fn queueCallWithDeinit(
        self: *Self,
        comptime T: type,
        ctx: *T,
        comptime func: *const fn (loop: *Self, ctx: *T) void,
        comptime destroy_fn: ?*const fn (loop: *Self, ctx: *T) void,
    ) !void {
        const message = IOMessage{
            .action = .{ .call = .{
                .func = makeCallFunc(T, func),
                .ctx = ctx,
                .deinit = if (destroy_fn) |df| makeDeinitFunc(T, df) else null,
            } },
        };

        try self.queueMessage(message);
    }

    fn makeCallFunc(comptime T: type, comptime func: *const fn (loop: *Self, ctx: *T) void) *const fn (loop: *Self, ctx: ?*anyopaque) void {
        return struct {
            fn invoke(loop: *Self, raw: ?*anyopaque) void {
                func(loop, @ptrCast(@alignCast(raw.?)));
            }
        }.invoke;
    }

    fn makeDeinitFunc(comptime T: type, comptime func: *const fn (loop: *Self, ctx: *T) void) *const fn (loop: *Self, ctx: ?*anyopaque) void {
        return struct {
            fn invoke(loop: *Self, raw: ?*anyopaque) void {
                func(loop, @ptrCast(@alignCast(raw.?)));
            }
        }.invoke;
    }

    pub fn makeDestroyTask(comptime T: type) *const fn (loop: *Self, ctx: *T) void {
        return struct {
            fn destroy(loop: *Self, ctx: *T) void {
                loop.allocator.destroy(ctx);
            }
        }.destroy;
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
            std.log.warn("Error in stop callback: {}\n", .{err});
            return .disarm;
        };

        loop.stop();

        return .disarm;
    }

    /// Callback for handling the async notifier.
    fn asyncCallback(
        instance: ?*Self,
        loop: *xev.Loop,
        _: *xev.Completion,
        r: xev.Async.WaitError!void,
    ) xev.CallbackAction {
        r catch |err| {
            std.log.warn("Error in async callback: {}\n", .{err});
            return .disarm;
        };
        const self = instance.?;

        while (self.task_queue.pop()) |m| {
            const action = m.action;
            self.allocator.destroy(m);

            const call = action.call;
            call.func(self, call.ctx);
            if (call.deinit) |deinit_fn| {
                deinit_fn(self, call.ctx);
            }
        }

        if (loop.stopped()) {
            return .disarm;
        }
        return .rearm;
    }
};
