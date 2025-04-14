const std = @import("std");
const Allocator = std.mem.Allocator;
const xev = @import("xev");
const Intrusive = @import("concurrent/mpsc_queue.zig").Intrusive;
const completion = @import("concurrent/completion.zig");
const Completion = completion.Completion;
const Completion1 = completion.Completion1;
const Stream = @import("muxer/yamux/stream.zig").Stream;
const conn = @import("conn.zig");
const Config = @import("muxer/yamux/Config.zig");
const session = @import("muxer/yamux/session.zig");
const xev_tcp = @import("transport/tcp/xev.zig");

pub const IOMessage = struct {
    const Self = @This();

    next: ?*Self = null,

    action: union(enum) {
        run_open_stream_timer: struct {
            stream: *Stream,
            timeout_ms: u64,
            l: ?*ThreadEventLoop = null,
        },
        run_close_stream_timer: struct {
            stream: *Stream,
            timeout_ms: u64,
            l: ?*ThreadEventLoop = null,
        },
        cancel_close_stream_timer: struct {
            stream: *Stream,
            l: ?*ThreadEventLoop = null,
        },
        connect: struct {
            /// The address to connect to.
            address: std.net.Address,
            /// The channel to connect.
            channel: *xev_tcp.XevSocketChannel,
            /// The completion for the connection.
            completion: *Completion1(void, xev_tcp.Transport.DialError),
            /// The loop reference.
            l: ?*ThreadEventLoop = null,
            /// The transport reference.
            transport: ?*xev_tcp.XevTransport = null,
        },
        accept: struct {
            /// The server to accept from.
            server: xev.TCP,
            /// The channel to accept.
            channel: *xev_tcp.XevSocketChannel,
            /// The completion for the connection.
            completion: *Completion1(void, xev_tcp.Listener.AcceptError),
            /// The loop reference.
            l: ?*ThreadEventLoop = null,
            /// The transport reference.
            transport: ?*xev_tcp.XevTransport = null,
        },
        write: struct {
            /// The buffer to write.
            buffer: []const u8,
            /// The channel to write to.
            channel: *xev_tcp.XevSocketChannel,
            /// The completion for the write.
            completion: *Completion1(usize, xev_tcp.Connection.WriteError),
            /// The loop reference.
            l: ?*ThreadEventLoop = null,
        },
        read: struct {
            /// The channel to read from.
            channel: *xev_tcp.XevSocketChannel,
            /// The buffer to read into.
            buffer: []u8,
            /// The completion for the read.
            completion: *Completion1(usize, xev_tcp.Connection.ReadError),
            /// The loop reference.
            l: ?*ThreadEventLoop = null,
        },
    },
};

const CompletionPool = std.heap.MemoryPool(xev.Completion);

pub const ThreadEventLoop = struct {
    /// The event loop.
    loop: xev.Loop,
    /// The stop notifier.
    stop_notifier: xev.Async,
    /// The async notifier.
    async_notifier: xev.Async,
    /// The async task queue.
    timer_queue: *Intrusive(IOMessage),
    /// The completion for stopping the transport.
    c_stop: xev.Completion,
    /// The completion for async I/O operations.
    c_async: xev.Completion,
    /// The thread for the event loop.
    loop_thread: std.Thread,
    /// The allocator.
    allocator: Allocator,

    completion_pool: CompletionPool,

    const Self = @This();

    pub fn init(self: *Self, allocator: Allocator) !void {
        var loop = try xev.Loop.init(.{});
        errdefer loop.deinit();

        var stop_notifier = try xev.Async.init();
        errdefer stop_notifier.deinit();

        var async_notifier = try xev.Async.init();
        errdefer async_notifier.deinit();

        var timer_queue = try allocator.create(Intrusive(IOMessage));
        timer_queue.init();
        errdefer allocator.destroy(timer_queue);

        var completion_pool = CompletionPool.init(allocator);
        errdefer completion_pool.deinit();

        self.* = .{
            .loop = loop,
            .stop_notifier = stop_notifier,
            .async_notifier = async_notifier,
            .timer_queue = timer_queue,
            .c_stop = .{},
            .c_async = .{},
            .loop_thread = undefined,
            .allocator = allocator,
            .completion_pool = completion_pool,
        };

        const thread = try std.Thread.spawn(.{}, start, .{self});
        self.loop_thread = thread;
    }

    pub fn deinit(self: *Self) void {
        self.loop.deinit();
        self.stop_notifier.deinit();
        self.async_notifier.deinit();
        while (self.timer_queue.pop()) |node| {
            self.allocator.destroy(node);
        }
        self.allocator.destroy(self.timer_queue);
        self.completion_pool.deinit();
    }

    pub fn start(self: *Self) !void {
        self.stop_notifier.wait(&self.loop, &self.c_stop, ThreadEventLoop, self, &stopCallback);

        self.async_notifier.wait(&self.loop, &self.c_async, ThreadEventLoop, self, &asyncCallback);

        try self.loop.run(.until_done);
    }

    pub fn close(self: *Self) void {
        self.stop_notifier.notify() catch |err| {
            std.debug.print("Error notifying stop: {}\n", .{err});
        };

        self.loop_thread.join();
    }

    pub fn queueMessage(
        self: *Self,
        message: IOMessage,
    ) !void {
        const m = try self.allocator.create(IOMessage);
        errdefer self.allocator.destroy(m);
        m.* = message;

        self.timer_queue.push(m);

        try self.async_notifier.notify();
    }

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

        while (self.timer_queue.pop()) |m| {
            switch (m.action) {
                .run_open_stream_timer => {
                    const timer = xev.Timer.init() catch unreachable;
                    const c_timer = self.completion_pool.create() catch unreachable;
                    m.action.run_open_stream_timer.l = self;
                    timer.run(loop, c_timer, m.action.run_open_stream_timer.timeout_ms, IOMessage, m, runOpenStreamTimerCB);
                },
                .run_close_stream_timer => {
                    const timer = m.action.run_close_stream_timer.stream.close_timer.?;
                    const c_timer = m.action.run_close_stream_timer.stream.c_close_timer.?;
                    m.action.run_close_stream_timer.l = self;
                    timer.run(loop, c_timer, m.action.run_close_stream_timer.timeout_ms, IOMessage, m, runCloseStreamTimerCB);
                },
                .cancel_close_stream_timer => {
                    const timer = m.action.cancel_close_stream_timer.stream.close_timer.?;
                    const c_timer = m.action.cancel_close_stream_timer.stream.c_close_timer.?;
                    const c_cancel_timer = self.completion_pool.create() catch unreachable;
                    c_cancel_timer.* = .{};
                    m.action.cancel_close_stream_timer.l = self;
                    timer.cancel(loop, c_timer, c_cancel_timer, IOMessage, m, cancelCloseStreamTimerCB);
                },
                .connect => {
                    const address = m.action.connect.address;
                    var socket = xev.TCP.init(address) catch unreachable;
                    const c = self.completion_pool.create() catch unreachable;
                    m.action.connect.l = self;
                    socket.connect(loop, c, address, IOMessage, m, connectCB);
                },
                .accept => {
                    const server = m.action.accept.server;
                    const c = self.completion_pool.create() catch unreachable;
                    m.action.accept.l = self;
                    server.accept(loop, c, IOMessage, m, acceptCB);
                },
                .write => {
                    const c = self.completion_pool.create() catch unreachable;
                    const channel = m.action.write.channel;
                    const buffer = m.action.write.buffer;
                    m.action.write.l = self;
                    channel.socket.write(loop, c, .{ .slice = buffer }, IOMessage, m, writeCB);
                },
                .read => {
                    const channel = m.action.read.channel;
                    const buffer = m.action.read.buffer;
                    const c = self.completion_pool.create() catch unreachable;
                    m.action.read.l = self;
                    channel.socket.read(loop, c, .{ .slice = buffer }, IOMessage, m, readCB);
                },
            }
        }

        return .rearm;
    }

    fn runOpenStreamTimerCB(
        ud: ?*IOMessage,
        _: *xev.Loop,
        c: *xev.Completion,
        tr: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.run_open_stream_timer;
        defer action.l.?.completion_pool.destroy(c);
        defer action.l.?.allocator.destroy(n);

        tr catch |err| {
            if (err != xev.Timer.RunError.Canceled) {
                std.debug.print("Error in timer callback: {}\n", .{err});
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

    fn runCloseStreamTimerCB(
        ud: ?*IOMessage,
        _: *xev.Loop,
        _: *xev.Completion,
        tr: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.run_close_stream_timer;
        defer action.l.?.allocator.destroy(n);

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

    fn cancelCloseStreamTimerCB(
        ud: ?*IOMessage,
        _: *xev.Loop,
        c: *xev.Completion,
        tr: xev.Timer.CancelError!void,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.cancel_close_stream_timer;
        defer action.l.?.completion_pool.destroy(c);
        defer action.l.?.allocator.destroy(n);

        tr catch |err| {
            std.debug.print("Error in timer callback: {}\n", .{err});
            return .disarm;
        };

        return .disarm;
    }

    fn connectCB(
        ud: ?*IOMessage,
        _: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.ConnectError!void,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.connect;
        defer action.l.?.completion_pool.destroy(c);
        defer action.l.?.allocator.destroy(n);

        _ = r catch |err| {
            std.debug.print("Error connecting: {}\n", .{err});
            action.completion.setError(err);

            return .disarm;
        };

        action.channel.init(socket, action.transport.?, .OUTBOUND);
        action.completion.setDone();

        return .disarm;
    }

    fn acceptCB(
        ud: ?*IOMessage,
        _: *xev.Loop,
        c: *xev.Completion,
        r: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const n = ud.?;
        const action = n.action.accept;
        defer action.l.?.completion_pool.destroy(c);
        defer action.l.?.allocator.destroy(n);

        const socket = r catch |err| {
            std.debug.print("Error accepting: {}\n", .{err});
            action.completion.setError(err);

            return .disarm;
        };

        action.channel.init(socket, action.transport.?, .INBOUND);
        action.completion.setDone();

        return .disarm;
    }

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
        defer action.l.?.completion_pool.destroy(c);
        defer action.l.?.allocator.destroy(n);

        const written = r catch |err| {
            std.debug.print("Error writing: {}\n", .{err});
            action.completion.setError(err);
            return .disarm;
        };

        action.completion.setValue(written);

        return .disarm;
    }

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
        defer action.l.?.completion_pool.destroy(c);
        defer action.l.?.allocator.destroy(n);

        const read = r catch |err| switch (err) {
            error.EOF => {
                const c_eof_error = action.channel.transport.allocator.create(xev.Completion) catch unreachable;
                socket.shutdown(loop, c_eof_error, xev_tcp.XevSocketChannel, action.channel, shutdownCB);
                action.completion.setError(err);

                return .disarm;
            },

            else => {
                const c_error = action.channel.transport.allocator.create(xev.Completion) catch unreachable;
                socket.shutdown(loop, c_error, xev_tcp.XevSocketChannel, action.channel, shutdownCB);
                std.log.warn("server read unexpected err={}", .{err});
                action.completion.setError(err);

                return .disarm;
            },
        };

        action.completion.setValue(read);

        return .disarm;
    }

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

test "TimerManager add timer for stream" {
    const allocator = std.testing.allocator;
    var event_loop: ThreadEventLoop = undefined;
    try event_loop.init(allocator);
    defer event_loop.deinit();

    // Create a test session and stream for timer
    var pipes = try conn.createPipeConnPair();
    defer {
        pipes.client.close();
        pipes.server.close();
    }

    const client_conn = pipes.client.conn().any();
    var config = Config.defaultConfig();

    var pool: std.Thread.Pool = undefined;
    try std.Thread.Pool.init(&pool, .{ .allocator = allocator });
    defer pool.deinit();

    var test_session: session.Session = undefined;
    try session.Session.init(allocator, &config, client_conn, &test_session, &pool);
    defer test_session.deinit();

    var test_stream: Stream = undefined;
    try Stream.init(&test_stream, &test_session, 1, .init, allocator);
    defer test_stream.deinit();

    const message = IOMessage{
        .action = .{
            .run_open_stream_timer = .{
                .stream = &test_stream,
                .timeout_ms = 100,
            },
        },
    };
    try event_loop.queueMessage(message);

    std.time.sleep(200 * std.time.ns_per_ms);

    try std.testing.expect(test_session.isClosed());

    event_loop.close();
}
