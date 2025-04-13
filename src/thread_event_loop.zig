const std = @import("std");
const Allocator = std.mem.Allocator;
const xev = @import("xev");
const Intrusive = @import("concurrent/mpsc_queue.zig").Intrusive;
const Completion = @import("concurrent/completion.zig").Completion;
const Stream = @import("muxer/yamux/stream.zig").Stream;
const conn = @import("conn.zig");
const Config = @import("muxer/yamux/Config.zig");
const session = @import("muxer/yamux/session.zig");

pub const IOMessage = struct {
    const Self = @This();

    pub const RunOpenStreamTimer = struct {
        stream: *Stream,
        timeout_ms: u64,
        l: ?*ThreadEventLoop = null,
    };

    next: ?*Self = null,

    action: union(enum) {
        run_open_stream_timer: RunOpenStreamTimer,
        run_close_stream_timer: struct {
            stream: *Stream,
            timeout_ms: u64,
            timer: *xev.Timer,
            c_timer: *xev.Completion,
        },
        cancel_close_stream_timer: struct {
            stream: *Stream,
            timer: *xev.Timer,
            c_timer: *xev.Completion,
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
                    timer.run(loop, c_timer, m.action.run_open_stream_timer.timeout_ms, IOMessage, m, run_open_stream_timer_cb);
                },
                .run_close_stream_timer => {},
                .cancel_close_stream_timer => {},
            }
        }

        return .rearm;
    }

    fn run_open_stream_timer_cb(
        ud: ?*IOMessage,
        _: *xev.Loop,
        c: *xev.Completion,
        tr: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        tr catch |err| {
            if (err != xev.Timer.RunError.Canceled) {
                std.debug.print("Error in timer callback: {}\n", .{err});
            }
            return .disarm;
        };

        const n = ud.?;
        const action = n.action.run_open_stream_timer;
        defer action.l.?.completion_pool.destroy(c);
        defer action.l.?.allocator.destroy(n);

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
            .run_open_stream_timer = IOMessage.RunOpenStreamTimer{
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
