const std = @import("std");
const Allocator = std.mem.Allocator;
const xev = @import("xev");
const Intrusive = @import("../../concurrent/mpsc_queue.zig").Intrusive;
const Completion = @import("../../concurrent/completion.zig").Completion;
const Stream = @import("stream.zig").Stream;
const conn = @import("../../conn.zig");
const Config = @import("Config.zig");
const session = @import("session.zig");

const TimerQueueNode = struct {
    const Self = @This();
    stream: *Stream,
    timeout_ms: u64,
    next: ?*Self = null,
    timer_manager: ?*TimerManager = null,
};

/// The event loop.
loop: xev.Loop,
/// The stop notifier.
stop_notifier: xev.Async,
/// The async notifier.
async_notifier: xev.Async,
/// The async task queue.
timer_queue: *Intrusive(TimerQueueNode),
/// The completion for stopping the transport.
c_stop: xev.Completion,
/// The completion for async I/O operations.
c_async: xev.Completion,
/// The thread for the event loop.
loop_thread: std.Thread,
/// The allocator.
allocator: Allocator,

completion_pool: CompletionPool,

const CompletionPool = std.heap.MemoryPool(xev.Completion);

const TimerManager = @This();

pub fn init(self: *TimerManager, allocator: Allocator) !void {
    var loop = try xev.Loop.init(.{});
    errdefer loop.deinit();

    var stop_notifier = try xev.Async.init();
    errdefer stop_notifier.deinit();

    var async_notifier = try xev.Async.init();
    errdefer async_notifier.deinit();

    var timer_queue = try allocator.create(Intrusive(TimerQueueNode));
    timer_queue.init();
    errdefer allocator.destroy(timer_queue);

    var completion_pool = CompletionPool.init(allocator);
    errdefer completion_pool.deinit();

    self.* = TimerManager{
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

pub fn deinit(self: *TimerManager) void {
    self.loop.deinit();
    self.stop_notifier.deinit();
    self.async_notifier.deinit();
    self.allocator.destroy(self.timer_queue);
    self.completion_pool.deinit();
}

pub fn start(self: *TimerManager) !void {
    self.stop_notifier.wait(&self.loop, &self.c_stop, TimerManager, self, &stopCallback);

    self.async_notifier.wait(&self.loop, &self.c_async, TimerManager, self, &asyncCallback);

    try self.loop.run(.until_done);
}

pub fn close(self: *TimerManager) void {
    self.stop_notifier.notify() catch |err| {
        std.debug.print("Error notifying stop: {}\n", .{err});
    };

    self.loop_thread.join();
}

pub fn addTimer(
    self: *TimerManager,
    stream: *Stream,
    timeout_ms: u64,
) !void {
    const timer_node = try self.allocator.create(TimerQueueNode);
    timer_node.* = .{
        .stream = stream,
        .timeout_ms = timeout_ms,
        .timer_manager = self,
    };

    self.timer_queue.push(timer_node);

    try self.async_notifier.notify();
}

fn stopCallback(
    self_: ?*TimerManager,
    loop: *xev.Loop,
    _: *xev.Completion,
    r: xev.Async.WaitError!void,
) xev.CallbackAction {
    _ = r catch unreachable;
    _ = self_.?;

    loop.stop();

    return .disarm;
}

fn asyncCallback(
    self_: ?*TimerManager,
    loop: *xev.Loop,
    _: *xev.Completion,
    r: xev.Async.WaitError!void,
) xev.CallbackAction {
    _ = r catch unreachable;
    const self = self_.?;

    while (self.timer_queue.pop()) |node| {
        const timer = xev.Timer.init() catch unreachable;
        const c1 = self.completion_pool.create() catch unreachable;
        node.timer_manager = self;
        timer.run(loop, c1, node.timeout_ms, TimerQueueNode, node, (struct {
            fn callback(
                ud: ?*TimerQueueNode,
                _: *xev.Loop,
                c: *xev.Completion,
                tr: xev.Timer.RunError!void,
            ) xev.CallbackAction {
                _ = tr catch unreachable;
                const n = ud.?;
                defer n.timer_manager.?.allocator.destroy(n);
                defer n.timer_manager.?.completion_pool.destroy(c);

                if (n.stream.session.isClosed()) {
                    return .disarm;
                }

                if (n.stream.establish_completion.isSet()) {
                    return .disarm;
                }

                // timer expired
                n.stream.session.close();
                return .disarm;
            }
        }).callback);
    }

    return .rearm;
}

test "TimerManager add timer for stream" {
    const allocator = std.testing.allocator;
    var timer_manager: TimerManager = undefined;
    try timer_manager.init(allocator);
    defer timer_manager.deinit();

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

    try timer_manager.addTimer(&test_stream, 100);

    std.time.sleep(200 * std.time.ns_per_ms);

    try std.testing.expect(test_session.isClosed());

    timer_manager.close();
}

test "TimerManager add timer for stream establish completion" {
    const allocator = std.testing.allocator;
    var timer_manager: TimerManager = undefined;
    try timer_manager.init(allocator);
    defer timer_manager.deinit();

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

    try timer_manager.addTimer(&test_stream, 100);

    test_stream.establish_completion.set();

    std.time.sleep(200 * std.time.ns_per_ms);

    try std.testing.expect(!test_session.isClosed());

    timer_manager.close();
}
