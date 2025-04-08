const std = @import("std");
const Allocator = std.mem.Allocator;
const xev = @import("xev");
const Intrusive = @import("../../concurrent/mpsc_queue.zig").Intrusive;
const Completion = @import("../../concurrent/completion.zig").Completion;
const Stream = @import("stream.zig").Stream;

const TimerQueueNode = struct {
    const Self = @This();
    stream: *Stream,
    timeout_ms: u64,
    next: ?*Self = null,
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
    self.* = TimerManager{
        .loop = loop,
        .stop_notifier = stop_notifier,
        .async_notifier = async_notifier,
        .timer_queue = timer_queue,
        .c_stop = .{},
        .c_async = .{},
        .loop_thread = undefined,
        .allocator = allocator,
    };

    const thread = try std.Thread.spawn(.{}, start, .{self});
    self.loop_thread = thread;
}

pub fn deinit(self: *TimerManager) void {
    self.loop.deinit();
    self.stop_notifier.deinit();
    self.async_notifier.deinit();
    self.allocator.destroy(self.timer_queue);
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
        var c1: xev.Completion = undefined;
        timer.run(loop, &c1, node.timeout_ms, TimerQueueNode, node, (struct {
            fn callback(
                ud: ?*TimerQueueNode,
                _: *xev.Loop,
                _: *xev.Completion,
                tr: xev.Timer.RunError!void,
            ) xev.CallbackAction {
                _ = tr catch unreachable;
                const n = ud.?;
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

        self.allocator.destroy(node);
    }

    return .rearm;
}
