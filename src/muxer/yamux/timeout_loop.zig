const std = @import("std");
const event_loop = @import("../../thread_event_loop.zig");
const IOEventLoop = event_loop.ThreadEventLoop;
const IOMessage = event_loop.IOMessage;
const stream = @import("stream.zig");
const Stream = stream.Stream;
const conn = @import("../../conn.zig");
const Config = @import("Config.zig");
const session = @import("session.zig");
const xev = @import("xev");

/// The `TimeoutLoop` is responsible for managing stream-related timeouts
/// in the context of an I/O event loop. It provides functionality to start
/// and cancel timers for opening and closing streams.
pub const TimeoutLoop = struct {
    /// Reference to the I/O event loop used for managing asynchronous operations.
    io_event_loop: *IOEventLoop,

    const Self = @This();

    /// Initializes the `TimeoutLoop` with the given I/O event loop.
    pub fn init(self: *Self, loop: *IOEventLoop) void {
        self.io_event_loop = loop;
    }

    /// Starts a timer for opening a stream. If the timer expires before the
    /// stream is established, the associated session will be closed.
    pub fn start_open_stream_timer(
        self: *Self,
        s: *Stream,
        timeout_ms: u64,
    ) !void {
        const message = IOMessage{
            .action = .{
                .run_open_stream_timer = .{
                    .stream = s,
                    .timeout_ms = timeout_ms,
                },
            },
        };
        try self.io_event_loop.queueMessage(message);
    }

    /// Starts a timer for closing a stream. If the timer expires before the
    /// stream is closed, the stream will be forcefully closed.
    pub fn start_close_stream_timer(
        self: *Self,
        s: *Stream,
        timeout_ms: u64,
    ) !void {
        const message = IOMessage{
            .action = .{
                .run_close_stream_timer = .{
                    .stream = s,
                    .timeout_ms = timeout_ms,
                },
            },
        };
        try self.io_event_loop.queueMessage(message);
    }

    /// Cancels the timer for closing a stream. This prevents the stream from
    /// being forcefully closed if the timer was still active.
    pub fn cancel_close_stream_timer(
        self: *Self,
        s: *Stream,
    ) !void {
        const message = IOMessage{
            .action = .{
                .cancel_close_stream_timer = .{
                    .stream = s,
                },
            },
        };
        try self.io_event_loop.queueMessage(message);
    }
};

test "TimeoutManager start_open_stream_timer" {
    const allocator = std.testing.allocator;
    var l: IOEventLoop = undefined;
    try l.init(allocator);
    defer l.deinit();

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

    var tm: TimeoutLoop = undefined;
    TimeoutLoop.init(&tm, &l);
    try tm.start_open_stream_timer(&test_stream, 100);

    std.time.sleep(200 * std.time.ns_per_ms);

    try std.testing.expect(test_session.isClosed());

    l.close();
}

test "TimeoutManager start_open_stream_timer, stream establish completion before timeout" {
    const allocator = std.testing.allocator;
    var l: IOEventLoop = undefined;
    try l.init(allocator);
    defer l.deinit();

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

    var tm: TimeoutLoop = undefined;
    TimeoutLoop.init(&tm, &l);
    try tm.start_open_stream_timer(&test_stream, 100);

    test_stream.establish_completion.set();

    std.time.sleep(200 * std.time.ns_per_ms);

    try std.testing.expect(!test_session.isClosed());

    l.close();
}

test "TimeoutManager start_close_stream_timer" {
    const allocator = std.testing.allocator;
    var l: IOEventLoop = undefined;
    try l.init(allocator);
    defer l.deinit();

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

    var tm: TimeoutLoop = undefined;
    TimeoutLoop.init(&tm, &l);
    var close_timer = try xev.Timer.init();
    test_stream.close_timer = &close_timer;
    const c_close_timer = try test_stream.allocator.create(xev.Completion);
    c_close_timer.* = .{};
    test_stream.c_close_timer = c_close_timer;
    try tm.start_close_stream_timer(&test_stream, 100);

    std.time.sleep(200 * std.time.ns_per_ms);

    try std.testing.expect(test_stream.establish_completion.isSet());
    try std.testing.expect(test_stream.state == .closed);

    l.close();
}

test "TimeoutManager cancel_close_stream_timer" {
    const allocator = std.testing.allocator;
    var l: IOEventLoop = undefined;
    try l.init(allocator);
    defer l.deinit();

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

    var tm: TimeoutLoop = undefined;
    TimeoutLoop.init(&tm, &l);
    var close_timer = try xev.Timer.init();
    test_stream.close_timer = &close_timer;
    const c_close_timer = try test_stream.allocator.create(xev.Completion);
    c_close_timer.* = .{};
    test_stream.c_close_timer = c_close_timer;
    try tm.start_close_stream_timer(&test_stream, 1000);

    std.time.sleep(100 * std.time.ns_per_ms);
    try tm.cancel_close_stream_timer(&test_stream);

    std.time.sleep(1000 * std.time.ns_per_ms);

    try std.testing.expect(!test_stream.establish_completion.isSet());
    try std.testing.expect(test_stream.state != .closed);

    l.close();
}
