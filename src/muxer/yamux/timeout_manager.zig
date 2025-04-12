const std = @import("std");
const event_loop = @import("../../thread_event_loop.zig");
const EventLoop = event_loop.ThreadEventLoop;
const IOMessage = event_loop.IOMessage;
const stream = @import("stream.zig");
const Stream = stream.Stream;
const conn = @import("../../conn.zig");
const Config = @import("Config.zig");
const session = @import("session.zig");

pub const TimeoutManager = struct {
    loop: *EventLoop,

    const Self = @This();

    pub fn init(self: *Self, loop: *EventLoop) void {
        self.loop = loop;
    }

    pub fn start_open_stream_timer(
        self: *Self,
        s: *Stream,
        timeout_ms: u64,
    ) !void {
        const message = IOMessage{
            .action = .{
                .run_open_stream_timer = IOMessage.RunOpenStreamTimer{
                    .stream = s,
                    .timeout_ms = timeout_ms,
                },
            },
        };
        try self.loop.queueMessage(message);
    }
};

test "TimeoutManager start_open_stream_timer" {
    const allocator = std.testing.allocator;
    var l: EventLoop = undefined;
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

    var tm: TimeoutManager = undefined;
    TimeoutManager.init(&tm, &l);
    try tm.start_open_stream_timer(&test_stream, 100);

    std.time.sleep(200 * std.time.ns_per_ms);

    try std.testing.expect(test_session.isClosed());

    l.close();
}

test "TimeoutManager start_open_stream_timer, stream establish completion before timeout" {
    const allocator = std.testing.allocator;
    var l: EventLoop = undefined;
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

    var tm: TimeoutManager = undefined;
    TimeoutManager.init(&tm, &l);
    try tm.start_open_stream_timer(&test_stream, 100);

    test_stream.establish_completion.set();

    std.time.sleep(200 * std.time.ns_per_ms);

    try std.testing.expect(!test_session.isClosed());

    l.close();
}
