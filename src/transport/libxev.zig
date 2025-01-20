const std = @import("std");
const xev = @import("xev");
const Intrusive = @import("../utils/queue_mpsc.zig").Intrusive;
const Queue=Intrusive(AsyncIOQueueElem);
const TCP = xev.TCP;
const Allocator = std.mem.Allocator;
const ThreadPool = xev.ThreadPool;

/// Memory pools for things that need stable pointers
const BufferPool = std.heap.MemoryPool([4096]u8);
const CompletionPool = std.heap.MemoryPool(xev.Completion);
const TCPPool = std.heap.MemoryPool(xev.TCP);

pub const Options = struct {
    backlog: u31,
};

pub const AsyncIOQueueElem = struct {
    const Self = @This();
    next: ?*Self = null,
    op: union(enum) {
        connect: struct {
            address: std.net.Address,
        },
        write: struct {
            buffer: []const u8,
        },
    },
};

pub const XevTransport = struct {
    loop: xev.Loop,
    buffer_pool: BufferPool,
    completion_pool: CompletionPool,
    socket_pool: TCPPool,
    threadPool: *xev.ThreadPool,
    listeners: struct {
        mutex: std.Thread.Mutex,
        m: std.StringHashMap(TCP),
    },
    sockets: struct {
        mutex: std.Thread.Mutex,
        l: std.ArrayList(TCP),
    },
    options: Options,
    stop_notifier: xev.Async,
    async_io_notifier: xev.Async,
    async_task_queue: Queue,
    allocator: Allocator,

    pub fn init(allocator: Allocator, opts: Options) !XevTransport {
        const thread_pool = try allocator.create(ThreadPool);
        thread_pool.* = ThreadPool.init(.{});
        const loop_opts = xev.Options{
            .thread_pool = thread_pool,
        };
        const server_loop = try xev.Loop.init(loop_opts);
        const shutdown_notifier = try xev.Async.init();
        const async_io_notifier = try xev.Async.init();
        var q: Queue = undefined;
        q.init();
        return XevTransport{
            .buffer_pool = BufferPool.init(allocator),
            .completion_pool = CompletionPool.init(allocator),
            .socket_pool = TCPPool.init(allocator),
            .loop = server_loop,
            .threadPool = thread_pool,
            .listeners = .{
                .mutex = .{},
                .m = std.StringHashMap(TCP).init(allocator),
            },
            .sockets = .{
                .mutex = .{},
                .l = std.ArrayList(TCP).init(allocator),
            },
            .options = opts,
            .stop_notifier = shutdown_notifier,
            .async_io_notifier = async_io_notifier,
            .async_task_queue = q,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *XevTransport) void {
        self.sockets.mutex.lock();
        self.sockets.l.deinit();
        self.sockets.mutex.unlock();
        self.listeners.mutex.lock();
        self.listeners.m.deinit();
        self.listeners.mutex.unlock();
        self.buffer_pool.deinit();
        self.completion_pool.deinit();
        self.socket_pool.deinit();
        self.stop_notifier.deinit();
        self.async_io_notifier.deinit();
        self.loop.deinit();
        self.threadPool.shutdown();
        self.threadPool.deinit();
        self.allocator.destroy(self.threadPool);
    }

    pub fn dial(self: *XevTransport, addr: std.net.Address) !void {
        const client = try TCP.init(addr);
        const c = try self.completion_pool.create();
        client.connect(&self.loop, c, addr, XevTransport, self, connectCallback);
    }

    pub fn listen(self: *XevTransport, addr: std.net.Address) !void {
        const server = try TCP.init(addr);
        try server.bind(addr);
        try server.listen(self.options.backlog);

        const c_accept = try self.completion_pool.create();
        server.accept(&self.loop, c_accept, XevTransport, self, acceptCallback);

        self.listeners.mutex.lock();
        const key = try formatAddress(addr, self.allocator);
        defer self.allocator.free(key);
        self.listeners.m.put(key, server) catch |err| {
            std.debug.print("Error adding server to map: {}\n", .{err});
        };
        self.listeners.mutex.unlock();
    }

    pub fn serve(self: *XevTransport) !void {
        const userdata: ?*void = null;
        var c: xev.Completion = undefined;
        self.stop_notifier.wait(&self.loop, &c, void, userdata, &stopCallback);
        try self.loop.run(.until_done);
    }

    pub fn stop(self: *XevTransport) void {
        self.stop_notifier.notify() catch |err| {
            std.debug.print("Error notifying stop: {}\n", .{err});
        };
    }

    fn stopCallback(
        _: ?*void,
        loop: *xev.Loop,
        _: *xev.Completion,
        r: xev.Async.WaitError!void,
    ) xev.CallbackAction {
        _ = r catch unreachable;

        loop.stop();
        return .disarm;
    }

    fn connectCallback(
        self_: ?*XevTransport,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.TCP.ConnectError!void,
    ) xev.CallbackAction {
        _ = r catch unreachable;

        const self = self_.?;
        const buf = self.buffer_pool.create() catch unreachable;
        socket.read(l, c, .{ .slice = buf }, XevTransport, self, clientReadCallback);
        return .disarm;
    }

    fn clientReadCallback(
        self_: ?*XevTransport,
        loop: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        buf: xev.ReadBuffer,
        r: xev.TCP.ReadError!usize,
    ) xev.CallbackAction {
        const self = self_.?;
        const n = r catch |err| switch (err) {
            error.EOF => {
                self.destroyBuf(buf.slice);
                socket.shutdown(loop, c, XevTransport, self, shutdownCallback);
                return .disarm;
            },

            else => {
                self.destroyBuf(buf.slice);
                self.completion_pool.destroy(c);
                std.log.warn("server read unexpected err={}", .{err});
                return .disarm;
            },
        };

        // Echo it back
        std.debug.print("client read {any}\n", .{buf.slice[0..n]});

        // Read again
        return .rearm;
    }

    fn acceptCallback(
        self_: ?*XevTransport,
        l: *xev.Loop,
        _: *xev.Completion,
        r: xev.TCP.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        std.debug.print("accept callback\n", .{});
        const self = self_.?;

        // Create our socket
        const socket = self.socket_pool.create() catch unreachable;
        socket.* = r catch unreachable;
        self.sockets.mutex.lock();
        self.sockets.l.append(socket.*) catch unreachable;
        self.sockets.mutex.unlock();

        const buf = self.buffer_pool.create() catch unreachable;
        const c_read = self.completion_pool.create() catch unreachable;
        socket.read(l, c_read, .{ .slice = buf }, XevTransport, self, readCallback);
        return .rearm;
    }

    fn readCallback(
        self_: ?*XevTransport,
        loop: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        buf: xev.ReadBuffer,
        r: xev.TCP.ReadError!usize,
    ) xev.CallbackAction {
        std.debug.print("read callback\n", .{});
        const self = self_.?;
        const n = r catch |err| switch (err) {
            error.EOF => {
                std.debug.print("EOF\n", .{});
                self.destroyBuf(buf.slice);
                socket.shutdown(loop, c, XevTransport, self, shutdownCallback);
                return .disarm;
            },

            else => {
                std.debug.print("Error reading: {}\n", .{err});
                self.destroyBuf(buf.slice);
                self.completion_pool.destroy(c);
                std.log.warn("server read unexpected err={}", .{err});
                return .disarm;
            },
        };

        // Echo it back
        const c_echo = self.completion_pool.create() catch unreachable;
        const buf_write = self.buffer_pool.create() catch unreachable;
        @memcpy(buf_write, buf.slice[0..n]);
        socket.write(loop, c_echo, .{ .slice = buf_write[0..n] }, XevTransport, self, writeCallback);

        // Read again
        return .rearm;
    }

    fn writeCallback(
        self_: ?*XevTransport,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        buf: xev.WriteBuffer,
        r: xev.TCP.WriteError!usize,
    ) xev.CallbackAction {
        std.debug.print("write callback\n", .{});
        _ = l;
        _ = s;
        _ = r catch unreachable;

        // We do nothing for write, just put back objects into the pool.
        const self = self_.?;
        self.completion_pool.destroy(c);
        self.buffer_pool.destroy(
            @alignCast(
                @as(*[4096]u8, @ptrFromInt(@intFromPtr(buf.slice.ptr))),
            ),
        );
        return .disarm;
    }

    fn shutdownCallback(
        self_: ?*XevTransport,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.TCP.ShutdownError!void,
    ) xev.CallbackAction {
        std.debug.print("shutdown callback\n", .{});
        _ = r catch {};

        const self = self_.?;
        s.close(l, c, XevTransport, self, closeCallback);
        return .disarm;
    }

    fn closeCallback(
        self_: ?*XevTransport,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.TCP.CloseError!void,
    ) xev.CallbackAction {
        std.debug.print("close callback\n", .{});
        _ = l;
        _ = r catch unreachable;
        _ = socket;

        const self = self_.?;

        self.completion_pool.destroy(c);
        return .disarm;
    }

    fn destroyBuf(self: *XevTransport, buf: []const u8) void {
        self.buffer_pool.destroy(
            @alignCast(
                @as(*[4096]u8, @ptrFromInt(@intFromPtr(buf.ptr))),
            ),
        );
    }

    pub fn formatAddress(addr: std.net.Address, allocator: Allocator) ![]const u8 {
        const addr_str = try std.fmt.allocPrint(allocator, "{}", .{addr});
        std.debug.print("Address: {s}\n", .{addr_str});
        return addr_str;
    }
};

test "transport serve and stop" {
    const allocator = std.testing.allocator;
    const opts = Options{ .backlog = 128 };
    var peer1 = try XevTransport.init(allocator, opts);
    defer peer1.deinit();

    const addr = try std.net.Address.parseIp("127.0.0.1", 8080);
    try peer1.listen(addr);

    const server_thr = try std.Thread.spawn(.{}, XevTransport.serve, .{&peer1});

    // sleep for 1 second
    std.time.sleep(3_000_000_000);
    peer1.stop();
    server_thr.join();
}

// test "test ping-pong" {
//     const allocator = std.testing.allocator;
//     const opts = Options{ .backlog = 128 };
//     const peer1 = try XevTransport.init(allocator, opts);
//     defer peer1.deinit();
//
//     const addr = try std.net.Address.parseIp("127.0.0.1", 8080);
//     try peer1.listen(addr);
//
//     const server_thr = try std.Thread.spawn(.{}, XevTransport.serve, .{&peer1});
// }
