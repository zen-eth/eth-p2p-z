const std = @import("std");
const xev = @import("xev");
const Intrusive = @import("../utils/queue_mpsc.zig").Intrusive;
const Queue = Intrusive(AsyncIOQueueElem);
const TCP = xev.TCP;
const Allocator = std.mem.Allocator;
const ThreadPool = xev.ThreadPool;

/// Memory pools for things that need stable pointers
const BufferPool = std.heap.MemoryPool([4096]u8);
const CompletionPool = std.heap.MemoryPool(xev.Completion);
const TCPPool = std.heap.MemoryPool(xev.TCP);
const SocketChannelPool = std.heap.MemoryPool(SocketChannel);

pub const SocketChannelManager = struct {
    listeners: struct {
        mutex: std.Thread.Mutex,
        m: std.StringHashMap(SocketChannel),
    },
    sockets: struct {
        mutex: std.Thread.Mutex,
        l: std.ArrayList(SocketChannel),
    },
    allocator: Allocator,

    pub fn init(allocator: Allocator) SocketChannelManager {
        return SocketChannelManager{
            .listeners = .{
                .mutex = .{},
                .m = std.StringHashMap(SocketChannel).init(allocator),
            },
            .sockets = .{
                .mutex = .{},
                .l = std.ArrayList(SocketChannel).init(allocator),
            },
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SocketChannelManager) void {
        self.sockets.mutex.lock();
        self.sockets.l.deinit();
        self.sockets.mutex.unlock();
        self.listeners.mutex.lock();
        self.listeners.m.deinit();
        self.listeners.mutex.unlock();
    }
};

pub const SocketChannel = struct {
    socket: *xev.TCP,
    transport: *XevTransport,
    read_buf: []u8,
    is_auto_read: bool,
    is_initiator: bool,

    pub fn init(self: *SocketChannel, socket: *xev.TCP, transport: *XevTransport, is_initiator: bool, is_auto_read: bool) void {
        self.socket = socket;
        self.transport = transport;
        self.is_auto_read = is_auto_read;
        self.is_initiator = is_initiator;
        self.read_buf = transport.buffer_pool.create() catch unreachable;
    }

    pub fn set_auto_read(self: *SocketChannel, is_auto_read: bool) void {
        self.is_auto_read = is_auto_read;
    }

    pub fn deinit(_: *SocketChannel) void {}

    pub fn write(self: *SocketChannel, buf: []const u8) void {
        if (self.transport.isInLoopThread()) {
            const c = self.transport.completion_pool.create() catch unreachable;
            self.socket.write(&self.transport.loop, c, .{ .slice = buf }, SocketChannel, self, writeCallback);
        } else {
            var element = AsyncIOQueueElem{
                .next = null,
                .op = .{ .write = .{ .channel = self, .buffer = buf } },
            };
            self.transport.async_task_queue.push(&element);
            self.transport.async_io_notifier.notify() catch |err| {
                std.debug.print("Error notifying async io: {}\n", .{err});
            };
        }
    }

    pub fn read(self: *SocketChannel) void {
        if (self.transport.isInLoopThread()) {
            const c = self.transport.completion_pool.create() catch unreachable;

            if (self.is_initiator) {
                self.socket.read(&self.transport.loop, c, .{ .slice = self.read_buf[0..] }, SocketChannel, self, outboundChannelReadCallback);
            } else {
                self.socket.read(&self.transport.loop, c, .{ .slice = self.read_buf[0..] }, SocketChannel, self, inboundChannelReadCallback);
            }
        } else {
            var element = AsyncIOQueueElem{
                .next = null,
                .op = .{ .read = .{ .channel = self } },
            };
            self.transport.async_task_queue.push(&element);
            self.transport.async_io_notifier.notify() catch |err| {
                std.debug.print("Error notifying async io: {}\n", .{err});
            };
        }
    }

    fn outboundChannelReadCallback(
        self_: ?*SocketChannel,
        loop: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        buf: xev.ReadBuffer,
        r: xev.TCP.ReadError!usize,
    ) xev.CallbackAction {
        const self = self_.?;
        const n = r catch |err| switch (err) {
            error.EOF => {
                self.transport.destroyBuf(buf.slice);
                socket.shutdown(loop, c, SocketChannel, self, shutdownCallback);
                return .disarm;
            },

            else => {
                if (self.transport.handler.io_error) |cb| {
                    cb(self, err);
                }
                self.transport.destroyBuf(buf.slice);
                socket.shutdown(loop, c, SocketChannel, self, shutdownCallback);
                std.log.warn("server read unexpected err={}", .{err});
                return .disarm;
            },
        };

        if (self.transport.handler.outbound_channel_read) |cb| {
            cb(self, buf.slice[0..n]);
        }

        if (self.is_auto_read) {
            return .rearm;
        } else {
            return .disarm;
        }
    }

    fn inboundChannelReadCallback(
        self_: ?*SocketChannel,
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
                self.transport.destroyBuf(buf.slice);
                socket.shutdown(loop, c, SocketChannel, self, shutdownCallback);
                return .disarm;
            },

            else => {
                if (self.transport.handler.io_error) |cb| {
                    cb(self, err);
                }
                std.debug.print("Error reading: {}\n", .{err});
                self.transport.destroyBuf(buf.slice);
                socket.shutdown(loop, c, SocketChannel, self, shutdownCallback);
                std.log.warn("server read unexpected err={}", .{err});
                return .disarm;
            },
        };

        if (self.transport.handler.outbound_channel_read) |cb| {
            cb(self, buf.slice[0..n]);
        }

        if (self.is_auto_read) {
            return .rearm;
        } else {
            return .disarm;
        }
    }

    fn writeCallback(
        self_: ?*SocketChannel,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        _: xev.WriteBuffer,
        r: xev.TCP.WriteError!usize,
    ) xev.CallbackAction {
        std.debug.print("write callback\n", .{});
        _ = l;
        _ = s;
        _ = r catch |err| {
            std.debug.print("Error writing: {}\n", .{err});
        };

        // We do nothing for write, just put back objects into the pool.
        const self = self_.?;
        self.transport.completion_pool.destroy(c);
        // self.transport.buffer_pool.destroy(
        //     @alignCast(
        //         @as(*[4096]u8, @ptrFromInt(@intFromPtr(buf.slice.ptr))),
        //     ),
        // );
        return .disarm;
    }

    fn shutdownCallback(
        self_: ?*SocketChannel,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.TCP.ShutdownError!void,
    ) xev.CallbackAction {
        std.debug.print("shutdown callback\n", .{});
        _ = r catch |err| {
            std.debug.print("Error shutting down: {}\n", .{err});
        };

        const self = self_.?;
        s.close(l, c, SocketChannel, self, closeCallback);
        return .disarm;
    }

    fn closeCallback(
        self_: ?*SocketChannel,
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

        self.transport.completion_pool.destroy(c);
        return .disarm;
    }
};

pub const ChannelHandler = struct {
    inbound_channel_read: ?*const fn (*SocketChannel, []const u8) void = null,
    outbound_channel_read: ?*const fn (*SocketChannel, []const u8) void = null,
    io_error: ?*const fn (*SocketChannel, anyerror) void = null,
};

pub const Options = struct {
    backlog: u31,
    inbound_channel_options: struct {
        is_auto_read: bool = true,
    },
    outbound_channel_options: struct {
        is_auto_read: bool = true,
    },
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
            channel: *SocketChannel,
        },
        read: struct {
            channel: *SocketChannel,
        },
    },
};

pub const XevTransport = struct {
    loop: xev.Loop,
    buffer_pool: BufferPool,
    completion_pool: CompletionPool,
    socket_pool: TCPPool,
    socket_channel_pool: SocketChannelPool,
    threadPool: *xev.ThreadPool,
    socket_channel_manager: SocketChannelManager,
    options: Options,
    stop_notifier: xev.Async,
    async_io_notifier: xev.Async,
    async_task_queue: Queue,
    handler: ChannelHandler,
    loop_thread_id: ?std.Thread.Id,
    allocator: Allocator,

    pub fn init(allocator: Allocator, opts: Options, handler: ChannelHandler) !XevTransport {
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
            .socket_channel_pool = SocketChannelPool.init(allocator),
            .loop = server_loop,
            .threadPool = thread_pool,
            .socket_channel_manager = SocketChannelManager.init(allocator),
            .options = opts,
            .stop_notifier = shutdown_notifier,
            .async_io_notifier = async_io_notifier,
            .async_task_queue = q,
            .handler = handler,
            .loop_thread_id = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *XevTransport) void {
        self.socket_channel_manager.deinit();
        self.buffer_pool.deinit();
        self.completion_pool.deinit();
        self.socket_pool.deinit();
        self.socket_channel_pool.deinit();
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

        // self.listeners.mutex.lock();
        // const key = try formatAddress(addr, self.allocator);
        // defer self.allocator.free(key);
        // self.listeners.m.put(key, server) catch |err| {
        //     std.debug.print("Error adding server to map: {}\n", .{err});
        // };
        // self.listeners.mutex.unlock();
    }

    pub fn serve(self: *XevTransport) !void {
        var c_stop: xev.Completion = undefined;
        self.stop_notifier.wait(&self.loop, &c_stop, void, null, &stopCallback);
        var c_async: xev.Completion = undefined;
        self.async_io_notifier.wait(&self.loop, &c_async, XevTransport, self, &asyncIOCallback);
        self.loop_thread_id = std.Thread.getCurrentId();
        try self.loop.run(.until_done);
    }

    pub fn stop(self: *XevTransport) void {
        self.stop_notifier.notify() catch |err| {
            std.debug.print("Error notifying stop: {}\n", .{err});
        };
    }

    pub fn isInLoopThread(self: *XevTransport) bool {
        return self.loop_thread_id == std.Thread.getCurrentId();
    }

    fn asyncIOCallback(
        self_: ?*XevTransport,
        _: *xev.Loop,
        _: *xev.Completion,
        r: xev.Async.WaitError!void,
    ) xev.CallbackAction {
        _ = r catch unreachable;
        const self = self_.?;

        while (self.async_task_queue.pop()) |elem| {
            switch (elem.op) {
                .connect => {
                    self.dial(elem.op.connect.address) catch |err| {
                        std.debug.print("Error dialing: {}\n", .{err});
                    };
                },
                .write => {
                    const c = self.completion_pool.create() catch unreachable;
                    elem.op.write.channel.socket.write(&self.loop, c, .{ .slice = elem.op.write.buffer }, SocketChannel, elem.op.write.channel, SocketChannel.writeCallback);
                },
                .read => {},
            }
        }

        return .rearm;
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
        _: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.TCP.ConnectError!void,
    ) xev.CallbackAction {
        _ = r catch |err| {
            // TODO: Handle timeout error and retry
            std.debug.print("Error connecting: {}\n", .{err});
            return .disarm;
        };

        const self = self_.?;
        const s = self.socket_pool.create() catch unreachable;
        s.* = socket;

        const channel = self.socket_channel_pool.create() catch unreachable;
        channel.init(s, self, true, self.options.outbound_channel_options.is_auto_read);
        self.socket_channel_manager.sockets.mutex.lock();
        self.socket_channel_manager.sockets.l.append(channel.*) catch unreachable;
        self.socket_channel_manager.sockets.mutex.unlock();

        if (self.options.outbound_channel_options.is_auto_read) {
            channel.read();
        }
        self.completion_pool.destroy(c);
        return .disarm;
    }

    fn acceptCallback(
        self_: ?*XevTransport,
        _: *xev.Loop,
        c: *xev.Completion,
        r: xev.TCP.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        _ = r catch |err| {
            // TODO: Check why this is happening and determine if we should retry
            std.debug.print("Error accepting: {}\n", .{err});
            return .rearm;
        };

        std.debug.print("accept callback\n", .{});
        const self = self_.?;

        // Create our socket
        const socket = self.socket_pool.create() catch unreachable;
        socket.* = r catch unreachable;

        // Create our channel
        const channel = self.socket_channel_pool.create() catch unreachable;
        channel.init(socket, self, false, self.options.inbound_channel_options.is_auto_read);
        self.socket_channel_manager.sockets.mutex.lock();
        self.socket_channel_manager.sockets.l.append(channel.*) catch unreachable;
        self.socket_channel_manager.sockets.mutex.unlock();

        if (self.options.inbound_channel_options.is_auto_read) {
            channel.read();
        }

        self.completion_pool.destroy(c);
        return .rearm;
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
    const opts = Options{
        .backlog = 128,
        .inbound_channel_options = .{ .is_auto_read = true },
        .outbound_channel_options = .{ .is_auto_read = true },
    };
    const handler = ChannelHandler{
        .inbound_channel_read = null,
        .outbound_channel_read = null,
    };
    var peer1 = try XevTransport.init(allocator, opts, handler);
    defer peer1.deinit();

    const addr = try std.net.Address.parseIp("127.0.0.1", 8080);
    try peer1.listen(addr);

    const server_thr = try std.Thread.spawn(.{}, XevTransport.serve, .{&peer1});

    // sleep for 1 second
    std.time.sleep(3_000_000_000);
    peer1.stop();
    server_thr.join();
    std.debug.print("Server stopped\n", .{});
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
