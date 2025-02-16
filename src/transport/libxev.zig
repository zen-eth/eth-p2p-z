const std = @import("std");
const xev = @import("xev");
const Intrusive = @import("../utils/queue_mpsc.zig").Intrusive;
const Queue = Intrusive(AsyncIOQueueNode);
const TCP = xev.TCP;
const Allocator = std.mem.Allocator;
const ThreadPool = xev.ThreadPool;
const Future = @import("../utils/future.zig").Future;

/// Memory pools for things that need stable pointers
const BufferPool = std.heap.MemoryPool([4096]u8);
const CompletionPool = std.heap.MemoryPool(xev.Completion);
const TCPPool = std.heap.MemoryPool(xev.TCP);
const ChannelPool = std.heap.MemoryPool(SocketChannel);

/// SocketChannelManager keeps track of all the inbound and outbound socket channels .
/// It also keeps track of all the listening sockets.
pub const SocketChannelManager = struct {
    listeners: struct {
        mutex: std.Thread.Mutex,
        m: std.StringHashMap(*TCP),
    },
    sockets: struct {
        mutex: std.Thread.Mutex,
        l: std.ArrayList(*SocketChannel),
    },
    socket_pool: TCPPool,
    channel_pool: ChannelPool,
    allocator: Allocator,

    pub fn init(allocator: Allocator) SocketChannelManager {
        return SocketChannelManager{
            .listeners = .{
                .mutex = .{},
                .m = std.StringHashMap(*TCP).init(allocator),
            },
            .sockets = .{
                .mutex = .{},
                .l = std.ArrayList(*SocketChannel).init(allocator),
            },
            .socket_pool = TCPPool.init(allocator),
            .channel_pool = ChannelPool.init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SocketChannelManager) void {
        self.sockets.mutex.lock();
        for (self.sockets.l.items) |socket_channel| {
            socket_channel.deinit();
            self.channel_pool.destroy(socket_channel);
        }
        self.sockets.l.deinit();
        self.sockets.mutex.unlock();
        self.listeners.mutex.lock();
        var iter = self.listeners.m.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.listeners.m.deinit();
        self.listeners.mutex.unlock();
        self.socket_pool.deinit();
        self.channel_pool.deinit();
    }
};

/// SocketChannel represents a socket channel. It is used to send and receive messages.
pub const SocketChannel = struct {
    socket: *TCP,
    transport: *XevTransport,
    read_buf: []u8,
    is_auto_read: bool,
    is_initiator: bool,

    pub fn init(self: *SocketChannel, socket: *TCP, transport: *XevTransport, is_initiator: bool, is_auto_read: bool) void {
        self.socket = socket;
        self.transport = transport;
        self.is_auto_read = is_auto_read;
        self.is_initiator = is_initiator;
        self.read_buf = transport.buffer_pool.create() catch unreachable;
    }

    pub fn set_auto_read(self: *SocketChannel, is_auto_read: bool) void {
        self.is_auto_read = is_auto_read;
    }

    pub fn deinit(self: *SocketChannel) void {
        self.transport.destroyBuf(self.read_buf);
        self.transport.destroySocket(self.socket);
    }

    // pub fn write(self: *SocketChannel, buf: []const u8) void {
    //     if (self.transport.isInLoopThread()) {
    //         const c = self.transport.completion_pool.create() catch unreachable;
    //         self.socket.write(&self.transport.loop, c, .{ .slice = buf }, SocketChannel, self, writeCallback);
    //     } else {
    //         const element = self.transport.allocator.create(AsyncIOQueueElem) catch unreachable;
    //         element.* = AsyncIOQueueElem{
    //             .next = null,
    //             .op = .{ .write = .{ .channel = self, .buffer = buf } },
    //         };
    //         self.transport.async_task_queue.push(element);
    //         self.transport.async_io_notifier.notify() catch |err| {
    //             std.debug.print("Error notifying async io: {}\n", .{err});
    //         };
    //     }
    // }
    //
    // pub fn read(self: *SocketChannel) void {
    //     if (self.transport.isInLoopThread()) {
    //         const c = self.transport.completion_pool.create() catch unreachable;
    //
    //         if (self.is_initiator) {
    //             self.socket.read(&self.transport.loop, c, .{ .slice = self.read_buf[0..] }, SocketChannel, self, outboundChannelReadCallback);
    //         } else {
    //             self.socket.read(&self.transport.loop, c, .{ .slice = self.read_buf[0..] }, SocketChannel, self, inboundChannelReadCallback);
    //         }
    //     } else {
    //         const element = self.transport.allocator.create(AsyncIOQueueElem) catch unreachable;
    //         element.* = AsyncIOQueueElem{
    //             .next = null,
    //             .op = .{ .read = .{ .channel = self } },
    //         };
    //         self.transport.async_task_queue.push(element);
    //         self.transport.async_io_notifier.notify() catch |err| {
    //             std.debug.print("Error notifying async io: {}\n", .{err});
    //         };
    //     }
    // }

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
                socket.shutdown(loop, c, SocketChannel, self, shutdownCallback);
                return .disarm;
            },

            else => {
                if (self.transport.handler.io_error) |cb| {
                    cb(self, err);
                }
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

        self.deinit();
        //TODO: need remove it from socket channel list
        self.transport.completion_pool.destroy(c);
        return .disarm;
    }
};

/// ChannelHandler is used to implement the protocol specific behavior of a channel.
pub const ChannelHandler = struct {
    inbound_channel_read: ?*const fn (*SocketChannel, []const u8) void = null,
    outbound_channel_read: ?*const fn (*SocketChannel, []const u8) void = null,
    io_error: ?*const fn (*SocketChannel, anyerror) void = null,
};

/// Options for the transport.
pub const Options = struct {
    backlog: u31,
    inbound_channel_options: struct {
        is_auto_read: bool = true,
    },
    outbound_channel_options: struct {
        is_auto_read: bool = true,
    },
};

/// AsyncIOQueueNode is used to store the operation to be performed on the transport.
pub const AsyncIOQueueNode = struct {
    const Self = @This();
    next: ?*Self = null,
    op: union(enum) {
        connect: struct {
            address: std.net.Address,
            channel_future: *Future(*SocketChannel),
        },
        // write: struct {
        //     buffer: []const u8,
        //     channel: *SocketChannel,
        // },
        // read: struct {
        //     channel: *SocketChannel,
        // },
    },
};

const ConnectCallbackData = struct {
    transport: *XevTransport,
    channel_future: *Future(*SocketChannel),
};

pub const XevTransport = struct {
    loop: xev.Loop,
    buffer_pool: BufferPool,
    completion_pool: CompletionPool,
    threadPool: *xev.ThreadPool,
    socket_channel_manager: SocketChannelManager,
    options: Options,
    stop_notifier: xev.Async,
    async_io_notifier: xev.Async,
    async_task_queue: *Queue,
    handler: ChannelHandler,
    mutex: std.Thread.Mutex,
    c_accept: *xev.Completion,
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
        var q = try allocator.create(Queue);
        q.init();
        return XevTransport{
            .mutex = .{},
            .c_accept = try allocator.create(xev.Completion),
            .buffer_pool = BufferPool.init(allocator),
            .completion_pool = CompletionPool.init(allocator),
            .loop = server_loop,
            .threadPool = thread_pool,
            .socket_channel_manager = SocketChannelManager.init(allocator),
            .options = opts,
            .stop_notifier = shutdown_notifier,
            .async_io_notifier = async_io_notifier,
            .async_task_queue = q,
            .handler = handler,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *XevTransport) void {
        self.stop_notifier.deinit();
        self.async_io_notifier.deinit();
        self.loop.deinit();
        self.threadPool.shutdown();
        self.threadPool.deinit();
        self.allocator.destroy(self.threadPool);
        self.allocator.destroy(self.c_accept);
    }

    pub fn dial(self: *XevTransport, addr: std.net.Address, channel_future: *Future(*SocketChannel)) !void {
        const node = try self.allocator.create(AsyncIOQueueNode);
        node.* = AsyncIOQueueNode{
            .next = null,
            .op = .{
                .connect = .{
                    .address = addr,
                    .channel_future = channel_future,
                },
            },
        };

        self.async_task_queue.push(node);

        self.async_io_notifier.notify() catch |err| {
            std.debug.print("Error notifying async io: {}\n", .{err});
        };
    }

    pub fn listen(self: *XevTransport, addr: std.net.Address) !void {
        const server = try self.allocator.create(TCP);
        server.* = try TCP.init(addr);
        try server.bind(addr);
        try server.listen(self.options.backlog);

        server.accept(&self.loop, self.c_accept, XevTransport, self, acceptCallback);

        self.socket_channel_manager.listeners.mutex.lock();
        const key = try formatAddress(addr, self.allocator);
        // defer self.allocator.free(key);
        self.socket_channel_manager.listeners.m.put(key, server) catch |err| {
            std.debug.print("Error adding server to map: {}\n", .{err});
        };
        self.socket_channel_manager.listeners.mutex.unlock();
    }

    pub fn serve(self: *XevTransport, addr: std.net.Address) !void {
        const server = try self.allocator.create(TCP);
        server.* = try TCP.init(addr);
        try server.bind(addr);
        try server.listen(self.options.backlog);

        server.accept(&self.loop, self.c_accept, XevTransport, self, acceptCallback);

        self.socket_channel_manager.listeners.mutex.lock();
        const key = try formatAddress(addr, self.allocator);
        // defer self.allocator.free(key);
        self.socket_channel_manager.listeners.m.put(key, server) catch |err| {
            std.debug.print("Error adding server to map: {}\n", .{err});
        };
        self.socket_channel_manager.listeners.mutex.unlock();

        const c_stop = try self.completion_pool.create();
        self.stop_notifier.wait(&self.loop, c_stop, XevTransport, self, &stopCallback);
        const c_async = try self.completion_pool.create();
        self.async_io_notifier.wait(&self.loop, c_async, XevTransport, self, &asyncIOCallback);
        try self.loop.run(.until_done);
    }

    pub fn stop(self: *XevTransport) void {
        self.stop_notifier.notify() catch |err| {
            std.debug.print("Error notifying stop: {}\n", .{err});
        };
    }

    fn asyncIOCallback(
        self_: ?*XevTransport,
        loop: *xev.Loop,
        _: *xev.Completion,
        r: xev.Async.WaitError!void,
    ) xev.CallbackAction {
        _ = r catch unreachable;
        const self = self_.?;

        while (self.async_task_queue.pop()) |node| {
            switch (node.op) {
                .connect => |*conn| {
                    // std.debug.print("Connect address{}\n", .{conn.*});
                    const address = conn.address;
                    var socket = TCP.init(address) catch unreachable;
                    const channel_future = conn.channel_future;
                    // channel_future.* = Future(*SocketChannel).init();

                    const connect_cb_data = self.allocator.create(ConnectCallbackData) catch unreachable;
                    connect_cb_data.* = ConnectCallbackData{
                        .transport = self,
                        .channel_future = channel_future,
                    };

                    const c = self.allocator.create(xev.Completion) catch unreachable;
                    socket.connect(loop, c, address, ConnectCallbackData, connect_cb_data, connectCallback);
                },
                // .write => |*w| {
                //     const c = self.completion_pool.create() catch unreachable;
                //     const channel = w.channel;
                //     const buffer = w.buffer;
                //
                //     // Move data out of elem before the write call
                //     // self.queue_element_pool.destroy(elem);
                //
                //     channel.socket.write(&self.loop, c, .{ .slice = buffer }, SocketChannel, channel, SocketChannel.writeCallback);
                // },
                // .read => |*read| {
                //     const c = self.completion_pool.create() catch unreachable;
                //     const channel = read.channel;
                //
                //     // Move data out of elem before the read call
                //     // self.queue_element_pool.destroy(elem);
                //
                //     if (channel.is_initiator) {
                //         channel.socket.read(&self.loop, c, .{ .slice = channel.read_buf[0..] }, SocketChannel, channel, SocketChannel.outboundChannelReadCallback);
                //     } else {
                //         channel.socket.read(&self.loop, c, .{ .slice = channel.read_buf[0..] }, SocketChannel, channel, SocketChannel.inboundChannelReadCallback);
                //     }
                // },
            }

            self.allocator.destroy(node);
        }

        return .rearm;
    }

    fn stopCallback(
        self_: ?*XevTransport,
        loop: *xev.Loop,
        c: *xev.Completion,
        r: xev.Async.WaitError!void,
    ) xev.CallbackAction {
        _ = r catch unreachable;
        const self = self_.?;

        loop.stop();
        self.completion_pool.destroy(c);

        self.socket_channel_manager.deinit();
        self.buffer_pool.deinit();
        self.completion_pool.deinit();
        return .disarm;
    }

    fn connectCallback(
        self_: ?*ConnectCallbackData,
        _: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.TCP.ConnectError!void,
    ) xev.CallbackAction {
        const self = self_.?;
        defer self.transport.allocator.destroy(c);
        _ = r catch |err| {
            // TODO: Handle timeout error and retry
            std.debug.print("Error connecting: {}\n", .{err});
            self.channel_future.completeError(err);
            return .disarm;
        };

        const s = self.transport.socket_channel_manager.socket_pool.create() catch unreachable;
        s.* = socket;
        const channel = self.transport.socket_channel_manager.channel_pool.create() catch unreachable;
        channel.init(s, self.transport, true, self.transport.options.outbound_channel_options.is_auto_read);
        self.transport.socket_channel_manager.sockets.mutex.lock();
        self.transport.socket_channel_manager.sockets.l.append(channel) catch unreachable;
        self.transport.socket_channel_manager.sockets.mutex.unlock();
        self.channel_future.complete(channel);

        if (self.transport.options.outbound_channel_options.is_auto_read) {
            // channel.read();
        }

        return .disarm;
    }

    fn acceptCallback(
        self_: ?*XevTransport,
        _: *xev.Loop,
        _: *xev.Completion,
        r: xev.TCP.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const self = self_.?;
        _ = r catch |err| {
            // TODO: Check why this is happening and determine if we should retry
            std.debug.print("Error accepting: {}\n", .{err});
            return .rearm;
        };

        const socket = self.socket_channel_manager.socket_pool.create() catch unreachable;
        socket.* = r catch unreachable;
        const channel = self.socket_channel_manager.channel_pool.create() catch unreachable;
        channel.init(socket, self, false, self.options.inbound_channel_options.is_auto_read);
        self.socket_channel_manager.sockets.mutex.lock();
        self.socket_channel_manager.sockets.l.append(channel) catch unreachable;
        self.socket_channel_manager.sockets.mutex.unlock();

        if (self.options.inbound_channel_options.is_auto_read) {
            // channel.read();
        }

        return .rearm;
    }

    fn destroyBuf(self: *XevTransport, buf: []const u8) void {
        self.buffer_pool.destroy(
            @alignCast(
                @as(*[4096]u8, @ptrFromInt(@intFromPtr(buf.ptr))),
            ),
        );
    }

    fn destroySocket(self: *XevTransport, socket: *xev.TCP) void {
        self.socket_channel_manager.socket_pool.destroy(
            @alignCast(socket),
        );
    }

    pub fn formatAddress(addr: std.net.Address, allocator: Allocator) ![]const u8 {
        const addr_str = try std.fmt.allocPrint(allocator, "{}", .{addr});
        return addr_str;
    }
};

test "echo client and server with multiple clients" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const opts = Options{
        .backlog = 128,
        .inbound_channel_options = .{ .is_auto_read = true },
        .outbound_channel_options = .{ .is_auto_read = true },
    };
    const handler = ChannelHandler{
        // .inbound_channel_read = struct {
        //     fn callback(channel: *SocketChannel, buf: []const u8) void {
        //         channel.write(buf); // Remove the catch since write() returns void
        //     }
        // }.callback,
        .inbound_channel_read = null,
        .outbound_channel_read = null,
    };
    var server = try XevTransport.init(allocator, opts, handler);
    defer server.deinit();

    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);

    var client = try XevTransport.init(allocator, opts, handler);
    defer client.deinit();

    const client_addr = try std.net.Address.parseIp("0.0.0.0", 8082);

    var client1 = try XevTransport.init(allocator, opts, handler);
    defer client1.deinit();

    const client_addr1 = try std.net.Address.parseIp("0.0.0.0", 8083);

    const server_thr = try std.Thread.spawn(.{}, XevTransport.serve, .{ &server, addr });
    const client_thr = try std.Thread.spawn(.{}, XevTransport.serve, .{ &client, client_addr });
    const client_thr1 = try std.Thread.spawn(.{}, XevTransport.serve, .{ &client1, client_addr1 });

    const server_addr = try std.net.Address.parseIp("127.0.0.1", 8081);

    var channel_future = Future(*SocketChannel).init();
    try client.dial(server_addr, &channel_future);

    var channel_future1 = Future(*SocketChannel).init();
    try client1.dial(server_addr, &channel_future1);

    _ = try channel_future.wait();
    _ = try channel_future1.wait();

    std.time.sleep(100_000_000);
    server.socket_channel_manager.sockets.mutex.lock();
    try std.testing.expectEqual(2, server.socket_channel_manager.sockets.l.items.len);
    server.socket_channel_manager.sockets.mutex.unlock();
    server.socket_channel_manager.listeners.mutex.lock();
    try std.testing.expectEqual(1, server.socket_channel_manager.listeners.m.count());
    server.socket_channel_manager.listeners.mutex.unlock();
    client.socket_channel_manager.sockets.mutex.lock();
    try std.testing.expectEqual(1, client.socket_channel_manager.sockets.l.items.len);
    client.socket_channel_manager.sockets.mutex.unlock();
    client.socket_channel_manager.listeners.mutex.lock();
    try std.testing.expectEqual(1, client.socket_channel_manager.listeners.m.count());
    client.socket_channel_manager.listeners.mutex.unlock();
    server.stop();
    client.stop();
    client1.stop();
    server_thr.join();
    client_thr.join();
    client_thr1.join();
}
