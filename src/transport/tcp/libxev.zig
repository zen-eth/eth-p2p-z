const std = @import("std");
const xev = @import("xev");
const Intrusive = @import("../../utils/queue_mpsc.zig").Intrusive;
const IOQueue = Intrusive(AsyncIOQueueNode);
const TCP = xev.TCP;
const Allocator = std.mem.Allocator;
const ThreadPool = xev.ThreadPool;
const ResetEvent = std.Thread.ResetEvent;

/// Memory pools for things that need stable pointers
const CompletionPool = std.heap.MemoryPool(xev.Completion);
const TCPPool = std.heap.MemoryPool(xev.TCP);

/// SocketChannel represents a socket channel. It is used to send and receive messages.
pub const SocketChannel = struct {
    socket: TCP,
    transport: *XevTransport,
    is_initiator: bool,

    pub fn init(self: *SocketChannel, socket: TCP, transport: *XevTransport, is_initiator: bool) void {
        self.socket = socket;
        self.transport = transport;
        self.is_initiator = is_initiator;
    }

    pub fn deinit(_: *SocketChannel) void {
        // self.transport.destroySocket(self.socket);
    }

    // pub fn write(self: *SocketChannel, buf: []const u8) void {
    //     if (self.transport.isInLoopThread()) {
    //         const c = self.transport.allocator.create(xev.Completion) catch unreachable;
    //         self.socket.write(&self.transport.loop, c, .{ .slice = buf }, SocketChannel, self, writeCallback);
    //     } else {
    //         const node = self.transport.allocator.create(AsyncIOQueueNode) catch unreachable;
    //         node.* = AsyncIOQueueNode{
    //             .next = null,
    //             .op = .{ .write = .{ .channel = self, .buffer = buf } },
    //         };
    //         self.transport.async_task_queue.push(node);
    //         self.transport.async_io_notifier.notify() catch |err| {
    //             std.debug.print("Error notifying async io: {}\n", .{err});
    //         };
    //     }
    // }
    //
    // pub fn read(self: *SocketChannel, buf: []u8) void {
    //     if (self.transport.isInLoopThread()) {
    //         std.debug.print("read called from loop thread\n", .{});
    //         const c = self.transport.allocator.create(xev.Completion) catch unreachable;
    //         if (self.is_initiator) {
    //             self.socket.read(&self.transport.loop, c, .{ .slice = buf }, SocketChannel, self, SocketChannel.outboundChannelReadCallback);
    //         } else {
    //             self.socket.read(&self.transport.loop, c, .{ .slice = buf }, SocketChannel, self, SocketChannel.inboundChannelReadCallback);
    //         }
    //         return;
    //     } else {
    //         const node = self.transport.allocator.create(AsyncIOQueueNode) catch unreachable;
    //         node.* = AsyncIOQueueNode{
    //             .next = null,
    //             .op = .{ .read = .{ .channel = self } },
    //         };
    //
    //         self.transport.async_task_queue.push(node);
    //
    //         self.transport.async_io_notifier.notify() catch |err| {
    //             std.debug.print("Error notifying async io: {}\n", .{err});
    //         };
    //     }
    // }
    //
    // fn outboundChannelReadCallback(
    //     self_: ?*SocketChannel,
    //     loop: *xev.Loop,
    //     c: *xev.Completion,
    //     socket: xev.TCP,
    //     buf: xev.ReadBuffer,
    //     r: xev.TCP.ReadError!usize,
    // ) xev.CallbackAction {
    //     const self = self_.?;
    //     const n = r catch |err| switch (err) {
    //         error.EOF => {
    //             const c_shutdown = self.transport.completion_pool.create() catch unreachable;
    //             socket.shutdown(loop, c_shutdown, SocketChannel, self, shutdownCallback);
    //             return .disarm;
    //         },
    //
    //         else => {
    //             if (self.transport.handler.io_error) |cb| {
    //                 cb(self, err);
    //             }
    //             const c_shutdown = self.transport.completion_pool.create() catch unreachable;
    //             socket.shutdown(loop, c_shutdown, SocketChannel, self, shutdownCallback);
    //             std.log.warn("server read unexpected err={}", .{err});
    //             return .disarm;
    //         },
    //     };
    //
    //     if (self.is_auto_read) {
    //         return .rearm;
    //     } else {
    //         self.transport.allocator.destroy(c);
    //         return .disarm;
    //     }
    // }
    //
    // fn inboundChannelReadCallback(
    //     self_: ?*SocketChannel,
    //     loop: *xev.Loop,
    //     c: *xev.Completion,
    //     socket: xev.TCP,
    //     buf: xev.ReadBuffer,
    //     r: xev.TCP.ReadError!usize,
    // ) xev.CallbackAction {
    //     const self = self_.?;
    //     const n = r catch |err| switch (err) {
    //         error.EOF => {
    //             const c_shutdown = self.transport.completion_pool.create() catch unreachable;
    //             socket.shutdown(loop, c_shutdown, SocketChannel, self, shutdownCallback);
    //             return .disarm;
    //         },
    //
    //         else => {
    //             const c_shutdown = self.transport.completion_pool.create() catch unreachable;
    //             socket.shutdown(loop, c_shutdown, SocketChannel, self, shutdownCallback);
    //             std.log.warn("server read unexpected err={}", .{err});
    //             return .disarm;
    //         },
    //     };
    //
    //     if (self.transport.handler.inbound_channel_read) |cb| {
    //         cb(self, buf.slice[0..n]);
    //     }
    //
    //     if (self.is_auto_read) {
    //         return .rearm;
    //     } else {
    //         self.transport.allocator.destroy(c);
    //         return .disarm;
    //     }
    // }
    //
    // fn writeCallback(
    //     self_: ?*SocketChannel,
    //     l: *xev.Loop,
    //     c: *xev.Completion,
    //     s: xev.TCP,
    //     _: xev.WriteBuffer,
    //     r: xev.TCP.WriteError!usize,
    // ) xev.CallbackAction {
    //     std.debug.print("write callback\n", .{});
    //     _ = l;
    //     _ = s;
    //     _ = r catch |err| {
    //         std.debug.print("Error writing: {}\n", .{err});
    //     };
    //
    //     // We do nothing for write, just put back objects into the pool.
    //     const self = self_.?;
    //     self.transport.allocator.destroy(c);
    //     return .disarm;
    // }

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
        // self.transport.socket_channel_manager.sockets.mutex.lock();
        // for (self.transport.socket_channel_manager.sockets.l.items, 0..) |s, i| {
        //     if (s == self) {
        //         _ = self.transport.socket_channel_manager.sockets.l.swapRemove(i);
        //         self.transport.socket_channel_manager.channel_pool.destroy(s);
        //         break;
        //     }
        // }
        // self.transport.socket_channel_manager.sockets.mutex.unlock();
        // self.transport.channel_pool.destroy(self);
        self.transport.completion_pool.destroy(c);
        return .disarm;
    }
};

/// Options for the transport.
pub const Options = struct {
    backlog: u31,
};

/// AsyncIOQueueNode is used to store the operation to be performed on the transport.
pub const AsyncIOQueueNode = struct {
    const Self = @This();
    next: ?*Self = null,
    op: union(enum) {
        connect: struct {
            address: std.net.Address,
            channel: *SocketChannel,
            reset_event: *ResetEvent,
            err: *?anyerror,
        },
        accept: struct {
            server: *TCP,
            channel: *SocketChannel,
            reset_event: *ResetEvent,
            err: *?anyerror,
        },
        // write: struct {
        //     buffer: []const u8,
        //     channel: *SocketChannel,
        // },
        // read: struct {
        //     channel: *SocketChannel,
        //     buffer: []u8,
        // },
    },
};

const OpenChannelCallbackData = struct {
    transport: *XevTransport,
    channel: *SocketChannel,
    err: *?anyerror,
};

pub const Listener = struct {
    address: std.net.Address,
    server: *TCP,
    transport: *XevTransport,

    pub fn init(self: *Listener, address: std.net.Address, backlog: u31, transport: *XevTransport) !void {
        const server = try transport.allocator.create(TCP);
        server.* = try TCP.init(address);
        try server.bind(address);
        try server.listen(backlog);

        self.address = address;
        self.server = server;
        self.transport = transport;
    }

    pub fn deinit(self: *Listener) void {
        self.transport.allocator.destroy(self.server);
    }

    // pub fn accept(self: *Listener, channel: *SocketChannel) !void {
    //     const reset_event = try self.transport.allocator.create(ResetEvent);
    //     reset_event.* = ResetEvent{};
    //     const accept_err = try self.transport.allocator.create(?anyerror);
    //     accept_err.* = null;
    //     const node = self.transport.allocator.create(AsyncIOQueueNode) catch unreachable;
    //     node.* = AsyncIOQueueNode{
    //         .next = null,
    //         .op = .{ .accept = .{
    //             .server = self.server,
    //             .channel = channel,
    //             .reset_event = reset_event,
    //             .err = accept_err,
    //         } },
    //     };
    //     self.transport.async_task_queue.push(node);
    //     self.transport.async_io_notifier.notify() catch |err| {
    //         std.debug.print("Error notifying async io: {}\n", .{err});
    //     };
    //
    //     reset_event.wait();
    //     if (accept_err.*) |err| {
    //         return err;
    //     }
    // }
};

pub const XevTransport = struct {
    loop: xev.Loop,
    thread_pool: *xev.ThreadPool,
    options: Options,
    allocator: Allocator,

    pub fn init(allocator: Allocator, opts: Options) !XevTransport {
        const thread_pool = try allocator.create(ThreadPool);
        thread_pool.* = ThreadPool.init(.{});
        const loop_opts = xev.Options{
            .thread_pool = thread_pool,
        };
        const loop = try xev.Loop.init(loop_opts);
        return XevTransport{
            .loop = loop,
            .thread_pool = thread_pool,
            .options = opts,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *XevTransport) void {
        self.loop.deinit();
        self.thread_pool.shutdown();
        self.thread_pool.deinit();
        self.allocator.destroy(self.thread_pool);
    }

    pub fn dial(self: *XevTransport, addr: std.net.Address, channel: *SocketChannel) !void {
        var socket = TCP.init(addr) catch unreachable;

        var err: ?anyerror = null;
        var connect_cb_data = OpenChannelCallbackData{
            .transport = self,
            .channel = channel,
            .err = &err,
        };

        var c: xev.Completion = undefined;
        socket.connect(&self.loop, &c, addr, OpenChannelCallbackData, &connect_cb_data, connectCallback);
        try self.loop.run(.once);
        if (err) |e| {
            return e;
        }
        // const reset_event = try self.allocator.create(std.Thread.ResetEvent);
        // reset_event.* = ResetEvent{};
        // const connect_error = try self.allocator.create(?anyerror);
        // connect_error.* = null;
        // const node = try self.allocator.create(AsyncIOQueueNode);
        // node.* = AsyncIOQueueNode{
        //     .next = null,
        //     .op = .{
        //         .connect = .{
        //             .address = addr,
        //             .channel = channel,
        //             .err = connect_error,
        //             .reset_event = reset_event,
        //         },
        //     },
        // };
        //
        // self.async_task_queue.push(node);
        //
        // self.async_io_notifier.notify() catch |err| {
        //     std.debug.print("Error notifying async io: {}\n", .{err});
        // };
        //
        // reset_event.wait();
        // if (connect_error.*) |err| {
        //     return err;
        // }
    }

    pub fn listen(self: *XevTransport, addr: std.net.Address, listener: *Listener) !void {
        const server = try self.allocator.create(TCP);
        server.* = try TCP.init(addr);
        try server.bind(addr);
        try server.listen(self.options.backlog);

        listener.* = Listener{
            .address = addr,
            .server = server,
            .transport = self,
        };

        // self.c_accept = try self.completion_pool.create();
        // server.accept(&self.loop, self.c_accept, XevTransport, self, acceptCallback);

        // self.socket_channel_manager.listeners.mutex.lock();
        // const key = try formatAddress(addr, self.allocator);
        // self.socket_channel_manager.listeners.m.put(key, server) catch |err| {
        //     std.debug.print("Error adding server to map: {}\n", .{err});
        // };
        // self.socket_channel_manager.listeners.mutex.unlock();

        // const c_stop = try self.completion_pool.create();
        // self.stop_notifier.wait(&self.loop, c_stop, XevTransport, self, &stopCallback);
        // self.c_async = try self.completion_pool.create();
        // self.async_io_notifier.wait(&self.loop, self.c_async, XevTransport, self, &asyncIOCallback);
        //
        // self.loop_thread_id = std.Thread.getCurrentId();
        // try self.loop.run(.until_done);
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
                    const address = conn.address;
                    var socket = TCP.init(address) catch unreachable;

                    const connect_cb_data = self.allocator.create(OpenChannelCallbackData) catch unreachable;
                    connect_cb_data.* = OpenChannelCallbackData{
                        .transport = self,
                        .channel = conn.channel,
                        .err = conn.err,
                        .reset_event = conn.reset_event,
                    };

                    const c = self.allocator.create(xev.Completion) catch unreachable;
                    socket.connect(loop, c, address, OpenChannelCallbackData, connect_cb_data, connectCallback);
                },
                .accept => |*accept| {
                    const server = accept.server;
                    const c = self.allocator.create(xev.Completion) catch unreachable;
                    const accept_cb_data = self.allocator.create(OpenChannelCallbackData) catch unreachable;
                    accept_cb_data.* = OpenChannelCallbackData{
                        .transport = self,
                        .channel = accept.channel,
                        .reset_event = accept.reset_event,
                        .err = accept.err,
                    };
                    server.accept(loop, c, OpenChannelCallbackData, accept_cb_data, acceptCallback);
                },
                // .write => |*w| {
                //     const c = self.allocator.create(xev.Completion) catch unreachable;
                //     const channel = w.channel;
                //     const buffer = w.buffer;
                //
                //     channel.socket.write(loop, c, .{ .slice = buffer }, SocketChannel, channel, SocketChannel.writeCallback);
                // },
                // .read => |*read| {
                //     const channel = read.channel;
                //     const c = if (channel.is_auto_read) channel.auto_read_c.? else self.allocator.create(xev.Completion) catch unreachable;
                //
                //     if (channel.is_initiator) {
                //         channel.socket.read(loop, c, .{ .slice = channel.read_buf[0..] }, SocketChannel, channel, SocketChannel.outboundChannelReadCallback);
                //     } else {
                //         channel.socket.read(loop, c, .{ .slice = channel.read_buf[0..] }, SocketChannel, channel, SocketChannel.inboundChannelReadCallback);
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
        _ = r catch |err| {
            std.log.err("Error waiting for stop: {}\n", .{err});
        };
        const self = self_.?;

        loop.stop();
        self.completion_pool.destroy(c);
        self.socket_pool.deinit();
        self.completion_pool.deinit();
        return .disarm;
    }

    fn connectCallback(
        self_: ?*OpenChannelCallbackData,
        _: *xev.Loop,
        _: *xev.Completion,
        socket: xev.TCP,
        r: xev.TCP.ConnectError!void,
    ) xev.CallbackAction {
        const self = self_.?;
        _ = r catch |err| {
            std.debug.print("Error connecting: {}\n", .{err});
            self.err.* = err;
            return .disarm;
        };

        self.channel.init(socket, self.transport, true);

        return .disarm;
    }

    fn acceptCallback(
        self_: ?*OpenChannelCallbackData,
        _: *xev.Loop,
        c: *xev.Completion,
        r: xev.TCP.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const self = self_.?;
        defer self.transport.allocator.destroy(c);
        _ = r catch |err| {
            std.debug.print("Error accepting: {}\n", .{err});
            self.err.* = err;
            self.reset_event.set();
            return .disarm;
        };

        const socket = self.transport.socket_pool.create() catch unreachable;
        socket.* = r catch unreachable;
        self.channel.init(socket, self.transport, false);
        self.reset_event.set();

        return .disarm;
    }

    fn destroyBuf(self: *XevTransport, buf: []const u8) void {
        self.buffer_pool.destroy(
            @alignCast(
                @as(*[4096]u8, @ptrFromInt(@intFromPtr(buf.ptr))),
            ),
        );
    }

    // fn destroySocket(self: *XevTransport, socket: *xev.TCP) void {
    //     self.socket_pool.destroy(
    //         @alignCast(socket),
    //     );
    // }
};

test "dial with error" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const opts = Options{
        .backlog = 128,
    };

    var transport = try XevTransport.init(allocator, opts);
    defer transport.deinit();

    var channel: SocketChannel = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel));
}

test "dial in separate thread with error" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const opts = Options{ .backlog = 128 };
    var transport = try XevTransport.init(allocator, opts);
    defer transport.deinit();

    // Use an invalid port to trigger connection refused
    const addr = try std.net.Address.parseIp("127.0.0.1", 1);
    var channel: SocketChannel = undefined;
    var result: ?anyerror = null;

    const thread = try std.Thread.spawn(.{}, struct {
        fn run(t: *XevTransport, a: std.net.Address, c: *SocketChannel, err: *?anyerror) void {
            t.dial(a, c) catch |e| {
                err.* = e;
            };
        }
    }.run, .{ &transport, addr, &channel, &result });

    thread.join();
    try std.testing.expectEqual(result.?, error.ConnectionRefused);
}

// test "echo client and server with multiple clients" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const opts = Options{
//         .backlog = 128,
//     };
//
//     var server = try XevTransport.init(allocator, opts);
//     defer server.deinit();
//
//     const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
//
//     var client = try XevTransport.init(allocator, opts);
//     defer client.deinit();
//
//     const client_addr = try std.net.Address.parseIp("0.0.0.0", 8082);
//
//     var client1 = try XevTransport.init(allocator, opts);
//     defer client1.deinit();
//
//     const client_addr1 = try std.net.Address.parseIp("0.0.0.0", 8083);
//
//     const server_thr = try std.Thread.spawn(.{}, XevTransport.listen, .{ &server, addr });
//     const client_thr = try std.Thread.spawn(.{}, XevTransport.listen, .{ &client, client_addr });
//     const client_thr1 = try std.Thread.spawn(.{}, XevTransport.listen, .{ &client1, client_addr1 });
//
//     const server_addr = try std.net.Address.parseIp("127.0.0.1", 8081);
//
//     var channel_future = Future(*SocketChannel).init();
//     try client.dial(server_addr, &channel_future);
//
//     var channel_future1 = Future(*SocketChannel).init();
//     try client1.dial(server_addr, &channel_future1);
//
//     const channel = try channel_future.wait();
//
//     const data: [10000]u8 = .{'a'} ** 10000;
//     channel.write(&data);
//     _ = try channel_future1.wait();
//
//     std.time.sleep(100_000_000);
//     // server.socket_channel_manager.sockets.mutex.lock();
//     // try std.testing.expectEqual(2, server.socket_channel_manager.sockets.l.items.len);
//     // server.socket_channel_manager.sockets.mutex.unlock();
//     // server.socket_channel_manager.listeners.mutex.lock();
//     // try std.testing.expectEqual(1, server.socket_channel_manager.listeners.m.count());
//     // server.socket_channel_manager.listeners.mutex.unlock();
//     // client.socket_channel_manager.sockets.mutex.lock();
//     // try std.testing.expectEqual(1, client.socket_channel_manager.sockets.l.items.len);
//     // client.socket_channel_manager.sockets.mutex.unlock();
//     // client.socket_channel_manager.listeners.mutex.lock();
//     // try std.testing.expectEqual(1, client.socket_channel_manager.listeners.m.count());
//     // client.socket_channel_manager.listeners.mutex.unlock();
//     server.stop();
//     client.stop();
//     client1.stop();
//     server_thr.join();
//     client_thr.join();
//     client_thr1.join();
// }
