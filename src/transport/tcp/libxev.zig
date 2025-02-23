const std = @import("std");
const xev = @import("xev").Dynamic;
// const xev = @import("xev");
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

    pub fn deinit(_: *SocketChannel) void {}

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
    server: TCP,
    transport: *XevTransport,

    pub fn init(self: *Listener, address: std.net.Address, backlog: u31, transport: *XevTransport) !void {
        const s = try TCP.init(address);
        try s.bind(address);
        try s.listen(backlog);

        self.address = address;
        self.server = s;
        self.transport = transport;
    }

    pub fn deinit(_: *Listener) void {}

    pub fn accept(self: *Listener, channel: *SocketChannel) !void {
        var err: ?anyerror = null;
        var connect_cb_data = OpenChannelCallbackData{
            .transport = self.transport,
            .channel = channel,
            .err = &err,
        };

        var c: xev.Completion = undefined;
        var l = XevTransport.getLoop();
        self.server.accept(&l, &c, OpenChannelCallbackData, &connect_cb_data, acceptCallback);
        try l.run(.until_done);
        if (err) |e| {
            return e;
        }
    }

    fn acceptCallback(
        self_: ?*OpenChannelCallbackData,
        _: *xev.Loop,
        _: *xev.Completion,
        r: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const self = self_.?;
        const socket = r catch |err| {
            std.debug.print("Error accepting: {}\n", .{err});
            self.err.* = err;
            return .disarm;
        };

        self.channel.init(socket, self.transport, false);

        return .disarm;
    }
};

pub const XevTransport = struct {
    threadlocal var thread_pool: ?xev.ThreadPool = null;
    threadlocal var loop: ?xev.Loop = null;
    options: Options,
    allocator: Allocator,

    fn getThreadPool() xev.ThreadPool {
        return thread_pool orelse {
            const tp = ThreadPool.init(.{});
            thread_pool = tp;
            return tp;
        };
    }

    pub fn getLoop() xev.Loop {
        return loop orelse {
            var tp = getThreadPool();
            const loop_opts = xev.Options{
                .thread_pool = &tp,
            };
            const l = xev.Loop.init(loop_opts) catch unreachable;
            loop = l;
            return l;
        };
    }

    pub fn init(allocator: Allocator, opts: Options) !XevTransport {
        // const thread_pool = try allocator.create(ThreadPool);
        // thread_pool.* = ThreadPool.init(.{});
        // const loop_opts = xev.Options{
        //     .thread_pool = thread_pool,
        // };
        // loop = try xev.Loop.init(loop_opts);
        return XevTransport{
            // .thread_pool = thread_pool,
            .options = opts,
            .allocator = allocator,
        };
    }

    pub fn deinit(_: *XevTransport) void {
        // loop.deinit();
        // self.thread_pool.shutdown();
        // self.thread_pool.deinit();
        // self.allocator.destroy(self.thread_pool);
    }

    pub fn dial(self: *XevTransport, addr: std.net.Address, channel: *SocketChannel) !void {
        const socket=try self.allocator.create(xev.TCP) catch unreachable;
        socket.* = TCP.init(addr) catch unreachable;
        std.debug.print("dialing {*}\n", .{socket});
        var err: ?anyerror = null;
        var connect_cb_data = OpenChannelCallbackData{
            .transport = self,
            .channel = channel,
            .err = &err,
        };

        var c: xev.Completion = undefined;
        var l = getLoop();
        socket.connect(&l, &c, addr, OpenChannelCallbackData, &connect_cb_data, connectCallback);
        try l.run(.until_done);
        if (err) |e| {
            return e;
        }
    }

    pub fn listen(self: *XevTransport, addr: std.net.Address, listener: *Listener) !void {
        try listener.init(addr, self.options.backlog, self);

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

    // fn asyncIOCallback(
    //     self_: ?*XevTransport,
    //     loop: *xev.Loop,
    //     _: *xev.Completion,
    //     r: xev.Async.WaitError!void,
    // ) xev.CallbackAction {
    //     _ = r catch unreachable;
    //     const self = self_.?;
    //
    //     while (self.async_task_queue.pop()) |node| {
    //         switch (node.op) {
    //             .connect => |*conn| {
    //                 const address = conn.address;
    //                 var socket = TCP.init(address) catch unreachable;
    //
    //                 const connect_cb_data = self.allocator.create(OpenChannelCallbackData) catch unreachable;
    //                 connect_cb_data.* = OpenChannelCallbackData{
    //                     .transport = self,
    //                     .channel = conn.channel,
    //                     .err = conn.err,
    //                     .reset_event = conn.reset_event,
    //                 };
    //
    //                 const c = self.allocator.create(xev.Completion) catch unreachable;
    //                 socket.connect(loop, c, address, OpenChannelCallbackData, connect_cb_data, connectCallback);
    //             },
    //             .accept => |*accept| {
    //                 const server = accept.server;
    //                 const c = self.allocator.create(xev.Completion) catch unreachable;
    //                 const accept_cb_data = self.allocator.create(OpenChannelCallbackData) catch unreachable;
    //                 accept_cb_data.* = OpenChannelCallbackData{
    //                     .transport = self,
    //                     .channel = accept.channel,
    //                     .reset_event = accept.reset_event,
    //                     .err = accept.err,
    //                 };
    //                 server.accept(loop, c, OpenChannelCallbackData, accept_cb_data, acceptCallback);
    //             },
    //             // .write => |*w| {
    //             //     const c = self.allocator.create(xev.Completion) catch unreachable;
    //             //     const channel = w.channel;
    //             //     const buffer = w.buffer;
    //             //
    //             //     channel.socket.write(loop, c, .{ .slice = buffer }, SocketChannel, channel, SocketChannel.writeCallback);
    //             // },
    //             // .read => |*read| {
    //             //     const channel = read.channel;
    //             //     const c = if (channel.is_auto_read) channel.auto_read_c.? else self.allocator.create(xev.Completion) catch unreachable;
    //             //
    //             //     if (channel.is_initiator) {
    //             //         channel.socket.read(loop, c, .{ .slice = channel.read_buf[0..] }, SocketChannel, channel, SocketChannel.outboundChannelReadCallback);
    //             //     } else {
    //             //         channel.socket.read(loop, c, .{ .slice = channel.read_buf[0..] }, SocketChannel, channel, SocketChannel.inboundChannelReadCallback);
    //             //     }
    //             // },
    //         }
    //
    //         self.allocator.destroy(node);
    //     }
    //
    //     return .rearm;
    // }

    // fn stopCallback(
    //     self_: ?*XevTransport,
    //     loop: *xev.Loop,
    //     c: *xev.Completion,
    //     r: xev.Async.WaitError!void,
    // ) xev.CallbackAction {
    //     _ = r catch |err| {
    //         std.log.err("Error waiting for stop: {}\n", .{err});
    //     };
    //     const self = self_.?;
    //
    //     loop.stop();
    //     self.completion_pool.destroy(c);
    //     self.socket_pool.deinit();
    //     self.completion_pool.deinit();
    //     return .disarm;
    // }

    fn connectCallback(
        self_: ?*OpenChannelCallbackData,
        _: *xev.Loop,
        _: *xev.Completion,
        socket: xev.TCP,
        r: xev.ConnectError!void,
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
};

test "dial with error" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    // xev.backend = .epoll;
    const opts = Options{
        .backlog = 128,
    };

    var transport = try XevTransport.init(allocator, opts);
    defer transport.deinit();

    var channel: SocketChannel = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8000);
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel));

    var channel1: SocketChannel = undefined;
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel1));
}

// test "dial and accept" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const opts = Options{
//         .backlog = 128,
//     };
//
//     const addr = try std.net.Address.parseIp("127.0.0.1", 8000);
//
//     var server = try XevTransport.init(allocator, opts);
//     defer server.deinit();
//
//     var listener: Listener = undefined;
//     try server.listen(addr, &listener);
//
//     var channel: SocketChannel = undefined;
//     const thread = try std.Thread.spawn(.{}, Listener.accept, .{ &listener, &channel });
//
//     var client = try XevTransport.init(allocator, opts);
//     defer client.deinit();
//
//     var channel1: SocketChannel = undefined;
//     try client.dial(addr, &channel1);
//
//     thread.join();
//
//     try std.testing.expectEqual(false, channel.is_initiator);
//     try std.testing.expectEqual(true, channel1.is_initiator);
// }
//
// test "dial and accept with multiple clients" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//     xev.backend = .epoll;
//     const opts = Options{
//         .backlog = 128,
//     };
//
//     const addr = try std.net.Address.parseIp("127.0.0.1", 7000);
//
//     var server = try XevTransport.init(allocator, opts);
//     defer server.deinit();
//     var listener: Listener = undefined;
//     try server.listen(addr, &listener);
//
//     var channels: [3]SocketChannel = [_]SocketChannel{ undefined, undefined, undefined };
//     const thread = try std.Thread.spawn(.{}, struct {
//         fn run(l: *Listener, chans: []SocketChannel) !void {
//             for (chans) |*chan| {
//                 try l.accept(chan);
//             }
//         }
//     }.run, .{ &listener, &channels });
//
//     var client = try XevTransport.init(allocator, opts);
//     defer client.deinit();
//
//     var client_channels: [3]SocketChannel = [_]SocketChannel{ undefined, undefined, undefined };
//     for (&client_channels) |*chan| {
//         try client.dial(addr, chan);
//     }
//
//     thread.join();
//
//     for (channels) |chan| {
//         try std.testing.expectEqual(false, chan.is_initiator);
//     }
//     for (client_channels) |chan| {
//         try std.testing.expectEqual(true, chan.is_initiator);
//     }
// }

// test "dial in separate thread with error" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     // xev.backend = .epoll;
//     const opts = Options{ .backlog = 128 };
//     var transport = try XevTransport.init(allocator, opts);
//     defer transport.deinit();
//
//     const addr = try std.net.Address.parseIp("127.0.0.1", 1);
//     var channel: SocketChannel = undefined;
//     var result: ?anyerror = null;
//
//     const thread = try std.Thread.spawn(.{}, struct {
//         fn run(t: *XevTransport, a: std.net.Address, c: *SocketChannel, err: *?anyerror) void {
//             t.dial(a, c) catch |e| {
//                 err.* = e;
//             };
//         }
//     }.run, .{ &transport, addr, &channel, &result });
//
//     // Add delay to ensure thread starts
//     std.time.sleep(200 * std.time.ms_per_s);
//
//     const channel1 = allocator.create(SocketChannel) catch unreachable;
//     const addr1 = try std.net.Address.parseIp("0.0.0.0", 8081);
//     try std.testing.expectError(error.ConnectionRefused, transport.dial(addr1, channel1));
//
//     thread.join();
//     try std.testing.expectEqual(result.?, error.ConnectionRefused);
// }

