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
            server: TCP,
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
    reset_event: *ResetEvent,
};

pub const Listener = struct {
    address: std.net.Address,
    server: TCP,
    transport: *XevTransport,

    pub fn init(self: *Listener, address: std.net.Address, backlog: u31, transport: *XevTransport) !void {
        const server = try TCP.init(address);
        try server.bind(address);
        try server.listen(backlog);

        self.address = address;
        self.server = server;
        self.transport = transport;
    }

    pub fn deinit(_: *Listener) void {}

    pub fn accept(self: *Listener, channel: *SocketChannel) !void {
        const reset_event = try self.transport.allocator.create(ResetEvent);
        errdefer self.transport.allocator.destroy(reset_event);
        reset_event.* = ResetEvent{};
        const accept_err = try self.transport.allocator.create(?anyerror);
        errdefer self.transport.allocator.destroy(accept_err);
        accept_err.* = null;
        const node = self.transport.allocator.create(AsyncIOQueueNode) catch unreachable;
        node.* = AsyncIOQueueNode{
            .next = null,
            .op = .{ .accept = .{
                .server = self.server,
                .channel = channel,
                .reset_event = reset_event,
                .err = accept_err,
            } },
        };
        self.transport.async_task_queue.push(node);
        try self.transport.async_io_notifier.notify();

        reset_event.wait();
        if (accept_err.*) |err| {
            return err;
        }
    }
};

pub const XevTransport = struct {
    loop: xev.Loop,
    options: Options,
    stop_notifier: xev.Async,
    async_io_notifier: xev.Async,
    async_task_queue: *IOQueue,
    c_stop: xev.Completion,
    c_async: xev.Completion,
    allocator: Allocator,

    pub fn init(allocator: Allocator, opts: Options) !XevTransport {
        var loop = try xev.Loop.init(.{});
        errdefer loop.deinit();

        var stop_notifier = try xev.Async.init();
        errdefer stop_notifier.deinit();

        var async_io_notifier = try xev.Async.init();
        errdefer async_io_notifier.deinit();

        var io_queue = try allocator.create(IOQueue);
        io_queue.init();
        errdefer allocator.destroy(io_queue);
        return XevTransport{
            .loop = loop,
            .options = opts,
            .stop_notifier = stop_notifier,
            .async_io_notifier = async_io_notifier,
            .async_task_queue = io_queue,
            .c_stop = .{},
            .c_async = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *XevTransport) void {
        self.loop.deinit();
        self.stop_notifier.deinit();
        self.async_io_notifier.deinit();
        self.allocator.destroy(self.async_task_queue);
    }

    pub fn dial(self: *XevTransport, addr: std.net.Address, channel: *SocketChannel) !void {
        const reset_event = try self.allocator.create(std.Thread.ResetEvent);
        errdefer self.allocator.destroy(reset_event);
        reset_event.* = ResetEvent{};
        const connect_error = try self.allocator.create(?anyerror);
        errdefer self.allocator.destroy(connect_error);
        connect_error.* = null;
        const node = try self.allocator.create(AsyncIOQueueNode);
        node.* = AsyncIOQueueNode{
            .next = null,
            .op = .{
                .connect = .{
                    .address = addr,
                    .channel = channel,
                    .err = connect_error,
                    .reset_event = reset_event,
                },
            },
        };

        self.async_task_queue.push(node);

        try self.async_io_notifier.notify();

        reset_event.wait();
        if (connect_error.*) |err| {
            return err;
        }
    }

    pub fn listen(self: *XevTransport, addr: std.net.Address, listener: *Listener) !void {
        try listener.init(addr, self.options.backlog, self);
    }

    pub fn start(self: *XevTransport) !void {
        self.stop_notifier.wait(&self.loop, &self.c_stop, XevTransport, self, &stopCallback);

        self.async_io_notifier.wait(&self.loop, &self.c_async, XevTransport, self, &asyncIOCallback);

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
        _: *xev.Completion,
        r: xev.Async.WaitError!void,
    ) xev.CallbackAction {
        _ = r catch unreachable;
        _ = self_.?;

        loop.stop();

        return .disarm;
    }

    fn connectCallback(
        self_: ?*OpenChannelCallbackData,
        _: *xev.Loop,
        _: *xev.Completion,
        socket: xev.TCP,
        r: xev.ConnectError!void,
    ) xev.CallbackAction {
        const self = self_.?;
        // defer self.transport.allocator.destroy(c);
        defer self.transport.allocator.destroy(self);
        _ = r catch |err| {
            std.debug.print("Error connecting: {}\n", .{err});
            self.err.* = err;
            self.reset_event.set();
            return .disarm;
        };

        self.channel.init(socket, self.transport, true);
        self.reset_event.set();
        return .disarm;
    }

    fn acceptCallback(
        self_: ?*OpenChannelCallbackData,
        _: *xev.Loop,
        _: *xev.Completion,
        r: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const self = self_.?;
        // defer self.transport.allocator.destroy(c);
        defer self.transport.allocator.destroy(self);
        const socket = r catch |err| {
            std.debug.print("Error accepting: {}\n", .{err});
            self.err.* = err;
            self.reset_event.set();
            return .disarm;
        };

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

// test "dial connection refused" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const opts = Options{
//         .backlog = 128,
//     };
//
//     var transport = try XevTransport.init(allocator, opts);
//     defer transport.deinit();
//
//     const thread = try std.Thread.spawn(.{}, XevTransport.start, .{&transport});
//
//     var channel: SocketChannel = undefined;
//     const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
//     try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel));
//
//     var channel1: SocketChannel = undefined;
//     try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel1));
//
//     const thread2 = try std.Thread.spawn(.{}, struct {
//         fn run(t: *XevTransport, a: std.net.Address) !void {
//             var ch: SocketChannel = undefined;
//             try std.testing.expectError(error.ConnectionRefused, t.dial(a, &ch));
//         }
//     }.run, .{ &transport, addr });
//
//     thread2.join();
//     transport.stop();
//     thread.join();
// }

test "dial and accept" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const opts = Options{
        .backlog = 128,
    };

    var transport = try XevTransport.init(allocator, opts);
    defer transport.deinit();

    const thread = try std.Thread.spawn(.{}, XevTransport.start, .{&transport});

    var listener: Listener = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
    try transport.listen(addr, &listener);

    var channel: SocketChannel = undefined;
    const accept_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener, _: *SocketChannel) !void {
            var accepted_count: usize = 0;
            while (accepted_count < 2) : (accepted_count += 1) {
                var accepted_channel: SocketChannel = undefined;
                try l.accept(&accepted_channel);
                try std.testing.expect(!accepted_channel.is_initiator);
            }
        }
    }.run, .{ &listener, &channel });

    var client = try XevTransport.init(allocator, opts);
    defer client.deinit();

    const thread1 = try std.Thread.spawn(.{}, XevTransport.start, .{&client});
    var channel1: SocketChannel = undefined;
    try client.dial(addr, &channel1);
    try std.testing.expect(channel1.is_initiator);

    var channel2: SocketChannel = undefined;
    try client.dial(addr, &channel2);
    try std.testing.expect(channel2.is_initiator);

    accept_thread.join();
    client.stop();
    transport.stop();
    thread1.join();
    thread.join();
}
