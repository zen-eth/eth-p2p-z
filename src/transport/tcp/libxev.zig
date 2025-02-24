const std = @import("std");
const xev = @import("xev");
const Intrusive = @import("../../utils/queue_mpsc.zig").Intrusive;
const IOQueue = Intrusive(AsyncIOQueueNode);
const TCP = xev.TCP;
const Allocator = std.mem.Allocator;
const ThreadPool = xev.ThreadPool;
const ResetEvent = std.Thread.ResetEvent;

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

    pub fn write(self: *SocketChannel, buf: []const u8) !void {
        const reset_event = try self.transport.allocator.create(ResetEvent);
        errdefer self.transport.allocator.destroy(reset_event);
        reset_event.* = ResetEvent{};
        const write_err = try self.transport.allocator.create(?anyerror);
        errdefer self.transport.allocator.destroy(write_err);
        write_err.* = null;
        const node = self.transport.allocator.create(AsyncIOQueueNode) catch unreachable;
        node.* = AsyncIOQueueNode{
            .next = null,
            .op = .{ .write = .{
                .channel = self,
                .buffer = buf,
                .reset_event = reset_event,
                .err = write_err,
            } },
        };
        self.transport.async_task_queue.push(node);
        try self.transport.async_io_notifier.notify();

        reset_event.wait();
        if (write_err.*) |err| {
            return err;
        }
    }

    pub fn read(self: *SocketChannel, buf: []u8) !usize {
        const reset_event = try self.transport.allocator.create(ResetEvent);
        errdefer self.transport.allocator.destroy(reset_event);
        reset_event.* = ResetEvent{};
        const read_err = try self.transport.allocator.create(?anyerror);
        errdefer self.transport.allocator.destroy(read_err);
        read_err.* = null;
        const bytes_read = try self.transport.allocator.create(usize);
        errdefer self.transport.allocator.destroy(bytes_read);
        const node = self.transport.allocator.create(AsyncIOQueueNode) catch unreachable;
        node.* = AsyncIOQueueNode{
            .next = null,
            .op = .{ .read = .{
                .channel = self,
                .buffer = buf,
                .reset_event = reset_event,
                .err = read_err,
                .bytes_read = bytes_read,
            } },
        };

        self.transport.async_task_queue.push(node);

        try self.transport.async_io_notifier.notify();

        reset_event.wait();
        if (read_err.*) |err| {
            return err;
        }
        return bytes_read.*;
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
        write: struct {
            buffer: []const u8,
            channel: *SocketChannel,
            reset_event: *ResetEvent,
            err: *?anyerror,
        },
        read: struct {
            channel: *SocketChannel,
            buffer: []u8,
            reset_event: *ResetEvent,
            err: *?anyerror,
            bytes_read: *usize,
        },
    },
};

const OpenChannelCallbackData = struct {
    transport: *XevTransport,
    channel: *SocketChannel,
    err: *?anyerror,
    reset_event: *ResetEvent,
};

const WriteCallbackData = struct {
    channel: *SocketChannel,
    buffer: []const u8,
    reset_event: *ResetEvent,
    err: *?anyerror,
};

const ReadCallbackData = struct {
    channel: *SocketChannel,
    buffer: []u8,
    reset_event: *ResetEvent,
    err: *?anyerror,
    bytes_read: *usize,
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
                .write => |*w| {
                    const c = self.allocator.create(xev.Completion) catch unreachable;
                    const channel = w.channel;
                    const buffer = w.buffer;
                    const write_cb_data = self.allocator.create(WriteCallbackData) catch unreachable;
                    write_cb_data.* = WriteCallbackData{
                        .buffer = buffer,
                        .channel = channel,
                        .reset_event = w.reset_event,
                        .err = w.err,
                    };

                    channel.socket.write(loop, c, .{ .slice = buffer }, WriteCallbackData, write_cb_data, writeCallback);
                },
                .read => |*read| {
                    const channel = read.channel;
                    const buffer = read.buffer;
                    const c = self.allocator.create(xev.Completion) catch unreachable;
                    const read_cb_data = self.allocator.create(ReadCallbackData) catch unreachable;
                    read_cb_data.* = ReadCallbackData{
                        .buffer = buffer,
                        .channel = channel,
                        .reset_event = read.reset_event,
                        .err = read.err,
                        .bytes_read = read.bytes_read,
                    };

                    channel.socket.read(loop, c, .{ .slice = buffer }, ReadCallbackData, read_cb_data, readCallback);
                },
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

    fn writeCallback(
        self_: ?*WriteCallbackData,
        _: *xev.Loop,
        c: *xev.Completion,
        _: xev.TCP,
        _: xev.WriteBuffer,
        r: xev.WriteError!usize,
    ) xev.CallbackAction {
        const self = self_.?;
        defer self.channel.transport.allocator.destroy(self);
        _ = r catch |err| {
            std.debug.print("Error writing: {}\n", .{err});
            self.err.* = err;
            self.reset_event.set();
            return .disarm;
        };

        self.channel.transport.allocator.destroy(c);

        self.reset_event.set();

        return .disarm;
    }

    fn readCallback(
        self_: ?*ReadCallbackData,
        loop: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        _: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        const self = self_.?;
        defer self.channel.transport.allocator.destroy(self);
        const n = r catch |err| switch (err) {
            error.EOF => {
                socket.shutdown(loop, c, SocketChannel, self.channel, shutdownCallback);
                self.err.* = err;
                self.reset_event.set();
                return .disarm;
            },

            else => {
                socket.shutdown(loop, c, SocketChannel, self.channel, shutdownCallback);
                std.log.warn("server read unexpected err={}", .{err});
                self.err.* = err;
                self.reset_event.set();
                return .disarm;
            },
        };

        self.bytes_read.* = n;
        self.channel.transport.allocator.destroy(c);
        self.reset_event.set();
        return .disarm;
    }

    fn shutdownCallback(
        self_: ?*SocketChannel,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.ShutdownError!void,
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
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        std.debug.print("close callback\n", .{});
        _ = l;
        _ = r catch unreachable;
        _ = socket;

        const self = self_.?;
        self.transport.allocator.destroy(c);
        return .disarm;
    }
};

test "dial connection refused" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const opts = Options{
        .backlog = 128,
    };

    var transport = try XevTransport.init(allocator, opts);
    defer transport.deinit();

    const thread = try std.Thread.spawn(.{}, XevTransport.start, .{&transport});

    var channel: SocketChannel = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel));

    var channel1: SocketChannel = undefined;
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel1));

    const thread2 = try std.Thread.spawn(.{}, struct {
        fn run(t: *XevTransport, a: std.net.Address) !void {
            var ch: SocketChannel = undefined;
            try std.testing.expectError(error.ConnectionRefused, t.dial(a, &ch));
        }
    }.run, .{ &transport, addr });

    thread2.join();
    transport.stop();
    thread.join();
}

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
    const addr = try std.net.Address.parseIp("0.0.0.0", 8082);
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

test "echo read and write" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const opts = Options{
        .backlog = 128,
    };

    var server = try XevTransport.init(allocator, opts);
    defer server.deinit();

    const thread = try std.Thread.spawn(.{}, XevTransport.start, .{&server});

    var listener: Listener = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
    try server.listen(addr, &listener);

    const accept_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener, alloc: Allocator) !void {
            var accepted_channel: SocketChannel = undefined;
            try l.accept(&accepted_channel);
            try std.testing.expect(!accepted_channel.is_initiator);

            const buf = try alloc.alloc(u8, 1024);
            defer alloc.free(buf);
            const n = try accepted_channel.read(buf);
            try std.testing.expectStringStartsWith(buf, "buf: []const u8");

            try std.testing.expect(n == 15);
            try accepted_channel.write(buf[0..n]);
        }
    }.run, .{ &listener, allocator });

    var client = try XevTransport.init(allocator, opts);
    defer client.deinit();

    const thread1 = try std.Thread.spawn(.{}, XevTransport.start, .{&client});
    var channel1: SocketChannel = undefined;
    try client.dial(addr, &channel1);
    try std.testing.expect(channel1.is_initiator);

    try channel1.write("buf: []const u8");
    const buf = try allocator.alloc(u8, 1024);
    defer allocator.free(buf);
    const n = try channel1.read(buf);
    try std.testing.expectStringStartsWith(buf, "buf: []const u8");
    try std.testing.expect(n == 15);

    accept_thread.join();
    client.stop();
    server.stop();
    thread1.join();
    thread.join();
}
