const std = @import("std");
const xev = @import("xev");
const Intrusive = @import("../../concurrent/mpsc_queue.zig").Intrusive;
const IOQueue = Intrusive(AsyncIOQueueNode);
const TCP = xev.TCP;
const Allocator = std.mem.Allocator;
const ThreadPool = xev.ThreadPool;
const ResetEvent = std.Thread.ResetEvent;
const p2p_conn = @import("../../conn.zig");
const p2p_transport = @import("../../transport.zig");

pub const DialError = Allocator.Error || xev.ConnectError || error{AsyncNotifyFailed};
pub const AcceptError = Allocator.Error || xev.AcceptError || error{AsyncNotifyFailed};

pub const OpenConnectError = DialError || AcceptError;

pub const GenericConnection = p2p_conn.GenericConn(
    *SocketChannel,
    SocketChannel.ReadError,
    SocketChannel.WriteError,
    SocketChannel.read,
    SocketChannel.write,
    SocketChannel.close,
);

pub const GenericListener = p2p_transport.GenericListener(
    *Listener,
    OpenConnectError,
    Listener.accept,
);

pub const GenericTransport = p2p_transport.GenericTransport(
    *Transport,
    DialError,
    AcceptError,
    Transport.dial,
    Transport.listen,
);

/// SocketChannel represents a socket channel. It is used to send and receive messages.
pub const SocketChannel = struct {
    pub const WriteError = Allocator.Error || xev.WriteError || error{AsyncNotifyFailed};
    pub const ReadError = Allocator.Error || xev.ReadError || error{AsyncNotifyFailed};
    /// The direction of the channel.
    pub const DIRECTION = enum {
        INBOUND,
        OUTBOUND,
    };
    /// The underlying socket.
    socket: TCP,

    /// The transport that created this channel.
    transport: *Transport,

    /// Whether this channel is the initiator of the connection. If true, this channel is the client. If false, this channel is the server.
    direction: DIRECTION,

    /// Initialize the channel with the given socket and transport.
    pub fn init(self: *SocketChannel, socket: TCP, transport: *Transport, direction: DIRECTION) void {
        self.socket = socket;
        self.transport = transport;
        self.direction = direction;
    }

    /// Write sends the buffer to the other end of the channel. It blocks until the write is complete. If an error occurs, it returns the error.
    pub fn write(self: *SocketChannel, buf: []const u8) WriteError!usize {
        const reset_event = try self.transport.allocator.create(ResetEvent);
        errdefer self.transport.allocator.destroy(reset_event);
        reset_event.* = ResetEvent{};

        const write_err = try self.transport.allocator.create(?SocketChannel.WriteError);
        errdefer self.transport.allocator.destroy(write_err);
        write_err.* = null;

        const node = try self.transport.allocator.create(AsyncIOQueueNode);
        node.* = AsyncIOQueueNode{
            .io_action = .{ .write = .{
                .channel = self,
                .buffer = buf,
                .reset_event = reset_event,
                .err = write_err,
            } },
        };

        self.transport.async_task_queue.push(node);
        self.transport.async_io_notifier.notify() catch |err| {
            std.debug.print("Error notifying async I/O: {}\n", .{err});
            return error.AsyncNotifyFailed;
        };

        reset_event.wait();
        if (write_err.*) |err| {
            return err;
        }

        //TODO: we should check the number of bytes written
        return 0;
    }

    /// Read reads from the channel into the buffer. It blocks until the read is complete. If an error occurs, it returns the error.
    pub fn read(self: *SocketChannel, buf: []u8) ReadError!usize {
        const reset_event = try self.transport.allocator.create(ResetEvent);
        errdefer self.transport.allocator.destroy(reset_event);
        reset_event.* = ResetEvent{};

        const read_err = try self.transport.allocator.create(?SocketChannel.ReadError);
        errdefer self.transport.allocator.destroy(read_err);
        read_err.* = null;

        const bytes_read = try self.transport.allocator.create(usize);
        errdefer self.transport.allocator.destroy(bytes_read);
        bytes_read.* = 0;

        const node = try self.transport.allocator.create(AsyncIOQueueNode);
        node.* = AsyncIOQueueNode{
            .io_action = .{ .read = .{
                .channel = self,
                .buffer = buf,
                .reset_event = reset_event,
                .err = read_err,
                .bytes_read = bytes_read,
            } },
        };

        self.transport.async_task_queue.push(node);

        self.transport.async_io_notifier.notify() catch |err| {
            std.debug.print("Error notifying async I/O: {}\n", .{err});
            return error.AsyncNotifyFailed;
        };

        reset_event.wait();
        if (read_err.*) |err| {
            return err;
        }
        return bytes_read.*;
    }

    /// Close closes the channel. It blocks until the close is complete.
    pub fn close(_: *SocketChannel) void {
        // TODO: we should close the socket here
    }

    pub fn toConn(self: *SocketChannel) GenericConnection {
        return .{ .context = self };
    }
};

/// AsyncIOQueueNode is used to store the operation to be performed on the transport. It is used to perform asynchronous I/O operations.
pub const AsyncIOQueueNode = struct {
    const Self = @This();
    next: ?*Self = null,
    io_action: union(enum) {
        connect: struct {
            /// The address to connect to.
            address: std.net.Address,
            /// The channel to connect.
            channel: *SocketChannel,
            /// The reset event to signal when the operation is complete.
            reset_event: *ResetEvent,
            /// The error that occurred during the operation.
            err: *?OpenConnectError,
        },
        accept: struct {
            /// The server to accept from.
            server: TCP,
            /// The channel to accept.
            channel: *SocketChannel,
            /// The reset event to signal when the operation is complete.
            reset_event: *ResetEvent,
            /// The error that occurred during the operation.
            err: *?OpenConnectError,
        },
        write: struct {
            /// The buffer to write.
            buffer: []const u8,
            /// The channel to write to.
            channel: *SocketChannel,
            /// The reset event to signal when the operation is complete.
            reset_event: *ResetEvent,
            /// The error that occurred during the operation.
            err: *?SocketChannel.WriteError,
        },
        read: struct {
            /// The channel to read from.
            channel: *SocketChannel,
            /// The buffer to read into.
            buffer: []u8,
            /// The reset event to signal when the operation is complete.
            reset_event: *ResetEvent,
            /// The error that occurred during the operation.
            err: *?SocketChannel.ReadError,
            /// The number of bytes read.
            bytes_read: *usize,
        },
    },
};

const OpenChannelCallbackData = struct {
    transport: *Transport,
    channel: *SocketChannel,
    err: *?OpenConnectError,
    reset_event: *ResetEvent,
};

const WriteCallbackData = struct {
    channel: *SocketChannel,
    buffer: []const u8,
    reset_event: *ResetEvent,
    err: *?SocketChannel.WriteError,
};

const ReadCallbackData = struct {
    channel: *SocketChannel,
    buffer: []u8,
    reset_event: *ResetEvent,
    err: *?SocketChannel.ReadError,
    bytes_read: *usize,
};

/// Listener listens for incoming connections. It is used to accept connections.
pub const Listener = struct {
    /// The address to listen on.
    address: std.net.Address,
    /// The server to accept connections from.
    server: TCP,
    /// The transport that created this listener.
    transport: *Transport,

    /// Initialize the listener with the given address, backlog, and transport.
    pub fn init(self: *Listener, address: std.net.Address, backlog: u31, transport: *Transport) !void {
        const server = try TCP.init(address);
        try server.bind(address);
        try server.listen(backlog);

        self.address = address;
        self.server = server;
        self.transport = transport;
    }

    /// Deinitialize the listener.
    pub fn deinit(_: *Listener) void {
        // TODO: should we close the server here?
    }

    /// Accept accepts a connection from the listener. It blocks until a connection is accepted. If an error occurs, it returns the error.
    pub fn accept(self: *Listener, channel: anytype) OpenConnectError!void {
        const reset_event = try self.transport.allocator.create(ResetEvent);
        errdefer self.transport.allocator.destroy(reset_event);
        reset_event.* = ResetEvent{};

        const accept_err = try self.transport.allocator.create(?OpenConnectError);
        errdefer self.transport.allocator.destroy(accept_err);
        accept_err.* = null;

        const node = try self.transport.allocator.create(AsyncIOQueueNode);
        node.* = AsyncIOQueueNode{
            .io_action = .{ .accept = .{
                .server = self.server,
                .channel = channel,
                .reset_event = reset_event,
                .err = accept_err,
            } },
        };

        self.transport.async_task_queue.push(node);
        self.transport.async_io_notifier.notify() catch |err| {
            std.debug.print("Error notifying async I/O: {}\n", .{err});
            return error.AsyncNotifyFailed;
        };

        reset_event.wait();
        if (accept_err.*) |err| {
            return err;
        }
    }
};

/// Transport is a transport that uses the xev library for asynchronous I/O operations.
pub const Transport = struct {
    /// The event loop.
    loop: xev.Loop,
    /// The options for the transport.
    options: Options,
    /// The stop notifier.
    stop_notifier: xev.Async,
    /// The async I/O notifier.
    async_io_notifier: xev.Async,
    /// The async task queue.
    async_task_queue: *IOQueue,
    /// The completion for stopping the transport.
    c_stop: xev.Completion,
    /// The completion for async I/O operations.
    c_async: xev.Completion,
    /// The thread for the event loop.
    loop_thread: std.Thread,
    /// The allocator.
    allocator: Allocator,

    /// Options for the transport.
    pub const Options = struct {
        /// The maximum number of pending connections.
        backlog: u31,
    };

    /// Initialize the transport with the given allocator and options.
    pub fn init(self: *Transport, allocator: Allocator, opts: Options) !void {
        var loop = try xev.Loop.init(.{});
        errdefer loop.deinit();

        var stop_notifier = try xev.Async.init();
        errdefer stop_notifier.deinit();

        var async_io_notifier = try xev.Async.init();
        errdefer async_io_notifier.deinit();

        var io_queue = try allocator.create(IOQueue);
        io_queue.init();
        errdefer allocator.destroy(io_queue);
        self.* = Transport{
            .loop = loop,
            .options = opts,
            .stop_notifier = stop_notifier,
            .async_io_notifier = async_io_notifier,
            .async_task_queue = io_queue,
            .c_stop = .{},
            .c_async = .{},
            .loop_thread = undefined,
            .allocator = allocator,
        };

        const thread = try std.Thread.spawn(.{}, start, .{self});
        self.loop_thread = thread;
    }

    /// Deinitialize the transport.
    pub fn deinit(self: *Transport) void {
        self.loop.deinit();
        self.stop_notifier.deinit();
        self.async_io_notifier.deinit();
        self.allocator.destroy(self.async_task_queue);
    }

    /// Dial connects to the given address and creates a channel for communication. It blocks until the connection is established. If an error occurs, it returns the error.
    pub fn dial(self: *Transport, addr: std.net.Address, channel: anytype) OpenConnectError!void {
        const reset_event = try self.allocator.create(std.Thread.ResetEvent);
        errdefer self.allocator.destroy(reset_event);
        reset_event.* = ResetEvent{};

        const connect_err = try self.allocator.create(?OpenConnectError);
        errdefer self.allocator.destroy(connect_err);
        connect_err.* = null;

        const node = try self.allocator.create(AsyncIOQueueNode);
        node.* = AsyncIOQueueNode{
            .io_action = .{
                .connect = .{
                    .address = addr,
                    .channel = channel,
                    .err = connect_err,
                    .reset_event = reset_event,
                },
            },
        };

        self.async_task_queue.push(node);

        self.async_io_notifier.notify() catch |err| {
            std.debug.print("Error notifying async I/O: {}\n", .{err});
            return error.AsyncNotifyFailed;
        };

        reset_event.wait();
        if (connect_err.*) |err| {
            return err;
        }
    }

    /// Listen listens for incoming connections on the given address. It blocks until the listener is initialized. If an error occurs, it returns the error.
    pub fn listen(self: *Transport, addr: std.net.Address, listener: *Listener) !void {
        try listener.init(addr, self.options.backlog, self);
    }

    /// Start starts the transport. It blocks until the transport is stopped.
    /// It is recommended to call this function in a separate thread.
    pub fn start(self: *Transport) !void {
        self.stop_notifier.wait(&self.loop, &self.c_stop, Transport, self, &stopCallback);

        self.async_io_notifier.wait(&self.loop, &self.c_async, Transport, self, &asyncIOCallback);

        try self.loop.run(.until_done);
    }

    /// Close closes the transport.
    pub fn close(self: *Transport) void {
        self.stop_notifier.notify() catch |err| {
            std.debug.print("Error notifying stop: {}\n", .{err});
        };

        self.loop_thread.join();
    }

    fn asyncIOCallback(
        self_: ?*Transport,
        loop: *xev.Loop,
        _: *xev.Completion,
        r: xev.Async.WaitError!void,
    ) xev.CallbackAction {
        _ = r catch unreachable;
        const self = self_.?;

        while (self.async_task_queue.pop()) |node| {
            switch (node.io_action) {
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
        self_: ?*Transport,
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
        defer self.transport.allocator.destroy(self);
        _ = r catch |err| {
            std.debug.print("Error connecting: {}\n", .{err});
            self.err.* = err;
            self.reset_event.set();
            return .disarm;
        };

        self.channel.init(socket, self.transport, .OUTBOUND);
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

        self.channel.init(socket, self.transport, .INBOUND);

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

    const opts = Transport.Options{
        .backlog = 128,
    };

    var transport: Transport = undefined;
    try transport.init(allocator, opts);
    defer transport.deinit();

    var channel: SocketChannel = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel));

    var channel1: SocketChannel = undefined;
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel1));

    const thread2 = try std.Thread.spawn(.{}, struct {
        fn run(t: *Transport, a: std.net.Address) !void {
            var ch: SocketChannel = undefined;
            try std.testing.expectError(error.ConnectionRefused, t.dial(a, &ch));
        }
    }.run, .{ &transport, addr });

    thread2.join();
    transport.close();
}

test "dial and accept" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const opts = Transport.Options{
        .backlog = 128,
    };

    var transport: Transport = undefined;
    try transport.init(allocator, opts);
    defer transport.deinit();

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
                try std.testing.expectEqual(accepted_channel.direction, .INBOUND);
            }
        }
    }.run, .{ &listener, &channel });

    var client: Transport = undefined;
    try client.init(allocator, opts);
    defer client.deinit();

    var channel1: SocketChannel = undefined;
    try client.dial(addr, &channel1);
    try std.testing.expectEqual(channel1.direction, .OUTBOUND);

    var channel2: SocketChannel = undefined;
    try client.dial(addr, &channel2);
    try std.testing.expectEqual(channel2.direction, .OUTBOUND);

    accept_thread.join();
    client.close();
    transport.close();
}

test "echo read and write" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const opts = Transport.Options{
        .backlog = 128,
    };

    var server: Transport = undefined;
    try server.init(allocator, opts);
    defer server.deinit();

    var listener: Listener = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
    try server.listen(addr, &listener);

    const accept_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener, alloc: Allocator) !void {
            var accepted_channel: SocketChannel = undefined;
            try l.accept(&accepted_channel);
            try std.testing.expectEqual(accepted_channel.direction, .INBOUND);

            const buf = try alloc.alloc(u8, 1024);
            defer alloc.free(buf);
            const n = try accepted_channel.read(buf);
            try std.testing.expectStringStartsWith(buf, "buf: []const u8");

            try std.testing.expect(n == 15);
            _ = try accepted_channel.write(buf[0..n]);
        }
    }.run, .{ &listener, allocator });

    var client: Transport = undefined;
    try client.init(allocator, opts);
    defer client.deinit();

    var channel1: SocketChannel = undefined;
    try client.dial(addr, &channel1);
    try std.testing.expectEqual(channel1.direction, .OUTBOUND);

    _ = try channel1.write("buf: []const u8");
    const buf = try allocator.alloc(u8, 1024);
    defer allocator.free(buf);
    const n = try channel1.read(buf);
    try std.testing.expectStringStartsWith(buf, "buf: []const u8");
    try std.testing.expect(n == 15);

    accept_thread.join();
    client.close();
    server.close();
}
