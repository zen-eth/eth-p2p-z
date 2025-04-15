const std = @import("std");
const xev = @import("xev");
const TCP = xev.TCP;
const Allocator = std.mem.Allocator;
const ThreadPool = xev.ThreadPool;
const ResetEvent = std.Thread.ResetEvent;
const p2p_conn = @import("../../conn.zig");
const p2p_transport = @import("../../transport.zig");
const io_loop = @import("../../thread_event_loop.zig");
const Future = @import("../../concurrent/lib.zig").Future;

pub const Connection = p2p_conn.GenericConn(
    *XevSocketChannel,
    XevSocketChannel.ReadError,
    XevSocketChannel.WriteError,
    XevSocketChannel.read,
    XevSocketChannel.write,
    XevSocketChannel.close,
);

pub const Listener = p2p_transport.GenericListener(
    *XevListener,
    XevListener.AcceptError,
    XevListener.accept,
);

pub const Transport = p2p_transport.GenericTransport(
    *XevTransport,
    XevTransport.DialError,
    XevListener.ListenError,
    XevTransport.dial,
    XevTransport.listen,
);

/// SocketChannel represents a socket channel. It is used to send and receive messages.
pub const XevSocketChannel = struct {
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
    transport: *XevTransport,

    /// Whether this channel is the initiator of the connection. If true, this channel is the client. If false, this channel is the server.
    direction: DIRECTION,

    /// Initialize the channel with the given socket and transport.
    pub fn init(self: *XevSocketChannel, socket: TCP, transport: *XevTransport, direction: DIRECTION) void {
        self.socket = socket;
        self.transport = transport;
        self.direction = direction;
    }

    /// Write sends the buffer to the other end of the channel. It blocks until the write is complete. If an error occurs, it returns the error.
    pub fn write(self: *XevSocketChannel, buf: []const u8) WriteError!usize {
        const f = try self.transport.allocator.create(Future(usize, WriteError));
        f.* = .{};
        defer self.transport.allocator.destroy(f);
        const message = io_loop.IOMessage{
            .action = .{ .write = .{
                .channel = self,
                .buffer = buf,
                .future = f,
            } },
        };

        self.transport.io_event_loop.queueMessage(message) catch |err| {
            std.debug.print("Error queuing message: {}\n", .{err});
            return error.AsyncNotifyFailed;
        };

        f.wait();
        if (f.getErr()) |err| {
            return err;
        }

        return f.getValue().?;
    }

    /// Read reads from the channel into the buffer. It blocks until the read is complete. If an error occurs, it returns the error.
    pub fn read(self: *XevSocketChannel, buf: []u8) ReadError!usize {
        const f = try self.transport.allocator.create(Future(usize, ReadError));
        f.* = .{};
        defer self.transport.allocator.destroy(f);
        const message = io_loop.IOMessage{
            .action = .{ .read = .{
                .channel = self,
                .buffer = buf,
                .future = f,
            } },
        };

        self.transport.io_event_loop.queueMessage(message) catch |err| {
            std.debug.print("Error queuing message: {}\n", .{err});
            return error.AsyncNotifyFailed;
        };

        f.wait();
        if (f.getErr()) |err| {
            return err;
        }

        return f.getValue().?;
    }

    /// Close closes the channel. It blocks until the close is complete.
    pub fn close(_: *XevSocketChannel) void {
        // TODO: we should close the socket here
    }

    pub fn toConn(self: *XevSocketChannel) Connection {
        return .{ .context = self };
    }
};

/// Listener listens for incoming connections. It is used to accept connections.
pub const XevListener = struct {
    pub const AcceptError = Allocator.Error || xev.AcceptError || error{AsyncNotifyFailed};

    pub const ListenError = error{
        BindFailed,
        ListenFailed,
        InitTCPFailed,
    };
    /// The address to listen on.
    address: std.net.Address,
    /// The server to accept connections from.
    server: TCP,
    /// The transport that created this listener.
    transport: *XevTransport,

    /// Initialize the listener with the given address, backlog, and transport.
    pub fn init(self: *XevListener, address: std.net.Address, backlog: u31, transport: *XevTransport) ListenError!void {
        const server = TCP.init(address) catch |err| {
            std.debug.print("Error initializing TCP: {}\n", .{err});
            return ListenError.InitTCPFailed;
        };
        server.bind(address) catch |err| {
            std.debug.print("Error binding to address: {}\n", .{err});
            return ListenError.BindFailed;
        };
        server.listen(backlog) catch |err| {
            std.debug.print("Error listening on address: {}\n", .{err});
            return ListenError.ListenFailed;
        };

        self.address = address;
        self.server = server;
        self.transport = transport;
    }

    /// Deinitialize the listener.
    pub fn deinit(_: *XevListener) void {
        // TODO: should we close the server here?
    }

    /// Accept accepts a connection from the listener. It blocks until a connection is accepted. If an error occurs, it returns the error.
    pub fn accept(self: *XevListener, channel: anytype) AcceptError!void {
        const f = try self.transport.allocator.create(Future(void, AcceptError));
        f.* = .{};
        defer self.transport.allocator.destroy(f);
        const message = io_loop.IOMessage{
            .action = .{ .accept = .{ .channel = channel, .server = self.server, .future = f, .transport = self.transport } },
        };

        self.transport.io_event_loop.queueMessage(message) catch |err| {
            std.debug.print("Error queuing message: {}\n", .{err});
            return error.AsyncNotifyFailed;
        };

        f.wait();
        if (f.getErr()) |err| {
            return err;
        }
    }
};

/// Transport is a transport that uses the xev library for asynchronous I/O operations.
pub const XevTransport = struct {
    pub const DialError = Allocator.Error || xev.ConnectError || error{AsyncNotifyFailed};

    io_event_loop: *io_loop.ThreadEventLoop,

    /// The options for the transport.
    options: Options,

    /// The allocator.
    allocator: Allocator,

    /// Options for the transport.
    pub const Options = struct {
        /// The maximum number of pending connections.
        backlog: u31,
    };

    /// Initialize the transport with the given allocator and options.
    pub fn init(self: *XevTransport, loop: *io_loop.ThreadEventLoop, allocator: Allocator, opts: Options) !void {
        self.* = .{
            .io_event_loop = loop,
            .options = opts,
            .allocator = allocator,
        };
    }

    /// Deinitialize the transport.
    pub fn deinit(_: *XevTransport) void {}

    /// Dial connects to the given address and creates a channel for communication. It blocks until the connection is established. If an error occurs, it returns the error.
    pub fn dial(self: *XevTransport, addr: std.net.Address, channel: anytype) DialError!void {
        const f = try self.allocator.create(Future(void, DialError));
        f.* = .{};
        defer self.allocator.destroy(f);
        const message = io_loop.IOMessage{
            .action = .{ .connect = .{
                .address = addr,
                .channel = channel,
                .transport = self,
                .future = f,
            } },
        };

        self.io_event_loop.queueMessage(message) catch |err| {
            std.debug.print("Error queuing message: {}\n", .{err});
            return error.AsyncNotifyFailed;
        };

        f.wait();
        if (f.getErr()) |err| {
            return err;
        }
    }

    /// Listen listens for incoming connections on the given address. It blocks until the listener is initialized. If an error occurs, it returns the error.
    pub fn listen(self: *XevTransport, addr: std.net.Address, listener: anytype) XevListener.ListenError!void {
        try listener.init(addr, self.options.backlog, self);
    }

    /// Start starts the transport. It blocks until the transport is stopped.
    /// It is recommended to call this function in a separate thread.
    pub fn start(_: *XevTransport) !void {}

    /// Close closes the transport.
    pub fn close(_: *XevTransport) void {}
};

test "dial connection refused" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var l: io_loop.ThreadEventLoop = undefined;
    try l.init(allocator);
    defer {
        l.close();
        l.deinit();
    }

    const opts = XevTransport.Options{
        .backlog = 128,
    };

    var transport: XevTransport = undefined;
    try transport.init(&l, allocator, opts);
    defer transport.deinit();

    var channel: XevSocketChannel = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel));

    var channel1: XevSocketChannel = undefined;
    try std.testing.expectError(error.ConnectionRefused, transport.dial(addr, &channel1));

    const thread2 = try std.Thread.spawn(.{}, struct {
        fn run(t: *XevTransport, a: std.net.Address) !void {
            var ch: XevSocketChannel = undefined;
            try std.testing.expectError(error.ConnectionRefused, t.dial(a, &ch));
        }
    }.run, .{ &transport, addr });

    thread2.join();
    transport.close();
}

test "dial and accept" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var sl: io_loop.ThreadEventLoop = undefined;
    try sl.init(allocator);
    defer {
        sl.close();
        sl.deinit();
    }

    const opts = XevTransport.Options{
        .backlog = 128,
    };

    var transport: XevTransport = undefined;
    try transport.init(&sl, allocator, opts);
    defer transport.deinit();

    var listener: XevListener = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8082);
    try transport.listen(addr, &listener);

    var channel: XevSocketChannel = undefined;
    const accept_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *XevListener, _: *XevSocketChannel) !void {
            var accepted_count: usize = 0;
            while (accepted_count < 2) : (accepted_count += 1) {
                var accepted_channel: XevSocketChannel = undefined;
                try l.accept(&accepted_channel);
                try std.testing.expectEqual(accepted_channel.direction, .INBOUND);
            }
        }
    }.run, .{ &listener, &channel });

    var cl: io_loop.ThreadEventLoop = undefined;
    try cl.init(allocator);
    defer {
        cl.close();
        cl.deinit();
    }
    var client: XevTransport = undefined;
    try client.init(&cl, allocator, opts);
    defer client.deinit();

    var channel1: XevSocketChannel = undefined;
    try client.dial(addr, &channel1);
    try std.testing.expectEqual(channel1.direction, .OUTBOUND);

    var channel2: XevSocketChannel = undefined;
    try client.dial(addr, &channel2);
    try std.testing.expectEqual(channel2.direction, .OUTBOUND);

    accept_thread.join();
    client.close();
    transport.close();
}

test "echo read and write" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const opts = XevTransport.Options{
        .backlog = 128,
    };

    var sl: io_loop.ThreadEventLoop = undefined;
    try sl.init(allocator);
    defer {
        sl.close();
        sl.deinit();
    }

    var server: XevTransport = undefined;
    try server.init(&sl, allocator, opts);
    defer server.deinit();

    var listener: XevListener = undefined;
    const addr = try std.net.Address.parseIp("0.0.0.0", 8081);
    try server.listen(addr, &listener);

    const accept_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *XevListener, alloc: Allocator) !void {
            var accepted_channel: XevSocketChannel = undefined;
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

    var cl: io_loop.ThreadEventLoop = undefined;
    try cl.init(allocator);
    defer {
        cl.close();
        cl.deinit();
    }
    var client: XevTransport = undefined;
    try client.init(&cl, allocator, opts);
    defer client.deinit();

    var channel1: XevSocketChannel = undefined;
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
