const std = @import("std");
const xev = @import("xev");
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

pub const LibxevTransport = struct {
    loop: xev.Loop,
    threadPool: *xev.ThreadPool,
    servers: struct {
        mutex: std.Thread.Mutex,
        map: std.StringHashMap(TCP),
    },
    clients: struct {
        mutex: std.Thread.Mutex,
        list: std.ArrayList(TCP),
    },
    options: Options,
    allocator: Allocator,

    pub fn init(allocator: Allocator, opts: Options) !LibxevTransport {
        const thread_pool = try allocator.create(ThreadPool);
        thread_pool.* = ThreadPool.init(.{});
        const loop_opts = xev.Options{
            .thread_pool = thread_pool,
        };
        const loop = try xev.Loop.init(loop_opts);
        return LibxevTransport{
            .loop = loop,
            .threadPool = thread_pool,
            .servers = .{
                .mutex = .{},
                .map = std.StringHashMap(TCP).init(allocator),
            },
            .clients = .{
                .mutex = .{},
                .list = std.ArrayList(TCP).init(allocator),
            },
            .options = opts,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *LibxevTransport) void {
        self.loop.deinit();
        self.threadPool.deinit();
        self.servers.mutex.lock();
        self.servers.map.deinit();
        self.servers.mutex.unlock();
        self.clients.mutex.lock();
        self.clients.list.deinit();
        self.clients.mutex.unlock();
    }

    pub fn listen(self: *LibxevTransport, addr: std.net.Address) !void {
        const server = try TCP.init(addr);
        try server.bind(addr);
        try server.listen(self.options.backlog);

        var c_accept: xev.Completion = undefined;
        var server_conn: ?TCP = null;
        server.accept(&self.loop, &c_accept, ?TCP, &server_conn, (struct {
            fn callback(
                user_data: ?*?TCP,
                _: *xev.Loop,
                _: *xev.Completion,
                result: xev.TCP.AcceptError!TCP,
            ) xev.CallbackAction {
                user_data.?.* = blk: {
                    break :blk result catch |err| {
                        std.log.err("TCP error: {}", .{err});
                        break :blk null;
                    };
                };
                return .disarm;
            }
        }).callback);

        self.servers.mutex.lock();
        self.servers.map.put(try formatAddress(addr, self.allocator), server) catch |err| {
            std.debug.print("Error adding server to map: {}\n", .{err});
        };
        self.servers.mutex.unlock();
        try self.loop.run(.until_done);
    }

    pub fn shutdown(_: *LibxevTransport) void {
        // self.servers.mutex.lock();
        // var iter = self.servers.map.iterator();
        // while (iter.next()) |entry| {
        //     const server = entry.value_ptr;
        //     server.close();
        // }
    }

    pub fn formatAddress(addr: std.net.Address, allocator: Allocator) ![]const u8 {
        const addr_str = try std.fmt.allocPrint(allocator, "{}", .{addr});
        std.debug.print("Address: {s}\n", .{addr_str});
        return addr_str;
    }
};

/// The client state
const Client = struct {
    loop: *xev.Loop,
    completion_pool: CompletionPool,
    read_buf: [1024]u8,
    pongs: u64,
    state: usize = 0,
    stop: bool = false,

    pub const PING = "PING\n";

    pub fn init(alloc: Allocator, loop: *xev.Loop) !Client {
        return .{
            .loop = loop,
            .completion_pool = CompletionPool.init(alloc),
            .read_buf = undefined,
            .pongs = 0,
            .state = 0,
            .stop = false,
        };
    }

    pub fn deinit(self: *Client) void {
        self.completion_pool.deinit();
    }

    /// Must be called with stable self pointer.
    pub fn start(self: *Client) !void {
        const addr = try std.net.Address.parseIp4("127.0.0.1", 3131);
        const socket = try xev.TCP.init(addr);

        const c = try self.completion_pool.create();
        socket.connect(self.loop, c, addr, Client, self, connectCallback);
    }

    fn connectCallback(
        self_: ?*Client,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.TCP.ConnectError!void,
    ) xev.CallbackAction {
        _ = r catch unreachable;

        const self = self_.?;

        // Send message
        socket.write(l, c, .{ .slice = PING[0..PING.len] }, Client, self, writeCallback);

        // Read
        const c_read = self.completion_pool.create() catch unreachable;
        socket.read(l, c_read, .{ .slice = &self.read_buf }, Client, self, readCallback);
        return .disarm;
    }

    fn writeCallback(
        self_: ?*Client,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        b: xev.WriteBuffer,
        r: xev.TCP.WriteError!usize,
    ) xev.CallbackAction {
        _ = r catch unreachable;
        _ = l;
        _ = s;
        _ = b;

        // Put back the completion.
        self_.?.completion_pool.destroy(c);
        return .disarm;
    }

    fn readCallback(
        self_: ?*Client,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        buf: xev.ReadBuffer,
        r: xev.TCP.ReadError!usize,
    ) xev.CallbackAction {
        const self = self_.?;
        const n = r catch unreachable;
        const data = buf.slice[0..n];

        // Count the number of pings in our message
        var i: usize = 0;
        while (i < n) : (i += 1) {
            std.debug.assert(data[i] == PING[self.state]);
            self.state = (self.state + 1) % (PING.len);
            if (self.state == 0) {
                self.pongs += 1;

                // If we're done then exit
                if (self.pongs > 500_000) {
                    socket.shutdown(l, c, Client, self, shutdownCallback);
                    return .disarm;
                }

                // Send another ping
                const c_ping = self.completion_pool.create() catch unreachable;
                socket.write(l, c_ping, .{ .slice = PING[0..PING.len] }, Client, self, writeCallback);
            }
        }

        // Read again
        return .rearm;
    }

    fn shutdownCallback(
        self_: ?*Client,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.TCP.ShutdownError!void,
    ) xev.CallbackAction {
        _ = r catch {};

        const self = self_.?;
        socket.close(l, c, Client, self, closeCallback);
        return .disarm;
    }

    fn closeCallback(
        self_: ?*Client,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.TCP.CloseError!void,
    ) xev.CallbackAction {
        _ = l;
        _ = socket;
        _ = r catch unreachable;

        const self = self_.?;
        self.stop = true;
        self.completion_pool.destroy(c);
        return .disarm;
    }
};

/// The server state
const Server = struct {
    loop: *xev.Loop,
    buffer_pool: BufferPool,
    completion_pool: CompletionPool,
    socket_pool: TCPPool,
    stop: bool,

    pub fn init(alloc: Allocator, loop: *xev.Loop) !Server {
        return .{
            .loop = loop,
            .buffer_pool = BufferPool.init(alloc),
            .completion_pool = CompletionPool.init(alloc),
            .socket_pool = TCPPool.init(alloc),
            .stop = false,
        };
    }

    pub fn deinit(self: *Server) void {
        self.buffer_pool.deinit();
        self.completion_pool.deinit();
        self.socket_pool.deinit();
    }

    /// Must be called with stable self pointer.
    pub fn start(self: *Server) !void {
        const addr = try std.net.Address.parseIp4("127.0.0.1", 3131);
        var socket = try xev.TCP.init(addr);

        const c = try self.completion_pool.create();
        try socket.bind(addr);
        try socket.listen(std.os.linux.SOMAXCONN);
        socket.accept(self.loop, c, Server, self, acceptCallback);
    }

    pub fn threadMain(self: *Server) !void {
        try self.loop.run(.until_done);
    }

    fn destroyBuf(self: *Server, buf: []const u8) void {
        self.buffer_pool.destroy(
            @alignCast(
                @as(*[4096]u8, @ptrFromInt(@intFromPtr(buf.ptr))),
            ),
        );
    }

    fn acceptCallback(
        self_: ?*Server,
        l: *xev.Loop,
        c: *xev.Completion,
        r: xev.TCP.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const self = self_.?;

        // Create our socket
        const socket = self.socket_pool.create() catch unreachable;
        socket.* = r catch unreachable;

        // Start reading -- we can reuse c here because its done.
        const buf = self.buffer_pool.create() catch unreachable;
        socket.read(l, c, .{ .slice = buf }, Server, self, readCallback);
        return .disarm;
    }

    fn readCallback(
        self_: ?*Server,
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
                socket.shutdown(loop, c, Server, self, shutdownCallback);
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
        const c_echo = self.completion_pool.create() catch unreachable;
        const buf_write = self.buffer_pool.create() catch unreachable;
        @memcpy(buf_write, buf.slice[0..n]);
        socket.write(loop, c_echo, .{ .slice = buf_write[0..n] }, Server, self, writeCallback);

        // Read again
        return .rearm;
    }

    fn writeCallback(
        self_: ?*Server,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        buf: xev.WriteBuffer,
        r: xev.TCP.WriteError!usize,
    ) xev.CallbackAction {
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
        self_: ?*Server,
        l: *xev.Loop,
        c: *xev.Completion,
        s: xev.TCP,
        r: xev.TCP.ShutdownError!void,
    ) xev.CallbackAction {
        _ = r catch {};

        const self = self_.?;
        s.close(l, c, Server, self, closeCallback);
        return .disarm;
    }

    fn closeCallback(
        self_: ?*Server,
        l: *xev.Loop,
        c: *xev.Completion,
        socket: xev.TCP,
        r: xev.TCP.CloseError!void,
    ) xev.CallbackAction {
        _ = l;
        _ = r catch unreachable;
        _ = socket;

        const self = self_.?;
        self.stop = true;
        self.completion_pool.destroy(c);
        return .disarm;
    }
};
