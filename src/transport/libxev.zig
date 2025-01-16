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

pub const XevTransport = struct {
    server_loop: xev.Loop,
    client_loop: xev.Loop,
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
    allocator: Allocator,

    pub fn init(allocator: Allocator, opts: Options) !XevTransport {
        const thread_pool = try allocator.create(ThreadPool);
        thread_pool.* = ThreadPool.init(.{});
        const loop_opts = xev.Options{
            .thread_pool = thread_pool,
        };
        const server_loop = try xev.Loop.init(loop_opts);
        const client_loop = try xev.Loop.init(loop_opts);
        return XevTransport{
            .buffer_pool = BufferPool.init(allocator),
            .completion_pool = CompletionPool.init(allocator),
            .socket_pool = TCPPool.init(allocator),
            .server_loop = server_loop,
            .client_loop = client_loop,
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
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *XevTransport) void {
        self.server_loop.deinit();
        self.client_loop.deinit();
        self.threadPool.deinit();
        self.listeners.mutex.lock();
        self.listeners.m.deinit();
        self.listeners.mutex.unlock();
        self.sockets.mutex.lock();
        self.sockets.l.deinit();
        self.sockets.mutex.unlock();
        self.buffer_pool.deinit();
        self.completion_pool.deinit();
        self.socket_pool.deinit();
    }

    pub fn listen(self: *XevTransport, addr: std.net.Address) !void {
        const server = try TCP.init(addr);
        try server.bind(addr);
        try server.listen(self.options.backlog);

        var c_accept: xev.Completion = undefined;
        server.accept(&self.server_loop, &c_accept, XevTransport, self, acceptCallback);

        self.listeners.mutex.lock();
        self.listeners.m.put(try formatAddress(addr, self.allocator), server) catch |err| {
            std.debug.print("Error adding server to map: {}\n", .{err});
        };
        self.listeners.mutex.unlock();
    }

    fn acceptCallback(
        self_: ?*XevTransport,
        l: *xev.Loop,
        _: *xev.Completion,
        r: xev.TCP.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const self = self_.?;

        // Create our socket
        const socket = self.socket_pool.create() catch unreachable;
        socket.* = r catch unreachable;
        self.sockets.mutex.lock();
        self.sockets.l.append(socket.*) catch unreachable;
        self.sockets.mutex.unlock();

        // Start reading -- we can reuse c here because its done.
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
