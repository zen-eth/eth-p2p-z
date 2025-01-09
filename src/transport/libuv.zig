const std = @import("std");
const uv = @import("libuv");
const Loop = uv.Loop;
const Tcp = uv.Tcp;
const ConnectReq = Tcp.ConnectReq;
const WriteReq = uv.WriteReq;
const Signal = uv.Signal;
const c = uv.c;
const Allocator = @import("std").mem.Allocator;
const Multiaddr = @import("multiformats-zig").multiaddr.Multiaddr;
const Transport = @import("../transport/transport.zig").Transport;
const testing = std.testing;

pub const DEFAULT_BACKLOG = 128;

pub const Config = struct {
    backlog: i32,

    pub fn init() Config {
        return Config{
            .backlog = DEFAULT_BACKLOG,
        };
    }

    pub fn deinit(_: *Config) void {}
};

pub const Connection = struct {
    socket: *Tcp,
    transport: *LibuvTransport,

    pub fn init(socket: *Tcp, transport: *LibuvTransport) Connection {
        return Connection{
            .socket = socket,
            .transport = transport,
        };
    }

    pub fn deinit(_: *Connection) void {}
};

pub const LibuvTransport = struct {
    loop: Loop,
    servers: struct {
        mutex: std.Thread.Mutex,
        listeners: std.StringHashMap(Tcp),
    },
    clients: struct {
        mutex: std.Thread.Mutex,
        sockets: std.ArrayList(Tcp),
    },
    config: Config,
    allocator: Allocator,
    sig: ?Signal,

    pub fn init(allocator: Allocator, config: Config) !LibuvTransport {
        // const loop = try Loop.init(allocator);
        const loop_ptr = try allocator.create(c.uv_loop_t);
        const init_result = c.uv_loop_init(loop_ptr);
        if (init_result < 0) return error.LoopInitError;

        const loop = Loop{ .loop = loop_ptr };

        std.debug.print("Created new loop: {*}\n", .{loop.loop});
        const provider = LibuvTransport{
            .loop = loop,
            .servers = .{
                .mutex = .{},
                .listeners = std.StringHashMap(Tcp).init(allocator),
            },
            .clients = .{
                .mutex = .{},
                .sockets = std.ArrayList(Tcp).init(allocator),
            },
            .config = config,
            .allocator = allocator,
            .sig = null,
        };
        return provider;
    }

    pub fn deinit(self: *LibuvTransport) void {
        self.servers.mutex.lock();
        var iter = self.servers.listeners.iterator();
        while (iter.next()) |l| {
            self.allocator.free(l.key_ptr.*);
            l.value_ptr.deinit(self.allocator);
        }
        self.servers.listeners.deinit();
        self.servers.mutex.unlock();

        self.clients.mutex.lock();
        for (self.clients.sockets.items) |*s| {
            s.deinit(self.allocator);
        }
        self.clients.sockets.deinit();
        self.clients.mutex.unlock();

        self.config.deinit();
        if (self.sig != null) {
            self.sig.?.deinit(self.allocator);
        }
        self.loop.deinit(self.allocator);
    }

    pub fn listen(self: *LibuvTransport, addr: *const Multiaddr) !void {
        // var sig = try Signal.init(self.loop, self.allocator);
        // std.debug.print("Created signal handle in loop: {*}\n", .{self.loop.loop});
        //
        // try sig.start(@as(c_int, std.posix.SIG.INT), onSignalReceived);
        // self.sig = sig;
        // sig.setData(self);

        const sa = try multiaddrToSockaddr(addr);
        var tcp = try Tcp.init(self.loop, self.allocator);
        tcp.setData(self);
        try tcp.bind(sa);
        try tcp.listen(self.config.backlog, onPeerConnected);

        self.servers.mutex.lock();
        defer self.servers.mutex.unlock();
        try self.servers.listeners.put(try addr.toString(self.allocator), tcp);
    }

    pub fn dial(self: *LibuvTransport, addr: *const Multiaddr) !void {
        const sa = try multiaddrToSockaddr(addr);
        var tcp = try Tcp.init(self.loop, self.allocator);
        var conn_req = try ConnectReq.init(self.allocator);
        tcp.setData(self);
        try tcp.connect(&conn_req, sa, onConnect);
    }

    pub fn start(self: *LibuvTransport) !void {
        const status = try self.loop.run(.default);
        if (status != 0) {
            std.debug.print("failed to start signal: {}\n", .{status});
        }
    }

    fn onSignalReceived(sig: *Signal, signum: c_int) void {
        std.debug.print("received signal: {}\n", .{signum});
        const transport = sig.getData(LibuvTransport).?;

        transport.close();
        sig.close(onSignalClosed);
    }

    fn onSignalClosed(_: *Signal) void {}

    fn onConnect(conn_req: *ConnectReq, status: i32) void {
        const client = conn_req.handle(Tcp).?;
        const transport = client.getData(LibuvTransport).?;
        defer conn_req.deinit(transport.allocator);
        if (status != 0) {
            std.debug.print("Connection failed\n", .{});
            client.close(onConnectErrClose);
            return;
        }

        transport.clients.mutex.lock();
        defer transport.clients.mutex.unlock();
        transport.clients.sockets.append(client) catch |err| {
            std.debug.print("failed to create socket: {}\n", .{err});
            client.close(onConnectErrClose);
            return;
        };

        var write_req = WriteReq.init(transport.allocator) catch |err| {
            std.debug.print("failed to create write request: {}\n", .{err});
            return;
        };
        const data = "hello world";
        const bufs: []const []const u8 = &[_][]const u8{data};
        std.debug.print("writing: {s}\n", .{data});
        client.write(write_req, bufs, onRequestWrite) catch |err| {
            std.debug.print("failed to write to socket: {}\n", .{err});
            write_req.deinit(transport.allocator);
            return;
        };
        std.debug.print("wrote: {s}\n", .{data});
        // client.readStart(onAlloc, onClientSocketRead) catch |err| {
        //     std.debug.print("failed to start reading: {}\n", .{err});
        //     client.close(onClientClose);
        //     return;
        // };
    }

    fn onClientSocketRead(_: *Tcp, nread: isize, buf: []const u8) void {
        if (nread > 0) {
            std.debug.print("read: {s}\n", .{buf});
        }
    }

    fn onClientClose(client: *Tcp) void {
        if (client.getData(Connection)) |conn| {
            conn.deinit();
            conn.transport.allocator.destroy(conn);
        }
    }

    fn onConnectErrClose(_: *Tcp) void {}

    fn onListenerClose(_: *Tcp) void {}

    fn serverCloseErrCallback(_: *Tcp) void {}

    fn onPeerConnected(server: *Tcp, status: i32) void {
        const transport = server.getData(LibuvTransport).?;
        if (status != 0) {
            std.debug.print("failed to listen on socket: {}\n", .{status});
            return;
        }

        var client = Tcp.init(server.loop(), transport.allocator) catch |err| {
            std.debug.print("failed to create client socket: {}\n", .{err});
            return;
        };

        server.accept(&client) catch |err| {
            std.debug.print("failed to accept connection: {}\n", .{err});
            client.close(onClientClose);
            client.deinit(transport.allocator);
            return;
        };

        transport.clients.mutex.lock();
        transport.clients.sockets.append(client) catch |err| {
            std.debug.print("failed to create socket: {}\n", .{err});
            client.close(onClientClose);
            client.deinit(transport.allocator);
            return;
        };
        transport.clients.mutex.unlock();

        const conn = transport.allocator.create(Connection) catch |err| {
            std.debug.print("failed to create connection: {}\n", .{err});
            client.close(onClientClose);
            client.deinit(transport.allocator);
            return;
        };
        conn.* = Connection.init(&client, transport);
        client.setData(conn);
        std.debug.print("accepted connection\n", .{});
        client.readStart(onAlloc, onServerSocketRead) catch |err| {
            std.debug.print("failed to start reading: {}\n", .{err});
            client.close(onClientClose);
            client.deinit(transport.allocator);
            return;
        };
    }

    fn onAlloc(tcp: *Tcp, size: usize) ?[]u8 {
        std.debug.print("allocating buffer {d}\n", .{size});
        const conn = tcp.getData(Connection).?;
        return conn.transport.allocator.alloc(u8, size) catch |err| {
            std.debug.print("failed to allocate buffer: {}\n", .{err});
            return null;
        };
    }

    fn onServerSocketRead(client: *Tcp, nread: isize, buf: []const u8) void {
        const conn = client.getData(Connection).?;
        defer conn.transport.allocator.free(buf);
        if (nread < 0) {
            client.close(onClientClose);
            conn.transport.clients.mutex.lock();
            for (conn.transport.clients.sockets.items, 0..) |s, i| {
                if (std.meta.eql(s, client.*)) {
                    _ = conn.transport.clients.sockets.swapRemove(i);
                    break;
                }
            }
            conn.transport.clients.mutex.unlock();
            return;
        }

        if (nread == 0) {
            return;
        }

        const data = buf[0..@as(usize, @intCast(nread))];
        std.debug.print("received: {s}\n", .{data});
        var write_req = WriteReq.init(conn.transport.allocator) catch |err| {
            std.debug.print("failed to create write request: {}\n", .{err});
            return;
        };
        const bufs: []const []const u8 = &[_][]const u8{data};
        client.write(write_req, bufs, onResponseWrite) catch |err| {
            std.debug.print("failed to write to socket: {}\n", .{err});
            write_req.deinit(conn.transport.allocator);
            return;
        };
        std.debug.print("response: {s}\n", .{data});
    }

    fn onResponseWrite(req: *WriteReq, status: i32) void {
        const conn = req.handle(Tcp).?.getData(Connection).?;
        defer req.deinit(conn.transport.allocator);
        if (status != 0) {
            std.debug.print("failed to write to socket: {}\n", .{status});
            return;
        }
    }

    fn onRequestWrite(req: *WriteReq, status: i32) void {
        const transport = req.handle(Tcp).?.getData(LibuvTransport).?;
        defer req.deinit(transport.allocator);
        if (status != 0) {
            std.debug.print("failed to write to socket: {}\n", .{status});
            return;
        }
    }

    fn close(self: *LibuvTransport) void {
        self.clients.mutex.lock();
        for (self.clients.sockets.items) |*s| {
            std.debug.print("closing client\n", .{});
            s.close(onClientClose);
        }
        self.clients.mutex.unlock();

        std.posix.timespec
            .self.servers.mutex.lock();
        defer self.servers.mutex.unlock();
        var iter = self.servers.listeners.valueIterator();
        while (iter.next()) |l| {
            l.close(onListenerClose);
        }
    }

    fn clientClose(self: *LibuvTransport) void {
        self.clients.mutex.lock();
        for (self.clients.sockets.items) |*s| {
            std.debug.print("CLIENT closing client\n", .{});
            s.close(onClientClose);
        }
        self.clients.mutex.unlock();
        std.debug.print("CLIENT closed\n", .{});
    }

    fn walkCallback(handle: ?*c.uv_handle_t, _: ?*anyopaque) callconv(.C) void {
        if (handle) |h| {
            const handle_type = h.*.type;
            const is_closing = c.uv_is_closing(h) != 0;
            const handle_data = h.*.data;
            std.debug.print("Found handle type: {}, is_closing: {}, data: {*}\n", .{
                handle_type,
                is_closing,
                handle_data,
            });

            if (!is_closing) {
                c.uv_close(h, null);
            }
        }
    }

    pub fn closeClient(self: *LibuvTransport) void {
        c.uv_walk(self.loop.loop, walkCallback, null);
    }

    fn multiaddrToSockaddr(addr: *const Multiaddr) !std.net.Address {
        var port: ?u16 = null;
        var address = addr.*;

        while (try address.pop()) |proto| {
            switch (proto) {
                .Ip4 => |*ipv4| {
                    if (port) |p| {
                        const mutable_ipv4 = @as(*std.net.Ip4Address, @constCast(ipv4));
                        mutable_ipv4.setPort(p);
                        return .{ .in = ipv4.* };
                    } else {
                        return error.MissingPort;
                    }
                },
                .Ip6 => |*ipv6| {
                    if (port) |p| {
                        const mutable_ipv6 = @as(*std.net.Ip6Address, @constCast(ipv6));
                        mutable_ipv6.setPort(p);
                        return .{ .in6 = ipv6.* };
                    } else {
                        return error.MissingPort;
                    }
                },
                .Tcp => |portnum| {
                    if (port != null) {
                        return error.DuplicatePort;
                    }
                    port = portnum;
                },
                // TODO: Add support for other protocols
                // .P2p => {},
                else => return error.UnsupportedProtocol,
            }
        }
        return error.InvalidAddress;
    }
};

test "multiaddr_to_sockaddr converts valid IPv4 address" {
    const allocator = testing.allocator;

    // Create a multiaddr for "127.0.0.1:8080"
    var addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/tcp/8080");
    defer addr.deinit();

    const sockaddr = try LibuvTransport.multiaddrToSockaddr(&addr);

    var buf: [22]u8 = undefined;
    const addr_str = try std.fmt.bufPrint(&buf, "{}", .{sockaddr});
    try testing.expectEqualStrings("127.0.0.1:8080", addr_str);
}

// test "transport_listen_accept" {
//     const allocator = testing.allocator;
//
//     const config = Config.init();
//     var transport = try LibuvTransport.init(allocator, config);
//
//     const addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/tcp/0");
//     defer addr.deinit();
//
//     try transport.listen(&addr);
//
//     try uv.convertError(c.uv_kill(c.uv_os_getpid(), @as(c_int, std.posix.SIG.INT)));
//
//     try transport.start();
//     transport.deinit();
// }

test "transport_dial" {
    const allocator = testing.allocator;
    const config = Config.init();

    // Start server
    var server_transport = try LibuvTransport.init(allocator, config);
    const addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/tcp/8081");
    defer addr.deinit();
    try server_transport.listen(&addr);

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(t: *LibuvTransport) !void {
            try t.start();
        }
    }.run, .{&server_transport});

    // Give server time to start
    std.time.sleep(std.time.ns_per_ms * 100);

    // Start client
    var client_transport = try LibuvTransport.init(allocator, config);
    try client_transport.dial(&addr);

    const client_thread = try std.Thread.spawn(.{}, struct {
        fn run(t: *LibuvTransport) !void {
            try t.start();
            // t.close();
        }
    }.run, .{&client_transport});

    // Let connection establish
    std.time.sleep(std.time.ns_per_ms * 500);

    // First stop client
    // client_transport.directShutdown();
    client_transport.closeClient();
    client_thread.join();
    client_transport.deinit();

    // Then stop server
    // try uv.convertError(c.uv_kill(c.uv_os_getpid(), @as(c_int, std.posix.SIG.INT)));
    server_transport.close();
    server_thread.join();

    server_transport.deinit();
}
