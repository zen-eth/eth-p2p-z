const std = @import("std");
const uv = @import("libuv");
const Loop = uv.Loop;
const Tcp = uv.Tcp;
const ConnectReq = Tcp.ConnectReq;
const Signal = uv.Signal;
const c = uv.c;
const Allocator = @import("std").mem.Allocator;
const Multiaddr = @import("multiformats-zig").multiaddr.Multiaddr;
const Transport = @import("../transport/transport.zig").Transport;

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
    sig: Signal,

    pub fn init(allocator: Allocator, config: Config) !LibuvTransport {
        const loop = try Loop.init(allocator);

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
            .sig = undefined,
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
        self.sig.deinit(self.allocator);
        self.loop.deinit(self.allocator);
    }

    pub fn listen(self: *LibuvTransport, addr: *const Multiaddr) !void {
        var sig = try Signal.init(self.loop, self.allocator);
        try sig.start(@as(c_int, std.posix.SIG.INT), onSignalReceived);
        self.sig = sig;
        sig.setData(self);

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

    fn onSignalReceived(sig: *Signal, _: c_int) void {
        const transport = sig.getData(LibuvTransport).?;

        transport.serverClose();
        sig.close(onSignalClosed);
    }

    fn onSignalClosed(_: *Signal) void {}

    fn onConnect(_: *ConnectReq, _: i32) void {
        // const client = conn_req.handle(Tcp).?;
        // const transport = client.getData(LibuvTransport).?;
        // if (status != 0) {
        //     std.debug.print("Connection failed\n", .{});
        //     const is_active = client.isActive() catch |err| {
        //         std.debug.print("failed to check if client is active: {}\n", .{err});
        //         return;
        //     };
        //     if (is_active) {
        //         client.close(onClientClose);
        //     }
        //     conn_req.deinit(transport.allocator);
        //     return;
        // }
        //
        // transport.clients.mutex.lock();
        // defer transport.clients.mutex.unlock();
        // std.debug.print("received signal5555\n", .{});
        // transport.clients.sockets.append(client) catch |err| {
        //     std.debug.print("failed to create socket: {}\n", .{err});
        //     client.close(onClientClose);
        //     conn_req.deinit(transport.allocator);
        //     return;
        // };
        //
        // std.Thread.sleep(std.time.ns_per_s * 10);
        // client.close(onClientClose);
        // conn_req.deinit(transport.allocator);
    }

    fn onClientClose(client: *Tcp) void {
        if (client.getData(Connection)) |conn| {
            conn.deinit();
        }
    }

    fn serverCloseErrCallback(_: *Tcp) void {}

    fn onPeerConnected(server: *Tcp, status: i32) void {
        const transport = server.getData(LibuvTransport).?;
        if (status != 0) {
            // std.debug.print("failed to listen on socket: {}\n", .{status});
            return;
        }

        var client = Tcp.init(server.loop(), transport.allocator) catch {
            // std.debug.print("failed to create client socket: {}\n", .{err});
            return;
        };

        server.accept(&client) catch {
            // std.debug.print("failed to accept connection: {}\n", .{err});
            client.close(onClientClose);
            client.deinit(transport.allocator);
            return;
        };

        transport.clients.mutex.lock();
        transport.clients.sockets.append(client) catch {
            // std.debug.print("failed to create socket: {}\n", .{err});
            client.close(onClientClose);
            client.deinit(transport.allocator);
            return;
        };
        transport.clients.mutex.unlock();

        var conn = Connection.init(&client, transport);
        client.setData(&conn);
        client.readStart(allocCallback, serverSocketReadCallback) catch {
            // std.debug.print("failed to start reading: {}\n", .{err});
            client.close(onClientClose);
            client.deinit(transport.allocator);
            return;
        };
    }

    fn allocCallback(tcp: *Tcp, _: usize) ?[]u8 {
        _ = tcp.getData(Connection).?;
        return null;
        // return conn.transport.allocator.alloc(u8, size) catch  {
        //     // std.debug.print("failed to allocate buffer: {}\n", .{err});
        //     return null;
        // };
    }

    fn serverSocketReadCallback(client: *Tcp, nread: isize, buf: []const u8) void {
        const conn = client.getData(Connection).?;
        if (nread < 0) {
            client.close(onClientClose);
            client.deinit(conn.transport.allocator);
            return;
        }

        if (nread == 0) {
            return;
        }

        _ = buf[0..@as(usize, @intCast(nread))];
        conn.transport.allocator.free(buf);
    }

    fn serverClose(self: *LibuvTransport) void {
        self.clients.mutex.lock();
        for (self.clients.sockets.items) |*s| {
            s.close(onClientClose);
            // s.deinit(self.allocator);
        }
        self.clients.mutex.unlock();

        self.servers.mutex.lock();
        defer self.servers.mutex.unlock();
        var iter = self.servers.listeners.valueIterator();
        while (iter.next()) |l| {
            l.close(null);
            // l.deinit(self.allocator);
        }
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

const testing = std.testing;

// test "multiaddr_to_sockaddr converts valid IPv4 address" {
//     const allocator = testing.allocator;
//
//     // Create a multiaddr for "127.0.0.1:8080"
//     var addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/tcp/8080");
//     defer addr.deinit();
//
//     const sockaddr = try LibuvTransport.multiaddrToSockaddr(&addr);
//
//     var buf: [22]u8 = undefined;
//     const addr_str = try std.fmt.bufPrint(&buf, "{}", .{sockaddr});
//     try testing.expectEqualStrings("127.0.0.1:8080", addr_str);
// }

test "transport_listen_accept" {
    const allocator = testing.allocator;

    const config = Config.init();
    var transport = try LibuvTransport.init(allocator, config);

    const addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/tcp/0");
    defer addr.deinit();

    try transport.listen(&addr);

    try uv.convertError(c.uv_kill(c.uv_os_getpid(), @as(c_int, std.posix.SIG.INT)));

    _ = try transport.loop.run(.default);
    transport.deinit();
}
