const std = @import("std");
const uv = @import("libuv");
const Loop = uv.Loop;
const Tcp = uv.Tcp;
const Allocator = @import("std").mem.Allocator;
const Multiaddr = @import("multiformats-zig").multiaddr.Multiaddr;
const Transport = @import("../transport/transport.zig").Transport;

pub const DEFAULT_BACKLOG = 128;

pub const Config = struct {
    backlog: i32 = 128,
};

pub const Connection = struct {
    socket: *Tcp,
    transport: *LibuvTransport,
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

    pub fn init(allocator: Allocator, config: Config) !LibuvTransport {
        const provider = LibuvTransport{
            .loop = try Loop.init(allocator),
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
        };
        return provider;
    }

    pub fn listen(self: *LibuvTransport, addr: *const Multiaddr) !void {
        const sa = try multiaddr_to_sockaddr(addr);
        var tcp = try Tcp.init(self.loop, self.allocator);
        tcp.setData(self);
        try tcp.bind(sa);
        try tcp.listen(self.config.backlog, create_listen_cb);

        self.servers.mutex.lock();
        defer self.servers.mutex.unlock();
        try self.servers.listeners.put(try addr.toString(self.allocator), tcp);
    }

    fn create_client_close_err_cb(_: *Tcp) void {}

    fn create_server_close_err_cb(_: *Tcp) void {}

    fn create_listen_cb(server: *Tcp, status: i32) void {
        const transport = server.getData(LibuvTransport).?;
        if (status != 0) {
            std.debug.print("failed to listen on socket: {}\n", .{status});
            server.close(create_server_close_err_cb);
            server.deinit(transport.allocator);
            return;
        }

        var client = Tcp.init(server.loop(), transport.allocator) catch |err| {
            std.debug.print("failed to create client socket: {}\n", .{err});
            server.close(create_server_close_err_cb);
            server.deinit(transport.allocator);
            return;
        };

        server.accept(&client) catch |err| {
            std.debug.print("failed to accept connection: {}\n", .{err});
            client.close(create_client_close_err_cb);
            client.deinit(transport.allocator);
            return;
        };

        transport.clients.mutex.lock();
        transport.clients.sockets.append(client) catch |err| {
            std.debug.print("failed to create socket: {}\n", .{err});
            client.close(create_client_close_err_cb);
            client.deinit(transport.allocator);
            return;
        };
        transport.clients.mutex.unlock();

        var conn = .{ .socket = client, .transport = transport };
        client.setData(&conn);
        client.readStart(create_alloc_cb, create_read_cb) catch |err| {
            std.debug.print("failed to start reading: {}\n", .{err});
            client.close(create_client_close_err_cb);
            client.deinit(transport.allocator);
            return;
        };
    }

    fn create_alloc_cb(tcp: *Tcp, size: usize) ?[]u8 {
        const conn = tcp.getData(Connection).?;
        return conn.transport.allocator.alloc(u8, size) catch |err| {
            std.debug.print("failed to allocate buffer: {}\n", .{err});
            return null;
        };
    }

    fn create_read_cb(client: *Tcp, nread: isize, buf: []const u8) void {
        const conn = client.getData(Connection).?;
        if (nread < 0) {
            client.close(create_client_close_err_cb);
            client.deinit(conn.transport.allocator);
            return;
        }

        if (nread == 0) {
            return;
        }

        const data = buf[0..@as(usize, @intCast(nread))];
        std.debug.print("received {d} bytes: {s}\n", .{ nread, data });
        conn.transport.allocator.free(buf);
    }

    fn multiaddr_to_sockaddr(addr: *const Multiaddr) !std.net.Address {
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

fn MyConnectionCallback(_: uv.Stream, _: ?uv.Stream.Error) void {
    // Handle connection result
    std.debug.print("Connection result\n", .{});
}

const testing = std.testing;

test "multiaddr_to_sockaddr converts valid IPv4 address" {
    const allocator = testing.allocator;

    // Create a multiaddr for "127.0.0.1:8080"
    var addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/tcp/8080");
    defer addr.deinit();

    const sockaddr = try LibuvTransport.multiaddr_to_sockaddr(&addr);

    var buf: [22]u8 = undefined;
    const addr_str = try std.fmt.bufPrint(&buf, "{}", .{sockaddr});

    try testing.expectEqualStrings("127.0.0.1:8080", addr_str);
}

// test "multiaddr_to_sockaddr handles invalid address" {
//     var allocator = testing.allocator;
//
//     // Create an invalid multiaddr without port
//     var addr = try Multiaddr.init(allocator, "/ip4/127.0.0.1");
//     defer addr.deinit();
//
//     try testing.expectError(error.MissingPort, multiaddr_to_sockaddr(&addr));
// }
//
// test "multiaddr_to_sockaddr handles p2p protocol" {
//     var allocator = testing.allocator;
//
//     // Create a multiaddr with p2p protocol
//     var addr = try Multiaddr.init(allocator, "/ip4/127.0.0.1/tcp/8080/p2p/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N");
//     defer addr.deinit();
//
//     const sockaddr = try multiaddr_to_sockaddr(&addr);
//
//     var buf: [22]u8 = undefined;
//     const addr_str = try std.fmt.bufPrint(&buf, "{}", .{sockaddr});
//
//     try testing.expectEqualStrings("127.0.0.1:8080", addr_str);
// }
