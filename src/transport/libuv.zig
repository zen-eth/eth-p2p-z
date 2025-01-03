const std = @import("std");
const uv = @import("libuv");
const Loop = uv.Loop;
const Tcp = uv.Tcp;
const Allocator = @import("std").mem.Allocator;
const Multiaddr = @import("multiformats-zig").multiaddr.Multiaddr;

pub const Config = struct {
    backlog: i32 = 128,
};

pub const Connection = struct {
    inner: *Tcp,
};

pub const LibuvTransport = struct {
    loop: Loop,
    listeners: std.AutoHashMap(Multiaddr, Tcp),
    streams: std.ArrayList(Tcp),
    mutex: std.Thread.Mutex,
    allocator: Allocator,

    pub fn init(allocator: Allocator) !LibuvTransport {
        const provider = LibuvTransport{
            .loop = try Loop.init(allocator),
            .listeners = std.AutoHashMap(Multiaddr, Tcp).init(allocator),
            .streams = std.ArrayList(Tcp).init(allocator),
            .mutex = .{},
            .allocator = allocator,
        };
        return provider;
    }

    fn create_stream(self: *LibuvTransport) !Tcp {
        const tcp = try Tcp.init(self.loop, self.allocator);
        return tcp;
    }

    pub fn listen(self: *LibuvTransport, addr: *const Multiaddr, backlog: i32, comptime cb: fn (*Tcp, i32) void) !void {
        const sa = try multiaddr_to_sockaddr(addr);
        const tcp = try self.create_tcp();

        self.mutex.lock();
        self.streams.append(tcp);
        self.mutex.unlock();

        try tcp.bind(sa);
        try tcp.listen(backlog, cb);
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.listeners.put(addr, tcp);
    }

    fn on_stream_closed(_: *Tcp) void {}

    fn multiaddr_to_sockaddr(addr: *Multiaddr) !std.net.Address {
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
