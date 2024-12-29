const std = @import("std");
const uv = @import("libuv");
const Allocator = @import("std").mem.Allocator;
const Multiaddr = @import("multiformats-zig").multiaddr.Multiaddr;

pub const LibuvProvider = struct {
    loop: *uv.Loop,
    tcp: *uv.Tcp,
    allocator: Allocator,

    pub fn init(allocator: Allocator) !LibuvProvider {
        const tcp: *uv.Tcp = undefined;
        const loop: *uv.Loop = undefined;
        try loop.Init();
        try tcp.Init(loop);
        const provider = LibuvProvider{
            .loop = loop,
            .tcp = tcp,
            .allocator = allocator,
        };
        return provider;
    }

    pub fn listen(self: *LibuvProvider, addr: *Multiaddr) !void {
        const sa = try multiaddr_to_sockaddr(addr);
        self.tcp.Bind(&sa, false) catch unreachable;
        const callback: uv.Stream.Callback.Connection = MyConnectionCallback;
        self.tcp.GetStream().Listen(128, callback) catch unreachable;
    }

    fn multiaddr_to_sockaddr(addr: *Multiaddr) !uv.SocketAddress {
        var port: ?u16 = null;
        var address = addr.*;

        while (try address.pop()) |proto| {
            switch (proto) {
                .Ip4 => |ipv4| {
                    if (port) |p| {
                        const addr_str: [:0]const u8 = std.mem.span(@as([*:0]const u8, @ptrCast(&ipv4.sa.addr)));
                        const sockaddr = try uv.SocketAddress.FromString(addr_str, p);
                        return sockaddr;
                    } else {
                        return error.MissingPort;
                    }
                },
                .Ip6 => |ipv6| {
                    if (port) |p| {
                        const addr_str: [:0]const u8 = std.mem.span(@as([*:0]const u8, @ptrCast(&ipv6.sa.addr)));
                        const sockaddr = try uv.SocketAddress.FromString(addr_str, p);
                        return sockaddr;
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

// test "multiaddr_to_sockaddr converts valid IPv4 address" {
//     const allocator = testing.allocator;
//
//     // Create a multiaddr for "127.0.0.1:8080"
//     var addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/tcp/8080");
//     defer addr.deinit();
//
//     const provider = try LibuvProvider.init(allocator);
//
//     const sockaddr = try provider.multiaddr_to_sockaddr(&addr);
//
//     var buf: [22]u8 = undefined;
//     const addr_str = try std.fmt.bufPrint(&buf, "{}", .{sockaddr});
//
//     try testing.expectEqualStrings("127.0.0.2:8080", addr_str);
// }

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
