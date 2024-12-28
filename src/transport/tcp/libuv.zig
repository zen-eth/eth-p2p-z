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

    pub fn listen(self: *LibuvProvider, addr: *const Multiaddr) !void {
        std.debug.print("Listening on {any}\n", .{addr});
        const sa = try uv.SocketAddress;
        self.tcp.Bind(&sa, false) catch unreachable;
        self.tcp.GetStream().Listen(128, null) catch unreachable;
    }

    fn multiaddr_to_sockaddr(addr: *Multiaddr) !uv.SocketAddress {
        var port: ?u16 = null;
        var address = addr.*;

        while (try address.pop()) |proto| {
            switch (proto) {
                .Ip4 => |ipv4| {
                    if (port) |p| {
                        const bytes = @as(*const [4]u8, @ptrCast(ipv4.sa.addr));
                        const sockaddr = try uv.SocketAddress.FromString(bytes, p);
                        return sockaddr;
                    } else {
                        return error.MissingPort;
                    }
                },
                .Ip6 => |ipv6| {
                    if (port) |p| {
                        const bytes = @as(*const [16]u8, @ptrCast(ipv6.sa.addr));
                        const sockaddr = try uv.SocketAddress.FromString(bytes, p);
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
                .P2p => {},
                else => return error.UnsupportedProtocol,
            }
        }
        return error.InvalidAddress;
    }
};
