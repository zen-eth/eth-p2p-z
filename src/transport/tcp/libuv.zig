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
};
