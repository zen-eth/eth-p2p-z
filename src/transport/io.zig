const std = @import("std");
const uv = @import("libuv");
const multiformats = @import("multiformats-zig");
const Multiaddr = multiformats.multiaddr.Multiaddr;

pub const IoError = error{
    InitFailed,
    BindFailed,
    ConnectFailed,
    ReadFailed,
    WriteFailed,
};

pub const IoBackend = enum {
    libuv,
};

pub const IoStreamType = enum {
    tcp,
};

pub const IoStream = struct {
    impl: union(IoStreamType) {
        tcp: struct {
            handle: switch (IoBackend) {
                .libuv => uv.Tcp,
            },
        },
    },

    // pub fn init(loop: IoLoop, stream_type: IoStreamType) !IoStream {
    //     return switch (stream_type) {
    //         .tcp => {
    //             var tcp_handle: uv.Tcp = undefined;
    //             switch (loop.impl) {
    //                 .libuv => |uv_loop| try tcp_handle.Init(uv_loop),
    //             }
    //             return IoStream{ .impl = .{ .tcp = .{ .handle = tcp_handle } } };
    //         },
    //     };
    // }
    //
    // pub fn connect(self: *IoStream, addr: Multiaddr) !void {
    //     switch (self.impl) {
    //         .tcp => |*t| {
    //             var connect_req = uv.Request.Connect{};
    //             const socket_addr = try addr.toSocketAddr();
    //             try t.handle.Connect(&connect_req, &socket_addr, null);
    //         },
    //         .udp => return error.UnsupportedOperation,
    //     }
    // }
    //
    // pub fn bind(self: *IoStream, addr: Multiaddr) !void {
    //     switch (self.impl) {
    //         .tcp => |*t| {
    //             const socket_addr = try addr.toSocketAddr();
    //             try t.handle.Bind(&socket_addr, false);
    //         },
    //         .udp => |*u| {
    //             const socket_addr = try addr.toSocketAddr();
    //             try u.handle.Bind(&socket_addr, false);
    //         },
    //     }
    // }
    //
    // pub fn listen(self: *IoStream, backlog: u32) !void {
    //     switch (self.impl) {
    //         .tcp => |*t| try t.handle.GetStream().Listen(backlog, null),
    //         .udp => return error.UnsupportedOperation,
    //     }
    // }
    //
    // pub const ReadError = error{
    //     ReadFailed,
    //     Closed,
    // };
    //
    // pub const WriteError = error{
    //     WriteFailed,
    //     Closed,
    // };
    //
    // pub fn read(self: *IoStream, buffer: []u8) !usize {
    //     switch (self.impl) {
    //         .tcp => |t| {
    //             const nread = try t.handle.GetStream().read(buffer);
    //             if (nread < 0) return ReadError.ReadFailed;
    //             return @intCast(usize, nread);
    //         },
    //         .udp => return error.UnsupportedOperation,
    //     }
    // }
    //
    // pub fn write(self: *IoStream, data: []const u8) !void {
    //     switch (self.impl) {
    //         .tcp => |t| {
    //             const write_req = try t.handle.GetStream().write(data);
    //             if (write_req < 0) return WriteError.WriteFailed;
    //         },
    //         .udp => return error.UnsupportedOperation,
    //     }
    // }
    //
    // pub fn getLocalAddr(self: *IoStream) Multiaddr {
    //     return switch (self.impl) {
    //         .tcp => |t| Multiaddr.fromSocketAddr(t.handle.GetSockName()),
    //         .udp => |u| Multiaddr.fromSocketAddr(u.handle.GetSockName()),
    //     };
    // }
    //
    // pub fn getRemoteAddr(self: *IoStream) MultiAddr {
    //     return switch (self.impl) {
    //         .tcp => |t| MultiAddr.fromSocketAddr(t.handle.GetPeerName()),
    //         .udp => |u| MultiAddr.fromSocketAddr(u.handle.GetPeerName()),
    //     };
    // }
    //
    // pub fn isConnected(self: *IoStream) bool {
    //     return switch (self.impl) {
    //         .tcp => |t| t.handle.isConnected(),
    //         .udp => true, // UDP is connectionless, so it's always "connected"
    //     };
    // }
};
pub const IoLoop = struct {
    impl: union(IoBackend) {
        libuv: *uv.Loop,
    },

    pub fn run(self: *IoLoop) !void {
        switch (self.impl) {
            .libuv => |loop| {
                try loop.Run(.Default);
            },
        }
    }
};
