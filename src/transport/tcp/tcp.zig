const std = @import("std");
const Multiaddr = @import("multiformats-zig").multiaddr.Multiaddr;
const libuv_transport = @import("../../transport/libuv.zig");
const LibuvTransport = libuv_transport.LibuvTransport;
const Connection = @import("../../network/network.zig").Connection;
const Tcp = @import("../../transport/libuv.zig").Tcp;


pub const Transport = union(enum) {
    libuvTransport: LibuvTransport,

    pub fn listen(self: *Transport, addr: *const Multiaddr, comptime cb: fn (*Connection) void) !void {
        return switch (self.*) {
            .libuvTransport => |transport| transport.listen(addr, libuv_transport.DEFAULT_BACKLOG, transport.wrap_on_peer_connect(cb)),
        };
    }


};

// pub const Transport = struct {
//     pub usingnamespace LibuvTransport;
//     // config: Config,
//     // impl: Provider,
//     //
//     // const Self = @This();
//     //
//     // pub fn init(config: *const Config) !Self {
//     //     var transport = try config.allocator.create(Self);
//     //     transport.* = .{
//     //         .config = config,
//     //         .impl = switch (config.backend) {
//     //             .libuv => try LibuvImpl.init(config.allocator),
//     //             .std => try StdImpl.init(config.allocator),
//     //         },
//     //     };
//     //     return transport;
//     // }
//     //
//     // pub fn listen(self: *Self, addr: Multiaddr) !void {
//     //     return self.impl.listen(addr);
//     // }
//     //
//     // pub fn dial(self: *Self, addr: Multiaddr) !void {
//     //     return self.impl.dial(addr);
//     // }
//     //
//     // pub fn deinit(self: *Self) void {
//     //     self.impl.deinit();
//     // }
// };

// const Provider = union(ProviderType) {
//     libuv: LibuvImpl,
//
//     pub fn listen(self: Provider, addr: multiaddr.Multiaddr) !void {
//         return switch (self) {
//             .libuv => |impl| impl.listen(addr),
//         };
//     }
//
//     pub fn dial(self: Provider, addr: multiaddr.Multiaddr) !void {
//         return switch (self) {
//             .libuv => |impl| impl.dial(addr),
//         };
//     }
//
//     pub fn deinit(self: Provider) void {
//         switch (self) {
//             .libuv => |impl| impl.deinit(),
//         }
//     }
// };
