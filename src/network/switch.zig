const std = @import("std");
const zaio = @import("../transport/zigaio.zig");
const Alloc = std.mem.Allocator;

pub const Switch = struct {
    listeners: struct {
        mutex: std.Thread.Mutex,
        m: std.AutoHashMap(std.posix.socket_t, void),
    },
    transports: struct {
        mutex: std.Thread.Mutex,
        m: std.AutoHashMap(u32, zaio.ZigAIOTransport),
    },
    alloc: Alloc,

    pub fn init(alloc: Alloc) !Switch {
        var transports_map = std.AutoHashMap(u32, zaio.ZigAIOTransport).init(alloc);
        try transports_map.put(6, zaio.ZigAIOTransport{
            .options = zaio.Options{
                .backlog = 128,
            },
        });
        return Switch{
            .alloc = alloc,
            .listeners = .{
                .mutex = .{},
                .m = std.AutoHashMap(std.posix.socket_t, void).init(alloc),
            },
            .transports = .{
                .mutex = .{},
                .m = transports_map,
            },
        };
    }

    pub fn Listen(self: *Switch, addresses: std.ArrayList(std.net.Address)) !void {
        for (addresses.items) |address| {
            var transport = self.transport_for_listener(address) orelse return error.TransportNotSupported;
            var server_socket: std.posix.socket_t = undefined;
            try transport.listen(address, &server_socket);
            self.listeners.mutex.lock();
            defer self.listeners.mutex.unlock();
            try self.listeners.m.put(server_socket, {});
        }
    }

    fn transport_for_listener(self: *Switch, _: std.net.Address) ?zaio.ZigAIOTransport {
        self.transports.mutex.lock();
        defer self.transports.mutex.unlock();
        return self.transports.m.get(6);
    }
};
