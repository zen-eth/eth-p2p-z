const std = @import("std");
const Multiaddr = @import("multiformats-zig").multiaddr.Multiaddr;

pub const ProviderType = enum {
    libuv,
    // zig_std,
    // Add more backends as needed
};

pub const Config = struct {
    provider_type: ProviderType,
};

pub const Transport = struct {
    config: Config,
    impl: Provider,

    const Self = @This();

    pub fn init(config: *const Config) !Self {
        var transport = try config.allocator.create(Self);
        transport.* = .{
            .config = config,
            .impl = switch (config.backend) {
                .libuv => try LibuvImpl.init(config.allocator),
                .std => try StdImpl.init(config.allocator),
            },
        };
        return transport;
    }

    pub fn listen(self: *Self, addr: Multiaddr) !void {
        return self.impl.listen(addr);
    }

    pub fn dial(self: *Self, addr: Multiaddr) !void {
        return self.impl.dial(addr);
    }

    pub fn deinit(self: *Self) void {
        self.impl.deinit();
    }
};

const Provider = union(ProviderType) {
    libuv: LibuvImpl,

    pub fn listen(self: Provider, addr: multiaddr.Multiaddr) !void {
        return switch (self) {
            .libuv => |impl| impl.listen(addr),
        };
    }

    pub fn dial(self: Provider, addr: multiaddr.Multiaddr) !void {
        return switch (self) {
            .libuv => |impl| impl.dial(addr),
        };
    }

    pub fn deinit(self: Provider) void {
        switch (self) {
            .libuv => |impl| impl.deinit(),
        }
    }
};
