const std = @import("std");
const xev_provider = @import("xev.zig");
const conn = @import("../../conn.zig");

pub const Transport = union(enum) {
    xev: xev_provider.Transport,

    pub const OpenConnectError = xev_provider.OpenConnectError;

    pub fn dial(self: *Transport, addr: std.net.Address, connection: anytype) OpenConnectError!void {
        return self.xev.dial(addr, connection);
    }
};
