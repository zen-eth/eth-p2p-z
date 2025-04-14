const std = @import("std");
const xev_provider = @import("xev.zig");
const conn = @import("../../conn.zig");

pub const Transport = union(enum) {
    xev: xev_provider.Transport,

    pub const DialError = xev_provider.DialError;
    // pub fn dial(self:*Transport, addr:std.net.Address, connection:*conn.AnyConn) DialError!void {
    //     return self.xev.dial(addr, connection);
    // }
};
