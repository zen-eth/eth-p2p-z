const std = @import("std");
const xev_transport = @import("libxev.zig");
const conn = @import("../../conn.zig");

pub const Transport = union(enum) {
    xev: xev_transport.XevTransport,

    pub const DialError = xev_transport.DialError;
    // pub fn dial(self:*Transport, addr:std.net.Address, connection:*conn.AnyConn) DialError!void {
    //     return self.xev.dial(addr, connection);
    // }
};
