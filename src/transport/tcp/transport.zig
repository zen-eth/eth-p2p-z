const std = @import("std");
const xev_tcp = @import("xev.zig");
const conn = @import("../../conn.zig");

pub const Transport = union(enum) {
    xev: xev_tcp.Transport,

    pub const DialError = xev_tcp.Transport.DialError || error{};

    pub fn dial(self: *Transport, addr: std.net.Address, connection: anytype) DialError!void {
        switch (self.*) {
            .xev => |*x| return x.dial(addr, connection),
        }
    }
};
