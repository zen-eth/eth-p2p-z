const p2p_conn = @import("../conn.zig");
pub const Upgrader = struct {
    pub fn initConn(self: *Upgrader, conn: p2p_conn.AnyRxConn) !void {}
};
