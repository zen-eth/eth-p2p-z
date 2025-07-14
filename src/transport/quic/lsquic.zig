const std = @import("std");
const c = @cImport({
    @cInclude("lsquic.h");
    @cInclude("lsquic_types.h");
    @cInclude("lsxpack_header.h");
});

pub const LsquicTransport = struct {
    pub fn init() void {
        // Initialize the QUIC transport layer
        const result = c.lsquic_global_init(c.LSQVER);
        if (result != 0) {
            std.debug.print("Failed to initialize lsquic: {}\n", .{result});
            return;
        }
        std.debug.print("lsquic initialized successfully.\n", .{});
    }

    pub fn cleanup() void {
        // Cleanup the QUIC transport layer
        c.lsquic_global_cleanup();
        std.debug.print("lsquic cleaned up successfully.\n", .{});
    }

    pub fn dial(self: *LsquicTransport, addr: std.net.Address, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void) void {

    }
};