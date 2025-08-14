const p2p_conn = @import("../conn.zig");
const libp2p = @import("../root.zig");
const ProtocolId = libp2p.protocols.ProtocolId;
const ProtocolDescriptor = @import("./protocol_descriptor.zig").ProtocolDescriptor;
const std = @import("std");

pub const ProtocolBindingVTable = struct { initConnFn: *const fn (instance: *anyopaque, conn: p2p_conn.AnyConn, protocol_id: ProtocolId, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void) void, protoDescFn: *const fn (instance: *anyopaque) *ProtocolDescriptor };

pub const AnyProtocolBinding = struct {
    vtable: *const ProtocolBindingVTable,
    instance: *anyopaque,

    const Self = @This();
    pub const Error = anyerror;

    pub fn initConn(
        self: *const Self,
        conn: p2p_conn.AnyConn,
        protocol_id: ProtocolId,
        user_data: ?*anyopaque,
        callback: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void,
    ) void {
        self.vtable.initConnFn(self.instance, conn, protocol_id, user_data, callback);
    }

    pub fn protoDesc(self: *const Self) *ProtocolDescriptor {
        return self.vtable.protoDescFn(self.instance);
    }
};
