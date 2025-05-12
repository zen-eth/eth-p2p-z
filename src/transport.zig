const std = @import("std");
const conn = @import("conn.zig");

/// Listener interface for accepting incoming connections.
pub const ListenerVTable = struct {
    acceptFn: *const fn (instance: *anyopaque, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!conn.AnyRxConn) void) void,
};

/// AnyListener is a struct that uses the VTable pattern to provide a type-erased
/// interface for accepting incoming connections. It contains a pointer to the
/// underlying listener implementation and a pointer to the VTable that defines
/// the interface for that implementation.
pub const AnyListener = struct {
    instance: *anyopaque,
    vtable: *const ListenerVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn accept(self: Self, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!conn.AnyRxConn) void) void {
        self.vtable.acceptFn(self.instance, user_data, callback);
    }
};

/// Transport interface for dialing and listening on network addresses.
pub const TransportVTable = struct {
    dialFn: *const fn (instance: *anyopaque, addr: std.net.Address, connection: *conn.AnyRxConn) anyerror!void,
    listenFn: *const fn (instance: *anyopaque, addr: std.net.Address, listener: *AnyListener) anyerror!void,
};

/// AnyTransport is a struct that uses the VTable pattern to provide a type-erased
/// interface for dialing and listening on network addresses. It contains a pointer
/// to the underlying transport implementation and a pointer to the VTable that
/// defines the interface for that implementation.
pub const AnyTransport = struct {
    instance: *anyopaque,
    vtable: *const TransportVTable,

    const Self = @This();
    pub const Error = anyerror;

    /// Dials a remote address via the underlying transport implementation.
    pub fn dial(self: Self, addr: std.net.Address, connection: *conn.AnyRxConn) Error!void {
        return self.vtable.dialFn(self.instance, addr, connection);
    }

    /// Starts listening on a local address via the underlying transport implementation.
    /// The actual listener logic is provided via the `listener` argument.
    pub fn listen(self: Self, addr: std.net.Address, listener: *AnyListener) Error!void {
        return self.vtable.listenFn(self.instance, addr, listener);
    }
};
