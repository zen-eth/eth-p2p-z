const std = @import("std");
const conn = @import("conn.zig");

pub const tcp = @import("transport/tcp.zig");
pub const TcpTransport = tcp.XevTransport;
pub const TcpListener = tcp.XevListener;
pub const TcpConnection = tcp.XevSocketChannel;

pub const quic = @import("transport/quic.zig");
pub const QuicTransport = quic.QuicTransport;
pub const QuicListener = quic.QuicListener;
pub const QuicConnection = quic.QuicConnection;
pub const QuicStream = quic.QuicStream;

pub const ConnectionUpgrader = @import("transport/upgrader.zig").ConnUpgrader;

/// Listener interface for accepting incoming connections.
/// This is a type-erased interface that allows
/// different implementations of listeners to be used interchangeably.
/// It uses the VTable pattern to provide a consistent interface for accepting
/// incoming connections. The `acceptFn` function pointer is used to call the
/// appropriate implementation of the accept function for the specific listener
/// instance. The `callback_instance` parameter is an optional user-defined data pointer
/// that can be passed to the callback function. The `callback` function is
/// called when a new connection is accepted. It takes a user-defined data pointer
/// and a result of type `anyerror!conn.AnyRxConn`, which represents the accepted
/// connection. The `anyerror` type is used to represent any error that may occur
/// during the acceptance of a connection.
pub const ListenerVTable = struct {
    acceptFn: *const fn (instance: *anyopaque, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!conn.AnyConn) void) void,
};

/// AnyListener is a struct that uses the VTable pattern to provide a type-erased
/// interface for accepting incoming connections. It contains a pointer to the
/// underlying listener implementation and a pointer to the VTable that defines
/// the interface for that implementation. The `instance` field is a pointer to
/// the underlying listener instance, and the `vtable` field is a pointer to the
/// VTable that defines the interface for that instance. The `accept` function
/// is used to accept incoming connections. It takes an optional user-defined
/// data pointer and a callback function that is called when a new connection
/// is accepted. The callback function takes a user-defined data pointer and a
/// result of type `anyerror!conn.AnyRxConn`, which represents the accepted
/// connection. The `anyerror` type is used to represent any error that may occur
/// during the acceptance of a connection.
pub const AnyListener = struct {
    instance: *anyopaque,
    vtable: *const ListenerVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn accept(self: Self, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!conn.AnyConn) void) void {
        self.vtable.acceptFn(self.instance, callback_instance, callback);
    }
};

/// Transport interface for dialing and listening on network addresses.
/// This is a type-erased interface that allows different implementations of
/// transports to be used interchangeably. It uses the VTable pattern to provide
/// a consistent interface for dialing and listening on network addresses.
/// The `dialFn` function pointer is used to call the appropriate implementation
/// of the dial function for the specific transport instance.
/// The `listenFn` function pointer is used to call the appropriate implementation
/// of the listen function for the specific transport instance.
pub const TransportVTable = struct {
    dialFn: *const fn (instance: *anyopaque, addr: std.net.Address, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!conn.AnyConn) void) void,
    listenFn: *const fn (instance: *anyopaque, addr: std.net.Address) anyerror!AnyListener,
};

/// AnyTransport is a struct that uses the VTable pattern to provide a type-erased
/// interface for dialing and listening on network addresses. It contains a
/// pointer to the underlying transport implementation and a pointer to the
/// VTable that defines the interface for that implementation. The `instance`
/// field is a pointer to the underlying transport instance, and the `vtable`
/// field is a pointer to the VTable that defines the interface for that instance.
/// The `dial` function is used to dial a remote address. It takes an address,
/// an optional user-defined data pointer, and a callback function that is
/// called when the dialing operation is complete. The callback function takes
/// a user-defined data pointer and a result of type `anyerror!conn.AnyRxConn`,
/// which represents the established connection. The `listen` function is used
/// to start listening on a local address. It takes an address and returns an
/// `anyerror!AnyListener`, which represents the listener that will accept
/// incoming connections.
pub const AnyTransport = struct {
    instance: *anyopaque,
    vtable: *const TransportVTable,

    const Self = @This();
    pub const Error = anyerror;

    /// Dials a remote address via the underlying transport implementation.
    pub fn dial(self: Self, addr: std.net.Address, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!conn.AnyConn) void) void {
        self.vtable.dialFn(self.instance, addr, callback_instance, callback);
    }

    /// Starts listening on a local address via the underlying transport implementation.
    pub fn listen(self: Self, addr: std.net.Address) Error!AnyListener {
        return self.vtable.listenFn(self.instance, addr);
    }
};
