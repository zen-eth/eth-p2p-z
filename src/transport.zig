const std = @import("std");
const conn = @import("conn.zig");

/// A generic listener type that provides an abstraction for accepting connections.
pub fn GenericListener(
    /// The type of the context used by the listener.
    comptime Context: type,
    /// The error type that can be returned by the `accept` function.
    comptime AcceptE: type,
    /// A function pointer that defines how to accept a connection.
    comptime acceptFn: fn (context: Context, connection: *conn.AnyConn) AcceptE!void,
) type {
    return struct {
        /// The context associated with the listener.
        context: Context,

        /// The error type returned by the `accept` function.
        pub const AcceptError = AcceptE;

        const Self = @This();

        /// Accepts a connection using the provided `acceptFn`.
        pub inline fn accept(self: Self, connection: *conn.AnyConn) AcceptError!void {
            return acceptFn(self.context, connection);
        }

        /// Converts the listener into a type-erased `AnyListener`.
        pub inline fn any(self: *const Self) AnyListener {
            return .{
                .context = @ptrCast(&self.context),
                .acceptFn = typeErasedAcceptFn,
            };
        }

        /// A type-erased accept function used by `AnyListener`.
        fn typeErasedAcceptFn(context: *const anyopaque, connection: *conn.AnyConn) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return acceptFn(ptr.*, connection);
        }
    };
}

/// A type-erased listener that can accept connections without knowing the specific context type.
pub const AnyListener = struct {
    /// A pointer to the type-erased context.
    context: *const anyopaque,

    /// A function pointer to the type-erased accept function.
    acceptFn: *const fn (context: *const anyopaque, connection: *conn.AnyRxConn) anyerror!void,

    const Self = @This();
    pub const Error = anyerror;

    /// Accepts a connection using the type-erased accept function.
    pub fn accept(self: Self, connection: *conn.AnyRxConn) Error!void {
        return self.acceptFn(self.context, connection);
    }
};

/// A generic transport type that provides an abstraction for dialing and listening on addresses.
pub fn GenericTransport(
    /// The type of the context used by the transport.
    comptime Context: type,
    /// The error type that can be returned by the `dial` function.
    comptime DialE: type,
    /// The error type that can be returned by the `listen` function.
    comptime ListenE: type,
    /// A function pointer that defines how to dial an address.
    comptime dialFn: fn (context: Context, addr: std.net.Address, connection: *conn.AnyRxConn) DialE!void,
    /// A function pointer that defines how to listen on an address.
    comptime listenFn: fn (context: Context, addr: std.net.Address, listener: *AnyListener) ListenE!void,
) type {
    return struct {
        /// The context associated with the transport.
        context: Context,

        /// The error type returned by the `dial` function.
        pub const DialError = DialE;

        /// The error type returned by the `listen` function.
        pub const ListenError = ListenE;

        const Self = @This();

        /// Dials an address using the provided `dialFn`.
        pub inline fn dial(self: Self, addr: std.net.Address, connection: *conn.AnyRxConn) DialError!void {
            return dialFn(self.context, addr, connection);
        }

        /// Listens on an address using the provided `listenFn`.
        pub inline fn listen(self: Self, addr: std.net.Address, listener: *AnyListener) ListenError!void {
            return listenFn(self.context, addr, listener);
        }

        /// Converts the transport into a type-erased `AnyTransport`.
        pub inline fn any(self: *const Self) AnyTransport {
            return .{
                .context = @ptrCast(&self.context),
                .dialFn = typeErasedDialFn,
                .listenFn = typeErasedListenFn,
            };
        }

        /// A type-erased dial function used by `AnyTransport`.
        fn typeErasedDialFn(context: *const anyopaque, addr: std.net.Address, connection: *conn.AnyRxConn) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return dialFn(ptr.*, addr, connection);
        }

        /// A type-erased listen function used by `AnyTransport`.
        fn typeErasedListenFn(context: *const anyopaque, addr: std.net.Address, listener: *AnyListener) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return listenFn(ptr.*, addr, listener);
        }
    };
}

/// A type-erased transport that can dial and listen on addresses without knowing the specific context type.
pub const AnyTransport = struct {
    /// A pointer to the type-erased context.
    context: *const anyopaque,

    /// A function pointer to the type-erased dial function.
    dialFn: *const fn (context: *const anyopaque, addr: std.net.Address, connection: *conn.AnyRxConn) anyerror!void,

    /// A function pointer to the type-erased listen function.
    listenFn: *const fn (context: *const anyopaque, addr: std.net.Address, listener: *AnyListener) anyerror!void,

    const Self = @This();
    pub const Error = anyerror;

    /// Dials an address using the type-erased dial function.
    pub fn dial(self: Self, addr: std.net.Address, connection: *conn.AnyRxConn) Error!void {
        return self.dialFn(self.context, addr, connection);
    }

    /// Listens on an address using the type-erased listen function.
    pub fn listen(self: Self, addr: std.net.Address, listener: *AnyListener) Error!void {
        return self.listenFn(self.context, addr, listener);
    }
};
