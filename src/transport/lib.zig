const std = @import("std");
const conn = @import("../conn.zig");

pub const tcp = @import("tcp/xev.zig");

pub fn GenericListener(
    comptime Context: type,
    comptime AcceptE: type,
    comptime acceptFn: fn (context: Context, connection: *conn.AnyConn) AcceptE!void,
) type {
    return struct {
        context: Context,

        pub const AcceptError = AcceptE;

        const Self = @This();

        pub inline fn accept(self: Self, connection: *conn.AnyConn) AcceptError!void {
            return acceptFn(self.context, connection);
        }

        pub inline fn any(self: *const Self) AnyListener {
            return .{
                .context = @ptrCast(&self.context),
                .acceptFn = typeErasedAcceptFn,
            };
        }

        fn typeErasedAcceptFn(context: *const anyopaque, connection: *conn.AnyConn) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return acceptFn(ptr.*, connection);
        }
    };
}

pub const AnyListener = struct {
    context: *const anyopaque,
    acceptFn: *const fn (context: *const anyopaque, connection: *conn.AnyConn) anyerror!void,

    const Self = @This();
    pub const Error = anyerror;

    pub fn accept(self: Self, connection: *conn.AnyConn) Error!void {
        return self.acceptFn(self.context, connection);
    }
};

pub fn GenericTransport(
    comptime Context: type,
    comptime DialE: type,
    comptime ListenE: type,
    comptime dialFn: fn (context: Context, addr: std.net.Address, connection: *conn.AnyConn) DialE!void,
    comptime listenFn: fn (context: Context, addr: std.net.Address, listener: *AnyListener) ListenE!void,
) type {
    return struct {
        context: Context,

        pub const DialError = DialE;
        pub const ListenError = ListenE;

        const Self = @This();

        pub inline fn dial(self: Self, addr: std.net.Address, connection: *conn.AnyConn) DialError!void {
            return dialFn(self.context, addr, connection);
        }

        pub inline fn listen(self: Self, addr: std.net.Address, listener: *AnyListener) ListenError!void {
            return listenFn(self.context, addr, listener);
        }

        pub inline fn any(self: *const Self) AnyTransport {
            return .{
                .context = @ptrCast(&self.context),
                .dialFn = typeErasedDialFn,
                .listenFn = typeErasedListenFn,
            };
        }

        fn typeErasedDialFn(context: *const anyopaque, addr: std.net.Address, connection: *conn.AnyConn) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return dialFn(ptr.*, addr, connection);
        }

        fn typeErasedListenFn(context: *const anyopaque, addr: std.net.Address, listener: *AnyListener) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return listenFn(ptr.*, addr, listener);
        }
    };
}

pub const AnyTransport = struct {
    context: *const anyopaque,
    dialFn: *const fn (context: *const anyopaque, addr: std.net.Address, connection: *conn.AnyConn) anyerror!void,
    listenFn: *const fn (context: *const anyopaque, addr: std.net.Address, listener: *AnyListener) anyerror!void,

    const Self = @This();
    pub const Error = anyerror;

    pub fn dial(self: Self, addr: std.net.Address, connection: *conn.AnyConn) Error!void {
        return self.dialFn(self.context, addr, connection);
    }

    pub fn listen(self: Self, addr: std.net.Address, listener: *AnyListener) Error!void {
        return self.listenFn(self.context, addr, listener);
    }
};

test {
    std.testing.refAllDeclsRecursive(@This());
}
