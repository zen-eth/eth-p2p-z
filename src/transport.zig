const std = @import("std");
const conn = @import("../conn.zig");

pub fn GenericListener(
    comptime Context: type,
    comptime AcceptE: type,
    comptime acceptFn: fn (context: Context, connection: anytype) AcceptE!void,
) type {
    return struct {
        context: Context,

        pub const AcceptError = AcceptE;

        const Self = @This();

        pub inline fn accept(self: Self, connection: anytype) AcceptError!void {
            return acceptFn(self.context, connection);
        }

        pub inline fn any(self: *const Self) AnyListener {
            return .{
                .context = @ptrCast(&self.context),
                .acceptFn = typeErasedAcceptFn,
            };
        }

        fn typeErasedAcceptFn(context: *const anyopaque, connection: anytype) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return acceptFn(ptr.*, connection);
        }
    };
}

pub const AnyListener = struct {
    context: *const anyopaque,
    acceptFn: *const fn (context: *const anyopaque, connection: anytype) anyerror!void,

    const Self = @This();
    pub const Error = anyerror;

    pub fn accept(self: Self, connection: anytype) Error!void {
        return self.acceptFn(self.context, connection);
    }
};

pub fn GenericTransport(
    comptime Context: type,
    comptime DialE: type,
    comptime ListenE: type,
    comptime dialFn: fn (context: Context, addr: std.net.Address, connection: anytype) DialE!void,
    comptime listenFn: fn (context: Context, addr: std.net.Address, listener: anytype) ListenE!void,
) type {
    return struct {
        context: Context,

        pub const DialError = DialE;
        pub const ListenError = ListenE;

        const Self = @This();

        pub inline fn dial(self: Self, addr: std.net.Address, connection: anytype) DialError!void {
            return dialFn(self.context, addr, connection);
        }

        pub inline fn listen(self: Self, addr: std.net.Address, listener: anytype) ListenError!void {
            return listenFn(self.context, addr, listener);
        }

        pub inline fn any(self: *const Self) AnyTransport {
            return .{
                .context = @ptrCast(&self.context),
                .dialFn = typeErasedDialFn,
                .listenFn = typeErasedListenFn,
            };
        }

        fn typeErasedDialFn(context: *const anyopaque, addr: std.net.Address, connection: anytype) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return dialFn(ptr.*, addr, connection);
        }

        fn typeErasedListenFn(context: *const anyopaque, addr: std.net.Address, listener: anytype) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return listenFn(ptr.*, addr, listener);
        }
    };
}

pub const AnyTransport = struct {
    context: *const anyopaque,
    dialFn: *const fn (context: *const anyopaque, addr: std.net.Address, connection: anytype) anyerror!void,
    listenFn: *const fn (context: *const anyopaque, addr: std.net.Address, listener: anytype) anyerror!void,

    const Self = @This();
    pub const Error = anyerror;

    pub fn dial(self: Self, addr: std.net.Address, connection: anytype) Error!void {
        return self.dialFn(self.context, addr, connection);
    }

    pub fn listen(self: Self, addr: std.net.Address, listener: anytype) Error!void {
        return self.listenFn(self.context, addr, listener);
    }
};

test {
    std.testing.refAllDeclsRecursive(@This());
}
