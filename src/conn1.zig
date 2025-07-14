const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const Allocator = mem.Allocator;
const io_loop = @import("thread_event_loop.zig");

pub const SecuritySession = @import("./security/session.zig").Session;
pub const Direction = enum { INBOUND, OUTBOUND };

/// ConnectionHandlerVTable defines the interface for all connection handlers, generic over ConnType.
pub fn ConnectionHandlerVTable(comptime ConnType: type) type {
    return struct {
        onActive: *const fn (context: *anyopaque, ctx: *ConnectionHandlerContext(ConnType)) !void,
        onInactive: *const fn (context: *anyopaque, ctx: *ConnectionHandlerContext(ConnType)) void,
        onRead: *const fn (context: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), msg: []const u8) !void,
        onReadComplete: *const fn (context: *anyopaque, ctx: *ConnectionHandlerContext(ConnType)) void,
        onErrorCaught: *const fn (context: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), err: anyerror) void,
        write: *const fn (context: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), buffer: []const u8, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void) void,
        close: *const fn (context: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!void) void) void,
    };
}

/// ConnectionHandler is the type-erased interface object for a connection handler.
pub fn ConnectionHandler(comptime ConnType: type) type {
    return struct {
        context: *anyopaque,
        vtable: *const ConnectionHandlerVTable(ConnType),

        const Self = @This();

        pub fn onActive(self: Self, ctx: *ConnectionHandlerContext(ConnType)) !void {
            return self.vtable.onActive(self.context, ctx);
        }

        pub fn onInactive(self: Self, ctx: *ConnectionHandlerContext(ConnType)) void {
            self.vtable.onInactive(self.context, ctx);
        }

        pub fn onRead(self: Self, ctx: *ConnectionHandlerContext(ConnType), msg: []const u8) !void {
            return self.vtable.onRead(self.context, ctx, msg);
        }

        pub fn onReadComplete(self: Self, ctx: *ConnectionHandlerContext(ConnType)) void {
            self.vtable.onReadComplete(self.context, ctx);
        }

        pub fn onErrorCaught(self: Self, ctx: *ConnectionHandlerContext(ConnType), err: anyerror) void {
            self.vtable.onErrorCaught(self.context, ctx, err);
        }

        pub fn write(self: Self, ctx: *ConnectionHandlerContext(ConnType), buffer: []const u8, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void) void {
            self.vtable.write(self.context, ctx, buffer, callback_instance, callback);
        }

        pub fn close(self: Self, ctx: *ConnectionHandlerContext(ConnType), callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!void) void) void {
            self.vtable.close(self.context, ctx, callback_instance, callback);
        }
    };
}

/// A generic wrapper for a connection that uses static dispatch.
pub fn Connection(comptime T: type) type {
    return struct {
        conn: *T,

        pub const Self = @This();

        pub fn direction(self: Self) Direction {
            return self.conn.direction();
        }

        pub fn handlerPipeline(self: Self) *HandlerPipeline(T) {
            return self.conn.handlerPipeline();
        }

        pub fn write(
            self: Self,
            buffer: []const u8,
            callback_instance: ?*anyopaque,
            callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void,
        ) void {
            self.conn.write(buffer, callback_instance, callback);
        }

        pub fn close(
            self: Self,
            callback_instance: ?*anyopaque,
            callback: *const fn (instance: ?*anyopaque, res: anyerror!void) void,
        ) void {
            self.conn.close(callback_instance, callback);
        }

        pub fn setSecuritySession(self: Self, session: SecuritySession) void {
            self.conn.setSecuritySession(session);
        }

        pub fn securitySession(self: Self) ?SecuritySession {
            return self.conn.securitySession();
        }
    };
}

/// ConnHandlerContext is now generic over ConnType to hold a specialized Handler.
pub fn ConnectionHandlerContext(comptime ConnType: type) type {
    return struct {
        name: []const u8,
        handler: ConnectionHandler(ConnType),
        pipeline: *HandlerPipeline(ConnType),
        conn: Connection(ConnType),
        next_context: ?*ConnectionHandlerContext(ConnType) = null,
        prev_context: ?*ConnectionHandlerContext(ConnType) = null,

        const Self = @This();

        pub fn fireActive(self: *Self) !void {
            if (self.findNextInbound()) |next_ctx| {
                return next_ctx.handler.onActive(next_ctx);
            }
        }

        pub fn fireInactive(self: *Self) void {
            if (self.findNextInbound()) |next_ctx| {
                next_ctx.handler.onInactive(next_ctx);
            }
        }

        pub fn fireErrorCaught(self: *Self, err: anyerror) void {
            if (self.findNextInbound()) |next_ctx| {
                next_ctx.handler.onErrorCaught(next_ctx, err);
            }
        }

        pub fn fireRead(self: *Self, msg: []const u8) !void {
            if (self.findNextInbound()) |next_ctx| {
                return next_ctx.handler.onRead(next_ctx, msg);
            }
        }

        pub fn fireReadComplete(self: *Self) void {
            if (self.findNextInbound()) |next_ctx| {
                next_ctx.handler.onReadComplete(next_ctx);
            }
        }

        pub fn write(self: *Self, msg: []const u8, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void) void {
            if (self.findPrevOutbound()) |prev_ctx| {
                prev_ctx.handler.write(prev_ctx, msg, callback_instance, callback);
            }
        }

        pub fn close(self: *Self, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!void) void) void {
            if (self.findPrevOutbound()) |prev_ctx| {
                prev_ctx.handler.close(prev_ctx, callback_instance, callback);
            }
        }

        fn findNextInbound(self: *const Self) ?*ConnectionHandlerContext(ConnType) {
            return self.next_context;
        }

        fn findPrevOutbound(self: *const Self) ?*ConnectionHandlerContext(ConnType) {
            return self.prev_context;
        }
    };
}

fn HeadConnHandlerImpl(comptime ConnType: type) type {
    return struct {
        const Self = @This();
        fn onActive(_: *Self, ctx: *ConnectionHandlerContext(ConnType)) !void {
            return ctx.fireActive();
        }
        fn onInactive(_: *Self, ctx: *ConnectionHandlerContext(ConnType)) void {
            ctx.fireInactive();
        }
        fn onRead(_: *Self, ctx: *ConnectionHandlerContext(ConnType), msg: []const u8) !void {
            return ctx.fireRead(msg);
        }
        fn onReadComplete(_: *Self, ctx: *ConnectionHandlerContext(ConnType)) void {
            ctx.fireReadComplete();
        }
        fn onErrorCaught(_: *Self, ctx: *ConnectionHandlerContext(ConnType), err: anyerror) void {
            ctx.fireErrorCaught(err);
        }
        fn write(_: *Self, ctx: *ConnectionHandlerContext(ConnType), buffer: []const u8, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void) void {
            ctx.conn.write(buffer, callback_instance, callback);
        }
        fn close(_: *Self, ctx: *ConnectionHandlerContext(ConnType), callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!void) void) void {
            ctx.conn.close(callback_instance, callback);
        }

        /// Static wrapper functions for the VTable
        fn vtableOnActiveFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType)) !void {
            const self: *Self = @ptrCast(@alignCast(instance));
            return try self.onActive(ctx);
        }

        fn vtableOnInactiveFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType)) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.onInactive(ctx);
        }

        fn vtableOnReadFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), msg: []const u8) !void {
            const self: *Self = @ptrCast(@alignCast(instance));
            return try self.onRead(ctx, msg);
        }

        fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType)) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.onReadComplete(ctx);
        }

        fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), err: anyerror) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.onErrorCaught(ctx, err);
        }

        fn vtableWriteFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), buffer: []const u8, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.write(ctx, buffer, callback_instance, callback);
        }

        fn vtableCloseFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!void) void) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.close(ctx, callback_instance, callback);
        }

        pub const vtable = ConnectionHandlerVTable(ConnType){
            .onActive = vtableOnActiveFn,
            .onInactive = vtableOnInactiveFn,
            .onRead = vtableOnReadFn,
            .onReadComplete = vtableOnReadCompleteFn,
            .onErrorCaught = vtableOnErrorCaughtFn,
            .write = vtableWriteFn,
            .close = vtableCloseFn,
        };

        pub fn any(self: *Self) ConnectionHandler(ConnType) {
            return ConnectionHandler(ConnType){
                .context = @ptrCast(@alignCast(self)),
                .vtable = &Self.vtable,
            };
        }
    };
}

fn TailConnHandlerImpl(comptime ConnType: type) type {
    return struct {
        const Self = @This();
        fn onActive(_: *Self, _: *ConnectionHandlerContext(ConnType)) !void {}
        fn onInactive(_: *Self, _: *ConnectionHandlerContext(ConnType)) void {}
        fn onRead(_: *Self, _: *ConnectionHandlerContext(ConnType), msg: []const u8) !void {}
        fn onReadComplete(_: *Self, ctx: *ConnectionHandlerContext(ConnType)) void {
            ctx.conn.close(null, io_loop.NoOpCallback.closeCallback);
        }
        fn onErrorCaught(_: *Self, ctx: *ConnectionHandlerContext(ConnType), err: anyerror) void {
            std.log.warn("Handler '{s}' error during onErrorCaught: {any}", .{ ctx.name, err });
            ctx.conn.close(null, io_loop.NoOpCallback.closeCallback);
        }
        fn write(_: *Self, _: *ConnectionHandlerContext(ConnType), _: []const u8, _: ?*anyopaque, _: ?*const fn (instance: ?*anyopaque, res: anyerror!usize) void) void {}
        fn close(_: *Self, _: *ConnectionHandlerContext(ConnType), _: ?*anyopaque, _: *const fn (instance: ?*anyopaque, res: anyerror!void) void) void {}

        /// Static wrapper functions for the VTable
        fn vtableOnActiveFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType)) !void {
            const self: *Self = @ptrCast(@alignCast(instance));
            return try self.onActive(ctx);
        }
        fn vtableOnInactiveFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType)) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.onInactive(ctx);
        }
        fn vtableOnReadFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), msg: []const u8) !void {
            const self: *Self = @ptrCast(@alignCast(instance));
            return try self.onRead(ctx, msg);
        }
        fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType)) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.onReadComplete(ctx);
        }
        fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), err: anyerror) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.onErrorCaught(ctx, err);
        }
        fn vtableWriteFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), buffer: []const u8, callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!usize) void) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.write(ctx, buffer, callback_instance, callback);
        }
        fn vtableCloseFn(instance: *anyopaque, ctx: *ConnectionHandlerContext(ConnType), callback_instance: ?*anyopaque, callback: *const fn (instance: ?*anyopaque, res: anyerror!void) void) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            self.close(ctx, callback_instance, callback);
        }

        pub const vtable = ConnectionHandlerVTable(ConnType){
            .onActive = vtableOnActiveFn,
            .onInactive = vtableOnInactiveFn,
            .onRead = vtableOnReadFn,
            .onReadComplete = vtableOnReadCompleteFn,
            .onErrorCaught = vtableOnErrorCaughtFn,
            .write = vtableWriteFn,
            .close = vtableCloseFn,
        };

        pub fn any(self: *Self) ConnectionHandler(ConnType) {
            return ConnectionHandler(ConnType){
                .context = @ptrCast(@alignCast(self)),
                .vtable = &Self.vtable,
            };
        }
    };
}

pub fn HandlerPipeline(comptime ConnType: type) type {
    const TypedConnHandlerContext = ConnectionHandlerContext(ConnType);
    const TypedHandler = ConnectionHandler(ConnType);

    return struct {
        allocator: Allocator,
        head: TypedConnHandlerContext,
        tail: TypedConnHandlerContext,
        conn: Connection(ConnType),
        head_handler_impl: HeadConnHandlerImpl(ConnType),
        tail_handler_impl: TailConnHandlerImpl(ConnType),

        const Self = @This();

        pub fn init(self: *Self, allocator: Allocator, conn_impl: *ConnType) void {
            const any_conn = Connection(ConnType){ .conn = conn_impl };
            self.* = .{
                .allocator = allocator,
                .conn = any_conn,
                .head_handler_impl = .{},
                .tail_handler_impl = .{},
                .head = undefined,
                .tail = undefined,
            };
            self.head = TypedConnHandlerContext{
                .name = "HEAD",
                .handler = .{ .context = &self.head_handler_impl, .vtable = &HeadConnHandlerImpl(ConnType).vtable },
                .pipeline = self,
                .conn = self.conn,
                .next_context = &self.tail,
                .prev_context = null,
            };
            self.tail = TypedConnHandlerContext{
                .name = "TAIL",
                .handler = .{ .context = &self.tail_handler_impl, .vtable = &TailConnHandlerImpl(ConnType).vtable },
                .pipeline = self,
                .conn = self.conn,
                .next_context = null,
                .prev_context = &self.head,
            };
        }

        pub fn deinit(self: *Self) void {
            var current = self.head.next_context;
            while (current != null and current != &self.tail) {
                const next = current.?.next_context;
                self.allocator.destroy(current.?);
                current = next;
            }
        }

        fn addBefore(self: *Self, next_ctx: *TypedConnHandlerContext, new_ctx: *TypedConnHandlerContext) void {
            const prev_ctx = next_ctx.prev_context orelse unreachable;
            new_ctx.prev_context = prev_ctx;
            new_ctx.next_context = next_ctx;
            prev_ctx.next_context = new_ctx;
            next_ctx.prev_context = new_ctx;
            new_ctx.pipeline = self;
            new_ctx.conn = self.conn;
        }

        pub fn addLast(self: *Self, name: []const u8, handler: TypedHandler) !void {
            const new_ctx = try self.allocator.create(TypedConnHandlerContext);
            new_ctx.* = .{
                .name = name,
                .handler = handler,
                .pipeline = self,
                .conn = self.conn,
            };
            self.addBefore(&self.tail, new_ctx);
        }

        pub fn remove(self: *Self, name: []const u8) !*TypedConnHandlerContext {
            var current = self.head.next_context;
            while (current != null and current != &self.tail) : (current = current.?.next_context) {
                const ctx_to_check = current.?;
                if (std.mem.eql(u8, ctx_to_check.name, name)) {
                    const prev_ctx = ctx_to_check.prev_context orelse unreachable;
                    const next_ctx = ctx_to_check.next_context orelse unreachable;
                    prev_ctx.next_context = next_ctx;
                    next_ctx.prev_context = prev_ctx;
                    return ctx_to_check;
                }
            }
            return error.NotFound;
        }

        pub fn fireActive(self: *Self) PipelineError!void {
            return self.head.fireActive();
        }
        pub fn fireInactive(self: *Self) void {
            self.head.fireInactive();
        }
        pub fn fireErrorCaught(self: *Self, err: anyerror) void {
            self.head.fireErrorCaught(err);
        }
        pub fn fireRead(self: *Self, msg: []const u8) PipelineError!void {
            return self.head.fireRead(msg);
        }
        pub fn fireReadComplete(self: *Self) void {
            self.head.fireReadComplete();
        }
        pub fn write(self: *Self, msg: []const u8, cb_inst: ?*anyopaque, cb: *const fn (instance: ?*anyopaque, res: anyerror!usize) void) void {
            self.tail.write(msg, cb_inst, cb);
        }
        pub fn close(self: *Self, cb_inst: ?*anyopaque, cb: *const fn (instance: ?*anyopaque, res: anyerror!void) void) void {
            self.tail.close(cb_inst, cb);
        }
    };
}

// --- Mocks for Testing ---

const MockConnImpl = struct {
    closed: bool = false,
    write_msg: [1024]u8 = .{0} ** 1024,
    pipeline: ?*HandlerPipeline(MockConnImpl) = null,

    pub fn write(self: *MockConnImpl, buffer: []const u8, cb_inst: ?*anyopaque, cb: *const fn (instance: ?*anyopaque, res: anyerror!usize) void) void {
        @memcpy(self.write_msg[0..buffer.len], buffer);
        cb(cb_inst, buffer.len);
    }
    pub fn close(self: *MockConnImpl, cb_inst: ?*anyopaque, cb: *const fn (instance: ?*anyopaque, res: anyerror!void) void) void {
        self.closed = true;
        cb(cb_inst, {});
    }
    pub fn direction(_: *MockConnImpl) Direction {
        return Direction.INBOUND;
    }
    pub fn handlerPipeline(self: *MockConnImpl) *HandlerPipeline(MockConnImpl) {
        return self.pipeline orelse @panic("Pipeline not set");
    }
    pub fn setSecuritySession(_: *MockConnImpl, _: SecuritySession) void {}
    pub fn securitySession(_: *MockConnImpl) ?SecuritySession {
        return null;
    }
};

const MockHandlerImpl = struct {
    read_msg: [1024]u8 = .{0} ** 1024,
    write_msg: [1024]u8 = .{0} ** 1024,

    const Self = @This();

    fn onActive(_: *anyopaque, ctx: *ConnectionHandlerContext(MockConnImpl)) PipelineError!void {
        return ctx.fireActive();
    }
    fn onInactive(_: *anyopaque, _: *ConnectionHandlerContext(MockConnImpl)) void {}
    fn onRead(context: *anyopaque, ctx: *ConnectionHandlerContext(MockConnImpl), msg: []const u8) PipelineError!void {
        const self: *Self = @ptrCast(@alignCast(context));
        @memcpy(self.read_msg[0..msg.len], msg);
        return ctx.fireRead(msg);
    }
    fn onReadComplete(_: *anyopaque, _: *ConnectionHandlerContext(MockConnImpl)) void {}
    fn onErrorCaught(_: *anyopaque, _: *ConnectionHandlerContext(MockConnImpl), _: anyerror) void {}
    fn write(context: *anyopaque, ctx: *ConnectionHandlerContext(MockConnImpl), msg: []const u8, cb_inst: ?*anyopaque, cb: *const fn (instance: ?*anyopaque, res: anyerror!usize) void) void {
        const self: *Self = @ptrCast(@alignCast(context));
        @memcpy(self.write_msg[0..msg.len], msg);
        ctx.write(msg, cb_inst, cb);
    }
    fn close(_: *anyopaque, ctx: *ConnectionHandlerContext(MockConnImpl), cb_inst: ?*anyopaque, cb: *const fn (instance: ?*anyopaque, res: anyerror!void) void) void {
        ctx.close(cb_inst, cb);
    }

    pub const vtable = ConnectionHandlerVTable(MockConnImpl){
        .onActive = onActive,
        .onInactive = onInactive,
        .onRead = onRead,
        .onReadComplete = onReadComplete,
        .onErrorCaught = onErrorCaught,
        .write = write,
        .close = close,
    };
};

const OnWriteCallback = struct {
    fn callback(_: ?*anyopaque, r: anyerror!usize) void {
        _ = r catch @panic("write callback error");
    }
};
const OnCloseCallback = struct {
    fn callback(_: ?*anyopaque, r: anyerror!void) void {
        _ = r catch @panic("close callback error");
    }
};

test "HandlerPipeline with vtable handler and generic conn" {
    const allocator = std.testing.allocator;
    var mock_conn_impl = MockConnImpl{};
    const TypedPipeline = HandlerPipeline(MockConnImpl);
    var pipeline: TypedPipeline = undefined;
    pipeline.init(allocator, &mock_conn_impl);
    defer pipeline.deinit();
    mock_conn_impl.pipeline = &pipeline;

    var mock_handler_1_impl = MockHandlerImpl{};
    const handler_1 = ConnectionHandler(MockConnImpl){ .context = &mock_handler_1_impl, .vtable = &MockHandlerImpl.vtable };

    var mock_handler_2_impl = MockHandlerImpl{};
    const handler_2 = ConnectionHandler(MockConnImpl){ .context = &mock_handler_2_impl, .vtable = &MockHandlerImpl.vtable };

    try pipeline.addLast("mock1", handler_1);
    try pipeline.addLast("mock2", handler_2);

    // Test Inbound Event (fireRead)
    const read_msg = "inbound data";
    try pipeline.fireRead(read_msg);
    try testing.expectEqualSlices(u8, read_msg, mock_handler_2_impl.read_msg[0..read_msg.len]);

    // Test Outbound Event (write)
    const write_msg = "outbound data";
    pipeline.write(write_msg, null, OnWriteCallback.callback);
    try testing.expectEqualSlices(u8, write_msg, mock_handler_1_impl.write_msg[0..write_msg.len]);
    try testing.expectEqualSlices(u8, write_msg, mock_conn_impl.write_msg[0..write_msg.len]);

    // Test Outbound Event (close)
    pipeline.close(null, OnCloseCallback.callback);
    try testing.expect(mock_conn_impl.closed);
}
