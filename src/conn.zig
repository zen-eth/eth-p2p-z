const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const Thread = std.Thread;
const Future = @import("concurrent/future.zig").Future;

// pub fn ConnInitializer(
//     comptime Context: type,
//     comptime InitError: type,
//     comptime InitFn: fn (context: Context, conn: AnyRxConn) InitError!void,
// ) type {
//     return struct {
//         context: Context,

//         const Self = @This();

//         pub const Error = InitError;

//         pub inline fn init(self: Self, conn: AnyRxConn) InitError!void {
//             return InitFn(self.context, conn);
//         }

//         pub inline fn any(self: *const Self) AnyConnInitializer {
//             return .{
//                 .context = @ptrCast(&self.context),
//                 .initFn = typeErasedInitFn,
//             };
//         }
//         fn typeErasedInitFn(context: *const anyopaque, conn: AnyRxConn) anyerror!void {
//             const ptr: *const Context = @alignCast(@ptrCast(context));
//             return InitFn(ptr.*, conn);
//         }
//     };
// }

// pub const AnyConnInitializer = struct {
//     context: *const anyopaque,
//     initFn: *const fn (context: *const anyopaque, conn: AnyRxConn) anyerror!void,

//     const Self = @This();
//     pub const Error = anyerror;

//     pub fn init(self: Self, conn: AnyRxConn) Error!void {
//         return self.initFn(self.context, conn);
//     }
// };

pub fn GenericHandler(
    comptime Context: type,
    comptime OnActiveError: type,
    comptime OnInactiveError: type,
    comptime OnReadError: type,
    comptime OnReadCompleteError: type,
    comptime OnErrorCaughtError: type,
    comptime WriteError: type,
    comptime CloseError: type,
    comptime onActiveFn: fn (handler_context: Context, ctx: *HandlerContext) OnActiveError!void,
    comptime onInactiveFn: fn (handler_context: Context, ctx: *HandlerContext) OnInactiveError!void,
    comptime onReadFn: fn (handler_context: Context, ctx: *HandlerContext, msg: []const u8) OnReadError!void,
    comptime onReadCompleteFn: fn (handler_context: Context, ctx: *HandlerContext) OnReadCompleteError!void,
    comptime onErrorCaughtFn: fn (handler_context: Context, ctx: *HandlerContext, err: anyerror) OnErrorCaughtError!void,
    comptime writeFn: fn (handler_context: Context, ctx: *HandlerContext, buffer: []const u8, future: *WriteFuture) WriteError!void,
    comptime closeFn: fn (handler_context: Context, ctx: *HandlerContext, future: *CloseFuture) CloseError!void,
) type {
    return struct {
        context: Context, // The specific handler's context

        const Self = @This();

        pub const OnActiveErr = OnActiveError;
        pub const OnInactiveErr = OnInactiveError;
        pub const OnReadCompleteErr = OnReadCompleteError;
        pub const OnReadErr = OnReadError;
        pub const OnErrorCaughtErr = OnErrorCaughtError;
        pub const WriteErr = WriteError;
        pub const CloseErr = CloseError;

        pub inline fn onActive(self: Self, ctx: *HandlerContext) OnActiveErr!void {
            return onActiveFn(self.context, ctx);
        }

        pub inline fn onInactive(self: Self, ctx: *HandlerContext) OnInactiveErr!void {
            return onInactiveFn(self.context, ctx);
        }

        pub inline fn onRead(self: Self, ctx: *HandlerContext, msg: []const u8) OnReadErr!void {
            return onReadFn(self.context, ctx, msg);
        }

        pub inline fn onReadComplete(self: Self, ctx: *HandlerContext) OnReadCompleteErr!void {
            return onReadCompleteFn(self.context, ctx);
        }

        pub inline fn onErrorCaught(self: Self, ctx: *HandlerContext, err: anyerror) OnErrorCaughtErr!void {
            return onErrorCaughtFn(self.context, ctx, err);
        }

        pub inline fn write(self: Self, ctx: *HandlerContext, buffer: []const u8, future: *WriteFuture) WriteError!void {
            return writeFn(self.context, ctx, buffer, future);
        }
        pub inline fn close(self: Self, ctx: *HandlerContext, future: *CloseFuture) CloseErr!void {
            return closeFn(self.context, ctx, future);
        }

        fn typeErasedOnActiveFn(handler_context: *const anyopaque, ctx: *HandlerContext) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(handler_context));
            return onActiveFn(ptr.*, ctx);
        }
        fn typeErasedOnInactiveFn(handler_context: *const anyopaque, ctx: *HandlerContext) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(handler_context));
            return onInactiveFn(ptr.*, ctx);
        }

        fn typeErasedOnReadFn(handler_context: *const anyopaque, ctx: *HandlerContext, msg: []const u8) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(handler_context));
            return onReadFn(ptr.*, ctx, msg);
        }
        fn typeErasedOnReadCompleteFn(handler_context: *const anyopaque, ctx: *HandlerContext) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(handler_context));
            return onReadCompleteFn(ptr.*, ctx);
        }
        fn typeErasedOnErrorCaughtFn(handler_context: *const anyopaque, ctx: *HandlerContext, err: anyerror) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(handler_context));
            return onErrorCaughtFn(ptr.*, ctx, err);
        }
        fn typeErasedWriteFn(handler_context: *const anyopaque, ctx: *HandlerContext, buffer: []const u8, future: *WriteFuture) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(handler_context));
            return writeFn(ptr.*, ctx, buffer, future);
        }
        fn typeErasedCloseFn(handler_context: *const anyopaque, ctx: *HandlerContext, future: *CloseFuture) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(handler_context));
            return closeFn(ptr.*, ctx, future);
        }

        pub inline fn any(self: *const Self) AnyHandler {
            return .{
                .context = @ptrCast(&self.context),
                .onActiveFn = typeErasedOnActiveFn,
                .onInactiveFn = typeErasedOnInactiveFn,
                .onReadFn = typeErasedOnReadFn,
                .onReadCompleteFn = typeErasedOnReadCompleteFn,
                .onErrorCaughtFn = typeErasedOnErrorCaughtFn,
                .writeFn = typeErasedWriteFn,
                .closeFn = typeErasedCloseFn,
            };
        }
    };
}

pub const WriteFuture = Future(usize, anyerror);
pub const CloseFuture = Future(void, anyerror);

pub const AnyHandler = struct {
    context: *const anyopaque,
    onActiveFn: *const fn (handler_context: *const anyopaque, ctx: *HandlerContext) anyerror!void,
    onInactiveFn: *const fn (handler_context: *const anyopaque, ctx: *HandlerContext) anyerror!void,
    onReadFn: *const fn (handler_context: *const anyopaque, ctx: *HandlerContext, msg: []const u8) anyerror!void,
    onReadCompleteFn: *const fn (handler_context: *const anyopaque, ctx: *HandlerContext) anyerror!void,
    onErrorCaughtFn: *const fn (handler_context: *const anyopaque, ctx: *HandlerContext, err: anyerror) anyerror!void,
    writeFn: *const fn (handler_context: *const anyopaque, ctx: *HandlerContext, buffer: []const u8, future: *WriteFuture) anyerror!void,
    closeFn: *const fn (handler_context: *const anyopaque, ctx: *HandlerContext, future: *CloseFuture) anyerror!void,

    const Self = @This();
    pub const Error = anyerror;

    pub fn onActive(self: Self, ctx: *HandlerContext) Error!void {
        return self.onActiveFn(self.context, ctx);
    }

    pub fn onInactive(self: Self, ctx: *HandlerContext) Error!void {
        return self.onInactiveFn(self.context, ctx);
    }
    pub fn onRead(self: Self, ctx: *HandlerContext, msg: []const u8) Error!void {
        return self.onReadFn(self.context, ctx, msg);
    }

    pub fn onReadComplete(self: Self, ctx: *HandlerContext) Error!void {
        return self.onReadCompleteFn(self.context, ctx);
    }
    pub fn onErrorCaught(self: Self, ctx: *HandlerContext, err: anyerror) Error!void {
        return self.onErrorCaughtFn(self.context, ctx, err);
    }
    pub fn write(self: Self, ctx: *HandlerContext, buffer: []const u8, future: *WriteFuture) Error!void {
        return self.writeFn(self.context, ctx, buffer, future);
    }

    pub fn close(self: Self, ctx: *HandlerContext, future: *CloseFuture) Error!void {
        return self.closeFn(self.context, ctx, future);
    }
};

pub const Direction = enum {
    INBOUND,
    OUTBOUND,
};

/// GenericRXConn provides a generic interface for reactive connections, read is automatic when data is available.
/// It need to be supported by the underlying event loop. It need to be used with a handler pipeline.
pub fn GenericRxConn(
    comptime Context: type,
    comptime WriteError: type,
    comptime CloseError: type,
    comptime writeFn: fn (context: Context, buffer: []const u8) WriteError!usize,
    comptime closeFn: fn (context: Context) CloseError!void,
    // comptime pipelineFn: fn (context: Context) *HandlerPipeline,
    comptime directionFn: fn (context: Context) Direction,
) type {
    return struct {
        context: Context,

        pub const WriteErr = WriteErr;

        pub const CloseErr = CloseErr;

        const Self = @This();

        pub inline fn write(self: Self, buffer: []const u8) WriteErr!usize {
            return writeFn(self.context, buffer);
        }

        pub inline fn close(self: Self) CloseErr!void {
            try closeFn(self.context);
        }

        // pub inline fn getPipeline(self: Self) *HandlerPipeline {
        //     return pipelineFn(self.context);
        // }

        pub inline fn direction(self: Self) Direction {
            return directionFn(self.context);
        }

        pub inline fn writer(self: Self) std.io.GenericWriter(
            Context,
            WriteErr,
            writeFn,
        ) {
            return .{ .context = self.context };
        }

        pub inline fn any(self: *const Self) AnyRxConn {
            return .{
                .context = @ptrCast(&self.context),
                .writeFn = typeErasedWriteFn,
                .closeFn = typeErasedCloseFn,
                // .getPipelineFn = typeErasedGetPipelineFn,
                .directionFn = typeErasedDirectionFn,
            };
        }

        fn typeErasedWriteFn(context: *const anyopaque, buffer: []const u8) anyerror!usize {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return writeFn(ptr.*, buffer);
        }

        fn typeErasedCloseFn(context: *const anyopaque) anyerror!void {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return closeFn(ptr.*);
        }

        // fn typeErasedGetPipelineFn(context: *const anyopaque) *HandlerPipeline {
        //     const ptr: *const Context = @alignCast(@ptrCast(context));
        //     return pipelineFn(ptr.*);
        // }
        fn typeErasedDirectionFn(context: *const anyopaque) Direction {
            const ptr: *const Context = @alignCast(@ptrCast(context));
            return directionFn(ptr.*);
        }
    };
}

/// AnyRxConn is a type-erased version of GenericRxConn.
/// It is used to pass around connections in a type-erased way.
pub const AnyRxConn = struct {
    context: *const anyopaque,
    writeFn: *const fn (context: *const anyopaque, buffer: []const u8) anyerror!usize,
    closeFn: *const fn (context: *const anyopaque) anyerror!void,
    // getPipelineFn: *const fn (context: *const anyopaque) *HandlerPipeline,
    directionFn: *const fn (context: *const anyopaque) Direction,

    const Self = @This();
    pub const Error = anyerror;

    pub fn write(self: Self, buffer: []const u8) Error!usize {
        return self.writeFn(self.context, buffer);
    }

    pub fn close(self: Self) Error!void {
        return self.closeFn(self.context);
    }

    pub fn direction(self: Self) Direction {
        return self.directionFn(self.context);
    }
    // pub fn getPipeline(self: Self) *HandlerPipeline {
    //     return self.getPipelineFn(self.context);
    // }
};

pub const HandlerContext = struct {
    name: []const u8,
    handler: AnyHandler,
    // pipeline: *HandlerPipeline,
    conn: AnyRxConn,
    next_context: ?*HandlerContext = null,
    prev_context: ?*HandlerContext = null,

    const Self = @This();

    fn findNextInbound(self: *const Self) ?*HandlerContext {
        return self.next_context;
    }

    fn findPrevOutbound(self: *const Self) ?*HandlerContext {
        return self.prev_context;
    }

    pub fn fireActive(self: *Self) void {
        if (self.findNextInbound()) |next_ctx| {
            next_ctx.handler.onActive(next_ctx) catch |err| {
                next_ctx.handler.onErrorCaught(next_ctx, err) catch |err2| {
                    std.log.err("Handler '{s}' error during onActive: {any}", .{ next_ctx.name, err2 });
                };

                return;
            };
        } else {
            std.log.debug("Pipeline: fireActive reached end.", .{});
        }
    }

    pub fn fireInactive(self: *Self) void {
        if (self.findNextInbound()) |next_ctx| {
            next_ctx.handler.onInactive(next_ctx) catch |err| {
                next_ctx.handler.onErrorCaught(next_ctx, err) catch |err2| {
                    std.log.err("Handler '{s}' error during onInactive: {any}", .{ next_ctx.name, err2 });
                };

                return;
            };
        } else {
            std.log.debug("Pipeline: fireInactive reached head.", .{});
        }
    }

    pub fn fireErrorCaught(self: *Self, err: anyerror) void {
        if (self.findNextInbound()) |next_ctx| {
            next_ctx.handler.onErrorCaught(next_ctx, err) catch |err2| {
                std.log.err("Handler '{s}' error during onErrorCaught: {any}", .{ next_ctx.name, err2 });
            };
        } else {
            std.log.debug("Pipeline: fireErrorCaught reached end.", .{});
        }
    }

    pub fn fireRead(self: *Self, msg: []const u8) void {
        if (self.findNextInbound()) |next_ctx| {
            next_ctx.handler.onRead(next_ctx, msg) catch |err| {
                next_ctx.handler.onErrorCaught(next_ctx, err) catch |err2| {
                    std.log.err("Handler '{s}' error during onRead: {any}", .{ next_ctx.name, err2 });
                };

                return;
            };
        } else {
            std.log.debug("Pipeline: fireRead reached end.", .{});
        }
    }

    pub fn fireReadComplete(self: *Self) void {
        if (self.findNextInbound()) |next_ctx| {
            next_ctx.handler.onReadComplete(next_ctx) catch |err| {
                next_ctx.handler.onErrorCaught(next_ctx, err) catch |err2| {
                    std.log.err("Handler '{s}' error during onReadComplete: {any}", .{ next_ctx.name, err2 });
                };

                return;
            };
        } else {
            std.log.debug("Pipeline: fireReadComplete reached end.", .{});
        }
    }

    pub fn write(self: *Self, msg: []const u8, future: *WriteFuture) void {
        if (self.findPrevOutbound()) |prev_ctx| {
            prev_ctx.handler.write(prev_ctx, msg, future) catch |err| {
                std.log.err("Handler '{s}' error during write: {any}", .{ prev_ctx.name, err });
                future.setError(err);
                return;
            };
        } else {
            std.log.debug("Pipeline: write reached head, writing to connection.", .{});
            const n = self.conn.write(msg) catch |err| {
                std.log.err("Handler '{s}' error during write to connection: {any}", .{ self.name, err });
                future.setError(err);
                return;
            };
            future.setValue(n);
        }
    }

    pub fn close(self: *Self, future: *CloseFuture) void {
        if (self.findPrevOutbound()) |prev_ctx| {
            prev_ctx.handler.close(prev_ctx, future) catch |err| {
                std.log.err("Handler '{s}' error during close: {any}", .{ prev_ctx.name, err });
                future.setError(err);
                return;
            };
        } else {
            std.log.debug("Pipeline: close reached head, closing connection.", .{});
            self.conn.close() catch |err| {
                std.log.err("Handler '{s}' error during close: {any}", .{ self.name, err });
                future.setError(err);
                return;
            };
            future.setDone();
        }
    }
};

// const HeadHandler= GenericHandler(*HeadHandlerImpl, // Context type
//     anyerror, // OnActiveError
//     anyerror, // OnInactiveError
//     anyerror, // OnReadError
//     anyerror, // OnReadCompleteError
//     anyerror, // OnErrorCaughtError
//     anyerror, // WriteError
//     anyerror, // CloseError
//     HeadHandlerImpl.onActive, // onActiveFn
//     HeadHandlerImpl.onInactive, // onInactiveFn
//     HeadHandlerImpl.onRead, // onReadFn
//     HeadHandlerImpl.onReadComplete, // onReadCompleteFn
//     HeadHandlerImpl.onErrorCaught, // onErrorCaughtFn
//     HeadHandlerImpl.write, // writeFn
//     HeadHandlerImpl.close // closeFn
// );

// const HeadHandlerImpl = struct {
//     pub const Self = @This();

//     pub fn onActive(_: *Self, _: *HandlerContext) !void {
//     }

//     pub fn onInactive(_: *Self, _: *HandlerContext) !void {
//     }

//     pub fn onRead(_: *Self, _: *HandlerContext, _: []const u8) !void {
//     }

//     pub fn onReadComplete(_: *Self, _: *HandlerContext) !void {
//     }
//     pub fn onErrorCaught(_: *Self, _: *HandlerContext, _: anyerror) !void {
//     }
//     pub fn write(_: *Self, _: *HandlerContext, _: []const u8, _: *WriteFuture) !void {
//     }
//     pub fn close(_: *Self, _: *HandlerContext, _: *CloseFuture) !void {
//     }

//     pub fn toHandler(self: *Self) HeadHandler {
//         return HeadHandler{ .context = self };
//     }
// };

// const TailHandler= GenericHandler(*TailHandlerImpl, // Context type
//     anyerror, // OnActiveError
//     anyerror, // OnInactiveError
//     anyerror, // OnReadError
//     anyerror, // OnReadCompleteError
//     anyerror, // OnErrorCaughtError
//     anyerror, // WriteError
//     anyerror, // CloseError
//     TailHandlerImpl.onActive, // onActiveFn
//     TailHandlerImpl.onInactive, // onInactiveFn
//     TailHandlerImpl.onRead, // onReadFn
//     TailHandlerImpl.onReadComplete, // onReadCompleteFn
//     TailHandlerImpl.onErrorCaught, // onErrorCaughtFn
//     TailHandlerImpl.write, // writeFn
//     TailHandlerImpl.close // closeFn
// );

// const TailHandlerImpl = struct {
//     pub const Self = @This();

//     pub fn onActive(_: *Self, _: *HandlerContext) !void {
//     }

//     pub fn onInactive(_: *Self, _: *HandlerContext) !void {
//     }

//     pub fn onRead(_: *Self, _: *HandlerContext, _: []const u8) !void {
//     }

//     pub fn onReadComplete(_: *Self, _: *HandlerContext) !void {
//     }
//     pub fn onErrorCaught(_: *Self, _: *HandlerContext, _: anyerror) !void {
//     }
//     pub fn write(_: *Self, _: *HandlerContext, _: []const u8, _: *WriteFuture) !void {
//     }
//     pub fn close(_: *Self, _: *HandlerContext, _: *CloseFuture) !void {
//     }

//     pub fn toHandler(self: *Self) TailHandler {
//         return TailHandler{ .context = self };
//     }
// };

// pub const HandlerPipeline = struct {
//     allocator: std.mem.Allocator,
//     head: HandlerContext,
//     tail: HandlerContext,
//     conn: AnyRxConn,

//     const Self = @This();

//     pub fn init(self: *Self, allocator: std.mem.Allocator, associated_conn: AnyRxConn) !void {
//         // Initialize the fields of the struct pointed to by 'self'
//         const head_handler = try allocator.create(HeadHandlerImpl);
//         const tail_handler = try allocator.create(TailHandlerImpl);
//         errdefer allocator.destroy(head_handler);
//         errdefer allocator.destroy(tail_handler);

//         head_handler.* = .{};
//         tail_handler.* = .{};
//         self.* = .{
//             .allocator = allocator,
//             .conn = associated_conn,
//             .head = HandlerContext{
//                 .name = "HEAD",
//                 .handler = head_handler.toHandler().any(),
//                 .pipeline = self,
//                 .conn = associated_conn,
//                 .next_context = undefined,
//                 .prev_context = null,
//             },
//             .tail = HandlerContext{
//                 .name = "TAIL",
//                 .handler = tail_handler.toHandler().any(),
//                 .pipeline = self,
//                 .conn = associated_conn,
//                 .next_context = null,
//                 .prev_context = undefined,
//             },
//         };
//         // Link head and tail using the pointer 'self'
//         self.head.next_context = &self.tail;
//         self.tail.prev_context = &self.head;
//     }

//     pub fn deinit(self: *Self) void {
//         var current = self.head.next_context;
//         while (current != null and current != &self.tail) {
//             const next = current.?.next_context;
//             self.allocator.destroy(current.?);
//             current = next;
//         }
//         // Reset pointers, but don't destroy 'self'
//         self.head.next_context = &self.tail;
//         self.tail.prev_context = &self.head;

//         const mutable_head_ptr = @constCast(self.head.handler.context);
//         const mutable_tail_ptr = @constCast(self.tail.handler.context);

//         self.allocator.destroy(@as(*HeadHandlerImpl, @ptrCast(mutable_head_ptr)));
//         self.allocator.destroy(@as(*TailHandlerImpl, @ptrCast(mutable_tail_ptr)));
//     }

//     /// Adds a handler context node *before* the specified 'next' node.
//     fn addBefore(next_ctx: *HandlerContext, new_ctx: *HandlerContext) void {
//         const prev_ctx = next_ctx.prev_context orelse unreachable;

//         new_ctx.prev_context = prev_ctx;
//         new_ctx.next_context = next_ctx;

//         prev_ctx.next_context = new_ctx;
//         next_ctx.prev_context = new_ctx;
//     }

//     /// Adds a handler to the beginning of the pipeline (just after the head sentinel).
//     pub fn addFirst(self: *Self, name: []const u8, handler: AnyHandler) !void {
//         const new_ctx = try self.allocator.create(HandlerContext);
//         errdefer self.allocator.destroy(new_ctx);

//         new_ctx.* = .{
//             .name = name,
//             .handler = handler,
//             .pipeline = self, // Use the valid 'self' pointer
//             .conn = self.conn,
//             .next_context = undefined,
//             .prev_context = undefined,
//         };
//         addBefore(self.head.next_context orelse unreachable, new_ctx);
//     }

//     /// Adds a handler to the end of the pipeline (just before the tail sentinel).
//     pub fn addLast(self: *Self, name: []const u8, handler: AnyHandler) !void {
//         const new_ctx = try self.allocator.create(HandlerContext);
//         errdefer self.allocator.destroy(new_ctx);

//         new_ctx.* = .{
//             .name = name,
//             .handler = handler,
//             .pipeline = self, // Use the valid 'self' pointer
//             .conn = self.conn,
//             .next_context = undefined,
//             .prev_context = undefined,
//         };
//         addBefore(&self.tail, new_ctx);
//     }

//     // --- Trigger Inbound Events ---
//     pub fn fireActive(self: *Self) void {
//         self.head.fireActive();
//     }

//     pub fn fireInactive(self: *Self) void {
//         self.head.fireInactive();
//     }
//     pub fn fireErrorCaught(self: *Self, err: anyerror) void {
//         self.head.fireErrorCaught(err);
//     }
//     pub fn fireRead(self: *Self, msg: []const u8) void {
//         self.head.fireRead(msg);
//     }

//     pub fn fireReadComplete(self: *Self) void {
//         self.head.fireReadComplete();
//     }
//     // --- Trigger Outbound Events ---

//     pub fn write(self: *Self, msg: []const u8, future:*WriteFuture) void {
//         self.tail.write(msg, future);
//     }

//     pub fn close(self: *Self, future: *CloseFuture) void {
//         self.tail.close(future);
//     }
// };

const MockHandlerImpl = struct {
    read_msg: []u8,

    write_msg: []u8,

    alloc: std.mem.Allocator,

    pub const Self = @This();

    pub fn init(alloc: std.mem.Allocator) Self {
        return MockHandlerImpl{
            .read_msg = alloc.alloc(u8, 1024) catch unreachable,
            .write_msg = alloc.alloc(u8, 1024) catch unreachable,
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *Self) void {
        self.alloc.free(self.read_msg);
        self.alloc.free(self.write_msg);
    }

    pub fn onActive(_: *Self, _: *HandlerContext) !void {
        // Simulate some work
    }

    pub fn onInactive(_: *Self, _: *HandlerContext) !void {
        // Simulate some work
    }

    pub fn onRead(self: *Self, _: *HandlerContext, msg: []const u8) !void {
        // Simulate some work
        @memcpy(self.read_msg[0..msg.len], msg);
    }

    pub fn onReadComplete(_: *Self, _: *HandlerContext) !void {
        // Simulate some work
    }
    pub fn onErrorCaught(_: *Self, _: *HandlerContext, _: anyerror) !void {
        // Simulate some work
    }
    pub fn write(self: *Self, _: *HandlerContext, msg: []const u8, _: *WriteFuture) !void {
        // Simulate some work
        @memcpy(self.write_msg[0..msg.len], msg);
    }
    pub fn close(_: *Self, _: *HandlerContext, _: *CloseFuture) !void {
        // Simulate some work
    }

    pub fn toHandler(self: *Self) GenericHandler(*MockHandlerImpl, anyerror, anyerror, anyerror, anyerror, anyerror, anyerror, anyerror, MockHandlerImpl.onActive, MockHandlerImpl.onInactive, MockHandlerImpl.onRead, MockHandlerImpl.onReadComplete, MockHandlerImpl.onErrorCaught, MockHandlerImpl.write, MockHandlerImpl.close) {
        return .{
            .context = self,
        };
    }
};

const MockRxConnImpl = struct {
    pub const Self = @This();
    pub const WriteErr = anyerror;
    pub const CloseErr = anyerror;

    pub fn write(_: *Self, buffer: []const u8) WriteErr!usize {
        return buffer.len;
    }

    pub fn close(_: *Self) CloseErr!void {
        return;
    }
    pub fn direction(_: *Self) Direction {
        return Direction.INBOUND;
    }

    pub fn toHandler(self: *Self) GenericRxConn(*MockRxConnImpl, anyerror, anyerror, MockRxConnImpl.write, MockRxConnImpl.close, MockRxConnImpl.direction) {
        return .{
            .context = self,
        };
    }
};

test "HandlerContext interaction with MockHandler and MockConn" {
    const allocator = std.testing.allocator;

    var mock_handler_impl = MockHandlerImpl.init(allocator);
    defer mock_handler_impl.deinit();
    var mock_conn_impl = MockRxConnImpl{};

    const any_handler = mock_handler_impl.toHandler().any();
    const any_conn = mock_conn_impl.toHandler().any();

    var handler_ctx = HandlerContext{
        .name = "mock_ctx",
        .handler = any_handler,
        .conn = any_conn,
        .next_context = null, // No next handler in this simple test
        .prev_context = null, // No previous handler in this simple test
    };

    // 4. Test Inbound Event Dispatch (fireRead -> handler.onRead)
    const read_msg = "test read";
    // Calling fireRead on the context should trigger handler.onRead
    try any_handler.onRead(&handler_ctx, read_msg);

    try testing.expectEqualSlices(u8, &read_msg.*, mock_handler_impl.read_msg[0..read_msg.len]);

    const write_msg = "test write";

    const future = try allocator.create(WriteFuture);
    defer allocator.destroy(future);
    try any_handler.write(&handler_ctx, write_msg, future);

    try testing.expectEqualSlices(u8, &write_msg.*, mock_handler_impl.write_msg[0..write_msg.len]);
}
