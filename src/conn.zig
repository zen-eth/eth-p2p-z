const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const Thread = std.Thread;
const Future = @import("concurrent/future.zig").Future;
const io_loop = @import("thread_event_loop.zig");

pub const WriteFuture = Future(usize, anyerror);
pub const CloseFuture = Future(void, anyerror);
pub const Direction = enum { INBOUND, OUTBOUND };

pub const ConnUpgraderVTable = struct {
    initConnFn: *const fn (instance: *anyopaque, conn: AnyRxConn, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void) void,
};

pub const AnyConnUpgrader = struct {
    instance: *anyopaque,
    vtable: *const ConnUpgraderVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn initConn(self: Self, conn: AnyRxConn, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void) void {
        return self.vtable.initConnFn(self.instance, conn, user_data, callback);
    }
};

/// ConnInitiator interface for initializing connections.
/// This is used to set up the connection before it is used.
pub const ConnInitiatorVTable = struct {
    initConnFn: *const fn (instance: *anyopaque, conn: AnyRxConn) anyerror!void,
};

/// AnyConnInitiator is a struct that holds the instance and vtable for the ConnInitiator interface.
/// It is used to initialize connections before they are used.
pub const AnyConnInitiator = struct {
    instance: *anyopaque,
    vtable: *const ConnInitiatorVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn initConn(self: Self, conn: AnyRxConn) Error!void {
        return self.vtable.initConnFn(self.instance, conn);
    }
};

/// Handler interface for handling events in the pipeline.
pub const HandlerVTable = struct {
    onActiveFn: *const fn (instance: *anyopaque, ctx: *HandlerContext) void,
    onInactiveFn: *const fn (instance: *anyopaque, ctx: *HandlerContext) void,
    onReadFn: *const fn (instance: *anyopaque, ctx: *HandlerContext, msg: []const u8) void,
    onReadCompleteFn: *const fn (instance: *anyopaque, ctx: *HandlerContext) void,
    onErrorCaughtFn: *const fn (instance: *anyopaque, ctx: *HandlerContext, err: anyerror) void,
    writeFn: *const fn (instance: *anyopaque, ctx: *HandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void,
    closeFn: *const fn (instance: *anyopaque, ctx: *HandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void,
};

/// AnyHandler is a struct that holds the instance and vtable for the Handler interface.
pub const AnyHandler = struct {
    instance: *anyopaque,
    vtable: *const HandlerVTable,

    const Self = @This();

    pub fn onActive(self: Self, ctx: *HandlerContext) void {
        return self.vtable.onActiveFn(self.instance, ctx);
    }
    pub fn onInactive(self: Self, ctx: *HandlerContext) void {
        return self.vtable.onInactiveFn(self.instance, ctx);
    }
    pub fn onRead(self: Self, ctx: *HandlerContext, msg: []const u8) void {
        return self.vtable.onReadFn(self.instance, ctx, msg);
    }
    pub fn onReadComplete(self: Self, ctx: *HandlerContext) void {
        return self.vtable.onReadCompleteFn(self.instance, ctx);
    }
    pub fn onErrorCaught(self: Self, ctx: *HandlerContext, err: anyerror) void {
        return self.vtable.onErrorCaughtFn(self.instance, ctx, err);
    }
    pub fn write(self: Self, ctx: *HandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        return self.vtable.writeFn(self.instance, ctx, buffer, user_data, callback);
    }
    pub fn close(self: Self, ctx: *HandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        return self.vtable.closeFn(self.instance, ctx, user_data, callback);
    }
};

/// Reactive connection interface for handling read/write operations.
/// This interface is used to abstract the underlying connection implementation.
pub const RxConnVTable = struct {
    writeFn: *const fn (
        instance: *anyopaque,
        buffer: []const u8,
        erased_userdata: ?*anyopaque,
        wrapped_cb: *const fn (ud: ?*anyopaque, r: anyerror!usize) void,
    ) void,
    closeFn: *const fn (instance: *anyopaque, erased_userdata: ?*anyopaque, wrapped_cb: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void,
    getPipelineFn: *const fn (instance: *anyopaque) *HandlerPipeline,
    directionFn: *const fn (instance: *anyopaque) Direction,
};

/// AnyRxConn is a struct that holds the instance and vtable for the Reactive connection interface.
/// It is used to perform read/write operations on the connection.
pub const AnyRxConn = struct {
    instance: *anyopaque,
    vtable: *const RxConnVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn direction(self: Self) Direction {
        return self.vtable.directionFn(self.instance);
    }
    pub fn getPipeline(self: Self) *HandlerPipeline {
        return self.vtable.getPipelineFn(self.instance);
    }
    pub fn write(
        self: Self,
        buffer: []const u8,
        userdata: ?*anyopaque,
        callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void,
    ) void {
        self.vtable.writeFn(self.instance, buffer, userdata, callback);
    }

    pub fn close(
        self: Self,
        userdata: ?*anyopaque,
        callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void,
    ) void {
        self.vtable.closeFn(self.instance, userdata, callback);
    }
};

/// HandlerContext is a struct that represents the context of a handler in the pipeline.
/// It holds the handler instance, the pipeline it belongs to, and the connection it is associated with.
/// It also provides methods for event propagation and finding next/previous contexts.
/// The context is used to manage the flow of events through the pipeline.
/// It is a linked list of handler contexts, where each context points to the next and previous contexts.
pub const HandlerContext = struct {
    name: []const u8,
    handler: AnyHandler,
    pipeline: *HandlerPipeline,
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
            next_ctx.handler.onActive(next_ctx);
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

    pub fn fireRead(self: *Self, msg: []const u8) void {
        if (self.findNextInbound()) |next_ctx| {
            next_ctx.handler.onRead(next_ctx, msg);
        }
    }

    pub fn fireReadComplete(self: *Self) void {
        if (self.findNextInbound()) |next_ctx| {
            next_ctx.handler.onReadComplete(next_ctx);
        }
    }

    pub fn write(self: *Self, msg: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        if (self.findPrevOutbound()) |prev_ctx| {
            prev_ctx.handler.write(prev_ctx, msg, user_data, callback);
        }
    }

    pub fn close(self: *Self, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        if (self.findPrevOutbound()) |prev_ctx| {
            prev_ctx.handler.close(prev_ctx, user_data, callback);
        }
    }
};

/// Head handler implementation.
/// This handler is responsible for managing the connection and triggering events.
/// It is the first handler in the pipeline and is responsible for managing the connection lifecycle.
/// It is also responsible for writing data to the connection and closing it.
/// The head handler is a special case in the pipeline, as it does not have a previous context.
/// It is the entry point for inbound events and the exit point for outbound events.
const HeadHandlerImpl = struct {
    conn: AnyRxConn,

    pub const Self = @This();

    // --- Actual Implementations ---
    pub fn onActive(_: *Self, ctx: *HandlerContext) void {
        ctx.fireActive();
    }

    pub fn onInactive(_: *Self, ctx: *HandlerContext) void {
        ctx.fireInactive();
    }

    pub fn onRead(_: *Self, ctx: *HandlerContext, msg: []const u8) void {
        ctx.fireRead(msg);
    }

    pub fn onReadComplete(_: *Self, ctx: *HandlerContext) void {
        ctx.fireReadComplete();
    }

    pub fn onErrorCaught(_: *Self, ctx: *HandlerContext, err: anyerror) void {
        ctx.fireErrorCaught(err);
    }

    pub fn write(
        self: *Self,
        _: *HandlerContext,
        buffer: []const u8,
        user_data: ?*anyopaque,
        callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void,
    ) void {
        self.conn.write(buffer, user_data, callback);
    }

    pub fn close(
        self: *Self,
        _: *HandlerContext,
        user_data: ?*anyopaque,
        callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void,
    ) void {
        std.debug.print("HeadHandlerImpl: Closing connection with user_data\n", .{});
        self.conn.close(user_data, callback);
    }

    // --- Static Wrapper Functions ---
    fn vtableOnActiveFn(instance: *anyopaque, ctx: *HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActive(ctx);
    }

    fn vtableOnInactiveFn(instance: *anyopaque, ctx: *HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInactive(ctx);
    }

    fn vtableOnReadFn(instance: *anyopaque, ctx: *HandlerContext, msg: []const u8) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onRead(ctx, msg);
    }

    fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onReadComplete(ctx);
    }

    fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *HandlerContext, err: anyerror) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onErrorCaught(ctx, err);
    }

    fn vtableWriteFn(instance: *anyopaque, ctx: *HandlerContext, buffer: []const u8, erased_userdata: ?*anyopaque, wrapped_cb: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.write(ctx, buffer, erased_userdata, wrapped_cb);
    }

    fn vtableCloseFn(instance: *anyopaque, ctx: *HandlerContext, erased_userdata: ?*anyopaque, wrapped_cb: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.close(ctx, erased_userdata, wrapped_cb);
    }

    // --- Static VTable Instance ---
    const vtable_instance = HandlerVTable{
        .onActiveFn = vtableOnActiveFn,
        .onInactiveFn = vtableOnInactiveFn,
        .onReadFn = vtableOnReadFn,
        .onReadCompleteFn = vtableOnReadCompleteFn,
        .onErrorCaughtFn = vtableOnErrorCaughtFn,
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
    };

    pub fn any(self: *Self) AnyHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

/// Tail handler implementation.
/// This handler is responsible for managing the connection and triggering events.
/// It is the last handler in the pipeline and is responsible for managing the connection lifecycle.
/// It is also responsible for writing data to the connection and closing it.
/// The tail handler is a special case in the pipeline, as it does not have a next context.
/// It is the exit point for inbound events and the entry point for outbound events.
const TailHandlerImpl = struct {
    pub const Self = @This();

    // --- Actual Implementations ---
    pub fn onActive(_: *Self, _: *HandlerContext) void {}

    pub fn onInactive(_: *Self, _: *HandlerContext) void {}

    pub fn onRead(_: *Self, _: *HandlerContext, _: []const u8) void {}

    pub fn onReadComplete(_: *Self, _: *HandlerContext) void {}

    pub fn onErrorCaught(_: *Self, ctx: *HandlerContext, err: anyerror) void {
        std.log.warn("Handler '{s}' error during onErrorCaught: {any}", .{ ctx.name, err });
    }

    pub fn write(
        _: *Self,
        _: *HandlerContext,
        _: []const u8,
        _: ?*anyopaque,
        _: ?*const fn (ud: ?*anyopaque, r: anyerror!usize) void,
    ) void {}

    pub fn close(
        _: *Self,
        _: *HandlerContext,
        _: ?*anyopaque,
        _: *const fn (ud: ?*anyopaque, r: anyerror!void) void,
    ) void {}

    // --- Static Wrapper Functions ---
    fn vtableOnActiveFn(instance: *anyopaque, ctx: *HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActive(ctx);
    }

    fn vtableOnInactiveFn(instance: *anyopaque, ctx: *HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInactive(ctx);
    }

    fn vtableOnReadFn(instance: *anyopaque, ctx: *HandlerContext, msg: []const u8) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onRead(ctx, msg);
    }

    fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onReadComplete(ctx);
    }

    fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *HandlerContext, err: anyerror) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onErrorCaught(ctx, err);
    }

    fn vtableWriteFn(instance: *anyopaque, ctx: *HandlerContext, buffer: []const u8, erased_userdata: ?*anyopaque, wrapped_cb: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.write(ctx, buffer, erased_userdata, wrapped_cb);
    }

    fn vtableCloseFn(instance: *anyopaque, ctx: *HandlerContext, erased_userdata: ?*anyopaque, wrapped_cb: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.close(ctx, erased_userdata, wrapped_cb);
    }

    // --- Static VTable Instance ---
    const vtable_instance = HandlerVTable{
        .onActiveFn = vtableOnActiveFn,
        .onInactiveFn = vtableOnInactiveFn,
        .onReadFn = vtableOnReadFn,
        .onReadCompleteFn = vtableOnReadCompleteFn,
        .onErrorCaughtFn = vtableOnErrorCaughtFn,
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
    };

    pub fn any(self: *Self) AnyHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

const MemoryPool = struct {
    allocator: std.mem.Allocator,
    io_no_op_context_pool: io_loop.NoOpContextMemoryPool,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) !Self {
        var no_op_ctx_pool = io_loop.NoOpContextMemoryPool.init(allocator);
        errdefer no_op_ctx_pool.deinit();

        return .{
            .allocator = allocator,
            .io_no_op_context_pool = no_op_ctx_pool,
        };
    }

    pub fn deinit(self: *Self) void {
        self.io_no_op_context_pool.deinit();
    }
};
/// HandlerPipeline manages a doubly-linked list of Handlers that process I/O
/// events and operations for an associated connection (AnyRxConn). It allows
/// for dynamic modification of the pipeline, enabling flexible processing logic.
///
/// Design:
/// 1. Structure: Implemented as a doubly-linked list of HandlerContext nodes.
/// 2. Sentinel Nodes: Uses 'head' and 'tail' HandlerContext nodes as sentinels.
///    These contain HeadHandlerImpl and TailHandlerImpl respectively, simplifying
///    list operations and marking boundaries.
/// 3. HandlerContext: Each node wraps an AnyHandler (the actual handler logic),
///    references the pipeline and connection, and holds next/prev pointers.
/// 4. Event Flow (Inbound): Events like read, active, inactive typically originate
///    from the underlying connection, are passed to the pipeline's `fire...` methods
///    (which start at the head), and propagate forward (via next_context) towards the tail.
/// 5. Operation Flow (Outbound): Operations like write, close are initiated via
///    the pipeline's `write`/`close` methods (which start at the tail), and propagate
///    backward (via prev_context) towards the head.
/// 6. HeadHandlerImpl Role: Interacts with the AnyRxConn for outbound operations
///    (write/close), terminating backward propagation. Starts forward propagation
///    for inbound events.
/// 7. TailHandlerImpl Role: Entry point for outbound operations started via
///    `pipeline.write/close`. Terminates forward propagation of inbound events.
/// 8. Memory: Manages HandlerContext nodes using its allocator. It does *not*
///    typically own the handler *instances* added via addFirst/addLast.
///
/// Diagram:
///
///   [ Inbound Events ]                                      [ Outbound Operations ]
///   (read, active, ...)                                     (write, close, ...)
///          |                                                        ^
///          v                                                        |
///     +----------+       next       +----------+       next       +----------+
///     |  Head    | ---------------> | Handler  | ---> ...... ---> |  Tail    |
///     | Context  | <--------------- | Context  | <--- ...... <--- | Context  |
///     +----------+       prev       +----------+       prev       +----------+
///          |                              |                              |
///          v                              v                              v
///  +---------------+              +---------------+              +---------------+
///  | HeadHandler   |              | User Handler  |              | TailHandler   |
///  | Impl          |              | Impl          |              | Impl          |
///  | (writes/closes|              | (processes    |              | (starts       |
///  |  to conn)     |              |  data/events) |              |  outbound ops)|
///  +---------------+              +---------------+              +---------------+
///          ^
///          | interacts with
///          v
///  +---------------+
///  | Connection    |
///  | (AnyRxConn)   |
///  +---------------+
///
/// Inbound Flow:  Connection -> Head -> Handler1 -> ... -> Tail (ends)
/// Outbound Flow: Pipeline.write/close -> Tail -> ... -> Handler1 -> Head -> Connection
///
pub const HandlerPipeline = struct {
    allocator: std.mem.Allocator,
    mempool: *MemoryPool,
    head: HandlerContext,
    tail: HandlerContext,
    conn: AnyRxConn,

    head_handler_impl: HeadHandlerImpl,
    tail_handler_impl: TailHandlerImpl,

    const Self = @This();

    pub fn init(self: *Self, allocator: std.mem.Allocator, associated_conn: AnyRxConn) !void {
        var mempool = try allocator.create(MemoryPool);
        errdefer allocator.destroy(mempool);

        mempool.* = try MemoryPool.init(allocator);
        errdefer mempool.deinit();

        self.* = .{
            .allocator = allocator,
            .mempool = mempool,
            .conn = associated_conn,
            .head_handler_impl = .{ .conn = associated_conn },
            .tail_handler_impl = .{},
            .head = HandlerContext{
                .name = "HEAD",
                .handler = undefined,
                .pipeline = self,
                .conn = associated_conn,
                .next_context = undefined,
                .prev_context = null,
            },

            .tail = HandlerContext{
                .name = "TAIL",
                .handler = undefined,
                .pipeline = self,
                .conn = associated_conn,
                .next_context = null,
                .prev_context = undefined,
            },
        };

        const head_any_handler = self.head_handler_impl.any();
        const tail_any_handler = self.tail_handler_impl.any();

        self.head.handler = head_any_handler;
        self.tail.handler = tail_any_handler;

        self.head.next_context = &self.tail;
        self.tail.prev_context = &self.head;
    }

    pub fn deinit(self: *Self) void {
        var current = self.head.next_context;
        while (current != null and current != &self.tail) {
            const next = current.?.next_context;
            self.allocator.destroy(current.?);
            current = next;
        }
        self.head.next_context = &self.tail;
        self.tail.prev_context = &self.head;
        self.mempool.deinit();
        self.allocator.destroy(self.mempool);
    }

    /// Adds a handler context node *before* the specified 'next' node.
    fn addBefore(next_ctx: *HandlerContext, new_ctx: *HandlerContext) void {
        const prev_ctx = next_ctx.prev_context orelse unreachable;

        new_ctx.prev_context = prev_ctx;
        new_ctx.next_context = next_ctx;

        prev_ctx.next_context = new_ctx;
        next_ctx.prev_context = new_ctx;
    }

    /// Adds a handler to the beginning of the pipeline (just after the head sentinel).
    pub fn addFirst(self: *Self, name: []const u8, handler: AnyHandler) !void {
        const new_ctx = try self.allocator.create(HandlerContext);
        errdefer self.allocator.destroy(new_ctx);

        new_ctx.* = .{
            .name = name,
            .handler = handler,
            .pipeline = self,
            .conn = self.conn,
            .next_context = undefined,
            .prev_context = undefined,
        };
        addBefore(self.head.next_context orelse unreachable, new_ctx);
    }

    /// Adds a handler to the end of the pipeline (just before the tail sentinel).
    pub fn addLast(self: *Self, name: []const u8, handler: AnyHandler) !void {
        const new_ctx = try self.allocator.create(HandlerContext);
        errdefer self.allocator.destroy(new_ctx);
        new_ctx.* = .{
            .name = name,
            .handler = handler,
            .pipeline = self,
            .conn = self.conn,
            .next_context = undefined,
            .prev_context = undefined,
        };
        addBefore(&self.tail, new_ctx);
    }

    /// Removes a handler by its name from the pipeline.
    /// Deallocates the HandlerContext. The handler instance itself is not deinitialized by this function.
    /// The caller is responsible for deinitializing the actual handler instance if needed.
    pub fn remove(self: *Self, name: []const u8) !AnyHandler {
        var current = self.head.next_context;
        while (current != null and current != &self.tail) : (current = current.?.next_context) {
            const ctx_to_check = current.?;
            if (std.mem.eql(u8, ctx_to_check.name, name)) {
                const prev_ctx = ctx_to_check.prev_context orelse unreachable;
                const next_ctx = ctx_to_check.next_context orelse unreachable;

                prev_ctx.next_context = next_ctx;
                next_ctx.prev_context = prev_ctx;

                const ctx_to_check_handler = ctx_to_check.handler;
                self.allocator.destroy(ctx_to_check);
                return ctx_to_check_handler;
            }
        }
        return error.NotFound;
    }

    // --- Trigger Inbound Events ---
    pub fn fireActive(self: *Self) void {
        self.head.fireActive();
    }

    pub fn fireInactive(self: *Self) void {
        self.head.fireInactive();
    }
    pub fn fireErrorCaught(self: *Self, err: anyerror) void {
        self.head.fireErrorCaught(err);
    }
    pub fn fireRead(self: *Self, msg: []const u8) void {
        self.head.fireRead(msg);
    }

    pub fn fireReadComplete(self: *Self) void {
        self.head.fireReadComplete();
    }

    // --- Trigger Outbound Events ---
    pub fn write(self: *Self, msg: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        self.tail.write(msg, user_data, callback);
    }

    pub fn close(self: *Self, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        self.tail.close(user_data, callback);
    }
};

/// Mock handler implementation for testing.
/// This is a mock implementation of the Handler interface.
/// It is used for testing purposes and does not perform any real operations.
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

    // --- Actual Implementations ---
    pub fn onActive(_: *Self, _: *HandlerContext) void {}

    pub fn onInactive(_: *Self, _: *HandlerContext) void {}

    pub fn onRead(self: *Self, ctx: *HandlerContext, msg: []const u8) void {
        @memcpy(self.read_msg[0..msg.len], msg);
        ctx.fireRead(msg);
    }

    pub fn onReadComplete(_: *Self, _: *HandlerContext) void {}

    pub fn onErrorCaught(_: *Self, _: *HandlerContext, _: anyerror) void {}

    pub fn write(self: *Self, ctx: *HandlerContext, msg: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        @memcpy(self.write_msg[0..msg.len], msg);
        ctx.write(msg, user_data, callback);
    }

    pub fn close(_: *Self, ctx: *HandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        ctx.close(user_data, callback);
    }

    // --- Static Wrapper Functions ---
    fn vtableOnActiveFn(instance: *anyopaque, ctx: *HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActive(ctx);
    }

    fn vtableOnInactiveFn(instance: *anyopaque, ctx: *HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInactive(ctx);
    }

    fn vtableOnReadFn(instance: *anyopaque, ctx: *HandlerContext, msg: []const u8) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onRead(ctx, msg);
    }

    fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onReadComplete(ctx);
    }

    fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *HandlerContext, err: anyerror) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onErrorCaught(ctx, err);
    }

    fn vtableWriteFn(instance: *anyopaque, ctx: *HandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.write(ctx, buffer, user_data, callback);
    }

    fn vtableCloseFn(instance: *anyopaque, ctx: *HandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.close(ctx, user_data, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = HandlerVTable{
        .onActiveFn = vtableOnActiveFn,
        .onInactiveFn = vtableOnInactiveFn,
        .onReadFn = vtableOnReadFn,
        .onReadCompleteFn = vtableOnReadCompleteFn,
        .onErrorCaughtFn = vtableOnErrorCaughtFn,
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
    };

    pub fn any(self: *Self) AnyHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

const MockRxConnImpl = struct {
    closed: bool = false,
    write_msg: []u8,
    alloc: std.mem.Allocator,
    pipeline: ?*HandlerPipeline = null,

    pub const Self = @This();

    pub fn init(alloc: std.mem.Allocator) Self {
        return MockRxConnImpl{
            .write_msg = alloc.alloc(u8, 1024) catch unreachable,
            .alloc = alloc,
            .pipeline = null,
        };
    }

    pub fn deinit(self: *Self) void {
        self.alloc.free(self.write_msg);
    }

    // --- Actual Implementations ---
    pub fn write(
        self: *Self,
        buffer: []const u8,
        erased_userdata: ?*anyopaque,
        wrapped_cb: *const fn (ud: ?*anyopaque, r: anyerror!usize) void,
    ) void {
        @memcpy(self.write_msg[0..buffer.len], buffer);
        wrapped_cb(erased_userdata, buffer.len);
    }

    pub fn close(
        self: *Self,
        erased_userdata: ?*anyopaque,
        wrapped_cb: *const fn (ud: ?*anyopaque, r: anyerror!void) void,
    ) void {
        self.closed = true;
        wrapped_cb(erased_userdata, {});
    }

    pub fn direction(_: *Self) Direction {
        return Direction.INBOUND;
    }

    pub fn getPipeline(self: *Self) *HandlerPipeline {
        return self.pipeline orelse @panic("Pipeline not set in MockRxConnImpl");
    }

    // --- Static Wrapper Functions ---
    fn vtableWriteFn(
        instance: *anyopaque,
        buffer: []const u8,
        erased_userdata: ?*anyopaque,
        wrapped_cb: *const fn (ud: ?*anyopaque, r: anyerror!usize) void,
    ) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.write(buffer, erased_userdata, wrapped_cb);
    }

    fn vtableCloseFn(
        instance: *anyopaque,
        erased_userdata: ?*anyopaque,
        wrapped_cb: *const fn (ud: ?*anyopaque, r: anyerror!void) void,
    ) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.close(erased_userdata, wrapped_cb);
    }

    fn vtableGetPipelineFn(instance: *anyopaque) *HandlerPipeline {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.getPipeline();
    }

    fn vtableDirectionFn(instance: *anyopaque) Direction {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.direction();
    }

    // --- Static VTable Instance ---
    const vtable_instance = RxConnVTable{
        .getPipelineFn = vtableGetPipelineFn,
        .directionFn = vtableDirectionFn,
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
    };

    pub fn any(self: *Self) AnyRxConn {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

const OnWriteCallback = struct {
    fn callback(
        _: ?*anyopaque,
        r: anyerror!usize,
    ) void {
        if (r) |_| {} else |_| {
            @panic("write callback received an unexpected error");
        }
    }
};

test "HandlerContext interaction with MockHandler and MockConn (VTable)" {
    const allocator = std.testing.allocator;

    var mock_handler_impl = MockHandlerImpl.init(allocator);
    defer mock_handler_impl.deinit();
    var mock_conn_impl = MockRxConnImpl.init(allocator);
    defer mock_conn_impl.deinit();

    const any_handler = mock_handler_impl.any();
    const any_conn = mock_conn_impl.any();

    var handler_ctx = HandlerContext{
        .name = "mock_ctx",
        .handler = any_handler, // Use value
        .conn = any_conn, // Use value
        .pipeline = undefined, // Not needed for this direct test
        .next_context = null,
        .prev_context = null,
    };

    // Test Inbound Event Dispatch (fireRead -> handler.onRead)
    const read_msg = "test read";
    handler_ctx.handler.onRead(&handler_ctx, read_msg);
    try testing.expectEqualSlices(u8, &read_msg.*, mock_handler_impl.read_msg[0..read_msg.len]);

    // Test Outbound Event Dispatch (write -> handler.write)
    const write_msg = "test write";
    var onWriteCallback = OnWriteCallback{};

    handler_ctx.handler.write(&handler_ctx, write_msg, &onWriteCallback, OnWriteCallback.callback);
    try testing.expectEqualSlices(u8, &write_msg.*, mock_handler_impl.write_msg[0..write_msg.len]);
}

test "HandlerPipeline interaction with MockHandler and MockConn (VTable)" {
    const allocator = std.testing.allocator;

    const mock_conn_impl = try allocator.create(MockRxConnImpl);
    defer allocator.destroy(mock_conn_impl);
    mock_conn_impl.* = MockRxConnImpl.init(allocator);
    defer mock_conn_impl.deinit();

    const any_conn = mock_conn_impl.any();

    var pipeline: HandlerPipeline = undefined;
    try pipeline.init(allocator, any_conn);
    defer pipeline.deinit();

    var mock_handler_1_impl = MockHandlerImpl.init(allocator);
    defer mock_handler_1_impl.deinit();
    const any_handler_1 = mock_handler_1_impl.any();

    var mock_handler_2_impl = MockHandlerImpl.init(allocator);
    defer mock_handler_2_impl.deinit();
    const any_handler_2 = mock_handler_2_impl.any();

    try pipeline.addLast("mock1", any_handler_1);
    try pipeline.addLast("mock2", any_handler_2);

    // Test Inbound Event (fireRead)
    // HEAD -> mock1 -> mock2 -> TAIL
    const read_msg = "inbound data";
    pipeline.fireRead(read_msg);

    try testing.expectEqualSlices(u8, &read_msg.*, mock_handler_2_impl.read_msg[0..read_msg.len]);

    // Test Outbound Event (write)
    // TAIL -> mock2 -> mock1 -> HEAD -> conn
    const write_msg = "outbound data";

    var onWriteCallback = OnWriteCallback{};
    pipeline.write(write_msg, &onWriteCallback, OnWriteCallback.callback);

    try testing.expectEqualSlices(u8, &write_msg.*, mock_handler_1_impl.write_msg[0..write_msg.len]);
    try testing.expectEqualSlices(u8, &write_msg.*, mock_conn_impl.write_msg[0..write_msg.len]);

    // Test Outbound Event (close)
    // TAIL -> mock2 -> mock1 -> HEAD -> conn
    pipeline.close(null, struct {
        fn callback(
            _: ?*anyopaque,
            r: anyerror!void,
        ) void {
            if (r) |_| {} else |_| {
                @panic("close callback received an unexpected error");
            }
        }
    }.callback);

    try testing.expect(mock_conn_impl.closed);
}

test "HandlerPipeline.remove functionality" {
    const allocator = std.testing.allocator;

    var mock_conn_impl_val = MockRxConnImpl.init(allocator);
    defer mock_conn_impl_val.deinit();
    const any_conn = mock_conn_impl_val.any();

    var pipeline: HandlerPipeline = undefined;
    try pipeline.init(allocator, any_conn);
    defer pipeline.deinit();

    var mh1_impl = MockHandlerImpl.init(allocator);
    defer mh1_impl.deinit();
    var mh2_impl = MockHandlerImpl.init(allocator);
    defer mh2_impl.deinit();
    var mh3_impl = MockHandlerImpl.init(allocator);
    defer mh3_impl.deinit();

    const h1 = mh1_impl.any();
    const h2 = mh2_impl.any();
    const h3 = mh3_impl.any();

    // Add handlers: HEAD <> h1 <> h2 <> h3 <> TAIL
    try pipeline.addLast("h1", h1);
    try pipeline.addLast("h2", h2);
    try pipeline.addLast("h3", h3);

    // Remove handler h2: HEAD <> h1 <> h3 <> TAIL
    _ = try pipeline.remove("h2");

    // 1. Test Inbound Event (fireRead)
    // Expected: HEAD -> h1.onRead -> h3.onRead -> TAIL
    const read_msg = "inbound after remove";
    // Clear previous read messages if any (or use fresh mocks)
    @memset(mh1_impl.read_msg, 0);
    @memset(mh2_impl.read_msg, 0);
    @memset(mh3_impl.read_msg, 0);

    pipeline.fireRead(read_msg);

    try testing.expectEqualSlices(u8, read_msg, mh1_impl.read_msg[0..read_msg.len]);
    try testing.expectEqualSlices(u8, read_msg, mh3_impl.read_msg[0..read_msg.len]);

    if (mh2_impl.read_msg.len > 0) {
        const expected_zeros = try allocator.alloc(u8, mh2_impl.read_msg.len);
        defer allocator.free(expected_zeros);
        @memset(expected_zeros, 0);
        try testing.expectEqualSlices(u8, expected_zeros, mh2_impl.read_msg);
    }

    // 2. Test Outbound Event (write)
    // Expected: TAIL -> h3.write -> h1.write -> HEAD -> conn.write
    const write_msg = "outbound after remove";
    // Clear previous write messages
    @memset(mh1_impl.write_msg, 0);
    @memset(mh2_impl.write_msg, 0);
    @memset(mh3_impl.write_msg, 0);
    @memset(mock_conn_impl_val.write_msg, 0);

    var on_write_cb_data = OnWriteCallback{};
    pipeline.write(write_msg, &on_write_cb_data, OnWriteCallback.callback);

    try testing.expectEqualSlices(u8, write_msg, mh3_impl.write_msg[0..write_msg.len]);
    try testing.expectEqualSlices(u8, write_msg, mh1_impl.write_msg[0..write_msg.len]);
    try testing.expectEqualSlices(u8, write_msg, mock_conn_impl_val.write_msg[0..write_msg.len]);

    if (mh2_impl.write_msg.len > 0) {
        const expected_zeros_for_write = try allocator.alloc(u8, mh2_impl.write_msg.len);
        defer allocator.free(expected_zeros_for_write);
        @memset(expected_zeros_for_write, 0);
        try testing.expectEqualSlices(u8, expected_zeros_for_write, mh2_impl.write_msg);
    }

    // 3. Test removing a non-existent handler
    const remove_non_existent_result = pipeline.remove("non_existent_handler");
    try testing.expectError(error.NotFound, remove_non_existent_result);

    // 4. Test removing handler h1: HEAD <> h3 <> TAIL
    _ = try pipeline.remove("h1");
    @memset(mh1_impl.read_msg, 0);
    @memset(mh3_impl.read_msg, 0);
    pipeline.fireRead("inbound after h1 remove");
    try testing.expectEqualSlices(u8, "inbound after h1 remove", mh3_impl.read_msg[0.."inbound after h1 remove".len]);
    if (mh1_impl.read_msg.len > 0) {
        const expected_zeros_h1_read = try allocator.alloc(u8, mh1_impl.read_msg.len);
        defer allocator.free(expected_zeros_h1_read);
        @memset(expected_zeros_h1_read, 0);
        try testing.expectEqualSlices(u8, expected_zeros_h1_read, mh1_impl.read_msg);
    }

    // 5. Test removing handler h3: HEAD <> TAIL (empty user pipeline)
    _ = try pipeline.remove("h3");
    @memset(mh3_impl.read_msg, 0);
    @memset(mock_conn_impl_val.write_msg, 0);

    pipeline.fireRead("inbound after h3 remove");
    if (mh3_impl.read_msg.len > 0) {
        const expected_zeros_h3_read = try allocator.alloc(u8, mh3_impl.read_msg.len);
        defer allocator.free(expected_zeros_h3_read);
        @memset(expected_zeros_h3_read, 0);
        try testing.expectEqualSlices(u8, expected_zeros_h3_read, mh3_impl.read_msg);
    }

    // For an empty pipeline, write goes from TailHandler -> HeadHandler -> Connection
    pipeline.write("outbound after h3 remove", &on_write_cb_data, OnWriteCallback.callback);
    try testing.expectEqualSlices(u8, "outbound after h3 remove", mock_conn_impl_val.write_msg[0.."outbound after h3 remove".len]);
}
