const proto_binding = @import("../multistream/protocol_binding.zig");
const ProtocolDescriptor = @import("../multistream/protocol_descriptor.zig").ProtocolDescriptor;
const ProtocolMatcher = @import("../multistream/protocol_matcher.zig").ProtocolMatcher;
const Allocator = @import("std").mem.Allocator;
const p2p_conn = @import("../conn.zig");
const std = @import("std");
const libp2p = @import("../root.zig");
const security = libp2p.security;
const io_loop = @import("../thread_event_loop.zig");
const xev_tcp = libp2p.transport.tcp;
const ProtocolId = libp2p.protocols.ProtocolId;

pub const InsecureChannel = struct {
    protocol_descriptor: ProtocolDescriptor,

    const Self = @This();

    const mock_protocol_id: ProtocolId = "/mock/1.0.0";

    const announcements: []const ProtocolId = &[_]ProtocolId{
        mock_protocol_id,
    };

    pub fn init(self: *Self, allocator: Allocator) !void {
        var proto_desc: ProtocolDescriptor = undefined;
        var proto_matcher: ProtocolMatcher = undefined;
        try proto_matcher.initAsStrict(allocator, Self.mock_protocol_id);
        errdefer proto_matcher.deinit();
        try proto_desc.init(allocator, Self.announcements, proto_matcher);
        errdefer proto_desc.deinit();

        self.* = InsecureChannel{
            .protocol_descriptor = proto_desc,
        };
    }

    pub fn deinit(self: *Self) void {
        self.protocol_descriptor.deinit();
    }

    // --- Actual Implementations ---
    pub fn getProtoDesc(self: *Self) *ProtocolDescriptor {
        return &self.protocol_descriptor;
    }

    pub fn initConn(
        _: *Self,
        conn: p2p_conn.AnyConn,
        _: ProtocolId,
        user_data: ?*anyopaque,
        callback: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void,
    ) void {
        const handler: *InsecureHandler = conn.getPipeline().allocator.create(InsecureHandler) catch unreachable;
        handler.init(user_data, callback);
        // Free the handler when it is removed from the pipeline
        // It should be removed when the handshake is successful
        conn.getPipeline().addLast("insecure_handler", handler.any()) catch unreachable;
    }

    // --- Static Wrapper Functions ---
    pub fn vtableProtoDescFn(instance: *anyopaque) *ProtocolDescriptor {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.getProtoDesc();
    }

    pub fn vtableInitConnFn(
        instance: *anyopaque,
        conn: p2p_conn.AnyConn,
        protocol_id: ProtocolId,
        user_data: ?*anyopaque,
        callback: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void,
    ) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.initConn(conn, protocol_id, user_data, callback);
    }

    const vtable_instance = proto_binding.ProtocolBindingVTable{
        .initConnFn = vtableInitConnFn,
        .protoDescFn = vtableProtoDescFn,
    };

    pub fn any(self: *Self) proto_binding.AnyProtocolBinding {
        return .{
            .instance = self,
            .vtable = &vtable_instance,
        };
    }
};

pub const InsecureHandler = struct {
    on_handshake_context: ?*anyopaque = null,

    on_handshake_callback: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void,

    handshake_state: HandshakeState = .NotStarted,

    buffer: [1024]u8 = std.mem.zeroes([1024]u8),

    buffer_pos: usize = 0,

    const mock_handshake_msg: []const u8 = "mock_handshake";

    const HandshakeState = enum {
        NotStarted,
        InProgress,
        Successful,
        Failed,
    };

    const Self = @This();

    pub fn init(
        self: *Self,
        on_handshake_context: ?*anyopaque,
        on_handshake_callback: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void,
    ) void {
        self.* = .{
            .on_handshake_context = on_handshake_context,
            .on_handshake_callback = on_handshake_callback,
        };
    }

    // --- Actual Implementations ---
    pub fn onActive(self: *Self, ctx: *p2p_conn.ConnHandlerContext) !void {
        const write_context = ctx.pipeline.pool_manager.no_op_ctx_pool.create() catch unreachable;
        write_context.* = .{
            .ctx = ctx,
        };
        ctx.write(Self.mock_handshake_msg, write_context, io_loop.NoOpCallback.writeCallback);
        self.handshake_state = .InProgress;
    }

    pub fn onInactive(self: *Self, ctx: *p2p_conn.ConnHandlerContext) void {
        _ = self;
        ctx.fireInactive();
    }

    pub fn onRead(self: *Self, ctx: *p2p_conn.ConnHandlerContext, msg: []const u8) !void {
        @memcpy(self.buffer[self.buffer_pos .. self.buffer_pos + msg.len], msg);
        self.buffer_pos += msg.len;
        if (self.buffer_pos >= Self.mock_handshake_msg.len) {
            if (std.mem.eql(u8, self.buffer[0..Self.mock_handshake_msg.len], Self.mock_handshake_msg)) {
                self.handshake_state = .Successful;
                const session = ctx.pipeline.allocator.create(security.Session) catch unreachable;
                session.* = .{
                    .local_id = "mock_local_id",
                    .remote_id = "mock_remote_id",
                    .remote_public_key = "mock_remote_key",
                };
                self.on_handshake_callback(self.on_handshake_context, session);
                if (ctx.conn.direction() == .INBOUND) {
                    const server_handler = xev_tcp.ServerEchoHandler.create(ctx.pipeline.allocator) catch unreachable;
                    const server_handler_any = server_handler.any();
                    ctx.pipeline.addLast("server_echo_handler", server_handler_any) catch unreachable;
                } else {
                    const client_handler = xev_tcp.ClientEchoHandler.create(ctx.pipeline.allocator) catch unreachable;
                    const client_handler_any = client_handler.any();
                    ctx.pipeline.addLast("client_echo_handler", client_handler_any) catch unreachable;
                }

                if (self.buffer_pos > Self.mock_handshake_msg.len) {
                    // If there is extra data in the buffer, we need to handle it
                    const extra_data = self.buffer[Self.mock_handshake_msg.len..self.buffer_pos];
                    try ctx.fireRead(extra_data);
                }
                _ = ctx.pipeline.remove("insecure_handler") catch unreachable;
                ctx.pipeline.allocator.destroy(self);
            } else {
                self.handshake_state = .Failed;
                ctx.fireErrorCaught(
                    error.InvalidHandshake,
                );
            }
        }
    }

    pub fn onReadComplete(self: *Self, ctx: *p2p_conn.ConnHandlerContext) void {
        _ = self;
        ctx.fireReadComplete();
    }

    pub fn onErrorCaught(self: *Self, ctx: *p2p_conn.ConnHandlerContext, err: anyerror) void {
        _ = self;
        ctx.fireErrorCaught(err);
    }

    pub fn write(self: *Self, ctx: *p2p_conn.ConnHandlerContext, msg: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        _ = self;
        ctx.write(msg, user_data, callback);
    }

    pub fn close(_: *Self, ctx: *p2p_conn.ConnHandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        ctx.close(user_data, callback);
    }

    // --- Static Wrapper Functions ---
    fn vtableOnActiveFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) !void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return try self.onActive(ctx);
    }

    fn vtableOnInactiveFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInactive(ctx);
    }

    fn vtableOnReadFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, msg: []const u8) !void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return try self.onRead(ctx, msg);
    }

    fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onReadComplete(ctx);
    }

    fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, err: anyerror) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onErrorCaught(ctx, err);
    }

    fn vtableWriteFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.write(ctx, buffer, user_data, callback);
    }

    fn vtableCloseFn(instance: *anyopaque, ctx: *p2p_conn.ConnHandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.close(ctx, user_data, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = p2p_conn.ConnHandlerVTable{
        .onActiveFn = vtableOnActiveFn,
        .onInactiveFn = vtableOnInactiveFn,
        .onReadFn = vtableOnReadFn,
        .onReadCompleteFn = vtableOnReadCompleteFn,
        .onErrorCaughtFn = vtableOnErrorCaughtFn,
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
    };

    pub fn any(self: *Self) p2p_conn.AnyConnHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};
