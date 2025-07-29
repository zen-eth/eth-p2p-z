const quic = @import("./transport/quic/root.zig").lsquic_transport;

// TODO: Make the stream type generic to allow different stream types.
pub const ProtocolHandlerVTable = struct {
    onInitiatorStartFn: *const fn (instance: *anyopaque, stream: *quic.QuicStream, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!*anyopaque) void) void,

    onResponderStartFn: *const fn (instance: *anyopaque, stream: *quic.QuicStream, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!*anyopaque) void) void,
};

pub const AnyProtocolHandler = struct {
    instance: *anyopaque,
    vtable: *const ProtocolHandlerVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn onInitiatorStart(self: *Self, stream: *quic.QuicStream, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!*anyopaque) void) void {
        self.vtable.onInitiatorStartFn(self.instance, stream, callback_ctx, callback);
    }

    pub fn onResponderStart(self: *Self, stream: *quic.QuicStream, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!*anyopaque) void) void {
        self.vtable.onResponderStartFn(self.instance, stream, callback_ctx, callback);
    }
};

pub const ProtocolMessageHandlerVTable = struct {
    onActivatedFn: *const fn (instance: *anyopaque, stream: *quic.QuicStream) anyerror!void,

    onMessageFn: *const fn (instance: *anyopaque, stream: *quic.QuicStream, message: []const u8) anyerror!void,

    onCloseFn: *const fn (instance: *anyopaque, stream: *quic.QuicStream) anyerror!void,
};

pub const AnyProtocolMessageHandler = struct {
    instance: *anyopaque,
    vtable: *const ProtocolMessageHandlerVTable,

    const Self = @This();
    pub const Error = anyerror;

    pub fn onActivated(self: *Self, stream: *quic.QuicStream) !void {
        try self.vtable.onActivatedFn(self.instance, stream);
    }

    pub fn onMessage(self: *Self, stream: *quic.QuicStream, message: []const u8) !void {
        try self.vtable.onMessageFn(self.instance, stream, message);
    }

    pub fn onClose(self: *Self, stream: *quic.QuicStream) !void {
        try self.vtable.onCloseFn(self.instance, stream);
    }
};
