const std = @import("std");
const quic = @import("../transport/quic/root.zig").lsquic_transport;
const proto_handler = @import("../proto_handler.zig");
const varint = @import("multiformats").uvarint;

const Allocator = std.mem.Allocator;
const log = std.log.scoped(.ping);

pub const PING_ID = "/ipfs/ping/1.0.0";
const PING_PAYLOAD_SIZE = 32;

pub const PingController = struct {
    inner: union(enum) {
        initiator: *PingInitiator,
        responder: *PingResponder,
    },

    const Self = @This();

    /// Represents the result of a ping operation.
    pub const PingResult = anyerror!u64;

    /// Sends a ping and waits for a response.
    /// The callback will be invoked with the round-trip time in milliseconds, or an error.
    pub fn ping(self: Self, callback_ctx: ?*anyopaque, callback: *const fn (ctx: ?*anyopaque, res: PingResult) void) void {
        switch (self.inner) {
            .initiator => |i| i.ping(callback_ctx, callback),
            .responder => |_| {
                // Responders cannot initiate pings.
                callback(callback_ctx, error.NotInitiator);
            },
        }
    }

    pub fn deinit(self: Self, allocator: Allocator) void {
        switch (self.inner) {
            .initiator => |i| i.deinit(allocator),
            .responder => |r| r.deinit(allocator),
        }
    }
};

/// PingProtocol is the main entry point that implements the ProtocolHandlerVTable.
/// It creates either an initiator or a responder controller.
pub const PingProtocol = struct {
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        _ = self;
    }

    fn onInitiatorStart(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!*anyopaque) void,
    ) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        log.debug("starting ping initiator", .{});

        const initiator = PingInitiator.create(self.allocator, stream) catch |err| {
            callback(callback_ctx, err);
            return;
        };

        var controller = PingController{ .inner = .{ .initiator = initiator } };
        callback(callback_ctx, @ptrCast(*anyopaque, &controller));
    }

    fn onResponderStart(
        instance: *anyopaque,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!*anyopaque) void,
    ) void {
        const self = @ptrCast(*Self, @alignCast(@alignOf(Self), instance));
        log.debug("starting ping responder", .{});

        const responder = PingResponder.create(self.allocator, stream) catch |err| {
            callback(callback_ctx, err);
            return;
        };

        var controller = PingController{ .inner = .{ .responder = responder } };
        callback(callback_ctx, @ptrCast(*anyopaque, &controller));
    }

    pub const vtable = proto_handler.ProtocolHandlerVTable{
        .onInitiatorStartFn = onInitiatorStart,
        .onResponderStartFn = onResponderStart,
    };
};

/// The responder simply echoes back any data it receives.
pub const PingResponder = struct {
    allocator: Allocator,
    stream: *quic.QuicStream,

    const Self = @This();

    pub fn create(allocator: Allocator, stream: *quic.QuicStream) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .stream = stream,
        };
        return self;
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        allocator.destroy(self);
    }

    fn onActivated(instance: *anyopaque, stream: *quic.QuicStream) void {
        _ = instance;
        _ = stream;
        log.debug("ping responder activated", .{});
    }

    fn onMessage(instance: *anyopaque, stream: *quic.QuicStream, message: []const u8) void {
        _ = instance;
        log.debug("responder received ping, echoing back", .{});
        // Echo the message back
        stream.write(message) catch |err| {
            log.err("responder failed to write: {s}", .{@errorName(err)});
            stream.close() catch {};
        };
    }

    fn onClose(instance: *anyopaque, stream: *quic.QuicStream) void {
        const self = @ptrCast(*Self, @alignCast(@alignOf(Self), instance));
        _ = stream;
        log.debug("ping responder stream closed", .{});
        self.deinit(self.allocator);
    }

    pub const vtable = proto_handler.ProtocolMessageHandlerVTable{
        .onActivatedFn = onActivated,
        .onMessageFn = onMessage,
        .onCloseFn = onClose,
    };
};

/// The initiator sends random data and measures the round-trip time.
pub const PingInitiator = struct {
    allocator: Allocator,
    stream: *quic.QuicStream,
    // Maps the random payload (as a string) to the start time and callback.
    pending_pings: std.StringHashMap(*const fn (ctx: ?*anyopaque, res: PingController.PingResult) void),

    const Self = @This();

    pub fn create(allocator: Allocator, stream: *quic.QuicStream) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .stream = stream,
            .pending_pings = std.StringHashMap(*const fn (ctx: ?*anyopaque, res: PingController.PingResult) void)
                .init(allocator),
        };
        return self;
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        // Fail any pending pings
        var it = self.pending_pings.iterator();
        while (it.next()) |entry| {
            const callback = entry.value_ptr.*;
            // We don't have the context here, so pass null.
            callback(null, error.ConnectionClosed);
        }
        self.pending_pings.deinit();
        allocator.destroy(self);
    }

    pub fn ping(
        self: *Self,
        callback_ctx: ?*anyopaque,
        callback: *const fn (ctx: ?*anyopaque, res: PingController.PingResult) void,
    ) void {
        var payload: [PING_PAYLOAD_SIZE]u8 = undefined;
        std.crypto.random.bytes(&payload);

        // Store the callback before sending
        // We use a hex-encoded string as the key for the hash map.
        var key_buf: [PING_PAYLOAD_SIZE * 2]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}", .{std.fmt.fmtSliceHexLower(&payload)}) catch unreachable;

        self.pending_pings.put(key, callback) catch |err| {
            callback(callback_ctx, err);
            return;
        };

        log.debug("initiator sending ping", .{});
        self.stream.write(&payload) catch |err| {
            // If write fails, remove the pending ping and notify the caller.
            _ = self.pending_pings.remove(key);
            callback(callback_ctx, err);
            return;
        };
    }

    fn onActivated(instance: *anyopaque, stream: *quic.QuicStream) void {
        _ = instance;
        _ = stream;
        log.debug("ping initiator activated", .{});
    }

    fn onMessage(instance: *anyopaque, stream: *quic.QuicStream, message: []const u8) void {
        const self = @ptrCast(*Self, @alignCast(@alignOf(Self), instance));
        _ = stream;

        var key_buf: [PING_PAYLOAD_SIZE * 2]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}", .{std.fmt.fmtSliceHexLower(message)}) catch unreachable;

        const entry = self.pending_pings.getEntry(key) orelse {
            log.warn("received unexpected ping response", .{});
            return;
        };

        const rtt = @intCast(u64, std.time.milliTimestamp() - entry.value_ptr.*);
        const callback = entry.value_ptr.*;

        // We don't have the original context, so we pass null.
        // A more complex implementation would store the context as well.
        callback(null, rtt);

        _ = self.pending_pings.remove(key);
    }

    fn onClose(instance: *anyopaque, stream: *quic.QuicStream) void {
        const self = @ptrCast(*Self, @alignCast(@alignOf(Self), instance));
        _ = stream;
        log.debug("ping initiator stream closed", .{});
        self.deinit(self.allocator);
    }

    pub const vtable = proto_handler.ProtocolMessageHandlerVTable{
        .onActivatedFn = onActivated,
        .onMessageFn = onMessage,
        .onCloseFn = onClose,
    };
};
