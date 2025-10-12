const std = @import("std");
const libp2p = @import("../root.zig");
const protocols = libp2p.protocols;
const quic = libp2p.transport.quic;
const Allocator = std.mem.Allocator;
const p2p_conn = libp2p.conn;
const uvarint = @import("multiformats").uvarint;

/// The Multistream Negotiator is responsible for negotiating the protocol to use
/// for a given connection/stream. It handles the multistream handshake process and
/// manages the state of the negotiation.
pub const Negotiator = struct {
    const MULTISTREAM_PROTO = "/multistream/1.0.0";
    const MESSAGE_SUFFIX = "\n";
    const NA = "na";
    const LS = "ls";

    const MAX_MULTISTREAM_MESSAGE_LENGTH = 1024;
    const MESSAGE_SUFFIX_LENGTH = MESSAGE_SUFFIX.len;
    const MAX_PROTOCOL_ID_LENGTH = MAX_MULTISTREAM_MESSAGE_LENGTH - MESSAGE_SUFFIX_LENGTH;
    const MAX_LENGTH_BYTES = 2;
    const TOTAL_MESSAGE_LENGTH = MAX_LENGTH_BYTES + MAX_MULTISTREAM_MESSAGE_LENGTH;
    const NEGOTIATION_TIMEOUT_SECONDS = 10;

    pub const NegotiatorError = error{
        ProtocolIdTooLong,
        InvalidMultistreamSuffix,
        FirstLineShouldBeMultistream,
        AllProposedProtocolsRejected,
        NoSupportedProtocols,
        NoProposedProtocols,
        ProposedProtocolNotSupported,
    };

    const State = enum {
        INIT,
        HEADER_RECEIVED,
        PROTOCOL_SELECTED,
    };

    // Proposed protocol IDs is the one that the initiator proposes to use.
    proposed_protocols: ?[]const []const u8,

    // Supported protocols by the negotiator.
    supported_protocols: *std.StringHashMap(protocols.AnyProtocolHandler),

    negotiation_timeout: u64,

    allocator: Allocator,

    buffer: *std.fifo.LinearFifo(u8, .Slice),

    // Current proposed protocol by the initiator.
    proposed_protocol_index: usize,

    on_selected_ctx: ?*anyopaque,

    on_selected: *const fn (instance: ?*anyopaque, proto_id: protocols.ProtocolId, selected_handler: *protocols.AnyProtocolHandler) void,

    is_initiator: bool,

    state: State,

    const Self = @This();

    const WriteCallbackContext = struct {
        negotiator: *Self,
        buffer: []const u8,
        proto_id: ?protocols.ProtocolId,
        selected_handler: ?*protocols.AnyProtocolHandler,
    };

    const WriteCallback = struct {
        pub fn callback(w: ?*anyopaque, n: anyerror!usize) void {
            const w_ctx: *WriteCallbackContext = @ptrCast(@alignCast(w.?));

            if (n) |_| {
                // Responder case: we have selected a protocol ID, we need to inform the negotiator
                if (w_ctx.selected_handler) |selected_handler| {
                    w_ctx.negotiator.on_selected(w_ctx.negotiator.on_selected_ctx, w_ctx.proto_id.?, selected_handler);
                }

                w_ctx.negotiator.allocator.free(w_ctx.buffer);
                w_ctx.negotiator.allocator.destroy(w_ctx);
            } else |err| {
                std.log.warn("Multistream Negotiator write error: {}\n", .{err});
                w_ctx.negotiator.allocator.free(w_ctx.buffer);
                w_ctx.negotiator.allocator.destroy(w_ctx);
            }
        }
    };

    pub fn init(
        self: *Self,
        allocator: std.mem.Allocator,
        negotiation_timeout: u64,
        proposed_protocols: ?[]const []const u8,
        supported_protocols: *std.StringHashMap(protocols.AnyProtocolHandler),
        buffer: *std.fifo.LinearFifo(u8, .Slice),
        is_initiator: bool,
        on_selected_ctx: ?*anyopaque,
        on_selected: *const fn (instance: ?*anyopaque, proto_id: protocols.ProtocolId, selected_handler: *protocols.AnyProtocolHandler) void,
    ) !void {
        if (supported_protocols.count() == 0) {
            return NegotiatorError.NoSupportedProtocols;
        }

        if (is_initiator) {
            if (proposed_protocols == null or proposed_protocols.?.len == 0) {
                return NegotiatorError.NoProposedProtocols;
            }

            // check proposed protocols exists in the supported protocols
            for (proposed_protocols.?) |proto_id| {
                if (!supported_protocols.contains(proto_id)) {
                    return NegotiatorError.ProposedProtocolNotSupported;
                }
            }
        }

        self.* = Negotiator{
            .proposed_protocols = proposed_protocols,
            .supported_protocols = supported_protocols,
            .negotiation_timeout = negotiation_timeout,
            .allocator = allocator,
            .buffer = buffer,
            .proposed_protocol_index = 0,
            .on_selected_ctx = on_selected_ctx,
            .on_selected = on_selected,
            .is_initiator = is_initiator,
            .state = .INIT,
        };
    }

    pub fn deinit(_: *Self) void {
        // No-op for now, as we don't have any resources to free
        // The buffer is managed by the caller
    }

    pub fn startNegotiate(self: *Self, writer: anytype) !void {
        const buffer = if (self.is_initiator) try self.allocator.alloc(u8, TOTAL_MESSAGE_LENGTH * 2) else try self.allocator.alloc(u8, TOTAL_MESSAGE_LENGTH);

        var proto_buffer = std.io.fixedBufferStream(buffer);
        const proto_writer = proto_buffer.writer();

        try Self.writePacket(proto_writer, MULTISTREAM_PROTO);

        if (self.is_initiator) {
            try Self.writePacket(proto_writer, self.proposed_protocols.?[0]);
        }

        const callback_ctx = self.allocator.create(WriteCallbackContext) catch unreachable;
        callback_ctx.* = .{
            .negotiator = self,
            .buffer = buffer,
            .proto_id = null,
            .selected_handler = null,
        };
        writer.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);
    }

    pub fn negotiate(self: *Self, writer: anytype) !void {
        while (true) {
            if (self.buffer.readableLength() < MAX_LENGTH_BYTES) {
                return;
            }

            // Read the max length bytes first, it may be longer than actual protocol ID length
            var length_bytes: [MAX_LENGTH_BYTES]u8 = undefined;
            _ = self.buffer.read(&length_bytes);

            const decoded_length_bytes = try uvarint.decode(u16, &length_bytes);
            // If there are remaining bytes in the buffer, put them back
            // so that we can read them again as it is not length bytes but actual protocol ID bytes.
            if (decoded_length_bytes.remaining.len > 0) {
                try self.buffer.unget(decoded_length_bytes.remaining);
            }

            const proto_id_length = decoded_length_bytes.value;

            if (proto_id_length > MAX_MULTISTREAM_MESSAGE_LENGTH) {
                return NegotiatorError.ProtocolIdTooLong;
            }

            if (self.buffer.readableLength() < proto_id_length) {
                return;
            }

            var proto_id_bytes: [MAX_MULTISTREAM_MESSAGE_LENGTH]u8 = undefined;
            _ = self.buffer.read(proto_id_bytes[0..proto_id_length]);

            if (proto_id_length < MESSAGE_SUFFIX_LENGTH or
                !std.mem.eql(u8, proto_id_bytes[proto_id_length - MESSAGE_SUFFIX_LENGTH .. proto_id_length], MESSAGE_SUFFIX))
            {
                return NegotiatorError.InvalidMultistreamSuffix;
            }

            const proto_id = proto_id_bytes[0 .. proto_id_length - MESSAGE_SUFFIX_LENGTH];
            if (self.state == .INIT) {
                // If we haven't received the header yet, we expect the multistream protocol ID
                if (!std.mem.eql(u8, proto_id, MULTISTREAM_PROTO)) {
                    return NegotiatorError.FirstLineShouldBeMultistream;
                } else {
                    self.state = .HEADER_RECEIVED;
                    continue;
                }
            }
            if (self.is_initiator) {
                // Initiator
                if (!std.mem.eql(u8, proto_id, self.proposed_protocols.?[self.proposed_protocol_index])) {
                    // If the protocol ID does not match the proposed one, we need to propose the next one
                    if (self.proposed_protocol_index < self.proposed_protocols.?.len - 1) {
                        // If we have more proposed protocols, increment the index
                        self.proposed_protocol_index += 1;

                        const buffer = self.allocator.alloc(u8, TOTAL_MESSAGE_LENGTH) catch unreachable;

                        var proto_buffer = std.io.fixedBufferStream(buffer);
                        const proto_writer = proto_buffer.writer();

                        try Self.writePacket(proto_writer, self.proposed_protocols.?[self.proposed_protocol_index]);

                        const callback_ctx = self.allocator.create(WriteCallbackContext) catch unreachable;
                        callback_ctx.* = .{
                            .negotiator = self,
                            .buffer = buffer,
                            .proto_id = null,
                            .selected_handler = null,
                        };
                        writer.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);
                        continue; // Continue to the next iteration to read the next message
                    } else {
                        // No more proposed protocols, handle error
                        return NegotiatorError.AllProposedProtocolsRejected;
                    }
                } else {
                    const selected_handler = self.supported_protocols.getPtr(proto_id).?;
                    return self.on_selected(self.on_selected_ctx, proto_id, selected_handler);
                }
            } else {
                // Responder
                var iterator = self.supported_protocols.keyIterator();
                while (iterator.next()) |s_proto_id| {
                    if (std.mem.eql(u8, proto_id, s_proto_id.*)) {
                        const buffer = self.allocator.alloc(u8, TOTAL_MESSAGE_LENGTH) catch unreachable;
                        var proto_buffer = std.io.fixedBufferStream(buffer);
                        const proto_writer = proto_buffer.writer();

                        try Self.writePacket(proto_writer, s_proto_id.*);

                        const selected_handler = self.supported_protocols.getPtr(s_proto_id.*).?;
                        const callback_ctx = self.allocator.create(WriteCallbackContext) catch unreachable;
                        callback_ctx.* = .{
                            .negotiator = self,
                            .buffer = buffer,
                            .proto_id = s_proto_id.*,
                            .selected_handler = selected_handler,
                        };
                        writer.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);

                        return;
                    }
                }

                const buffer = self.allocator.alloc(u8, TOTAL_MESSAGE_LENGTH) catch unreachable;
                var proto_buffer = std.io.fixedBufferStream(buffer);
                const proto_writer = proto_buffer.writer();

                try Self.writePacket(proto_writer, NA);

                const callback_ctx = self.allocator.create(WriteCallbackContext) catch unreachable;
                callback_ctx.* = .{
                    .negotiator = self,
                    .buffer = buffer,
                    .proto_id = null,
                    .selected_handler = null,
                };
                writer.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);
            }
        }
    }

    fn writePacket(writer: anytype, proto: []const u8) !void {
        const n = try uvarint.encodeStream(writer, u16, @intCast(proto.len + MESSAGE_SUFFIX_LENGTH));
        std.log.debug("Multistream Negotiator writePacket length: {}\n", .{n});
        try writer.writeAll(proto);
        try writer.writeAll(MESSAGE_SUFFIX);
    }
};

/// The MultistreamSelectHandler is responsible for managing the multistream negotiation process.
/// It handles the selection of the appropriate protocol for a given connection/stream.
pub const MultistreamSelectHandler = struct {
    allocator: Allocator,
    // Flat map of protocol IDs to their handlers, if a group of protocols
    // share the same handler, they can be registered under multiple IDs
    // This allows for easy lookup and management of protocol handlers
    supported_protocols: std.StringHashMap(protocols.AnyProtocolHandler),

    const Self = @This();

    // Context for managing a single negotiation session
    const NegotiationSession = struct {
        allocator: Allocator,

        stream: *quic.QuicStream,
        // Only used by initiator
        proposed_protocols: ?[]const []const u8,

        user_callback_ctx: ?*anyopaque,

        user_callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,

        negotiator: Negotiator,

        is_initiator: bool,

        buffer: std.fifo.LinearFifo(u8, .Slice),

        fn init(
            self: *NegotiationSession,
            allocator: Allocator,
            proposed_protocols: ?[]const []const u8,
            supported_protocols: *std.StringHashMap(protocols.AnyProtocolHandler),
            stream: *quic.QuicStream,
            user_callback_ctx: ?*anyopaque,
            user_callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
            is_initiator: bool,
        ) !void {
            const buffer = try allocator.alloc(u8, Negotiator.TOTAL_MESSAGE_LENGTH);
            errdefer allocator.free(buffer);

            self.* = .{
                .allocator = allocator,
                .stream = stream,
                .buffer = std.fifo.LinearFifo(u8, .Slice).init(buffer),
                .user_callback_ctx = user_callback_ctx,
                .user_callback = user_callback,
                .negotiator = undefined, // Will be initialized below
                .is_initiator = is_initiator,
                .proposed_protocols = proposed_protocols,
            };

            try self.negotiator.init(allocator, std.time.ns_per_s * Negotiator.NEGOTIATION_TIMEOUT_SECONDS, proposed_protocols, supported_protocols, &self.buffer, is_initiator, self, NegotiationSession.onNegotiationComplete);
            errdefer self.negotiator.deinit();
        }

        fn deinit(self: *NegotiationSession) void {
            self.allocator.free(self.buffer.buf);
            self.negotiator.deinit();
            self.allocator.destroy(self);
        }

        // Callback for when multistream negotiation completes
        fn onNegotiationComplete(instance: ?*anyopaque, proto_id: protocols.ProtocolId, selected_handler: *protocols.AnyProtocolHandler) void {
            const self: *NegotiationSession = @ptrCast(@alignCast(instance.?));
            const stable_proto_id = blk: {
                if (self.is_initiator) {
                    if (self.proposed_protocols) |proposed| {
                        for (proposed) |candidate| {
                            if (std.mem.eql(u8, candidate, proto_id)) {
                                break :blk candidate;
                            }
                        }
                    }
                }

                break :blk proto_id;
            };

            self.stream.negotiated_protocol = stable_proto_id;

            if (self.is_initiator) {
                selected_handler.onInitiatorStart(self.stream, self.user_callback_ctx, self.user_callback) catch |err| {
                    std.log.warn("Failed to start initiator: {}. ", .{err});
                    self.user_callback(self.user_callback_ctx, err);

                    self.stream.close(null, struct {
                        fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
                    }.callback);
                    return;
                };
            } else {
                selected_handler.onResponderStart(self.stream, self.user_callback_ctx, self.user_callback) catch |err| {
                    std.log.warn("Failed to start responder: {}. ", .{err});
                    self.user_callback(self.user_callback_ctx, err);

                    self.stream.close(null, struct {
                        fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
                    }.callback);
                    return;
                };
            }

            self.stream.proto_msg_handler.?.onActivated(self.stream) catch |err| {
                std.log.warn("Proto message handler failed with error: {}. ", .{err});
                self.user_callback(self.user_callback_ctx, err);

                self.stream.close(null, struct {
                    fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
                }.callback);
                return;
            };

            if (self.buffer.readableLength() > 0) {
                // If there are still bytes in the buffer, we need to propagate them
                // to the selected protocol handler.
                // This is necessary to ensure that any remaining data in the buffer
                // is not lost and can be processed by the selected protocol handler.
                self.stream.proto_msg_handler.?.onMessage(self.stream, self.buffer.readableSlice(0)) catch |err| {
                    std.log.warn("Proto message handler onMessage failed with error: {}. ", .{err});
                    self.user_callback(self.user_callback_ctx, err);

                    self.stream.close(null, struct {
                        fn callback(_: ?*anyopaque, _: anyerror!*quic.QuicStream) void {}
                    }.callback);
                    return;
                };
            }

            // Clean up the negotiation session
            self.deinit();
        }

        // Protocol message handler implementation
        pub fn onActivated(self: *NegotiationSession, stream: *quic.QuicStream) !void {
            try self.negotiator.startNegotiate(stream);
        }

        pub fn onMessage(self: *NegotiationSession, stream: *quic.QuicStream, message: []const u8) !void {
            std.log.debug("Multistream Negotiator onRead: {any}", .{message});
            try self.buffer.write(message);
            try self.negotiator.negotiate(stream);
        }

        pub fn onClose(self: *NegotiationSession, _: *quic.QuicStream) !void {
            // Always clean up the session itself
            self.deinit();
        }

        pub fn vtableOnActivatedFn(instance: *anyopaque, stream: *quic.QuicStream) anyerror!void {
            const self: *NegotiationSession = @ptrCast(@alignCast(instance));
            return self.onActivated(stream);
        }

        pub fn vtableOnMessageFn(instance: *anyopaque, stream: *quic.QuicStream, message: []const u8) anyerror!void {
            const self: *NegotiationSession = @ptrCast(@alignCast(instance));
            return self.onMessage(stream, message);
        }

        pub fn vtableOnCloseFn(instance: *anyopaque, stream: *quic.QuicStream) anyerror!void {
            const self: *NegotiationSession = @ptrCast(@alignCast(instance));
            return self.onClose(stream);
        }

        const vtable = protocols.ProtocolMessageHandlerVTable{
            .onActivatedFn = vtableOnActivatedFn,
            .onMessageFn = vtableOnMessageFn,
            .onCloseFn = vtableOnCloseFn,
        };

        pub fn any(self: *NegotiationSession) protocols.AnyProtocolMessageHandler {
            return .{ .instance = self, .vtable = &vtable };
        }
    };

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .supported_protocols = std.StringHashMap(protocols.AnyProtocolHandler).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.supported_protocols.deinit();
    }

    pub fn addProtocolHandler(self: *Self, proto_id: protocols.ProtocolId, handler: protocols.AnyProtocolHandler) !void {
        try self.supported_protocols.put(proto_id, handler);
    }

    // Protocol handler implementation
    pub fn onInitiatorStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(NegotiationSession) catch unreachable;
        errdefer self.allocator.destroy(handler);
        try handler.init(self.allocator, stream.proposed_protocols, &self.supported_protocols, stream, callback_ctx, callback, true);
        // Set the negotiation session as the stream's handler
        // This handler will NEVER be replaced - it stays for the entire stream lifetime
        stream.setProtoMsgHandler(handler.any());
    }

    pub fn onResponderStart(
        self: *Self,
        stream: *quic.QuicStream,
        callback_ctx: ?*anyopaque,
        callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void,
    ) !void {
        const handler = self.allocator.create(NegotiationSession) catch unreachable;
        errdefer self.allocator.destroy(handler);
        try handler.init(self.allocator, null, &self.supported_protocols, stream, callback_ctx, callback, false);
        // Set the negotiation session as the stream's handler
        // This handler will NEVER be replaced - it stays for the entire stream lifetime
        stream.setProtoMsgHandler(handler.any());
    }

    pub fn vtableOnResponderStartFn(instance: *anyopaque, stream: *quic.QuicStream, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onResponderStart(stream, callback_ctx, callback);
    }

    pub fn vtableOnInitiatorStartFn(instance: *anyopaque, stream: *quic.QuicStream, callback_ctx: ?*anyopaque, callback: *const fn (callback_ctx: ?*anyopaque, controller: anyerror!?*anyopaque) void) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInitiatorStart(stream, callback_ctx, callback);
    }

    pub fn any(self: *Self) protocols.AnyProtocolHandler {
        return .{ .instance = self, .vtable = &handler_vtable };
    }

    const handler_vtable = protocols.ProtocolHandlerVTable{
        .onInitiatorStartFn = vtableOnInitiatorStartFn,
        .onResponderStartFn = vtableOnResponderStartFn,
    };
};
