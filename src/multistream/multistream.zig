const std = @import("std");
const proto_binding = @import("protocol_binding.zig");
const AnyProtocolBinding = proto_binding.AnyProtocolBinding;
const ArrayList = std.ArrayList;
const p2p_conn = @import("../conn.zig");
const AnyRxConn = p2p_conn.AnyConn;
const ProtocolId = @import("../protocol_id.zig").ProtocolId;
const ProtoMatcher = @import("protocol_matcher.zig").ProtocolMatcher;
const multiformats = @import("multiformats");
const uvarint = multiformats.uvarint;
const Allocator = std.mem.Allocator;
const LinearFifo = std.fifo.LinearFifo;
const io_loop = @import("../thread_event_loop.zig");
const Upgrader = @import("../transport/upgrader.zig").Upgrader;
const insecure = @import("../security/insecure.zig");
const xev_tcp = @import("../transport/tcp/xev.zig");
const p2p_transport = @import("../transport.zig");

pub const Multistream = struct {
    bindings: []const AnyProtocolBinding,

    negotiation_time_limit: u64,

    const Self = @This();

    pub fn init(
        self: *Self,
        negotiation_time_limit: u64,
        bindings: []const AnyProtocolBinding,
    ) !void {
        self.* = Multistream{
            .bindings = bindings,
            .negotiation_time_limit = negotiation_time_limit,
        };
    }

    pub fn initConn(self: *Self, conn: AnyRxConn, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void) void {
        // free negotiator when it be removed from the pipeline
        const negotiator = conn.getPipeline().allocator.create(Negotiator) catch unreachable;

        negotiator.init(conn.getPipeline().allocator, self.negotiation_time_limit, self.bindings, conn.direction(), user_data, callback) catch |err| {
            conn.getPipeline().allocator.destroy(negotiator);
            callback(user_data, err);
            return;
        };

        const handler = negotiator.any();
        conn.getPipeline().addLast("mss", handler) catch |err| {
            negotiator.deinit();
            conn.getPipeline().allocator.destroy(negotiator);
            callback(user_data, err);
            return;
        };
    }
};

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

    pub const NegotiatorError = error{
        ProtocolIdTooLong,
        InvalidMultistreamSuffix,
        FirstLineShouldBeMultistream,
        AllProposedProtocolsRejected,
        NoBindingsProvided,
    };

    const State = enum {
        INIT,
        HEADER_RECEIVED,
        PROTOCOL_SELECTED,
    };

    // Proposed protocol IDs is the one that the initiator proposes to use.
    protocols: ?ArrayList(*const ProtocolId) = null,

    // Matchers are used by the responder to match the protocol ID received from the initiator.
    matchers: ?ArrayList(*const ProtoMatcher) = null,

    // Supported protocols by the negotiator.
    bindings: []const AnyProtocolBinding,

    negotiation_time_limit: u64,

    allocator: Allocator,

    buffer: LinearFifo(u8, .Slice),

    // Current proposed protocol by the initiator.
    proposed_proto_index: usize = 0,

    callback_ctx: ?*anyopaque = null,

    callback: *const fn (ctx: ?*anyopaque, session: anyerror!?*anyopaque) void,

    state: State = .INIT,

    const Self = @This();

    const WriteCallbackContext = struct {
        negotiator: *Self,
        buffer: []const u8,
        ctx: *p2p_conn.HandlerContext,
    };

    const WriteCallback = struct {
        pub fn callback(w: ?*anyopaque, n: anyerror!usize) void {
            const w_ctx: *WriteCallbackContext = @ptrCast(@alignCast(w.?));

            if (n) |_| {
                w_ctx.negotiator.allocator.free(w_ctx.buffer);
                w_ctx.negotiator.allocator.destroy(w_ctx);
            } else |err| {
                w_ctx.ctx.fireErrorCaught(err);
                w_ctx.negotiator.deinit();
                w_ctx.negotiator.allocator.free(w_ctx.buffer);
                w_ctx.negotiator.allocator.destroy(w_ctx);
                const close_ctx = w_ctx.ctx.pipeline.mempool.io_no_op_context_pool.create() catch unreachable;
                close_ctx.* = .{
                    .ctx = w_ctx.ctx,
                };
                w_ctx.ctx.close(close_ctx, io_loop.NoOPCallback.closeCallback);
            }
        }
    };

    pub fn init(self: *Self, allocator: std.mem.Allocator, negotiation_time_limit: u64, bindings: []const AnyProtocolBinding, direction: p2p_conn.Direction, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void) !void {
        const buffer = try allocator.alloc(u8, TOTAL_MESSAGE_LENGTH);
        errdefer allocator.free(buffer);

        if (bindings.len == 0) {
            return NegotiatorError.NoBindingsProvided;
        }

        self.* = Negotiator{
            .bindings = bindings,
            .negotiation_time_limit = negotiation_time_limit,
            .allocator = allocator,
            .buffer = LinearFifo(u8, .Slice).init(buffer),
            .callback_ctx = user_data,
            .callback = callback,
        };

        switch (direction) {
            .OUTBOUND => {
                var protos = ArrayList(*const ProtocolId).init(allocator);
                errdefer protos.deinit();

                for (bindings) |binding| {
                    const proto_desc = binding.protoDesc();
                    for (proto_desc.announce_protocols.items) |*proto_id| {
                        try protos.append(proto_id);
                    }
                }
                self.protocols = protos;
            },
            .INBOUND => {
                var matchers = ArrayList(*const ProtoMatcher).init(allocator);
                errdefer matchers.deinit();
                for (bindings) |binding| {
                    const proto_desc = binding.protoDesc();
                    const matcher = &proto_desc.protocol_matcher;
                    try matchers.append(matcher);
                }
                self.matchers = matchers;
            },
        }
    }

    pub fn deinit(self: *Self) void {
        if (self.protocols) |*protos| {
            protos.deinit();
        }
        if (self.matchers) |*matchers| {
            matchers.deinit();
        }
        self.allocator.free(self.buffer.buf);
    }

    // --- Actual Handler Implementations ---
    pub fn onActiveImpl(self: *Self, ctx: *p2p_conn.HandlerContext) !void {
        const is_initiator = ctx.conn.direction() == p2p_conn.Direction.OUTBOUND;
        const buffer = if (is_initiator) self.allocator.alloc(u8, TOTAL_MESSAGE_LENGTH * 2) catch unreachable else self.allocator.alloc(u8, TOTAL_MESSAGE_LENGTH) catch unreachable;

        var proto_buffer = std.io.fixedBufferStream(buffer);
        const proto_writer = proto_buffer.writer();

        Self.writePacket(proto_writer, MULTISTREAM_PROTO) catch |err| {
            self.handleError(ctx, buffer, err);
            return err;
        };

        if (is_initiator) {
            Self.writePacket(proto_writer, self.protocols.?.items[0].*) catch |err| {
                self.handleError(ctx, buffer, err);
                return err;
            };
        }

        const callback_ctx = self.allocator.create(WriteCallbackContext) catch unreachable;
        callback_ctx.* = .{
            .negotiator = self,
            .buffer = buffer,
            .ctx = ctx,
        };

        std.debug.print("Multistream Negotiator onActive: {}\n", .{proto_buffer.getWritten().len});
        ctx.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);
    }

    pub fn onInactiveImpl(self: *Self, ctx: *p2p_conn.HandlerContext) void {
        ctx.fireInactive();
        self.deinit();
        ctx.pipeline.allocator.destroy(self);
    }

    pub fn onReadImpl(self: *Self, ctx: *p2p_conn.HandlerContext, msg: []const u8) !void {
        std.debug.print("Multistream Negotiator onRead: {}\n", .{msg.len});
        self.buffer.write(msg) catch |err| {
            self.handleError(ctx, null, err);
            return err;
        };
        while (true) {
            if (self.buffer.readableLength() < MAX_LENGTH_BYTES) {
                return;
            }

            // Read the max length bytes first, it may be longer than actual protocol ID length
            var length_bytes: [MAX_LENGTH_BYTES]u8 = undefined;
            _ = self.buffer.read(&length_bytes);

            const decoded_length_bytes = uvarint.decode(u16, &length_bytes) catch |err| {
                self.handleError(ctx, null, err);
                return err;
            };

            // If there are remaining bytes in the buffer, put them back
            // so that we can read them again as it is not length bytes but actual protocol ID bytes.
            if (decoded_length_bytes.remaining.len > 0) {
                std.debug.print("Multistream Negotiator remain buffer length: {}\n", .{self.buffer.readableLength()});
                std.debug.print("Decoded remaining length: {}\n", .{decoded_length_bytes.remaining.len});
                std.debug.print("Decoded remaining bytes: {s}\n", .{decoded_length_bytes.remaining});
                self.buffer.unget(decoded_length_bytes.remaining) catch |err| {
                    self.handleError(ctx, null, err);
                    return err;
                };
                std.debug.print("Multistream Negotiator ungeted remain buffer length: {}\n", .{self.buffer.readableLength()});
                std.debug.print("Multistream Negotiator ungeted remain buffer: {s}\n", .{self.buffer.readableSlice(0)});
            }

            const proto_id_length = decoded_length_bytes.value;

            if (proto_id_length > MAX_MULTISTREAM_MESSAGE_LENGTH) {
                self.handleError(ctx, null, NegotiatorError.ProtocolIdTooLong);
                return NegotiatorError.ProtocolIdTooLong;
            }

            if (self.buffer.readableLength() < proto_id_length) {
                return;
            }

            var proto_id_bytes: [MAX_MULTISTREAM_MESSAGE_LENGTH]u8 = undefined;
            _ = self.buffer.read(proto_id_bytes[0..proto_id_length]);

            std.debug.print("Multistream Negotiator read protocol ID: {s}\n", .{proto_id_bytes[0..proto_id_length]});
            if (proto_id_length < MESSAGE_SUFFIX_LENGTH or
                !std.mem.eql(u8, proto_id_bytes[proto_id_length - MESSAGE_SUFFIX_LENGTH .. proto_id_length], MESSAGE_SUFFIX))
            {
                self.handleError(ctx, null, NegotiatorError.InvalidMultistreamSuffix);
                return NegotiatorError.InvalidMultistreamSuffix;
            }

            const proto_id = proto_id_bytes[0 .. proto_id_length - MESSAGE_SUFFIX_LENGTH];
            if (self.state == .INIT) {
                // If we haven't received the header yet, we expect the multistream protocol ID
                if (!std.mem.eql(u8, proto_id, MULTISTREAM_PROTO)) {
                    self.handleError(ctx, null, NegotiatorError.FirstLineShouldBeMultistream);
                    return NegotiatorError.FirstLineShouldBeMultistream;
                } else {
                    self.state = .HEADER_RECEIVED;
                    continue;
                }
            }
            if (ctx.conn.direction() == p2p_conn.Direction.OUTBOUND) {
                // Initiator
                if (!std.mem.eql(u8, proto_id, self.protocols.?.items[self.proposed_proto_index].*)) {
                    // If the protocol ID does not match the proposed one, we need to propose the next one
                    if (self.proposed_proto_index < self.protocols.?.items.len - 1) {
                        // If we have more proposed protocols, increment the index
                        self.proposed_proto_index += 1;

                        const buffer = self.allocator.alloc(u8, TOTAL_MESSAGE_LENGTH) catch unreachable;

                        var proto_buffer = std.io.fixedBufferStream(buffer);
                        const proto_writer = proto_buffer.writer();

                        Self.writePacket(proto_writer, self.protocols.?.items[self.proposed_proto_index].*) catch |err| {
                            self.handleError(ctx, buffer, err);
                            return err;
                        };

                        const callback_ctx = self.allocator.create(WriteCallbackContext) catch unreachable;
                        callback_ctx.* = .{
                            .negotiator = self,
                            .buffer = buffer,
                            .ctx = ctx,
                        };
                        ctx.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);

                        continue; // Continue to the next iteration to read the next message
                    } else {
                        // No more proposed protocols, handle error
                        self.handleError(ctx, null, NegotiatorError.AllProposedProtocolsRejected);
                        return NegotiatorError.AllProposedProtocolsRejected;
                    }
                } else {
                    return self.onProtoSelected(proto_id, ctx);
                }
            } else {
                // Responder
                for (self.matchers.?.items) |matcher| {
                    if (matcher.matches(proto_id)) {
                        const buffer = self.allocator.alloc(u8, TOTAL_MESSAGE_LENGTH) catch unreachable;
                        var proto_buffer = std.io.fixedBufferStream(buffer);
                        const proto_writer = proto_buffer.writer();

                        Self.writePacket(proto_writer, proto_id) catch |err| {
                            self.handleError(ctx, buffer, err);
                            return err;
                        };

                        const callback_ctx = self.allocator.create(WriteCallbackContext) catch unreachable;
                        callback_ctx.* = .{
                            .negotiator = self,
                            .buffer = buffer,
                            .ctx = ctx,
                        };
                        ctx.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);

                        return self.onProtoSelected(proto_id, ctx);
                    }
                }

                const buffer = self.allocator.alloc(u8, TOTAL_MESSAGE_LENGTH) catch unreachable;
                var proto_buffer = std.io.fixedBufferStream(buffer);
                const proto_writer = proto_buffer.writer();

                Self.writePacket(proto_writer, NA) catch |err| {
                    self.handleError(ctx, buffer, err);
                    return err;
                };

                const callback_ctx = self.allocator.create(WriteCallbackContext) catch unreachable;
                callback_ctx.* = .{
                    .negotiator = self,
                    .buffer = buffer,
                    .ctx = ctx,
                };
                ctx.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);
            }
        }
    }

    pub fn onReadCompleteImpl(self: *Self, ctx: *p2p_conn.HandlerContext) void {
        _ = self;
        ctx.fireReadComplete();
    }

    pub fn onErrorCaughtImpl(self: *Self, ctx: *p2p_conn.HandlerContext, err: anyerror) void {
        _ = self;
        ctx.fireErrorCaught(err);
    }

    pub fn writeImpl(self: *Self, ctx: *p2p_conn.HandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        _ = self;
        ctx.write(buffer, user_data, callback);
    }

    pub fn closeImpl(self: *Self, ctx: *p2p_conn.HandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        _ = self;
        ctx.close(user_data, callback);
    }

    // --- Static Wrapper Functions for HandlerVTable ---
    fn vtableOnActiveFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) !void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return try self.onActiveImpl(ctx);
    }

    fn vtableOnInactiveFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInactiveImpl(ctx);
    }

    fn vtableOnReadFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, msg: []const u8) !void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return try self.onReadImpl(ctx, msg);
    }

    fn vtableOnReadCompleteFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onReadCompleteImpl(ctx);
    }

    fn vtableOnErrorCaughtFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, err: anyerror) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onErrorCaughtImpl(ctx, err);
    }

    fn vtableWriteFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, buffer: []const u8, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!usize) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.writeImpl(ctx, buffer, user_data, callback);
    }

    fn vtableCloseFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, user_data: ?*anyopaque, callback: *const fn (ud: ?*anyopaque, r: anyerror!void) void) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.closeImpl(ctx, user_data, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = p2p_conn.HandlerVTable{
        .onActiveFn = vtableOnActiveFn,
        .onInactiveFn = vtableOnInactiveFn,
        .onReadFn = vtableOnReadFn,
        .onReadCompleteFn = vtableOnReadCompleteFn,
        .onErrorCaughtFn = vtableOnErrorCaughtFn,
        .writeFn = vtableWriteFn,
        .closeFn = vtableCloseFn,
    };

    pub fn any(self: *Self) p2p_conn.AnyHandler {
        return .{ .instance = self, .vtable = &vtable_instance };
    }

    // Helper function for error handling with buffer cleanup
    fn handleError(self: *Self, ctx: *p2p_conn.HandlerContext, buffer_slice: ?[]u8, err: anyerror) void {
        if (buffer_slice) |slice| {
            self.allocator.free(slice);
        }
        ctx.fireErrorCaught(err);
    }

    fn writePacket(writer: anytype, proto: []const u8) !void {
        const n = try uvarint.encodeStream(writer, u16, @intCast(proto.len + MESSAGE_SUFFIX_LENGTH));
        std.debug.print("Multistream Negotiator writePacket length: {}", .{n});
        try writer.writeAll(proto);
        try writer.writeAll(MESSAGE_SUFFIX);
    }

    fn onProtoSelected(self: *Self, proto_id: []const u8, ctx: *p2p_conn.HandlerContext) !void {
        self.state = .PROTOCOL_SELECTED;
        var selected_proto_binding: AnyProtocolBinding = undefined;

        for (self.bindings) |binding| {
            if (binding.protoDesc().protocol_matcher.matches(proto_id)) {
                std.debug.print("Multistream Negotiator found matching protocol binding for: {s}\n", .{proto_id});
                selected_proto_binding = binding;
                break;
            }
        }

        selected_proto_binding.initConn(ctx.conn, proto_id, self.callback_ctx, self.callback);
        if (self.buffer.readableLength() > 0) {
            // If there are still bytes in the buffer, we need to propagate them
            // to the selected protocol handler.
            // This is necessary to ensure that any remaining data in the buffer
            // is not lost and can be processed by the selected protocol handler.
            try ctx.fireRead(self.buffer.readableSlice(0));
        }
        _ = try ctx.pipeline.remove("mss");
        std.debug.print("Multistream Negotiator onProtoSelected: removed 'mss' handler\n", .{});
        self.deinit();
        ctx.pipeline.allocator.destroy(self);
    }
};

const ConnHolder = struct {
    channel: ?p2p_conn.AnyConn = null,
    ready: std.Thread.ResetEvent = .{},
    err: ?anyerror = null,

    const Self = @This();

    pub fn init(opaque_userdata: ?*anyopaque, accept_result: anyerror!p2p_conn.AnyConn) void {
        const self: *ConnHolder = @ptrCast(@alignCast(opaque_userdata.?));

        const accepted_channel = accept_result catch |err| {
            self.err = err;
            self.ready.set();
            return;
        };

        self.channel = accepted_channel;
        self.ready.set();
    }
};

test "Multistream Negotiator with Insecure Protocol" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var insecure_channel: insecure.InsecureChannel = undefined;
    try insecure_channel.init(allocator); // this should be freed by pipeline
    const proposed_bindings = &[_]AnyProtocolBinding{insecure_channel.any()};
    var upgrader: Upgrader = undefined;

    try upgrader.init(proposed_bindings, std.time.ns_per_s * 10);
    const any_upgrader = upgrader.any();
    var sl: io_loop.ThreadEventLoop = undefined;
    try sl.init(allocator);
    defer {
        sl.close();
        sl.deinit();
    }

    const opts = xev_tcp.XevTransport.Options{
        .backlog = 128,
    };

    var transport: xev_tcp.XevTransport = undefined;
    try transport.init(any_upgrader, &sl, allocator, opts);
    defer transport.deinit();

    const addr = try std.net.Address.parseIp("0.0.0.0", 8093);
    var listener = try transport.listen(addr);

    var conn_holder: ConnHolder = .{};
    const accept_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *p2p_transport.AnyListener, ch: *ConnHolder) !void {
            var accepted_count: usize = 0;
            while (accepted_count < 1) : (accepted_count += 1) {
                l.accept(ch, ConnHolder.init);
                ch.ready.wait();
                try std.testing.expectEqual(ch.channel.?.direction(), p2p_conn.Direction.INBOUND);
            }
        }
    }.run, .{ &listener, &conn_holder });

    var cl: io_loop.ThreadEventLoop = undefined;
    try cl.init(allocator);
    defer {
        cl.close();
        cl.deinit();
    }
    var client: xev_tcp.XevTransport = undefined;
    try client.init(any_upgrader, &cl, allocator, opts);
    defer client.deinit();

    var dial_conn_holder: ConnHolder = .{};
    client.dial(addr, &dial_conn_holder, ConnHolder.init);
    dial_conn_holder.ready.wait();
    try std.testing.expectEqual(dial_conn_holder.channel.?.direction(), p2p_conn.Direction.OUTBOUND);

    accept_thread.join();
    // temporary sleep to ensure the multistream negotiation completes
    // will make it more robust later
    std.time.sleep(std.time.ns_per_s * 4);
}
