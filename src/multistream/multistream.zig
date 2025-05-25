const std = @import("std");
const proto_binding = @import("protocol_binding.zig");
const ArrayList = std.ArrayList;
const p2p_conn = @import("../conn.zig");
const ProtocolId = @import("../protocol_id.zig").ProtocolId;
const ProtoMatcher = @import("protocol_matcher.zig").ProtocolMatcher;
const multiformats = @import("multiformats");
const uvarint = multiformats.uvarint;

pub const Multistream = struct {
    bindings: ArrayList(proto_binding.AnyProtocolBinding),
    negotiation_time_limit: u64,

    const Self = @This();

    pub fn initConn(_: *Self, _: p2p_conn.AnyRxConn, _: ?*anyopaque, _: *const fn (ud: ?*anyopaque, r: anyerror!*anyopaque) void) void {
        // const handler: p2p_conn.AnyHandler = if (conn.direction() == p2p_conn.Direction.OUTBOUND) {

        // } else {

        // };

        // conn.getPipeline().addLast(name: []const u8, handler: AnyHandler)
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
    const POOL_ITEM_SIZE = MAX_LENGTH_BYTES + MAX_MULTISTREAM_MESSAGE_LENGTH;

    pub const NegotiatorError = error{
        ProtocolIdTooLong,
        InvalidMultistreamSuffix,
        FirstLineShouldBeMultistream,
        AllProposedProtocolsRejected,
    };

    protocols: ?ArrayList(*const ProtocolId),

    matchers: ?ArrayList(*const ProtoMatcher),

    bindings: ArrayList(proto_binding.AnyProtocolBinding),

    negotiation_time_limit: u64,

    message_pool: std.heap.MemoryPool([MAX_LENGTH_BYTES + MAX_MULTISTREAM_MESSAGE_LENGTH]u8),

    allocator: std.mem.Allocator,

    buffer: std.fifo.LinearFifo(u8, .Slice),

    header_received: bool = false,

    proposed_proto_index: usize = 0,

    const Self = @This();

    const WriteCallbackContext = struct {
        negotiator: *Self,
        buffer: []u8,
        ctx: *p2p_conn.HandlerContext,
    };

    const WriteCallback = struct {
        pub fn callback(w: ?*anyopaque, n: anyerror!usize) void {
            const w_ctx: *WriteCallbackContext = @ptrCast(@alignCast(w.?));

            if (n) |_| {
                w_ctx.negotiator.destroyPooledBuffer(w_ctx.buffer);
                w_ctx.negotiator.allocator.destroy(w_ctx);
            } else |err| {
                w_ctx.ctx.fireErrorCaught(err);
                w_ctx.negotiator.destroyPooledBuffer(w_ctx.buffer);
                w_ctx.negotiator.deinit();
                w_ctx.negotiator.allocator.destroy(w_ctx);
                w_ctx.ctx.close(null, struct {
                    pub fn callback(_: ?*anyopaque, _: anyerror!void) void {
                        // Callback after close
                    }
                }.callback);
            }
        }
    };

    // Initiator-specific write callback
    const InitiatorWriteCallback = struct {
        pub fn callback(w: ?*anyopaque, n: anyerror!usize) void {
            const w_ctx: *WriteCallbackContext = @ptrCast(@alignCast(w.?));
            const allocator = w_ctx.negotiator.allocator;

            if (n) |_| {
                allocator.free(w_ctx.buffer);
                allocator.destroy(w_ctx);
            } else |err| {
                w_ctx.ctx.fireErrorCaught(err);
                w_ctx.negotiator.deinit();
                allocator.free(w_ctx.buffer);
                allocator.destroy(w_ctx);
                w_ctx.ctx.close(null, struct {
                    pub fn callback(_: ?*anyopaque, _: anyerror!void) void {}
                }.callback);
            }
        }
    };

    pub fn init(
        self: *Self,
        allocator: std.mem.Allocator,
        negotiation_time_limit: u64,
        bindings: []proto_binding.AnyProtocolBinding,
        direction: p2p_conn.Direction,
    ) !void {
        var message_pool = std.heap.MemoryPool([POOL_ITEM_SIZE]u8).init(allocator);
        errdefer message_pool.deinit();

        const buffer_slice = try message_pool.create();
        errdefer message_pool.destroy(@alignCast(
            @as(*[POOL_ITEM_SIZE]u8, @ptrFromInt(@intFromPtr(buffer_slice.ptr))),
        ));

        const binding_list = ArrayList(proto_binding.AnyProtocolBinding).fromOwnedSlice(allocator, bindings);
        errdefer binding_list.deinit();

        self.* = Negotiator{
            .bindings = binding_list,
            .negotiation_time_limit = negotiation_time_limit,
            .message_pool = message_pool,
            .allocator = allocator,
            .buffer = std.fifo.LinearFifo(u8, .Slice).init(buffer_slice),
            .protocols = null,
            .matchers = null,
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
        self.bindings.deinit();
        self.message_pool.destroy(
            @alignCast(
                @as(*[POOL_ITEM_SIZE]u8, @ptrFromInt(@intFromPtr(self.buffer.buf.ptr))),
            ),
        );
        self.message_pool.deinit();
        self.buffer.deinit();
    }

    // --- Actual Handler Implementations ---
    pub inline fn onActiveImpl(self: *Self, ctx: *p2p_conn.HandlerContext) void {
        if (ctx.conn.direction() == p2p_conn.Direction.OUTBOUND) {
            // Initiator
            const buffer = self.allocator.alloc(u8, (MAX_LENGTH_BYTES + MAX_MULTISTREAM_MESSAGE_LENGTH) * 2) catch |err| {
                ctx.fireErrorCaught(err);
                self.deinit();
                ctx.close(null, struct {
                    pub fn callback(_: ?*anyopaque, _: anyerror!void) void {}
                }.callback);
                return;
            };

            var proto_buffer = std.io.fixedBufferStream(buffer);
            var proto_writer = proto_buffer.writer();

            _ = uvarint.encodeStream(proto_writer, u16, MULTISTREAM_PROTO.len) catch |err| {
                self.handleInitiatorError(ctx, buffer, err);
                return;
            };

            proto_writer.writeAll(MULTISTREAM_PROTO) catch |err| {
                self.handleInitiatorError(ctx, buffer, err);
                return;
            };

            proto_writer.writeAll(MESSAGE_SUFFIX) catch |err| {
                self.handleInitiatorError(ctx, buffer, err);
                return;
            };

            _ = uvarint.encodeStream(proto_writer, u16, @intCast(self.protocols.?.items[0].len)) catch |err| {
                self.handleInitiatorError(ctx, buffer, err);
                return;
            };
            proto_writer.writeAll(self.protocols.?.items[0].*) catch |err| {
                self.handleInitiatorError(ctx, buffer, err);
                return;
            };
            proto_writer.writeAll(MESSAGE_SUFFIX) catch |err| {
                self.handleInitiatorError(ctx, buffer, err);
                return;
            };

            const callback_ctx = self.allocator.create(WriteCallbackContext) catch |err| {
                self.handleInitiatorError(ctx, buffer, err);
                return;
            };
            callback_ctx.* = .{
                .negotiator = self,
                .buffer = buffer,
                .ctx = ctx,
            };

            ctx.write(proto_buffer.getWritten(), callback_ctx, InitiatorWriteCallback.callback);
        } else {
            // Responder
            const buffer = self.message_pool.create() catch |err| {
                ctx.fireErrorCaught(err);
                self.deinit();
                ctx.close(null, struct {
                    pub fn callback(_: ?*anyopaque, _: anyerror!void) void {}
                }.callback);
                return;
            };
            var proto_buffer = std.io.fixedBufferStream(buffer);
            var proto_writer = proto_buffer.writer();

            _ = uvarint.encodeStream(proto_writer, u16, MULTISTREAM_PROTO.len) catch |err| {
                self.handleErrorWithBufferCleanup(ctx, buffer, err);
                return;
            };

            proto_writer.writeAll(MULTISTREAM_PROTO) catch |err| {
                self.handleErrorWithBufferCleanup(ctx, buffer, err);
                return;
            };

            proto_writer.writeAll(MESSAGE_SUFFIX) catch |err| {
                self.handleErrorWithBufferCleanup(ctx, buffer, err);
                return;
            };

            const callback_ctx = self.allocator.create(WriteCallbackContext) catch |err| {
                self.handleErrorWithBufferCleanup(ctx, buffer, err);
                return;
            };
            callback_ctx.* = .{
                .negotiator = self,
                .buffer = buffer,
                .ctx = ctx,
            };
            ctx.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);
        }
    }

    pub inline fn onInactiveImpl(_: *Self, ctx: *p2p_conn.HandlerContext) void {
        ctx.fireInactive();
    }

    pub fn onReadImpl(self: *Self, ctx: *p2p_conn.HandlerContext, msg: []const u8) void {
        self.buffer.write(msg) catch |err| {
            self.handleErrorWithBufferCleanup(ctx, null, err);
            return;
        };
        while (true) {
            if (self.buffer.readableLength() < MAX_LENGTH_BYTES) {
                return;
            }

            var length_bytes: [MAX_LENGTH_BYTES]u8 = undefined;
            _ = self.buffer.read(&length_bytes);

            const decoded_length_bytes = uvarint.decode(u16, &length_bytes) catch |err| {
                self.handleErrorWithBufferCleanup(ctx, null, err);
                return;
            };
            if (decoded_length_bytes.remaining.len > 0) {
                self.buffer.unget(decoded_length_bytes.remaining) catch |err| {
                    self.handleErrorWithBufferCleanup(ctx, null, err);
                    return;
                };
            }
            const proto_id_length = decoded_length_bytes.value;
            if (proto_id_length > MAX_MULTISTREAM_MESSAGE_LENGTH) {
                self.handleErrorWithBufferCleanup(ctx, null, NegotiatorError.ProtocolIdTooLong);
                return;
            }
            if (self.buffer.readableLength() < proto_id_length) {
                return;
            }
            var proto_id_bytes: [MAX_MULTISTREAM_MESSAGE_LENGTH]u8 = undefined;
            _ = self.buffer.read(proto_id_bytes[0..proto_id_length]);

            if (proto_id_length < MESSAGE_SUFFIX_LENGTH or
                !std.mem.eql(u8, proto_id_bytes[proto_id_length - MESSAGE_SUFFIX_LENGTH .. proto_id_length], MESSAGE_SUFFIX))
            {
                self.handleErrorWithBufferCleanup(ctx, null, NegotiatorError.InvalidMultistreamSuffix);
                return;
            }
            const proto_id = proto_id_bytes[0 .. proto_id_length - MESSAGE_SUFFIX_LENGTH];
            if (!self.header_received) {
                // If we haven't received the header yet, we expect the multistream protocol ID
                if (!std.mem.eql(u8, proto_id, MULTISTREAM_PROTO)) {
                    self.handleErrorWithBufferCleanup(ctx, null, NegotiatorError.FirstLineShouldBeMultistream);
                    return;
                } else {
                    self.header_received = true;
                    continue; // Continue to read the next protocol ID
                }
            }
            if (ctx.conn.direction() == p2p_conn.Direction.OUTBOUND) {
                // Initiator
                if (!std.mem.eql(u8, proto_id, self.protocols.?.items[self.proposed_proto_index].*)) {
                    // If the protocol ID does not match the proposed one, we need to handle it
                    if (self.proposed_proto_index < self.protocols.?.items.len - 1) {
                        // If we have more proposed protocols, increment the index
                        self.proposed_proto_index += 1;

                        const buffer = self.message_pool.create() catch |err| {
                            ctx.fireErrorCaught(err);
                            self.deinit();
                            ctx.close(null, struct {
                                pub fn callback(_: ?*anyopaque, _: anyerror!void) void {}
                            }.callback);
                            return;
                        };
                        var proto_buffer = std.io.fixedBufferStream(buffer);
                        var proto_writer = proto_buffer.writer();

                        _ = uvarint.encodeStream(proto_writer, u16, @intCast(self.protocols.?.items[self.proposed_proto_index].len)) catch |err| {
                            self.handleErrorWithBufferCleanup(ctx, buffer, err);
                            return;
                        };

                        proto_writer.writeAll(self.protocols.?.items[self.proposed_proto_index].*) catch |err| {
                            self.handleErrorWithBufferCleanup(ctx, buffer, err);
                            return;
                        };

                        proto_writer.writeAll(MESSAGE_SUFFIX) catch |err| {
                            self.handleErrorWithBufferCleanup(ctx, buffer, err);
                            return;
                        };

                        const callback_ctx = self.allocator.create(WriteCallbackContext) catch |err| {
                            self.handleErrorWithBufferCleanup(ctx, buffer, err);
                            return;
                        };
                        callback_ctx.* = .{
                            .negotiator = self,
                            .buffer = buffer,
                            .ctx = ctx,
                        };
                        ctx.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);

                        continue; // Read the next protocol ID
                    } else {
                        // No more proposed protocols, handle error
                        self.handleErrorWithBufferCleanup(ctx, null, NegotiatorError.AllProposedProtocolsRejected);
                        return;
                    }
                } else {
                    // We found a match
                    // TODO: Negotiate the protocol binding here
                }
            } else {
                // Responder
                for (self.matchers.?.items) |matcher| {
                    if (matcher.matches(proto_id)) {
                        const buffer = self.message_pool.create() catch |err| {
                            ctx.fireErrorCaught(err);
                            self.deinit();
                            ctx.close(null, struct {
                                pub fn callback(_: ?*anyopaque, _: anyerror!void) void {}
                            }.callback);
                            return;
                        };
                        var proto_buffer = std.io.fixedBufferStream(buffer);
                        var proto_writer = proto_buffer.writer();

                        _ = uvarint.encodeStream(proto_writer, u16, @intCast(proto_id.len)) catch |err| {
                            self.handleErrorWithBufferCleanup(ctx, buffer, err);
                            return;
                        };

                        proto_writer.writeAll(proto_id) catch |err| {
                            self.handleErrorWithBufferCleanup(ctx, buffer, err);
                            return;
                        };

                        proto_writer.writeAll(MESSAGE_SUFFIX) catch |err| {
                            self.handleErrorWithBufferCleanup(ctx, buffer, err);
                            return;
                        };

                        const callback_ctx = self.allocator.create(WriteCallbackContext) catch |err| {
                            self.handleErrorWithBufferCleanup(ctx, buffer, err);
                            return;
                        };
                        callback_ctx.* = .{
                            .negotiator = self,
                            .buffer = buffer,
                            .ctx = ctx,
                        };
                        ctx.write(proto_buffer.getWritten(), callback_ctx, WriteCallback.callback);

                        // TODO: Negotiate the protocol binding here
                        return;
                    }
                }

                const buffer = self.message_pool.create() catch |err| {
                    ctx.fireErrorCaught(err);
                    self.deinit();
                    ctx.close(null, struct {
                        pub fn callback(_: ?*anyopaque, _: anyerror!void) void {}
                    }.callback);
                    return;
                };
                var proto_buffer = std.io.fixedBufferStream(buffer);
                var proto_writer = proto_buffer.writer();

                _ = uvarint.encodeStream(proto_writer, u16, @intCast(NA.len)) catch |err| {
                    self.handleErrorWithBufferCleanup(ctx, buffer, err);
                    return;
                };

                proto_writer.writeAll(NA) catch |err| {
                    self.handleErrorWithBufferCleanup(ctx, buffer, err);
                    return;
                };

                proto_writer.writeAll(MESSAGE_SUFFIX) catch |err| {
                    self.handleErrorWithBufferCleanup(ctx, buffer, err);
                    return;
                };

                const callback_ctx = self.allocator.create(WriteCallbackContext) catch |err| {
                    self.handleErrorWithBufferCleanup(ctx, buffer, err);
                    return;
                };
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
    fn vtableOnActiveFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onActiveImpl(ctx);
    }

    fn vtableOnInactiveFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onInactiveImpl(ctx);
    }

    fn vtableOnReadFn(instance: *anyopaque, ctx: *p2p_conn.HandlerContext, msg: []const u8) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.onReadImpl(ctx, msg);
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

    // Helper function to destroy a buffer from the message pool
    fn destroyPooledBuffer(self: *Self, buffer_slice: []u8) void {
        self.message_pool.destroy(@alignCast(
            @as(*[POOL_ITEM_SIZE]u8, @ptrFromInt(@intFromPtr(buffer_slice.ptr))),
        ));
    }

    // Helper function for error handling with buffer cleanup
    fn handleErrorWithBufferCleanup(self: *Self, ctx: *p2p_conn.HandlerContext, buffer_slice: ?[]u8, err: anyerror) void {
        ctx.fireErrorCaught(err);
        if (buffer_slice) |slice| {
            self.destroyPooledBuffer(slice);
        }
        self.deinit();
        ctx.close(null, struct {
            pub fn callback(_: ?*anyopaque, _: anyerror!void) void {
                // Callback after close
            }
        }.callback);
    }

    // Helper function for write callback cleanup
    fn cleanupWriteCallback(w_ctx: *WriteCallbackContext, success: bool, ctx: *p2p_conn.HandlerContext) void {
        const allocator = w_ctx.negotiator.allocator;

        // Always destroy the buffer
        w_ctx.negotiator.destroyPooledBuffer(w_ctx.buffer);

        if (!success) {
            w_ctx.negotiator.deinit();
            ctx.close();
        }

        // Always destroy the callback context
        allocator.destroy(w_ctx);
    }

    // Helper function for Initiator error handling with allocator buffer cleanup
    fn handleInitiatorError(self: *Self, ctx: *p2p_conn.HandlerContext, buffer: []u8, err: anyerror) void {
        ctx.fireErrorCaught(err);
        self.allocator.free(buffer);
        self.deinit();
        ctx.close(null, struct {
            pub fn callback(_: ?*anyopaque, _: anyerror!void) void {
                // Callback after close
            }
        }.callback);
    }
};
