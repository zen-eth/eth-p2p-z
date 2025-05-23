const std = @import("std");
const proto_binding = @import("protocol_binding.zig");
const ArrayList = std.ArrayList;
const p2p_conn = @import("../conn.zig");
const ProtocolId = @import("../protocol_id.zig").ProtocolId;
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
    const UVARINT16_MAX_LENGTH = 3;

    bindings: ArrayList(ProtocolId),

    negotiation_time_limit: u64,

    message_pool: std.heap.MemoryPool([UVARINT16_MAX_LENGTH + MAX_MULTISTREAM_MESSAGE_LENGTH]u8),

    allocator: std.mem.Allocator,

    const Self = @This();

    // --- Actual Handler Implementations ---
    pub fn onActiveImpl(self: *Self, ctx: *p2p_conn.HandlerContext) void {
        const WriteCallbackContext = struct {
            negotiator: *Self,
            buffer: []u8,
        };

        if (ctx.conn.direction() == p2p_conn.Direction.OUTBOUND) {
            // Initiator
            const buffer = self.allocator.alloc(u8, (UVARINT16_MAX_LENGTH + MAX_MULTISTREAM_MESSAGE_LENGTH) * 2) catch unreachable;
            var proto_buffer = std.io.fixedBufferStream(buffer);
            var proto_writer = proto_buffer.writer();

            _ = uvarint.encodeStream(proto_writer, u8, MULTISTREAM_PROTO.len) catch unreachable;

            proto_writer.writeAll(MULTISTREAM_PROTO) catch unreachable;
            proto_writer.writeAll(MESSAGE_SUFFIX) catch unreachable;

            _ = uvarint.encodeStream(proto_writer, u16, @intCast(self.bindings.items[0].len)) catch unreachable;
            proto_writer.writeAll(self.bindings.items[0]) catch unreachable;
            proto_writer.writeAll(MESSAGE_SUFFIX) catch unreachable;

            const callback_ctx = self.allocator.create(WriteCallbackContext) catch unreachable;
            callback_ctx.* = .{
                .negotiator = self,
                .buffer = buffer,
            };

            ctx.write(proto_buffer.getWritten(), callback_ctx, struct {
                pub fn callback(w: ?*anyopaque, _: anyerror!usize) void {
                    const w_ctx: *WriteCallbackContext = @ptrCast(@alignCast(w.?));
                    const allocator = w_ctx.negotiator.allocator;
                    allocator.free(w_ctx.buffer);
                    allocator.destroy(w_ctx);
                }
            }.callback);
        } else {
            // Responder
            const buffer = self.message_pool.create() catch unreachable;
            var proto_buffer = std.io.fixedBufferStream(buffer);
            var proto_writer = proto_buffer.writer();

            _ = uvarint.encodeStream(proto_writer, u8, MULTISTREAM_PROTO.len) catch unreachable;

            proto_writer.writeAll(MULTISTREAM_PROTO) catch unreachable;
            proto_writer.writeAll(MESSAGE_SUFFIX) catch unreachable;

            const callback_ctx = self.allocator.create(WriteCallbackContext) catch unreachable;
            callback_ctx.* = .{
                .negotiator = self,
                .buffer = buffer,
            };
            ctx.write(proto_buffer.getWritten(), null, struct {
                pub fn callback(w: ?*anyopaque, _: anyerror!usize) void {
                    const w_ctx: *WriteCallbackContext = @ptrCast(@alignCast(w.?));
                    const allocator = w_ctx.negotiator.allocator;
                    w_ctx.negotiator.message_pool.destroy(
                        @alignCast(
                            @as(*[UVARINT16_MAX_LENGTH + MAX_MULTISTREAM_MESSAGE_LENGTH]u8, @ptrFromInt(@intFromPtr(w_ctx.buffer.ptr))),
                        ),
                    );
                    allocator.destroy(w_ctx);
                }
            }.callback);
        }
        ctx.fireActive();
    }

    pub fn onRead(_: *Self, _: p2p_conn.AnyRxConn, _: []const u8, _: usize, _: *const fn (ud: ?*anyopaque, r: anyerror!*anyopaque) void) void {
        // Handle incoming messages
    }
};
