const std = @import("std");
const p2p_conn = @import("../conn.zig");
const multistream = @import("../multistream/lib.zig").multistream;
const proto_binding = @import("../multistream/protocol_binding.zig");
const Multistream = multistream.Multistream;
const Allocator = std.mem.Allocator;
const AnyProtocolBinding = proto_binding.AnyProtocolBinding;
const security = @import("../security/lib.zig");
const SecuritySession = security.session.Session;
const io_loop = @import("../thread_event_loop.zig");

pub const Upgrader = struct {
    security_bindings: []const AnyProtocolBinding,

    negotiate_time_limit: u64 = std.time.ms_per_s * 10,

    const Self = @This();

    const SecurityUpgradeCallbackContext = struct {
        conn: p2p_conn.AnyConn,
    };

    const SecurityUpgradeCallback = struct {
        pub fn callback(ud: ?*anyopaque, r: anyerror!?*anyopaque) void {
            const s_ctx: *SecurityUpgradeCallbackContext = @ptrCast(@alignCast(ud.?));

            if (r) |result| {
                const security_session: *SecuritySession = @ptrCast(@alignCast(result.?));
                // TODO: Set the security session on the connection
                std.debug.print("Security session upgraded successfully: {}\n", .{security_session.*});
            } else |err| {
                s_ctx.conn.getPipeline().fireErrorCaught(err);
                const close_ctx = s_ctx.conn.getPipeline().mempool.io_no_op_context_pool.create() catch unreachable;
                close_ctx.* = .{
                    .conn = s_ctx.conn,
                };
                s_ctx.conn.getPipeline().close(close_ctx, io_loop.NoOPCallback.closeCallback);
            }
        }
    };

    pub fn init(
        self: *Self,
        security_bindings: []const AnyProtocolBinding,
        negotiate_time_limit: u64,
    ) !void {
        self.security_bindings = security_bindings;
        self.negotiate_time_limit = negotiate_time_limit;
    }

    pub fn upgradeSecuritySession(
        self: *const Upgrader,
        conn: p2p_conn.AnyConn,
    ) void {
        const security_ctx = conn.getPipeline().allocator.create(SecurityUpgradeCallbackContext) catch |err| {
            conn.getPipeline().fireErrorCaught(err);
            conn.getPipeline().close(null, struct {
                pub fn callback(_: ?*anyopaque, _: anyerror!void) void {
                    // Callback after close
                }
            }.callback);
            return;
        };
        defer conn.getPipeline().allocator.destroy(security_ctx);
        security_ctx.* = SecurityUpgradeCallbackContext{
            .conn = conn,
        };

        var ms: Multistream = undefined;
        ms.init(self.negotiate_time_limit, self.security_bindings) catch |err| {
            SecurityUpgradeCallback.callback(security_ctx, err);
            return;
        };
        ms.initConn(conn, security_ctx, SecurityUpgradeCallback.callback);
    }

    pub fn initConnImpl(self: *Self, conn: p2p_conn.AnyConn) !void {
        // Start the security upgrade process
        self.upgradeSecuritySession(conn);
    }

    // Static wrapper function for the VTable
    fn vtableInitConnFn(instance: *anyopaque, conn: p2p_conn.AnyConn) !void {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.initConnImpl(conn);
    }

    // Static VTable instance
    const vtable_instance = p2p_conn.ConnInitiatorVTable{
        .initConnFn = vtableInitConnFn,
    };

    pub fn any(self: *Self) p2p_conn.AnyConnInitiator {
        return .{
            .instance = self,
            .vtable = &vtable_instance,
        };
    }
};
