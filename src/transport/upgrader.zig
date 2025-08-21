const std = @import("std");
const libp2p = @import("../root.zig");
const p2p_conn = @import("../conn.zig");
const multistream = @import("../multistream/lib.zig").multistream;
const proto_binding = @import("../multistream/protocol_binding.zig");
const Multistream = multistream.Multistream;
const Allocator = std.mem.Allocator;
const AnyProtocolBinding = proto_binding.AnyProtocolBinding;
const security = libp2p.security;
const SecuritySession = security.Session;
const io_loop = @import("../thread_event_loop.zig");

/// ConnUpgrader is a struct that manages the security upgrade process for P2P connections.
/// It uses a set of protocol bindings to negotiate a security session with the peer.
pub const ConnUpgrader = struct {
    /// security_bindings is a list of protocol bindings that can be used to negotiate a security session.
    security_bindings: []const AnyProtocolBinding,

    /// negotiate_timeout_ms is the timeout for the security negotiation process in milliseconds.
    negotiate_timeout_ms: u64 = std.time.ms_per_s * 10,

    const Self = @This();

    const SecurityUpgradeCallbackCtx = struct {
        conn: p2p_conn.AnyConn,
    };

    const SecurityUpgradeCallback = struct {
        pub fn callback(instance: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const s_ctx: *SecurityUpgradeCallbackCtx = @ptrCast(@alignCast(instance.?));

            if (res) |result| {
                const security_session: *SecuritySession = @ptrCast(@alignCast(result.?));

                s_ctx.conn.setSecuritySession(security_session.*);
                s_ctx.conn.getPipeline().allocator.destroy(security_session);
            } else |err| {
                s_ctx.conn.getPipeline().fireErrorCaught(err);
                const close_ctx = s_ctx.conn.getPipeline().pool_manager.no_op_ctx_pool.create() catch unreachable;
                close_ctx.* = .{
                    .conn = s_ctx.conn,
                };
                s_ctx.conn.getPipeline().close(close_ctx, io_loop.NoOpCallback.closeCallback);
            }
        }
    };

    pub fn init(
        self: *Self,
        security_bindings: []const AnyProtocolBinding,
        negotiate_timeout_ms: u64,
    ) !void {
        self.security_bindings = security_bindings;
        self.negotiate_timeout_ms = negotiate_timeout_ms;
    }

    pub fn upgradeSecuritySession(
        self: *const ConnUpgrader,
        conn: p2p_conn.AnyConn,
    ) void {
        const security_ctx = conn.getPipeline().allocator.create(SecurityUpgradeCallbackCtx) catch unreachable;
        defer conn.getPipeline().allocator.destroy(security_ctx);
        security_ctx.* = SecurityUpgradeCallbackCtx{
            .conn = conn,
        };

        var ms: Multistream = undefined;
        ms.init(self.negotiate_timeout_ms, self.security_bindings) catch |err| {
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
    const vtable_instance = p2p_conn.ConnEnhancerVTable{
        .enhanceConnFn = vtableInitConnFn,
    };

    pub fn any(self: *Self) p2p_conn.AnyConnEnhancer {
        return .{
            .instance = self,
            .vtable = &vtable_instance,
        };
    }
};
