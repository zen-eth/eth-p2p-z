const proto_binding = @import("../multistream/protocol_binding.zig");
const ProtocolDescriptor = @import("../multistream/protocol_descriptor.zig").ProtocolDescriptor;
const ProtocolMatcher = @import("../multistream/protocol_matcher.zig").ProtocolMatcher;
const ProtocolId = @import("../protocol_id.zig").ProtocolId;
const Allocator = @import("std").mem.Allocator;
const p2p_conn = @import("../conn.zig");

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
        _: p2p_conn.AnyRxConn,
        _: ProtocolId,
        _: ?*anyopaque,
        _: *const fn (ud: ?*anyopaque, r: anyerror!?*anyopaque) void,
    ) void {
        // Mock implementation does nothing
    }

    // --- Static Wrapper Functions ---
    pub fn vtableProtoDescFn(instance: *anyopaque) *ProtocolDescriptor {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.getProtoDesc();
    }

    pub fn vtableInitConnFn(
        instance: *anyopaque,
        conn: p2p_conn.AnyRxConn,
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

pub const InsecureHandler = struct {};
