const libp2p = @import("../root.zig");
const proto_binding = @import("../multistream/protocol_binding.zig");
const ProtocolDescriptor = @import("../multistream/protocol_descriptor.zig").ProtocolDescriptor;
const ProtocolMatcher = @import("../multistream/protocol_matcher.zig").ProtocolMatcher;
const ProtocolId = libp2p.protocols.ProtocolId;
const Allocactor = @import("std").mem.Allocator;
const p2p_conn = @import("../conn.zig");

pub const PlainSecureChannel = struct {
    local_key: []const u8,

    protocol_descriptor: ProtocolDescriptor,

    const Self = @This();

    pub fn init(self: *Self, local_key: []const u8, allocator: Allocactor) !void {
        var proto_desc: ProtocolDescriptor = undefined;
        const protocol_id: ProtocolId = "/plaintext/2.0.0";
        const announcements: []const ProtocolId = &[_]ProtocolId{
            protocol_id,
        };
        var proto_matcher: ProtocolMatcher = undefined;
        try proto_matcher.initAsStrict(allocator, protocol_id);
        errdefer proto_matcher.deinit();
        try proto_desc.init(allocator, announcements, proto_matcher);
        errdefer proto_desc.deinit();

        const duped_local_key = try allocator.dupe(u8, local_key);
        errdefer allocator.free(duped_local_key);

        self.* = PlainSecureChannel{
            .local_key = duped_local_key,
            .protocol_descriptor = proto_desc,
        };
    }

    pub fn deinit(self: *Self, allocator: Allocactor) void {
        allocator.free(self.local_key);
        self.protocol_descriptor.deinit();
    }

    // --- Actual Implementations ---
    pub fn getProtocolDescriptor(self: *Self) *ProtocolDescriptor {
        return &self.protocol_descriptor;
    }

    pub fn initConn(
        _: *Self,
        _: p2p_conn.AnyConn,
        _: ProtocolId,
        _: ?*anyopaque,
        _: *const fn (ud: ?*anyopaque, r: anyerror!*anyopaque) void,
    ) void {}

    // --- Static Wrapper Functions ---
    pub fn vtableGetProtocolDescriptionFn(instance: *anyopaque) *ProtocolDescriptor {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.getProtocolDescriptor();
    }

    pub fn vtableInitConnFn(
        instance: *anyopaque,
        conn: p2p_conn.AnyConn,
        protocol_id: ProtocolId,
        user_data: ?*anyopaque,
        callback: *const fn (ud: ?*anyopaque, r: anyerror!*anyopaque) void,
    ) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.initConn(conn, protocol_id, user_data, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = proto_binding.ProtocolBindingVTable{
        .initConnFn = vtableInitConnFn,
        .protoDescFn = vtableGetProtocolDescriptionFn,
    };

    pub fn any(self: *Self) proto_binding.AnyProtocolBinding {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};
