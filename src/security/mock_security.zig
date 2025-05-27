const proto_binding = @import("../multistream/protocol_binding.zig");
const ProtocolDescriptor = @import("../multistream/protocol_descriptor.zig").ProtocolDescriptor;
const ProtocolMatcher = @import("../multistream/protocol_matcher.zig").ProtocolMatcher;
const ProtocolId = @import("../protocol_id.zig").ProtocolId;
const Allocator = @import("std").mem.Allocator;
const p2p_conn = @import("../conn.zig");

pub const MockSecureChannel = struct {
    protocol_descriptor: ProtocolDescriptor,

    const Self = @This();

    const protocol_id: ProtocolId = "/mock/1.0.0";

    const announcements: []const ProtocolId = &[_]ProtocolId{
        protocol_id,
    };

    pub fn init(self: *Self, allocator: Allocator) !void {
        var proto_desc: ProtocolDescriptor = undefined;
        var proto_matcher: ProtocolMatcher = undefined;
        try proto_matcher.initAsStrict(allocator, Self.protocol_id);
        errdefer proto_matcher.deinit();
        try proto_desc.init(allocator, Self.announcements, proto_matcher);
        errdefer proto_desc.deinit();

        self.* = MockSecureChannel{
            .protocol_descriptor = proto_desc,
        };
    }

    pub fn deinit(self: *Self) void {
        self.protocol_descriptor.deinit();
    }
};
