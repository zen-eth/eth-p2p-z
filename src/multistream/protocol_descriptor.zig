const std = @import("std");
const libp2p = @import("../root.zig");
const Allocator = std.mem.Allocator;
const ProtocolId = libp2p.protocols.ProtocolId;
const ProtocolMatcher = @import("./protocol_matcher.zig").ProtocolMatcher;

pub const ProtocolDescriptor = struct {
    announce_protocols: std.ArrayList(ProtocolId),
    protocol_matcher: ProtocolMatcher,
    allocator: Allocator,

    pub fn init(
        self: *ProtocolDescriptor,
        allocator: Allocator,
        initial_announce_protocols: []const ProtocolId,
        matcher: ProtocolMatcher,
    ) !void {
        var announce_list = std.ArrayList(ProtocolId).init(allocator);
        errdefer announce_list.deinit();

        for (initial_announce_protocols) |p_id| {
            const owned_pid = try allocator.dupe(u8, p_id);
            errdefer allocator.free(owned_pid);
            try announce_list.append(owned_pid);
        }

        self.* = ProtocolDescriptor{
            .allocator = allocator,
            .protocol_matcher = matcher,
            .announce_protocols = announce_list,
        };
    }

    pub fn deinit(self: *ProtocolDescriptor) void {
        for (self.announce_protocols.items) |p_id| {
            self.allocator.free(p_id);
        }
        self.announce_protocols.deinit();
        self.protocol_matcher.deinit();
    }

    pub fn matchesAny(self: *const ProtocolDescriptor, proposed_protocols: []const ProtocolId) ?ProtocolId {
        for (proposed_protocols) |proposed_id| {
            if (self.protocol_matcher.matches(proposed_id)) {
                return proposed_id;
            }
        }
        return null;
    }
};

const testing = std.testing;

test "ProtocolDescriptor init and deinit" {
    const allocator = std.testing.allocator;

    var matcher: ProtocolMatcher = undefined;
    try matcher.initAsList(allocator, &[_][]const u8{"test_match"});

    var pd: ProtocolDescriptor = undefined;
    try pd.init(allocator, &[_][]const u8{"test_announce"}, matcher);
    defer pd.deinit();

    try testing.expectEqual(@as(usize, 1), pd.announce_protocols.items.len);
    try testing.expect(std.mem.eql(u8, "test_announce", pd.announce_protocols.items[0]));
    try testing.expect(pd.protocol_matcher.matches("test_match"));
}

test "ProtocolDescriptor init with empty announce and deinit" {
    const allocator = std.testing.allocator;

    var matcher: ProtocolMatcher = undefined;
    try matcher.initAsList(allocator, &[_][]const u8{"test_match_empty_announce"});

    var pd: ProtocolDescriptor = undefined;
    try pd.init(allocator, &[_][]const u8{}, matcher);
    defer pd.deinit();

    try testing.expectEqual(@as(usize, 0), pd.announce_protocols.items.len);
    try testing.expect(pd.protocol_matcher.matches("test_match_empty_announce"));
}

test "ProtocolDescriptor init with multiple announce protocols and deinit" {
    const allocator = std.testing.allocator;

    const announce_protocols_slice = [_][]const u8{
        "announce1",
        "announce2",
        "announce3",
    };
    var matcher: ProtocolMatcher = undefined;
    try matcher.initAsList(allocator, &[_][]const u8{"test_match_multi_announce"});

    var pd: ProtocolDescriptor = undefined;
    try pd.init(allocator, &announce_protocols_slice, matcher);
    defer pd.deinit();

    try testing.expectEqual(@as(usize, 3), pd.announce_protocols.items.len);
    try testing.expect(std.mem.eql(u8, "announce1", pd.announce_protocols.items[0]));
    try testing.expect(std.mem.eql(u8, "announce2", pd.announce_protocols.items[1]));
    try testing.expect(std.mem.eql(u8, "announce3", pd.announce_protocols.items[2]));
    try testing.expect(pd.protocol_matcher.matches("test_match_multi_announce"));
}

test "ProtocolDescriptor matchesAny - single match" {
    const allocator = std.testing.allocator;

    var matcher: ProtocolMatcher = undefined;
    try matcher.initAsList(allocator, &[_][]const u8{"/match/1.0"});

    var pd: ProtocolDescriptor = undefined;
    try pd.init(allocator, &[_][]const u8{"/announce/1.0"}, matcher);
    defer pd.deinit();

    const proposed = [_][]const u8{ "/other/1.0", "/match/1.0" };
    const matched_id = pd.matchesAny(&proposed);

    try testing.expect(matched_id != null);
    try testing.expect(std.mem.eql(u8, "/match/1.0", matched_id.?));
}

test "ProtocolDescriptor matchesAny - no match" {
    const allocator = std.testing.allocator;

    var matcher: ProtocolMatcher = undefined;
    try matcher.initAsList(allocator, &[_][]const u8{"/match/1.0"});

    var pd: ProtocolDescriptor = undefined;
    try pd.init(allocator, &[_][]const u8{"/announce/1.0"}, matcher);
    defer pd.deinit();

    const proposed = [_][]const u8{ "/other/1.0", "/nomatch/2.0" };
    const matched_id = pd.matchesAny(&proposed);

    try testing.expect(matched_id == null);
}

test "ProtocolDescriptor matchesAny - empty proposed" {
    const allocator = std.testing.allocator;

    var matcher: ProtocolMatcher = undefined;
    try matcher.initAsList(allocator, &[_][]const u8{"/match/1.0"});

    var pd: ProtocolDescriptor = undefined;
    try pd.init(allocator, &[_][]const u8{"/announce/1.0"}, matcher);
    defer pd.deinit();

    const proposed = [_][]const u8{};
    const matched_id = pd.matchesAny(&proposed);

    try testing.expect(matched_id == null);
}

test "ProtocolDescriptor matchesAny - matcher has no protocols" {
    const allocator = std.testing.allocator;

    var matcher: ProtocolMatcher = undefined;
    try matcher.initAsList(allocator, &[_][]const u8{}); // Empty matcher

    var pd: ProtocolDescriptor = undefined;
    try pd.init(allocator, &[_][]const u8{"/announce/1.0"}, matcher);
    defer pd.deinit();

    const proposed = [_][]const u8{ "/anything/1.0", "/match/1.0" };
    const matched_id = pd.matchesAny(&proposed);

    try testing.expect(matched_id == null);
}
