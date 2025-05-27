const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

pub const ProtocolId = []const u8;

// TODO: Avoid copying protocol IDs in matchers.
const StrictMatcherData = struct {
    protocol_id: []u8,
    allocator: Allocator,

    fn matches(self: *const StrictMatcherData, proposed: ProtocolId) bool {
        return std.mem.eql(u8, self.protocol_id, proposed);
    }

    fn deinit(self: *StrictMatcherData) void {
        self.allocator.free(self.protocol_id);
    }

    fn init(self: *StrictMatcherData, allocator_arg: Allocator, protocol_to_match: ProtocolId) !void {
        self.protocol_id = try allocator_arg.dupe(u8, protocol_to_match);
        self.allocator = allocator_arg;
    }
};

const PrefixMatcherData = struct {
    prefix: []u8,
    allocator: Allocator,

    fn matches(self: *const PrefixMatcherData, proposed: ProtocolId) bool {
        return std.mem.startsWith(u8, proposed, self.prefix);
    }

    fn deinit(self: *PrefixMatcherData) void {
        self.allocator.free(self.prefix);
    }

    fn init(self: *PrefixMatcherData, allocator_arg: Allocator, prefix_to_match: []const u8) !void {
        self.prefix = try allocator_arg.dupe(u8, prefix_to_match);
        self.allocator = allocator_arg;
    }
};

const ListMatcherData = struct {
    protocols: ArrayList([]u8),
    allocator: Allocator,

    fn matches(self: *const ListMatcherData, proposed: ProtocolId) bool {
        for (self.protocols.items) |p| {
            if (std.mem.eql(u8, p, proposed)) {
                return true;
            }
        }
        return false;
    }

    fn deinit(self: *ListMatcherData) void {
        for (self.protocols.items) |p| {
            self.allocator.free(p);
        }
        self.protocols.deinit();
    }

    fn init(self: *ListMatcherData, allocator_arg: Allocator, protocols_to_match: []const ProtocolId) !void {
        self.allocator = allocator_arg;
        self.protocols = ArrayList([]u8).init(allocator_arg);

        var success = false;
        defer {
            if (!success) {
                for (self.protocols.items) |p_created| self.allocator.free(p_created);
                self.protocols.deinit();
            }
        }

        for (protocols_to_match) |p_const| {
            try self.protocols.append(try self.allocator.dupe(u8, p_const));
        }
        success = true;
    }
};

/// A matcher that evaluates whether a given protocol activates based on its protocol ID.
/// Implemented as a tagged union.
pub const ProtocolMatcher = union(enum) {
    strict: StrictMatcherData,
    prefix: PrefixMatcherData,
    list: ListMatcherData,

    /// Initializes this ProtocolMatcher as a strict matcher.
    /// The `data_allocator` is used by StrictMatcherData for its internal allocations.
    pub fn initAsStrict(self: *ProtocolMatcher, data_allocator: Allocator, protocol_to_match: ProtocolId) !void {
        var data: StrictMatcherData = undefined;
        try data.init(data_allocator, protocol_to_match);
        self.* = .{ .strict = data };
    }

    /// Initializes this ProtocolMatcher as a prefix matcher.
    /// The `data_allocator` is used by PrefixMatcherData for its internal allocations.
    pub fn initAsPrefix(self: *ProtocolMatcher, data_allocator: Allocator, prefix_to_match: []const u8) !void {
        var data: PrefixMatcherData = undefined;
        try data.init(data_allocator, prefix_to_match);
        self.* = .{ .prefix = data };
    }

    /// Initializes this ProtocolMatcher as a list matcher.
    /// The `data_allocator` is used by ListMatcherData for its internal allocations.
    pub fn initAsList(self: *ProtocolMatcher, data_allocator: Allocator, protocols_to_match: []const ProtocolId) !void {
        var data: ListMatcherData = undefined;
        try data.init(data_allocator, protocols_to_match);
        self.* = .{ .list = data };
    }

    /// Evaluates this matcher against a proposed protocol ID.
    pub fn matches(self: *const ProtocolMatcher, proposed: ProtocolId) bool {
        return switch (self.*) {
            .strict => |*s_data| s_data.matches(proposed),
            .prefix => |*p_data| p_data.matches(proposed),
            .list => |*l_data| l_data.matches(proposed),
        };
    }

    /// Deinitializes the active variant's data.
    /// This must be called before the ProtocolMatcher itself is destroyed (if heap-allocated)
    pub fn deinit(self: *ProtocolMatcher) void {
        switch (self.*) {
            .strict => |*s_data| s_data.deinit(),
            .prefix => |*p_data| p_data.deinit(),
            .list => |*l_data| l_data.deinit(),
        }
    }
};

test "StrictMatcher" {
    const allocator = std.testing.allocator;
    var matcher: ProtocolMatcher = undefined;

    try matcher.initAsStrict(allocator, "test/1.0");
    defer matcher.deinit();

    try std.testing.expect(matcher.matches("test/1.0"));
    try std.testing.expect(!matcher.matches("test/2.0"));
    try std.testing.expect(!matcher.matches("test/1.0.0"));
    try std.testing.expect(!matcher.matches("another/1.0"));
}

test "PrefixMatcher" {
    const allocator = std.testing.allocator;
    var matcher: ProtocolMatcher = undefined;

    try matcher.initAsPrefix(allocator, "test/");
    defer matcher.deinit();

    try std.testing.expect(matcher.matches("test/1.0"));
    try std.testing.expect(matcher.matches("test/2.0"));
    try std.testing.expect(matcher.matches("test/1.0.0/extra"));
    try std.testing.expect(!matcher.matches("another/1.0"));
    try std.testing.expect(!matcher.matches("tes"));
}

test "ListMatcher" {
    const allocator = std.testing.allocator;
    const list_protocols = [_]ProtocolId{ "test/1.0", "test/2.0", "another/1.0" };
    var matcher: ProtocolMatcher = undefined;

    try matcher.initAsList(allocator, &list_protocols);
    defer matcher.deinit();

    try std.testing.expect(matcher.matches("test/1.0"));
    try std.testing.expect(matcher.matches("test/2.0"));
    try std.testing.expect(matcher.matches("another/1.0"));
    try std.testing.expect(!matcher.matches("test/3.0"));
    try std.testing.expect(!matcher.matches("test/1.0.0"));
}

test "ListMatcher empty list" {
    const allocator = std.testing.allocator;
    const empty_list_protocols = [_]ProtocolId{};
    var matcher: ProtocolMatcher = undefined;

    try matcher.initAsList(allocator, &empty_list_protocols);
    defer matcher.deinit();

    try std.testing.expect(!matcher.matches("test/1.0"));
}

test "ListMatcher single item list" {
    const allocator = std.testing.allocator;
    const single_item_list_protocols = [_]ProtocolId{"only/one"};
    var matcher: ProtocolMatcher = undefined;

    try matcher.initAsList(allocator, &single_item_list_protocols);
    defer matcher.deinit();

    try std.testing.expect(matcher.matches("only/one"));
    try std.testing.expect(!matcher.matches("test/1.0"));
}
