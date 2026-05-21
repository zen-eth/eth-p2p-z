const std = @import("std");

pub const local_cid_len = 20;
pub const max_cid_len = 20;
pub const LocalCid = [local_cid_len]u8;

pub const CidKey = struct {
    len: u8,
    data: [max_cid_len]u8,

    pub fn init(cid: []const u8) ?CidKey {
        if (cid.len > max_cid_len) return null;
        var key = CidKey{
            .len = @intCast(cid.len),
            .data = [_]u8{0} ** max_cid_len,
        };
        @memcpy(key.data[0..cid.len], cid);
        return key;
    }

    pub fn slice(key: *const CidKey) []const u8 {
        return key.data[0..key.len];
    }

    pub fn local(key: *const CidKey) ?LocalCid {
        if (key.len != local_cid_len) return null;
        var out: LocalCid = undefined;
        @memcpy(&out, key.data[0..local_cid_len]);
        return out;
    }
};

test "cid keys preserve length for routing map lookups" {
    const short = CidKey.init(&[_]u8{ 1, 2, 3 }) orelse return error.TestUnexpectedResult;
    const with_trailing_zero = CidKey.init(&[_]u8{ 1, 2, 3, 0 }) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!std.meta.eql(short, with_trailing_zero));

    var map = std.AutoHashMap(CidKey, u8).init(std.testing.allocator);
    defer map.deinit();

    try map.put(short, 1);
    try map.put(with_trailing_zero, 2);
    try std.testing.expectEqual(@as(usize, 2), map.count());
    try std.testing.expectEqual(@as(u8, 1), map.get(short).?);
    try std.testing.expectEqual(@as(u8, 2), map.get(with_trailing_zero).?);

    var too_long: [max_cid_len + 1]u8 = undefined;
    @memset(&too_long, 0xaa);
    try std.testing.expect(CidKey.init(&too_long) == null);
}
