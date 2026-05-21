const std = @import("std");
const cid_mod = @import("cid.zig");

pub const CidKey = cid_mod.CidKey;
pub const LocalCid = cid_mod.LocalCid;

pub const Registry = struct {
    registered: std.ArrayList(CidKey) = .empty,

    pub fn deinit(r: *Registry, allocator: std.mem.Allocator) void {
        r.registered.deinit(allocator);
    }

    pub fn count(r: *const Registry) usize {
        return r.registered.items.len;
    }

    /// Borrowed slice of currently-registered CIDs. Valid until the next
    /// `register`/`unregister`/`clearRetainingCapacity` call (those may
    /// reallocate or shift elements).
    pub fn slice(r: *const Registry) []const CidKey {
        return r.registered.items;
    }

    pub fn local(r: *const Registry, index: usize) ?LocalCid {
        return if (r.key(index)) |cid| cid.local() else null;
    }

    pub fn key(r: *const Registry, index: usize) ?CidKey {
        if (index >= r.registered.items.len) return null;
        return r.registered.items[index];
    }

    pub fn register(r: *Registry, allocator: std.mem.Allocator, cid: []const u8) std.mem.Allocator.Error!bool {
        const new_key = CidKey.init(cid) orelse return false;
        for (r.registered.items) |existing| {
            if (std.meta.eql(existing, new_key)) return false;
        }
        try r.registered.append(allocator, new_key);
        return true;
    }

    pub fn unregister(r: *Registry, cid: []const u8) bool {
        const key_to_remove = CidKey.init(cid) orelse return false;
        return r.unregisterKey(key_to_remove);
    }

    pub fn unregisterKey(r: *Registry, key_to_remove: CidKey) bool {
        for (r.registered.items, 0..) |existing, i| {
            if (std.meta.eql(existing, key_to_remove)) {
                _ = r.registered.orderedRemove(i);
                return true;
            }
        }
        return false;
    }

    pub fn clearRetainingCapacity(r: *Registry) void {
        r.registered.clearRetainingCapacity();
    }
};
