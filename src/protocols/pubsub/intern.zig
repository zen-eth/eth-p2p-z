//! Interned, reference-counted message ids shared across the gossipsub router's
//! per-id maps (`seen`, each peer's `dont_send`/`iwant_counts`/`iwant_promises`)
//! and the message cache. An id held by several maps at once is ONE heap
//! allocation; `refs` = the holder count, bytes freed when the last releases.
//!
//! Id bytes are alloc'd exactly once (`InternTable.intern`, new id) and freed
//! exactly once (`MsgId.deinit`, last `release`) — no other `dupe`/`free` site.

const std = @import("std");
const RefCount = @import("ref_count").RefCount;

/// One interned message-id: a single heap copy of the id bytes, wrapped in a
/// `RefCount(MsgId)` that counts holders and frees the box. `deinit` runs on the
/// last `release` (refs 1→0): it must `unlink` from the table (whose key aliases
/// these bytes) BEFORE freeing them, else the table indexes freed memory.
pub const MsgId = struct {
    bytes: []u8,
    table: *InternTable,

    /// Must be `pub`: `RefCount(MsgId)` lives in another module and discovers this
    /// deinit via `@hasDecl`, which only sees public declarations across modules.
    pub fn deinit(self: *MsgId, allocator: std.mem.Allocator) void {
        self.table.unlink(self.bytes);
        allocator.free(self.bytes);
    }
};

/// A `RefCount` box over a `MsgId`: one per distinct id while any holder is live.
pub const InternedId = RefCount(MsgId);

/// Router-owned index of live interned message ids, keyed by the id BYTES. The
/// table holds NO reference of its own — it is purely an index, so an id is in
/// the table iff some holder references the box (`refs > 0`) and duplicate
/// `intern`s coalesce onto one allocation. Each map entry that interns an id
/// holds exactly one ref; the last `release` → `MsgId.deinit` → `unlink` (drop
/// the entry whose key aliases the bytes) → free the bytes → `RefCount` frees
/// the box. `deinit` asserts the table is empty at router destroy.
pub const InternTable = struct {
    allocator: std.mem.Allocator,
    /// id bytes → the live interned box. The key ALIASES the box's owned
    /// `value.bytes`, so the table never owns its keys: the box frees them in
    /// `MsgId.deinit`, after `unlink` removes the entry.
    entries: std.StringHashMapUnmanaged(*InternedId) = .empty,

    pub fn init(allocator: std.mem.Allocator) InternTable {
        return .{ .allocator = allocator };
    }

    /// Asserts the table is empty (a straggler is a leaked id-byte copy) and
    /// frees the index storage.
    pub fn deinit(self: *InternTable) void {
        std.debug.assert(self.entries.count() == 0);
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }

    /// Return an interned box for `bytes`, adding one reference for the caller:
    ///   - already interned: `retain` the existing box, no new allocation.
    ///   - new id: heap-copy the bytes, mint a box (refs = 1), and re-key the
    ///     entry to the OWNED copy so the key outlives the borrowed lookup slice.
    /// Returns null on OOM; the caller then safely skips recording the id (cost is
    /// only a possible duplicate forward / missed bookkeeping entry).
    pub fn intern(self: *InternTable, bytes: []const u8) ?*InternedId {
        const gop = self.entries.getOrPut(self.allocator, bytes) catch return null;
        if (gop.found_existing) return gop.value_ptr.*.retain();

        const owned = self.allocator.dupe(u8, bytes) catch {
            // Undo the placeholder entry (its key aliases the borrowed slice).
            _ = self.entries.remove(bytes);
            return null;
        };
        const box = InternedId.init(self.allocator, .{ .bytes = owned, .table = self }) catch {
            self.allocator.free(owned);
            _ = self.entries.remove(bytes);
            return null;
        };
        // Re-key from the placeholder (which aliases the caller's borrowed slice)
        // to the box's OWNED bytes, which it frees in `MsgId.deinit` after unlink.
        gop.key_ptr.* = box.value.bytes;
        gop.value_ptr.* = box;
        return box;
    }

    /// Remove the table entry for `bytes`. Called ONLY from `MsgId.deinit` on the
    /// last release, BEFORE the bytes are freed (the key aliases them).
    pub fn unlink(self: *InternTable, bytes: []const u8) void {
        _ = self.entries.remove(bytes);
    }

    /// Number of distinct live interned ids (one per allocation). Used by tests to
    /// assert sharing and that the table empties as holders release.
    pub fn count(self: *const InternTable) usize {
        return self.entries.count();
    }
};

/// One entry in an interned-id-keyed map: the shared box (holding this entry's
/// single ref) plus a per-map `Payload` (an expiry tick or a count). The map key
/// aliases `rc.value.bytes`, so the entry must be removed from the map BEFORE its
/// `rc` is released (the release may free those bytes).
pub fn IdEntry(comptime Payload: type) type {
    return struct {
        rc: *InternedId,
        payload: Payload,
    };
}
