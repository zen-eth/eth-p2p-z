//! Interned, reference-counted message ids shared across the gossipsub router's
//! per-id maps (`seen`, each peer's `dont_send`/`iwant_counts`/`iwant_promises`)
//! AND the message cache. A given id present in several of those maps at once is
//! ONE heap allocation whose reference count equals the number of holders; the
//! bytes are freed only when the last holder releases.
//!
//! The id bytes are allocated exactly once (in `InternTable.intern`, on a new
//! id) and freed exactly once (in `MsgId.deinit`, on the last `release`). No
//! `dupe`/`free` of id bytes happens anywhere else: `release` is the single free
//! path and the table is the single index of which ids are live.

const std = @import("std");
const RefCount = @import("../../ref_count.zig").RefCount;

/// One interned message-id: a single heap copy of the id bytes, shared across
/// every map that references it. Wrapped in a `RefCount(MsgId)`, so it is
/// the `RefCount` that counts holders and frees the box; this struct's `deinit`
/// runs exactly once, on the last `release` (refs 1→0), and does the id-byte
/// teardown: it unlinks itself from the owning intern table (whose key aliases
/// these very bytes) BEFORE freeing the bytes, so the table never indexes freed
/// memory. `unlink`-then-`free` order is the only safe order — see `InternTable`.
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

/// An interned reference to a message id: a `RefCount` box over a `MsgId`. One
/// per distinct id while any holder is live; the same id in `seen` + a peer's
/// `dont_send` + `iwant_promises` + the message cache is ONE allocation with
/// `refs` = the number of holders, freed only when the last holder releases.
pub const InternedId = RefCount(MsgId);

/// Router-owned index of live interned message ids, keyed by the id BYTES. It is
/// the single place id bytes are allocated and freed, and the single index of
/// which ids are live.
///
/// Lifecycle invariant (the whole point of interning): id bytes live exactly as
/// long as some holder references the box (`refs > 0`). `intern` (new id) and
/// `RefCount.retain` are the ONLY id-byte alloc / ref-add sites; `RefCount.release`
/// is the ONLY ref-drop site; the last release runs `MsgId.deinit`, which is the
/// ONLY id-byte free site. No `dupe`/`free` of id bytes happens anywhere else.
///
/// The table holds NO reference of its own: an id is in the table iff `refs > 0`.
/// Each map entry that interns an id holds exactly one reference; the table is
/// purely an index so duplicate `intern`s coalesce onto one allocation. The last
/// `release` → `MsgId.deinit` → `unlink` (drop the table entry whose key aliases
/// the bytes) → free the bytes → `RefCount` frees the box. `deinit` asserts the
/// table is empty at router destroy (every interned id was released).
pub const InternTable = struct {
    allocator: std.mem.Allocator,
    /// id bytes → the live interned box for that id. The key ALIASES the box's
    /// `value.bytes` (the heap copy the box owns), so the table never owns its
    /// keys: the box frees them in `MsgId.deinit`, after `unlink` removes the
    /// entry. While an id is interned the key and the box's bytes are the same
    /// allocation.
    entries: std.StringHashMapUnmanaged(*InternedId) = .empty,

    pub fn init(allocator: std.mem.Allocator) InternTable {
        return .{ .allocator = allocator };
    }

    /// Every interned id has been released. Asserts the table is empty (no
    /// straggler implies no leaked id byte copy) and frees the index storage.
    pub fn deinit(self: *InternTable) void {
        std.debug.assert(self.entries.count() == 0);
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }

    /// Return an interned box for `bytes`, adding one reference for the caller:
    ///   - already interned: `retain` the existing box (a shared holder) and
    ///     return it; no new allocation.
    ///   - new id: allocate one heap copy of the bytes, mint a box around it
    ///     (refs = 1), and FIX the table key to the box's OWNED copy (not the
    ///     borrowed lookup slice the caller passed), so the key outlives the call.
    /// Returns null on OOM (the caller then skips recording the id — safe; the
    /// only cost is a possible duplicate forward / a missed bookkeeping entry).
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
        // Re-key the entry to the box's OWNED bytes (the placeholder key still
        // aliases the caller's borrowed slice). After this the key aliases
        // `box.value.bytes`, which the box frees in `MsgId.deinit` after unlink.
        gop.key_ptr.* = box.value.bytes;
        gop.value_ptr.* = box;
        return box;
    }

    /// Remove the table entry for `bytes`. Called ONLY from `MsgId.deinit` on the
    /// last release, BEFORE the bytes are freed (the key aliases them). After this
    /// the id is no longer interned; a later `intern` of the same bytes allocates
    /// afresh.
    pub fn unlink(self: *InternTable, bytes: []const u8) void {
        _ = self.entries.remove(bytes);
    }

    /// Number of distinct live interned ids (one per allocation). Used by tests to
    /// assert sharing (one entry for an id held by several maps) and that the table
    /// empties as holders release.
    pub fn count(self: *const InternTable) usize {
        return self.entries.count();
    }
};

/// One entry in an interned-id-keyed map: the shared interned-id box plus a
/// per-map payload. The box holds this entry's single reference (added by
/// `intern`, dropped by `release` on remove/teardown); the map key aliases
/// `rc.value.bytes`, so the entry must be removed from the map BEFORE its `rc` is
/// released (the release may free those bytes). `Payload` is the per-map value:
/// an expiry tick (`seen`, `dont_send`, `iwant_promises`) or a count
/// (`iwant_counts`).
pub fn IdEntry(comptime Payload: type) type {
    return struct {
        rc: *InternedId,
        payload: Payload,
    };
}
