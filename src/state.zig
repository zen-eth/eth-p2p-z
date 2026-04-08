//! Flat State Storage for Ethereum Beacon State.
//!
//! This module provides five novel storage schemes inspired by academic papers
//! and modern database systems, each optimized for different aspects of
//! Beacon Chain state management:
//!
//! 1. **SSZ-Native Chunked Arena + CoW B-Tree Index** (`chunked_arena`)
//!    Stores SSZ data natively in 32-byte chunks with a copy-on-write B-tree
//!    index for O(1) random access and multi-version state support.
//!    *Inspired by MonadDb, LMDB, SSZ-QL.*
//!
//! 2. **Log-Structured Diff Journal + Flat Snapshot** (`diff_journal`)
//!    Maintains a flat snapshot of the latest state plus reverse-diff journals
//!    for historical reconstruction, with bloom filters for fast epoch skipping.
//!    *Inspired by SlimArchive (USENIX ATC 2024), Geth Snapshot Layer.*
//!
//! 3. **io_uring Direct-IO Column Store** (`column_store`)
//!    Organizes BeaconState into semantic column families (validators, balances,
//!    etc.) for efficient columnar access with io_uring batch IO.
//!    *Inspired by TigerBeetle, Nethermind Flat DB.*
//!
//! 4. **Persistent Adaptive Radix Tree + WAL** (`adaptive_radix_tree`)
//!    Uses an Adaptive Radix Tree for cache-friendly O(k) lookups with
//!    lazy Merkle annotation and a write-ahead log for crash recovery.
//!    *Inspired by ART (ICDE 2013), LMDB, Verkle trie addressing.*
//!
//! 5. **Hybrid Epoch-Partitioned Storage** (`epoch_storage`)
//!    Partitions state by epoch into immutable packs with delta encoding
//!    and structural sharing, similar to Git packfiles.
//!    *Inspired by persistent data structures, Portal Network, Git packfiles.*

const std = @import("std");
const testing = std.testing;

pub const chunked_arena = @import("state/chunked_arena.zig");
pub const diff_journal = @import("state/diff_journal.zig");
pub const column_store = @import("state/column_store.zig");
pub const adaptive_radix_tree = @import("state/adaptive_radix_tree.zig");
pub const epoch_storage = @import("state/epoch_storage.zig");

// Re-export primary types for convenience.
pub const ChunkedArenaStore = chunked_arena.ChunkedArenaStore;
pub const DiffJournalStore = diff_journal.DiffJournalStore;
pub const ColumnStore = column_store.ColumnStore;
pub const ArtIndex = adaptive_radix_tree.ArtIndex;
pub const EpochStorage = epoch_storage.EpochStorage;

test {
    testing.refAllDeclsRecursive(@This());
}
