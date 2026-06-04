//! The gossipsub Router actor: a single fiber that owns ALL per-peer state
//! lock-free (the go-libp2p processLoop model). Every event — a peer connecting
//! or disconnecting, an inbound RPC arriving, a shutdown request — reaches the
//! router as a `Command` on its one inbox queue. Producers (the Switch's
//! peer-event callback, the per-stream inbound handlers) only POST commands;
//! they never touch router state, so the single fiber serialises all mutation
//! with no locks.
//!
//! This layer handles per-peer I/O lifecycle plus floodsub pub/sub: on connect
//! it opens a per-peer outbound stream (lazily, via a writer fiber draining an
//! OutboundQueue) and starts reading the peer's inbound stream; on disconnect it
//! tears that down cleanly. On top of that it implements floodsub — the local
//! node subscribes to topics and publishes messages; inbound subscriptions are
//! tracked per peer and published messages are forwarded to EVERY peer that
//! subscribes to the topic (no mesh/gossip/scoring yet — those are later layers).
//! Control messages (GRAFT/PRUNE/IHAVE/IWANT/IDONTWANT) are parsed-but-ignored.
//!
//! The Router is generic over a comptime `Transport` so its lifecycle logic can
//! be unit-tested against an in-memory fake (no real QUIC), exactly how
//! go-libp2p-pubsub and rust-libp2p test their gossipsub cores. The real
//! Switch/QUIC binding (`SwitchTransport`, the concrete stream sink/source, the
//! inbound service) lives in gossipsub.zig; this file knows nothing about
//! `*Switch`/`*SwitchConnection`/`*quic.Stream`.

const std = @import("std");
const pubsub = @import("pubsub.zig");
const peer_io = @import("peer_io.zig");
const rpc = @import("rpc.zig");
const rpc_pb = @import("../../protobuf.zig").rpc;
const io_time = @import("../../quic/io/time.zig");
const PeerId = @import("peer_id").PeerId;

/// Upper bound on remembered message-ids in the seen-cache (loop/duplicate
/// suppression). A bounded FIFO of owned id copies: once full, inserting a new id
/// evicts the oldest. This is a deliberately minimal dedup window — a proper
/// time-bounded seen-cache (with per-entry expiry, as go-libp2p-pubsub keeps)
/// arrives with the message cache in a later phase. The bound guarantees no
/// unbounded growth.
const seen_cache_capacity = 1024;

/// Invoked on the router fiber for each delivered message on a topic WE
/// subscribe to. The `topic`/`from`/`data` slices are only valid for the
/// duration of the call; a handler that needs to retain them must copy. Keep it
/// cheap: it runs inline on the single router fiber and stalls every other event
/// while it executes.
pub const MessageHandler = struct {
    ctx: *anyopaque,
    on_message: *const fn (ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) void,
};

/// A bounded FIFO set of owned message-id byte copies, used to suppress
/// duplicate/looping messages. Membership is a hash set; eviction order is a ring
/// of the same owned id slices. Inserting past `capacity` evicts (and frees) the
/// oldest id. Owns every id copy; `deinit` frees them all.
const SeenCache = struct {
    allocator: std.mem.Allocator,
    /// Set of currently-remembered ids. Keys are owned copies (the same slices
    /// stored in `ring`), so freeing happens exactly once, on eviction/deinit.
    set: std.StringHashMapUnmanaged(void) = .empty,
    /// Insertion-ordered ring of the owned id slices, for oldest-first eviction.
    ring: []?[]u8,
    head: usize = 0,
    count: usize = 0,

    fn init(allocator: std.mem.Allocator) std.mem.Allocator.Error!SeenCache {
        const ring = try allocator.alloc(?[]u8, seen_cache_capacity);
        @memset(ring, null);
        return .{ .allocator = allocator, .ring = ring };
    }

    fn deinit(self: *SeenCache) void {
        var it = self.set.keyIterator();
        while (it.next()) |key| self.allocator.free(key.*);
        self.set.deinit(self.allocator);
        self.allocator.free(self.ring);
        self.* = undefined;
    }

    /// Whether `id` was seen recently (still in the bounded window).
    fn contains(self: *const SeenCache, id: []const u8) bool {
        return self.set.contains(id);
    }

    /// Remember `id` (copying it). No-op if already present. On capacity the
    /// oldest id is evicted and freed first. On OOM the id is simply not
    /// remembered (dedup degrades to forwarding a possible duplicate — safe, and
    /// the only alternative is to drop the message, which is worse).
    fn add(self: *SeenCache, id: []const u8) void {
        if (self.set.contains(id)) return;
        const owned = self.allocator.dupe(u8, id) catch return;
        self.set.put(self.allocator, owned, {}) catch {
            self.allocator.free(owned);
            return;
        };
        // Evict the slot we are about to overwrite (the oldest) before storing.
        if (self.ring[self.head]) |old| {
            std.debug.assert(self.count == self.ring.len);
            _ = self.set.remove(old);
            self.allocator.free(old);
            self.count -= 1;
        }
        self.ring[self.head] = owned;
        self.head = (self.head + 1) % self.ring.len;
        self.count += 1;
    }
};

/// The router inbox holds at most this many un-processed commands. The single
/// fiber drains it continuously, so this only needs to absorb bursts of
/// connect/disconnect/inbound-rpc events between drains.
const inbox_capacity = 256;

/// A peer key usable as an AutoHashMap key. A PeerId is `[64]u8` plus a length;
/// the bytes past `len` are undefined, so the struct is not directly hashable.
/// Zero-padding the unused tail makes a fixed-size, content-defined key (two
/// peer ids with the same meaningful prefix and length hash and compare equal).
const PeerKey = [64]u8;

fn peerKey(peer: *const PeerId) PeerKey {
    var key: PeerKey = [_]u8{0} ** 64;
    @memcpy(key[0..peer.len], peer.bytes[0..peer.len]);
    return key;
}

/// The single-fiber gossipsub router, generic over a comptime `Transport` that
/// supplies the per-peer outbound sink (and the opaque connection handle the
/// `peer_connected` command carries). The router owns the peer map and inbox;
/// the main fiber serialises every mutation.
///
/// The `Transport` type must provide:
///
///   pub const ConnHandle = ...;
///       An opaque per-peer connection handle, carried in the `peer_connected`
///       Command and handed back to `makeSink`. The router treats it as a value
///       it stores and forwards; it never dereferences it. (For the real
///       transport this is `*SwitchConnection`; for tests it is a fake handle.)
///
///   pub const Sink = ...;
///       The per-peer outbound sink type. Must satisfy the PeerWriter Sink
///       contract:
///         fn open(self: *Sink, io: std.Io) anyerror!void
///             (re)establish the current outbound stream. No I/O happens until
///             the writer fiber calls this lazily on its first frame.
///         fn writeFrame(self: *Sink, io: std.Io, bytes: []const u8) anyerror!void
///             write framed bytes to the current stream.
///         fn close(self: *Sink, io: std.Io) void
///             tear down the current stream; idempotent (safe when none open).
///
///   pub fn makeSink(self: *Transport, allocator: std.mem.Allocator,
///                   peer: PeerId, conn: ConnHandle) !*Sink;
///       Allocate + initialise (NO I/O — the writer calls `open` lazily) an
///       outbound sink for the peer. The Router OWNS the returned sink: on
///       teardown it calls `sink.close(io)` then `allocator.destroy(sink)`.
pub fn Router(comptime Transport: type) type {
    return struct {
        const Self = @This();

        const Sink = Transport.Sink;
        const ConnHandle = Transport.ConnHandle;
        const Writer = peer_io.PeerWriter(Sink);

        /// A command posted to the router's single inbox. Producers post; the
        /// router fiber processes. (subscribe/publish/heartbeat variants arrive
        /// in later layers.) Generic in the transport's connection handle.
        pub const Command = union(enum) {
            /// A peer's connection is up (handshake done, peer id known). The
            /// `conn` handle stays valid until the matching `peer_disconnected`.
            peer_connected: struct {
                peer: PeerId,
                conn: ConnHandle,
                remote_addr: std.Io.net.IpAddress,
            },
            /// A peer's connection is gone. Tear the peer down.
            peer_disconnected: struct { peer: PeerId },
            /// An RPC arrived on a peer's inbound stream. The router owns it and
            /// must free it after parsing + forward-frame construction.
            inbound_rpc: peer_io.InboundRpc,
            /// Subscribe the local node to a topic: announce it to every peer and
            /// start delivering matching messages to the message handler. Owns
            /// `topic`; the router frees it once processed.
            subscribe: struct { topic: []u8 },
            /// Unsubscribe the local node from a topic: announce the withdrawal to
            /// every peer. Owns `topic`; freed once processed.
            unsubscribe: struct { topic: []u8 },
            /// Publish application data on a topic from the local node: forward it
            /// to every subscribed peer (and deliver locally if we subscribe).
            /// Owns `topic` and `data`; both freed once processed.
            publish: struct { topic: []u8, data: []u8 },
            /// Stop the router: tear down every peer, then exit the main fiber.
            shutdown,
            /// Test-only: enqueue a frame reference onto a tracked peer's outbound
            /// queue, on the router fiber (so the peer-map lookup is race-free).
            /// Used by router unit tests to make a peer's writer actually attempt
            /// to open its stream (the writer opens lazily on its first frame).
            /// The command carries one reference; if the push fails or the peer is
            /// not tracked the reference is released. The reply event is set once
            /// the push (or release) is done.
            enqueue_for_test: struct {
                peer: PeerId,
                frame: *peer_io.OutboundFrame,
                reply: *std.Io.Event,
            },
        };

        /// All state for one connected peer, owned by the router fiber. The
        /// writer fiber drains `queue` through `sink`; the writer's lifetime is
        /// the `writer_future`.
        const PeerState = struct {
            peer: PeerId,
            queue: peer_io.OutboundQueue,
            /// Topics this peer announced it subscribes to (its SUBSCRIBE
            /// SubOpts). Keys are owned copies; freed on remove and on teardown.
            /// Floodsub forwards a message to every peer whose set contains the
            /// message's topic.
            topics: std.StringHashMapUnmanaged(void) = .empty,
            /// Heap-owned (Transport-allocated) so its address is stable while
            /// the writer fiber holds it. The sink (and its stream) MUST outlive
            /// the writer fiber — torn down only after the writer is awaited.
            sink: *Sink,
            writer: *Writer,
            writer_future: std.Io.Future(void),
            /// Back-pointer for the writer's on_disconnect callback, which
            /// receives this PeerState as its context and needs the router to
            /// post peer_disconnected. Set in the initializer below so it is
            /// valid before the writer fiber spawns.
            router_for_disconnect: *Self,
        };

        /// Posts inbound RPCs to the router's single Command inbox by wrapping
        /// each in the `inbound_rpc` Command variant. This is the bridge the
        /// generalised PeerReader posts through, so inbound RPCs share one
        /// ordered queue with every other router event. Transport-agnostic
        /// (references only the router + Command), so it lives here.
        pub const InboxPoster = struct {
            router: *Self,

            pub fn post(self: *InboxPoster, io: std.Io, in: peer_io.InboundRpc) anyerror!void {
                return self.router.inbox.putOne(io, .{ .inbound_rpc = in });
            }
        };

        allocator: std.mem.Allocator,
        io: std.Io,
        /// The per-peer sink factory. Held by value: the real `SwitchTransport`
        /// is empty, so by-value avoids an extra borrowed pointer.
        transport: Transport,
        inbox_storage: []Command,
        inbox: std.Io.Queue(Command),
        /// Keyed by zero-padded peer bytes (see PeerKey). Values are heap-owned.
        peers: std.AutoHashMap(PeerKey, *PeerState),
        /// Topics the local node subscribes to. Keys are owned copies; freed on
        /// unsubscribe and on teardown. We announce these to every peer (on our
        /// own subscribe, and to each newly-connected peer) and deliver matching
        /// inbound/published messages to `message_handler`.
        my_topics: std.StringHashMapUnmanaged(void) = .empty,
        /// Bounded dedup window over recently-seen message ids.
        seen: SeenCache,
        /// Monotonic sequence number for messages WE originate; encoded big-endian
        /// into Message.seqno so (from, seqno) is a unique message id.
        seqno: u64 = 0,
        /// Our own peer id, used as Message.from on publish.
        local_peer: PeerId,
        /// Optional sink for messages delivered on topics we subscribe to.
        message_handler: ?MessageHandler,
        main_future: ?std.Io.Future(void) = null,
        /// Set once when teardown begins so the main loop stops after the inbox
        /// drains/closes. Atomic because it is read on the main fiber but set on
        /// the caller's fiber (deinit path).
        stopping: std.atomic.Value(bool) = .init(false),
        /// Number of live peers, published for observers (e.g. tests).
        peer_count: std.atomic.Value(usize) = .init(0),

        pub fn create(
            allocator: std.mem.Allocator,
            io: std.Io,
            transport: Transport,
            local_peer: PeerId,
            message_handler: ?MessageHandler,
        ) !*Self {
            const inbox_storage = try allocator.alloc(Command, inbox_capacity);
            errdefer allocator.free(inbox_storage);

            var seen = try SeenCache.init(allocator);
            errdefer seen.deinit();

            const router = try allocator.create(Self);
            router.* = .{
                .allocator = allocator,
                .io = io,
                .transport = transport,
                .inbox_storage = inbox_storage,
                .inbox = std.Io.Queue(Command).init(inbox_storage),
                .peers = std.AutoHashMap(PeerKey, *PeerState).init(allocator),
                .seen = seen,
                .local_peer = local_peer,
                .message_handler = message_handler,
            };
            return router;
        }

        /// Spawn the main fiber. Call once after `create`.
        pub fn start(router: *Self) std.Io.ConcurrentError!void {
            router.main_future = try std.Io.concurrent(router.io, mainLoop, .{router});
        }

        /// Stop the router and free all of its resources. Sets the persistent
        /// stopping flag, closes the inbox to wake the main loop, posts a
        /// `shutdown` as a backstop, cancels + awaits the main fiber, then frees
        /// the inbox storage and the router. The main fiber tears down every
        /// peer on its way out, so this is safe to call from any other fiber.
        pub fn destroy(router: *Self) void {
            router.stopping.store(true, .release);

            // Post shutdown first (so a main loop parked in getOne wakes and runs
            // the peer teardown on its own fiber), then close the inbox. close()
            // alone would make getOne return Closed before processing shutdown,
            // but the main loop also tears peers down on Closed via
            // `teardownAllPeers`, so either path is safe. Use the uncancelable
            // post to avoid losing it.
            router.inbox.putOneUncancelable(router.io, .shutdown) catch {};

            if (router.main_future) |*future| {
                // The main loop exits on shutdown / closed inbox; cancel is a
                // backstop in case it is parked in a non-inbox cancellation
                // point. Every blocking point in the loop is a cancel point, so
                // this cannot re-park. cancel before await so a parked loop
                // unparks.
                future.cancel(router.io);
                future.await(router.io);
                router.main_future = null;
            } else {
                // Never spawned: tear down directly.
                router.teardownAllPeers();
            }

            router.peers.deinit();
            router.freeMyTopics();
            router.seen.deinit();
            router.allocator.free(router.inbox_storage);
            router.allocator.destroy(router);
        }

        /// Free every key in `my_topics` and the map itself. The main fiber is no
        /// longer running by the time this is called (destroy joined it), so the
        /// map is quiescent.
        fn freeMyTopics(router: *Self) void {
            var it = router.my_topics.keyIterator();
            while (it.next()) |key| router.allocator.free(key.*);
            router.my_topics.deinit(router.allocator);
        }

        pub fn peerCount(router: *const Self) usize {
            return router.peer_count.load(.acquire);
        }

        /// Test-only: enqueue a data frame reference onto `peer`'s outbound queue
        /// and block until the router fiber has processed the push (or released
        /// the reference, if the peer is gone). Doing the lookup + push on the
        /// router fiber keeps the peer map single-threaded. Used to make a peer's
        /// writer attempt to open its stream, exercising the writer give-up path.
        pub fn enqueueDataForTest(router: *Self, peer: PeerId, frame: *peer_io.OutboundFrame) !void {
            var reply: std.Io.Event = .unset;
            try router.inbox.putOne(router.io, .{ .enqueue_for_test = .{
                .peer = peer,
                .frame = frame,
                .reply = &reply,
            } });
            reply.waitUncancelable(router.io);
        }

        // ----- main fiber --------------------------------------------------

        fn mainLoop(router: *Self) void {
            // On any exit (shutdown, closed inbox, cancellation) tear down every
            // peer and drain the inbox so nothing leaks.
            defer {
                router.teardownAllPeers();
                router.drainInbox();
            }

            while (true) {
                const command = router.inbox.getOne(router.io) catch return; // Closed/Canceled
                switch (command) {
                    .peer_connected => |c| router.onPeerConnected(c.peer, c.conn, c.remote_addr),
                    .peer_disconnected => |c| router.onPeerDisconnected(c.peer),
                    .inbound_rpc => |in| router.onInboundRpc(in),
                    .subscribe => |s| router.onSubscribe(s.topic),
                    .unsubscribe => |u| router.onUnsubscribe(u.topic),
                    .publish => |p| router.onPublish(p.topic, p.data),
                    .enqueue_for_test => |e| router.onEnqueueForTest(e.peer, e.frame, e.reply),
                    .shutdown => return,
                }
            }
        }

        /// Handle a peer connecting: dedup, then create per-peer state and spawn
        /// its writer fiber. The writer opens the outbound stream lazily on its
        /// first frame; on open-exhaustion it posts `peer_disconnected` so the
        /// peer is torn down through the normal path.
        fn onPeerConnected(router: *Self, peer: PeerId, conn: ConnHandle, remote_addr: std.Io.net.IpAddress) void {
            _ = remote_addr;
            const key = peerKey(&peer);
            // A second connection to a peer we already track: keep the first,
            // ignore the new one's gossipsub setup. One logical peer entry.
            if (router.peers.contains(key)) return;

            const state = router.allocator.create(PeerState) catch return;
            var state_live = false;
            defer if (!state_live) router.allocator.destroy(state);

            // The transport allocates + initialises the sink (no I/O). The router
            // owns it from here: it is closed + destroyed in teardownPeer.
            const sink = router.transport.makeSink(router.allocator, peer, conn) catch return;
            var sink_live = false;
            defer if (!sink_live) router.allocator.destroy(sink);

            const writer = router.allocator.create(Writer) catch return;
            var writer_live = false;
            defer if (!writer_live) router.allocator.destroy(writer);

            state.* = .{
                .peer = peer,
                .queue = peer_io.OutboundQueue.init(router.allocator, .{}),
                .sink = sink,
                .writer = writer,
                .writer_future = undefined,
                // Set up-front (not on a later line) so the back-pointer is valid
                // before the writer fiber spawns and can fire on_disconnect.
                .router_for_disconnect = router,
            };
            // Past this point `state.queue` is initialised; deinit it on any
            // failure before the writer fiber takes ownership of draining it.
            var queue_live = false;
            defer if (!queue_live) state.queue.deinit(router.io);

            writer.* = .{
                .queue = &state.queue,
                .sink = sink,
                .on_disconnect = onWriterDisconnect,
                .disconnect_ctx = state,
            };
            // The writer reaches the router via `state.router_for_disconnect`
            // (set in the PeerState initializer above) and tags
            // peer_disconnected with `state.peer`, recovering both from
            // `disconnect_ctx`.

            const future = std.Io.concurrent(router.io, Writer.run, .{ writer, router.io }) catch return;

            // All fallible steps done: the entry is live. Disarm the cleanups so
            // the PeerState (and the writer fiber draining its queue) survives.
            state.writer_future = future;
            router.peers.put(key, state) catch {
                // The map insert failed after the writer fiber started. Tear the
                // writer down cleanly (close queue, cancel+await fiber) before
                // freeing, so we don't leak the fiber or use-after-free the sink.
                // Cancel before await so a writer parked in its reopen backoff
                // does not stall this fiber; the backoff sleep is a cancellation
                // point.
                state.queue.close(router.io);
                var f = future;
                f.cancel(router.io);
                f.await(router.io);
                state.sink.close(router.io);
                return; // defers free writer/sink/state; queue deinit fires too
            };
            state_live = true;
            sink_live = true;
            writer_live = true;
            queue_live = true;
            _ = router.peer_count.fetchAdd(1, .release);

            // Tell the new peer which topics we already subscribe to, so it can
            // start forwarding matching messages to us right away (mirrors how
            // go-libp2p-pubsub sends the full current subscription set on a new
            // connection). Best-effort: a framing/push failure just means the peer
            // learns our subscriptions on our next subscribe/unsubscribe.
            router.sendCurrentSubscriptions(state);
        }

        /// Send the local node's full current subscription set to one peer's
        /// `.subscribe` lane as a single subscription RPC. No-op when we have no
        /// subscriptions. Best-effort: drops the frame on a push failure.
        ///
        /// The transient SubOpts array (one entry per local topic) is the only
        /// genuinely multi-allocation scratch on this path, so it goes in a
        /// per-command arena that is freed in one shot — no per-entry bookkeeping.
        /// The shared frame itself stays gpa-owned (it outlives this call inside
        /// the peer's queue).
        fn sendCurrentSubscriptions(router: *Self, state: *PeerState) void {
            if (router.my_topics.count() == 0) return;

            var arena = std.heap.ArenaAllocator.init(router.allocator);
            defer arena.deinit();
            const scratch = arena.allocator();

            var subs: std.ArrayListUnmanaged(?rpc_pb.RPC.SubOpts) = .empty;
            var it = router.my_topics.keyIterator();
            while (it.next()) |key| {
                subs.append(scratch, rpc.buildSubscription(key.*, true)) catch return;
            }

            const framed = pubsub.frameRpc(router.allocator, (rpc.RpcOut{ .subscriptions = subs.items }).toRpc()) catch return;
            const frame = makeSubscribeFrame(router.allocator, framed) orelse return;
            defer frame.release(); // builder reference
            frame.retain();
            state.queue.push(router.io, .subscribe, frame) catch frame.release();
        }

        /// Build a refcounted shared subscribe frame (empty message_ids) taking
        /// ownership of `framed`, with one builder reference. Returns null (after
        /// freeing `framed`) on allocation failure.
        fn makeSubscribeFrame(allocator: std.mem.Allocator, framed: []u8) ?*peer_io.OutboundFrame {
            const ids = allocator.alloc([]u8, 0) catch {
                allocator.free(framed);
                return null;
            };
            return peer_io.OutboundFrame.create(allocator, framed, ids, 1) catch {
                allocator.free(framed);
                allocator.free(ids);
                return null;
            };
        }

        /// Handle a peer disconnecting: look up, tear down its writer + state.
        /// Absent peer (already removed, or a dedup'd second connection) is a
        /// no-op.
        fn onPeerDisconnected(router: *Self, peer: PeerId) void {
            const key = peerKey(&peer);
            const entry = router.peers.fetchRemove(key) orelse return;
            router.teardownPeer(entry.value);
            _ = router.peer_count.fetchSub(1, .release);
        }

        /// Handle an inbound RPC from a peer: apply its subscription changes to
        /// the SOURCE peer's announced-topics set, then floodsub-forward each
        /// published message to every OTHER subscribed peer (and deliver locally
        /// if we subscribe). Control messages are parsed-but-ignored in this
        /// floodsub layer. The OwnedRpc is freed only after all parsing AND
        /// forward-frame construction, since its bytes back the readers and are
        /// copied by frameRpc.
        fn onInboundRpc(router: *Self, in: peer_io.InboundRpc) void {
            var owned = in;
            defer owned.rpc.deinit(router.allocator);
            const source = owned.peer;
            var reader = owned.rpc.reader;

            // Subscription changes update the source peer's announced topics.
            while (reader.subscriptionsNext()) |sub| {
                router.applyPeerSubscription(source, sub.getTopicid(), sub.getSubscribe());
            }

            // Published messages: dedup, deliver locally, floodsub-forward.
            while (reader.publishNext()) |msg| {
                router.handleIncomingMessage(
                    source,
                    msg.getFrom(),
                    msg.getSeqno(),
                    msg.getTopic(),
                    msg.getData(),
                );
            }
            // getControl() is intentionally not consumed: mesh/gossip control is a
            // later layer; floodsub ignores it.
        }

        /// Apply one inbound SUBSCRIBE/UNSUBSCRIBE from `source` to that peer's
        /// announced-topics set. Untracked source → ignored. Subscribe inserts an
        /// owned key copy (no-op if already present); unsubscribe removes + frees
        /// the stored key.
        fn applyPeerSubscription(router: *Self, source: PeerId, topic: []const u8, subscribe: bool) void {
            const state = router.peers.get(peerKey(&source)) orelse return;
            if (subscribe) {
                if (state.topics.contains(topic)) return;
                const key = router.allocator.dupe(u8, topic) catch return;
                state.topics.put(router.allocator, key, {}) catch {
                    router.allocator.free(key);
                    return;
                };
            } else if (state.topics.fetchRemove(topic)) |kv| {
                router.allocator.free(kv.key);
            }
        }

        /// Process one incoming published message: dedup on its id, deliver to the
        /// local handler if we subscribe, and floodsub-forward to every peer
        /// (optionally excluding `exclude`) whose announced topics include the
        /// message's topic. `exclude` is the source peer for relayed messages (no
        /// echo back to sender); null for locally-originated publishes.
        fn handleIncomingMessage(
            router: *Self,
            exclude: PeerId,
            from: []const u8,
            seqno: []const u8,
            topic: []const u8,
            data: []const u8,
        ) void {
            var id = rpc.messageId(router.allocator, from, seqno) catch return;
            defer id.deinit(router.allocator);
            if (router.seen.contains(id.bytes)) return;
            router.seen.add(id.bytes);

            router.deliverLocal(topic, from, data);
            router.forwardMessage(.{ .peer = exclude }, from, seqno, topic, data, id.bytes);
        }

        /// Invoke the message handler if the local node subscribes to `topic`.
        /// The slices are valid only for the call (the handler copies to retain).
        fn deliverLocal(router: *Self, topic: []const u8, from: []const u8, data: []const u8) void {
            if (!router.my_topics.contains(topic)) return;
            if (router.message_handler) |h| h.on_message(h.ctx, topic, from, data);
        }

        /// Floodsub-forward a single message to every peer whose announced topics
        /// include `topic`, optionally excluding one peer (the relay source).
        /// Frames the message ONCE into a single refcounted shared frame (carrying
        /// the message id for later IDONTWANT purge), then hands one reference to
        /// each target's `.data` lane — no per-peer copy of the (up-to-1 MiB) wire
        /// bytes. The builder holds the initial reference and drops it at the end;
        /// each accepted push consumes a `retain`, each rejected push releases its
        /// retain, and the final builder release frees the frame iff no queue kept
        /// a reference.
        fn forwardMessage(
            router: *Self,
            exclude: ?struct { peer: PeerId },
            from: []const u8,
            seqno: []const u8,
            topic: []const u8,
            data: []const u8,
            id: []const u8,
        ) void {
            const msg = rpc_pb.Message{ .from = from, .seqno = seqno, .topic = topic, .data = data };
            const framed = pubsub.frameRpc(router.allocator, (rpc.RpcOut{ .publish = &.{msg} }).toRpc()) catch return;
            // `framed`/`ids` are handed to the frame on a successful create; a
            // failure before that frees whatever is already allocated. (This fn
            // returns void, so explicit cleanup — not errdefer — handles the
            // partial-allocation paths.)
            const ids = router.allocator.alloc([]u8, 1) catch {
                router.allocator.free(framed);
                return;
            };
            ids[0] = router.allocator.dupe(u8, id) catch {
                router.allocator.free(ids);
                router.allocator.free(framed);
                return;
            };

            const frame = peer_io.OutboundFrame.create(router.allocator, framed, ids, 1) catch {
                router.allocator.free(ids[0]);
                router.allocator.free(ids);
                router.allocator.free(framed);
                return;
            };
            // Builder reference dropped at the end; queues hold the rest. If no
            // queue accepted a push this release frees the whole frame.
            defer frame.release();

            var it = router.peers.iterator();
            while (it.next()) |entry| {
                const state = entry.value_ptr.*;
                if (exclude) |e| if (state.peer.eql(&e.peer)) continue;
                if (!state.topics.contains(topic)) continue;
                frame.retain();
                state.queue.push(router.io, .data, frame) catch frame.release();
            }
        }

        /// Local subscribe: record the topic and announce it to every peer.
        /// Owns `topic` (frees it); the stored key is a separate copy.
        fn onSubscribe(router: *Self, topic: []u8) void {
            defer router.allocator.free(topic);
            if (router.my_topics.contains(topic)) return;

            const key = router.allocator.dupe(u8, topic) catch return;
            router.my_topics.put(router.allocator, key, {}) catch {
                router.allocator.free(key);
                return;
            };
            router.announceSubscription(topic, true);
        }

        /// Local unsubscribe: drop the topic and announce the withdrawal to every
        /// peer. Owns `topic` (frees it). No-op if we were not subscribed.
        fn onUnsubscribe(router: *Self, topic: []u8) void {
            defer router.allocator.free(topic);
            const removed = router.my_topics.fetchRemove(topic) orelse return;
            router.allocator.free(removed.key);
            router.announceSubscription(topic, false);
        }

        /// Frame a single (un)subscription RPC ONCE into a refcounted shared frame
        /// and hand one reference to every peer's `.subscribe` lane — no per-peer
        /// copy. Builder holds the initial reference and drops it at the end; each
        /// accepted push consumes a `retain`, each rejected push releases it.
        fn announceSubscription(router: *Self, topic: []const u8, subscribe: bool) void {
            const sub = rpc.buildSubscription(topic, subscribe);
            const framed = pubsub.frameRpc(router.allocator, (rpc.RpcOut{ .subscriptions = &.{sub} }).toRpc()) catch return;
            const frame = makeSubscribeFrame(router.allocator, framed) orelse return;
            defer frame.release(); // builder reference

            var it = router.peers.iterator();
            while (it.next()) |entry| {
                frame.retain();
                entry.value_ptr.*.queue.push(router.io, .subscribe, frame) catch frame.release();
            }
        }

        /// Local publish: build a Message from us, dedup it, deliver locally if we
        /// subscribe, and floodsub-forward to every subscribed peer. Owns `topic`
        /// and `data` (frees both AFTER framing/handler, since frameRpc copies
        /// them and the handler reads them).
        fn onPublish(router: *Self, topic: []u8, data: []u8) void {
            defer router.allocator.free(topic);
            defer router.allocator.free(data);

            const from = router.local_peer.bytes[0..router.local_peer.len];

            var seqno_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &seqno_buf, router.seqno, .big);
            router.seqno += 1;
            const seqno = seqno_buf[0..];

            var id = rpc.messageId(router.allocator, from, seqno) catch return;
            defer id.deinit(router.allocator);
            router.seen.add(id.bytes);

            router.deliverLocal(topic, from, data);
            router.forwardMessage(null, from, seqno, topic, data, id.bytes);
        }

        /// Test-only: push a frame reference onto a tracked peer's outbound queue,
        /// running on the router fiber so the peer-map lookup never races the
        /// router's own mutations. Releases the reference if the peer is gone or
        /// the push fails. Always sets the reply event.
        fn onEnqueueForTest(router: *Self, peer: PeerId, frame: *peer_io.OutboundFrame, reply: *std.Io.Event) void {
            if (router.peers.get(peerKey(&peer))) |state| {
                state.queue.push(router.io, .data, frame) catch frame.release();
            } else {
                frame.release();
            }
            reply.set(router.io);
        }

        /// Tear down one peer's state. Order matters: close the queue (the writer
        /// drains remaining frames and exits), then CANCEL+AWAIT the writer fiber
        /// BEFORE freeing the sink (the writer's trailing `sink.close` must not
        /// race a freed sink), then close the sink, deinit the queue, and free
        /// the heap allocations.
        ///
        /// Cancel before await so a writer parked in `ensureStream`'s reopen
        /// backoff does not stall this single router fiber for up to
        /// max_open_retries × reopen_backoff_ms (which would serialize every
        /// peer's teardown). The backoff `sleep` is a cancellation point, so
        /// cancel collapses the wait; await then joins. cancel+await on the same
        /// Future is safe here: cancel is idempotent and clears the future, so
        /// the following await returns the cached result without double-
        /// consuming. Under cancellation the writer unwinds cleanly —
        /// popBlocking returns Closed (queue already closed) or Canceled, or a
        /// Cancelable surfaces from open/writeFrame/sleep — runs its
        /// `defer sink.close`, and returns; the router's own `sink.close` below
        /// is idempotent.
        fn teardownPeer(router: *Self, state: *PeerState) void {
            state.queue.close(router.io);
            state.writer_future.cancel(router.io);
            state.writer_future.await(router.io);
            state.sink.close(router.io);
            state.queue.deinit(router.io);
            router.freePeerTopics(state);
            router.allocator.destroy(state.writer);
            router.allocator.destroy(state.sink);
            router.allocator.destroy(state);
        }

        /// Free every key in a peer's announced-topics map and the map itself.
        fn freePeerTopics(router: *Self, state: *PeerState) void {
            var it = state.topics.keyIterator();
            while (it.next()) |key| router.allocator.free(key.*);
            state.topics.deinit(router.allocator);
        }

        fn teardownAllPeers(router: *Self) void {
            var it = router.peers.iterator();
            while (it.next()) |entry| {
                router.teardownPeer(entry.value_ptr.*);
                _ = router.peer_count.fetchSub(1, .release);
            }
            router.peers.clearRetainingCapacity();
        }

        /// Drain and free any commands still buffered in the inbox after the loop
        /// exits, so an inbound RPC posted concurrently with teardown is not
        /// leaked.
        fn drainInbox(router: *Self) void {
            router.inbox.close(router.io);
            var buf: [16]Command = undefined;
            while (true) {
                const n = router.inbox.getUncancelable(router.io, &buf, 0) catch return;
                if (n == 0) return;
                for (buf[0..n]) |command| switch (command) {
                    .inbound_rpc => |in| {
                        var owned = in;
                        owned.rpc.deinit(router.allocator);
                    },
                    .subscribe => |s| router.allocator.free(s.topic),
                    .unsubscribe => |u| router.allocator.free(u.topic),
                    .publish => |p| {
                        router.allocator.free(p.topic);
                        router.allocator.free(p.data);
                    },
                    .enqueue_for_test => |e| {
                        // The peer map is already torn down; release the frame
                        // reference and wake any waiter so a test post in flight at
                        // shutdown neither leaks nor hangs.
                        e.frame.release();
                        e.reply.set(router.io);
                    },
                    else => {},
                };
            }
        }

        /// PeerWriter on_disconnect callback. The writer exhausted its open
        /// retries, so hand the peer back to the router by posting
        /// `peer_disconnected`. Runs on the writer fiber, so it must only post —
        /// never free the state/sink (the writer's trailing `sink.close` still
        /// fires after this returns).
        fn onWriterDisconnect(ctx: ?*anyopaque) void {
            const state: *PeerState = @ptrCast(@alignCast(ctx.?));
            const router = state.router_for_disconnect;
            router.inbox.putOneUncancelable(router.io, .{ .peer_disconnected = .{ .peer = state.peer } }) catch {};
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "peerKey distinguishes peers and matches equal ids" {
    const a = try PeerId.random();
    var b_bytes = a.bytes;
    b_bytes[0] +%= 1;
    const b = PeerId{ .bytes = b_bytes, .len = a.len };

    const ka = peerKey(&a);
    const ka2 = peerKey(&a);
    const kb = peerKey(&b);
    try std.testing.expectEqual(ka, ka2);
    try std.testing.expect(!std.mem.eql(u8, &ka, &kb));
}

// --- FakeTransport: an in-memory transport for router-level unit tests -----

/// A fake outbound sink that records every byte written to each "stream" it
/// opens, into transport-owned storage so the recording survives the sink being
/// closed + destroyed during teardown. Supports a configurable open-failure mode
/// to exercise the writer's give-up path. Satisfies the PeerWriter Sink
/// contract.
///
/// The recording (`record`) is borrowed from the FakeTransport; the sink only
/// appends to it, so a sink destroyed in teardownPeer does not lose the frames a
/// test wants to assert on.
const FakeSink = struct {
    allocator: std.mem.Allocator,
    record: *FakeRecord,
    /// Number of leading open() calls that should fail. `maxInt` = every open
    /// fails, driving the writer to exhaust its retries and give up.
    fail_open_count: usize,

    pub fn open(self: *FakeSink, io: std.Io) anyerror!void {
        _ = io;
        self.record.open_calls += 1;
        if (self.record.open_calls <= self.fail_open_count) return error.OpenFailed;
        self.record.streams_opened += 1;
    }

    pub fn writeFrame(self: *FakeSink, io: std.Io, bytes: []const u8) anyerror!void {
        _ = io;
        try self.record.written.appendSlice(self.allocator, bytes);
    }

    pub fn close(self: *FakeSink, io: std.Io) void {
        _ = self;
        _ = io;
    }
};

/// Transport-owned, per-peer recording of what a peer's sink did. Outlives the
/// sink so tests can inspect it after teardown frees the sink.
const FakeRecord = struct {
    allocator: std.mem.Allocator,
    open_calls: usize = 0,
    streams_opened: usize = 0,
    written: std.ArrayList(u8) = .empty,

    fn deinit(self: *FakeRecord) void {
        self.written.deinit(self.allocator);
    }
};

/// An in-memory transport for router unit tests. `ConnHandle` is a tiny fake
/// connection that carries the per-peer recording; `Sink` is a FakeSink that
/// records into that connection's recording. `makeSink` allocates a FakeSink
/// and points it at the connection's transport-owned record.
const FakeTransport = struct {
    /// When non-zero every sink made by this transport fails its first N opens
    /// (use `maxInt` for "always fail"), exercising the writer give-up path.
    fail_open_count: usize = 0,

    pub const ConnHandle = *FakeConn;
    pub const Sink = FakeSink;

    /// A fake per-peer connection. Owns its recording; the test owns the conn.
    const FakeConn = struct {
        record: FakeRecord,
    };

    pub fn makeSink(self: *FakeTransport, allocator: std.mem.Allocator, peer: PeerId, conn: ConnHandle) !*FakeSink {
        _ = peer;
        const sink = try allocator.create(FakeSink);
        sink.* = .{
            .allocator = allocator,
            .record = &conn.record,
            .fail_open_count = self.fail_open_count,
        };
        return sink;
    }
};

/// Build a FakeConn on the heap with a fresh recording. The test owns it and
/// frees it with `destroyFakeConn` once the router has fully torn the peer down
/// (so the sink can no longer touch the record).
fn makeFakeConn(allocator: std.mem.Allocator) !*FakeTransport.FakeConn {
    const conn = try allocator.create(FakeTransport.FakeConn);
    conn.* = .{ .record = .{ .allocator = allocator } };
    return conn;
}

fn destroyFakeConn(allocator: std.mem.Allocator, conn: *FakeTransport.FakeConn) void {
    conn.record.deinit();
    allocator.destroy(conn);
}

const FakeRouter = Router(FakeTransport);

/// Build a deterministic, distinct test PeerId. `PeerId.random()` is seeded with
/// a fixed constant (so it returns the same id every call); `testPeer(seed)`
/// stamps `seed` into the digest so each value is unique, which the floodsub
/// tests need to track several peers at once.
fn testPeer(seed: u8) PeerId {
    var id = PeerId.random() catch unreachable;
    id.bytes[2] = seed;
    return id;
}

/// A local peer id distinct from every `testPeer` seed used below, so a
/// forwarded message's `from` (our id) never collides with a peer's id.
const local_test_peer = blk: {
    var id = PeerId{ .bytes = [_]u8{0} ** 64, .len = 34 };
    id.bytes[0] = 0x00;
    id.bytes[1] = 32;
    id.bytes[2] = 0xff;
    break :blk id;
};

/// Whether `state.topics` for the peer tracked under `peer` contains `topic`.
/// Reads private router/peer state directly (the tests are in-file).
fn peerTracksTopic(router: *FakeRouter, peer: PeerId, topic: []const u8) bool {
    const state = router.peers.get(peerKey(&peer)) orelse return false;
    return state.topics.contains(topic);
}

/// Spin until `pred()` holds or the bounded wait elapses, yielding the fiber
/// between checks (so the router's own fiber can make progress). Mirrors the
/// poll loop the real 2-node test uses. Returns whether the predicate held.
fn waitFor(io: std.Io, comptime pred: fn (*FakeRouter) bool, router: *FakeRouter) bool {
    var waited_ms: u64 = 0;
    while (waited_ms < 2000) : (waited_ms += 5) {
        if (pred(router)) return true;
        io_time.ms(5).sleep(io) catch {};
    }
    return pred(router);
}

fn peerCountIsOne(router: *FakeRouter) bool {
    return router.peerCount() == 1;
}

fn peerCountIsZero(router: *FakeRouter) bool {
    return router.peerCount() == 0;
}

// The router ignores remote_addr; a loopback placeholder keeps the command
// well-formed.
const dummy_addr = std.Io.net.IpAddress{ .ip4 = .loopback(0) };

test "router peer lifecycle: connect makes a sink + writer, disconnect tears down" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null);
    defer router.destroy();
    try router.start();

    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));

    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));
    // The sink was freed in teardown; the conn's record still lives (it is the
    // test's, freed by destroyFakeConn). std.testing.allocator confirms the
    // sink/writer/state allocations were all freed — no leak.
}

test "router dedups a second peer_connected for the same peer id" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null);
    defer router.destroy();
    try router.start();

    const peer = try PeerId.random();
    const conn_a = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_b);

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn_a, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));

    // Second connect for the same peer id: the first entry is kept, the second
    // makes no sink (no clobber, no second count). Post it and then post a probe
    // disconnect for a DIFFERENT peer to flush the queue, so we can assert the
    // dedup'd connect did not raise the count.
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn_b, .remote_addr = dummy_addr } });

    // conn_b's sink was never made (dedup returned before makeSink), so its
    // record stays at zero opens; conn_a's is the live peer.
    try std.testing.expect(waitFor(io, peerCountIsOne, router));
    try std.testing.expectEqual(@as(usize, 1), router.peerCount());
    try std.testing.expectEqual(@as(usize, 0), conn_b.record.open_calls);

    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));
}

test "router tears the peer down when the writer exhausts open retries" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Every open fails → the writer's ensureStream exhausts retries → it fires
    // on_disconnect → the router posts peer_disconnected → the peer is torn down
    // and peerCount drops back to 0. Tiny retry/backoff so the give-up is fast.
    const router = try FakeRouter.create(allocator, io, .{ .fail_open_count = std.math.maxInt(usize) }, local_test_peer, null);
    defer router.destroy();
    try router.start();

    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));

    // Push a frame so the writer actually tries to open (the open is lazy on the
    // first frame). It cannot open, so it gives up and the router tears the peer
    // down on its own.
    try router.enqueueDataForTest(peer, try testDataFrame(allocator));

    try std.testing.expect(waitFor(io, peerCountIsZero, router));
    // The give-up freed the popped frame and the router freed the sink/state; no
    // leak. The writer never opened a stream.
    try std.testing.expectEqual(@as(usize, 0), conn.record.streams_opened);
}

test "router frees an inbound RPC (peer tracked and peer absent)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null);
    defer router.destroy();
    try router.start();

    // Peer-absent case: no peer_connected was posted, so the inbound RPC arrives
    // for an untracked peer. The router must still free it.
    const stranger = try PeerId.random();
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = stranger, .rpc = try buildInboundRpc(allocator, "t-absent") } });

    // Peer-tracked case: connect first, then deliver an inbound RPC for it.
    const peer = try PeerId.random();
    const conn = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn);
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, peerCountIsOne, router));
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundRpc(allocator, "t-present") } });

    // Disconnect to flush + tear down; on test exit destroy() drains any leftover
    // inbox commands. std.testing.allocator confirms both RPCs were freed.
    try router.inbox.putOne(io, .{ .peer_disconnected = .{ .peer = peer } });
    try std.testing.expect(waitFor(io, peerCountIsZero, router));
}

test "router clean shutdown tears down registered peers" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null);
    // Tear the router down on every exit (not just the happy path): an early
    // assertion failure must NOT leave the main + writer fibers orphaned, or
    // threaded.deinit() would hang joining them.
    defer router.destroy();
    try router.start();

    // Register two peers, then destroy() without disconnecting them: the main
    // fiber's teardownAllPeers must close + free every peer. No leak.
    //
    // The two ids must be DISTINCT or onPeerConnected dedups the second (one
    // logical peer entry per id). `PeerId.random()` is not collision-free here,
    // so derive a second id by flipping a byte of the first — same approach the
    // peerKey test uses to get two non-equal ids.
    const peer_a = try PeerId.random();
    var peer_b = peer_a;
    peer_b.bytes[2] +%= 1;
    const conn_a = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_a);
    const conn_b = try makeFakeConn(allocator);
    defer destroyFakeConn(allocator, conn_b);

    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer_a, .conn = conn_a, .remote_addr = dummy_addr } });
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer_b, .conn = conn_b, .remote_addr = dummy_addr } });
    try std.testing.expect(waitFor(io, struct {
        fn pred(r: *FakeRouter) bool {
            return r.peerCount() == 2;
        }
    }.pred, router));

    // destroy() runs teardownAllPeers on the main fiber on its way out (via the
    // defer above, so an assertion failure still tears the fibers down).
}

// --- test helpers for the router-level tests -------------------------------

/// Build a one-byte shared data frame (one reference) for the writer-give-up
/// test (the writer pops it, fails to open, releases the reference).
fn testDataFrame(allocator: std.mem.Allocator) !*peer_io.OutboundFrame {
    const bytes = try allocator.alloc(u8, 1);
    errdefer allocator.free(bytes);
    bytes[0] = 0x7f;
    const ids = try allocator.alloc([]u8, 0);
    errdefer allocator.free(ids);
    return peer_io.OutboundFrame.create(allocator, bytes, ids, 1);
}

/// Build a real OwnedRpc carrying a single subscription, mirroring what the
/// inbound reader produces, so onInboundRpc has genuine heap bytes to free.
fn buildInboundRpc(allocator: std.mem.Allocator, topic: []const u8) !pubsub.OwnedRpc {
    return buildInboundSub(allocator, topic, true);
}

/// Build an OwnedRpc carrying a single SUBSCRIBE/UNSUBSCRIBE SubOpts for `topic`.
fn buildInboundSub(allocator: std.mem.Allocator, topic: []const u8, subscribe: bool) !pubsub.OwnedRpc {
    const sub = rpc_pb.RPC{
        .subscriptions = &[_]?rpc_pb.RPC.SubOpts{.{ .subscribe = subscribe, .topicid = topic }},
    };
    return ownedFromRpc(allocator, sub);
}

/// Build an OwnedRpc carrying a single published Message, mirroring what an
/// inbound reader yields for a peer relaying/originating a message.
fn buildInboundPublish(
    allocator: std.mem.Allocator,
    from: []const u8,
    seqno: []const u8,
    topic: []const u8,
    data: []const u8,
) !pubsub.OwnedRpc {
    const msg = rpc_pb.Message{ .from = from, .seqno = seqno, .topic = topic, .data = data };
    const frame = rpc_pb.RPC{ .publish = &[_]?rpc_pb.Message{msg} };
    return ownedFromRpc(allocator, frame);
}

/// Encode `frame` into a heap buffer the RPCReader can borrow, wrapped as an
/// OwnedRpc (matching what readRpc produces). The bytes are the raw protobuf
/// payload — NOT length-prefixed — which is what RPCReader.init expects.
fn ownedFromRpc(allocator: std.mem.Allocator, frame: rpc_pb.RPC) !pubsub.OwnedRpc {
    const encoded = try frame.encode(allocator);
    defer if (encoded.len > 0) allocator.free(encoded);
    const payload = try allocator.dupe(u8, encoded);
    errdefer allocator.free(payload);
    return .{ .bytes = payload, .reader = try rpc_pb.RPCReader.init(payload) };
}

/// Decode a length-prefixed RPC frame at the front of `written` (the byte stream
/// a FakeSink records). Returns the parsed RPCReader plus the slice consumed, so
/// a multi-frame stream can be walked. Frames are `uvarint(len) || payload`.
const DecodedFrame = struct { reader: rpc_pb.RPCReader, total_len: usize };

fn decodeFrame(written: []const u8) ?DecodedFrame {
    var len: usize = 0;
    var shift: u6 = 0;
    var i: usize = 0;
    while (i < written.len) : (i += 1) {
        len |= @as(usize, written[i] & 0x7f) << shift;
        if ((written[i] & 0x80) == 0) {
            i += 1;
            break;
        }
        shift += 7;
    } else return null;
    if (i + len > written.len) return null;
    const payload = written[i .. i + len];
    const reader = rpc_pb.RPCReader.init(payload) catch return null;
    return .{ .reader = reader, .total_len = i + len };
}

/// Whether `written` contains a frame whose first subscription matches
/// (`topic`, `subscribe`). Walks every recorded frame.
fn recordHasSubscription(written: []const u8, topic: []const u8, subscribe: bool) bool {
    var rest = written;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        while (reader.subscriptionsNext()) |sub| {
            if (sub.getSubscribe() == subscribe and std.mem.eql(u8, sub.getTopicid(), topic)) return true;
        }
        rest = rest[decoded.total_len..];
    }
    return false;
}

/// Count recorded frames carrying a published Message on `topic` with the given
/// `data`. Used to assert exactly-once forwarding (dedup).
fn recordCountPublishes(written: []const u8, topic: []const u8, data: []const u8) usize {
    var count: usize = 0;
    var rest = written;
    while (decodeFrame(rest)) |decoded| {
        var reader = decoded.reader;
        while (reader.publishNext()) |msg| {
            if (std.mem.eql(u8, msg.getTopic(), topic) and std.mem.eql(u8, msg.getData(), data)) count += 1;
        }
        rest = rest[decoded.total_len..];
    }
    return count;
}

/// A recording message handler: captures the last (topic, from, data) delivered
/// to the local node, into testing-allocator-owned buffers it frees on deinit.
const RecordingHandler = struct {
    allocator: std.mem.Allocator,
    calls: usize = 0,
    topic: ?[]u8 = null,
    from: ?[]u8 = null,
    data: ?[]u8 = null,

    fn deinit(self: *RecordingHandler) void {
        if (self.topic) |t| self.allocator.free(t);
        if (self.from) |f| self.allocator.free(f);
        if (self.data) |d| self.allocator.free(d);
    }

    fn handler(self: *RecordingHandler) MessageHandler {
        return .{ .ctx = self, .on_message = onMessage };
    }

    fn onMessage(ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) void {
        const self: *RecordingHandler = @ptrCast(@alignCast(ctx));
        // Replace any prior capture (the tests deliver once); copy the borrowed
        // slices, which are only valid for this call.
        if (self.topic) |t| self.allocator.free(t);
        if (self.from) |f| self.allocator.free(f);
        if (self.data) |d| self.allocator.free(d);
        self.topic = self.allocator.dupe(u8, topic) catch null;
        self.from = self.allocator.dupe(u8, from) catch null;
        self.data = self.allocator.dupe(u8, data) catch null;
        self.calls += 1;
    }
};

// --- floodsub pub/sub fake tests -------------------------------------------

/// Connect a fake peer to a running router and wait until it is tracked. Returns
/// the conn (owned by the caller; free with destroyFakeConn after teardown).
fn connectFakePeer(io: std.Io, allocator: std.mem.Allocator, router: *FakeRouter, peer: PeerId) !*FakeTransport.FakeConn {
    const conn = try makeFakeConn(allocator);
    errdefer destroyFakeConn(allocator, conn);
    const before = router.peerCount();
    try router.inbox.putOne(io, .{ .peer_connected = .{ .peer = peer, .conn = conn, .remote_addr = dummy_addr } });
    // Spin until the count rises (peer fully wired) with a bounded wait.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (router.peerCount() > before) break;
        io_time.ms(5).sleep(io) catch {};
    }
    return conn;
}

test "subscribe announces the subscription to a connected peer" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null);
    defer router.destroy();
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // Subscribe locally; the router announces it to every peer on the subscribe
    // lane. The writer fiber opens the fake stream and records the framed RPC.
    const topic = try allocator.dupe(u8, "t");
    try router.inbox.putOne(io, .{ .subscribe = .{ .topic = topic } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordHasSubscription(conn.record.written.items, "t", true)) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(recordHasSubscription(conn.record.written.items, "t", true));
}

test "inbound subscription is tracked on the source peer" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null);
    defer router.destroy();
    try router.start();

    const peer = testPeer(1);
    const conn = try connectFakePeer(io, allocator, router, peer);
    defer destroyFakeConn(allocator, conn);

    // Peer X announces it subscribes to "t"; the router records it on X's state.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", true) } });

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer, "t"));

    // And an unsubscribe removes it.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer, .rpc = try buildInboundSub(allocator, "t", false) } });
    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (!peerTracksTopic(router, peer, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(!peerTracksTopic(router, peer, "t"));
}

test "floodsub forwards an inbound publish to subscribed peers, not the source" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null);
    defer router.destroy();
    try router.start();

    const peer_x = testPeer(1);
    const peer_y = testPeer(2);
    const conn_x = try connectFakePeer(io, allocator, router, peer_x);
    defer destroyFakeConn(allocator, conn_x);
    const conn_y = try connectFakePeer(io, allocator, router, peer_y);
    defer destroyFakeConn(allocator, conn_y);

    // Both peers announce they subscribe to "t".
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_x, .rpc = try buildInboundSub(allocator, "t", true) } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_y, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer_x, "t") and peerTracksTopic(router, peer_y, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer_x, "t") and peerTracksTopic(router, peer_y, "t"));

    // X publishes a message on "t". It must be forwarded to Y but NOT echoed to X.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_x,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x01", "t", "hello"),
    } });

    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_y.record.written.items, "t", "hello") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_y.record.written.items, "t", "hello"));
    // No echo back to the source peer X.
    try std.testing.expectEqual(@as(usize, 0), recordCountPublishes(conn_x.record.written.items, "t", "hello"));
}

test "floodsub dedups a repeated publish (same from+seqno) to forward once" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, null);
    defer router.destroy();
    try router.start();

    const peer_x = testPeer(1);
    const peer_y = testPeer(2);
    const conn_x = try connectFakePeer(io, allocator, router, peer_x);
    defer destroyFakeConn(allocator, conn_x);
    const conn_y = try connectFakePeer(io, allocator, router, peer_y);
    defer destroyFakeConn(allocator, conn_y);

    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_y, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer_y, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer_y, "t"));

    // Post the identical publish (same from + seqno) twice. The seen-cache must
    // suppress the second so Y receives exactly one forward.
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_x,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x07", "t", "dup"),
    } });
    try router.inbox.putOne(io, .{ .inbound_rpc = .{
        .peer = peer_x,
        .rpc = try buildInboundPublish(allocator, "origin", "\x00\x00\x00\x07", "t", "dup"),
    } });

    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (recordCountPublishes(conn_y.record.written.items, "t", "dup") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    // Give the (suppressed) second a chance to wrongly land before asserting.
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_y.record.written.items, "t", "dup"));
}

test "local publish forwards to subscribers and delivers to the local handler" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var rec = RecordingHandler{ .allocator = allocator };
    defer rec.deinit();

    const router = try FakeRouter.create(allocator, io, .{}, local_test_peer, rec.handler());
    defer router.destroy();
    try router.start();

    // Subscribe locally so the publish is delivered to our own handler too.
    try router.inbox.putOne(io, .{ .subscribe = .{ .topic = try allocator.dupe(u8, "t") } });

    const peer_y = testPeer(2);
    const conn_y = try connectFakePeer(io, allocator, router, peer_y);
    defer destroyFakeConn(allocator, conn_y);
    try router.inbox.putOne(io, .{ .inbound_rpc = .{ .peer = peer_y, .rpc = try buildInboundSub(allocator, "t", true) } });
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 5) {
        if (peerTracksTopic(router, peer_y, "t")) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expect(peerTracksTopic(router, peer_y, "t"));

    // Publish locally: Y gets the forwarded frame and our handler fires.
    try router.inbox.putOne(io, .{ .publish = .{
        .topic = try allocator.dupe(u8, "t"),
        .data = try allocator.dupe(u8, "hello"),
    } });

    waited = 0;
    while (waited < 2000) : (waited += 5) {
        if (rec.calls > 0 and recordCountPublishes(conn_y.record.written.items, "t", "hello") >= 1) break;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 1), recordCountPublishes(conn_y.record.written.items, "t", "hello"));
    try std.testing.expectEqual(@as(usize, 1), rec.calls);
    try std.testing.expectEqualSlices(u8, "t", rec.topic.?);
    try std.testing.expectEqualSlices(u8, "hello", rec.data.?);
    // The delivered `from` is our own peer id (the publish origin).
    try std.testing.expectEqualSlices(u8, local_test_peer.bytes[0..local_test_peer.len], rec.from.?);
}
