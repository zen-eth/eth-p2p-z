//! ConnectionActor: the per-connection fiber that owns the quiche_conn and
//! drives all of its state. Runtime operations communicate via `SharedState`
//! (channel endpoints + atomic flags); the connection handle owns a concrete
//! actor pointer only so `deinit` can cancel/await the spawned fiber and free
//! the actor allocation.
//!
//! Lifetime:
//!   - Allocated on the heap by the spawning code (router accepts, dialer
//!     connects). Its main fiber owns actor-private logical resources.
//!   - The shared state allocation is refcounted (init = 2: one for handle,
//!     one for actor). When the actor's main loop exits, it releases its
//!     `shared` ref. Whichever side drops the last ref frees `SharedState`.
//!   - The connection handle cancels/awaits spawned futures and frees the actor
//!     allocation after the runtime is done touching it.

const std = @import("std");
const builtin = @import("builtin");
const quiche = @import("quiche").c;

const address = @import("../address.zig");
const cid_registry = @import("cid_registry.zig");
const cid_mod = @import("cid.zig");
const commands = @import("commands.zig");
const cid_gen = @import("cid_gen.zig");
const tls = @import("../../security/tls.zig");
const conn_limits = @import("limits.zig");
const conn_stats = @import("stats.zig");
const stage_mod = @import("stage.zig");
const io_time = @import("../io/time.zig");
const packet_route = @import("../io/packet_route.zig");
const socket_control = @import("../io/socket_control.zig");
const shared_state_mod = @import("shared_state.zig");
const transport_mod = @import("../io/transport.zig");
const waitset_mod = @import("../io/waitset.zig");

const stream_handle = @import("../stream/handle.zig");
const stream_record_mod = @import("../stream/record.zig");
const stream_shared_mod = @import("../stream/shared_state.zig");

const Allocator = std.mem.Allocator;
const Atomic = std.atomic.Value;
const ArrayList = std.ArrayList;

pub const Command = commands.Command;
pub const SharedState = shared_state_mod.SharedState;
pub const ConnectionStats = conn_stats.ConnectionStats;
pub const NetworkTransport = transport_mod.NetworkTransport;
pub const NetworkError = transport_mod.NetworkError;
pub const RoutedPacket = packet_route.RoutedPacket;
pub const IncomingPacketChannel = packet_route.IncomingPacketChannel;
pub const PacketEnqueueResult = packet_route.EnqueueResult;
pub const CidKey = cid_mod.CidKey;
pub const local_cid_len = cid_mod.local_cid_len;

const StreamHandle = stream_handle.Stream;
const StreamRecord = stream_record_mod.Record;
const StreamShared = stream_shared_mod.SharedState;

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

pub const max_outbound_batch_size: usize = 32;
pub const max_flush_packet_len: usize = 2048;

const packet_buf_len = 65_535;
const actor_recv_budget: usize = 128;
const actor_send_budget: usize = 128;
const max_inline_ready_rounds: usize = 16;
const pacing_release_slack_ns = 250 * std.time.ns_per_us;

pub const WriteState = struct {
    pub const Path = struct {
        from: std.Io.net.IpAddress,
        to: std.Io.net.IpAddress,
    };

    bytes_written: usize = 0,
    next_release_time_ns: ?i96 = null,
    destination: ?std.Io.net.IpAddress = null,
    selected_path: ?Path = null,
    pending_paths: ?*quiche.quiche_socket_addr_iter = null,
    data: [max_flush_packet_len]u8 = undefined,

    pub fn written(w: *WriteState) []u8 {
        return w.data[0..w.bytes_written];
    }

    pub fn reset(w: *WriteState) void {
        w.bytes_written = 0;
        w.next_release_time_ns = null;
        w.destination = null;
        w.selected_path = null;
    }

    pub fn deinit(w: *WriteState) void {
        if (w.pending_paths) |iter| quiche.quiche_socket_addr_iter_free(iter);
        w.pending_paths = null;
    }
};

fn applyOutgoingControl(
    message: *std.Io.net.OutgoingMessage,
    control_buffer: []u8,
    source_caps: socket_control.Capabilities,
    selected_path: ?WriteState.Path,
) void {
    message.control = socket_control.encodeOutgoingControl(control_buffer, .{
        .caps = source_caps,
        .from = if (selected_path) |path| path.from else null,
    });
}

// ---------------------------------------------------------------------------
// ConnectionActor
// ---------------------------------------------------------------------------

pub const ConnectionActor = struct {
    // -- identity & infrastructure --
    allocator: Allocator,
    io: std.Io,
    shared: *SharedState,

    // -- network state (actor-only) --
    conn: ?*quiche.quiche_conn = null,
    transport: ?NetworkTransport = null,
    cids: cid_registry.Registry = .{},
    incoming_packets: ?*IncomingPacketChannel = null,

    // -- stream management (actor-only) --
    streams: std.AutoHashMap(u64, *StreamRecord),
    next_bidi_stream_id: u64 = 0,
    ready_outbound_streams: ArrayList(*StreamRecord) = .empty,
    ready_outbound_head: usize = 0,
    force_outbound_stream_scan: bool = false,
    pending_accept_streams: ArrayList(*StreamHandle) = .empty,
    pending_accept_head: usize = 0,
    pending_accept_cap: usize = 0,
    reserved_peer_bidi_streams: u64 = 0,
    stream_inbound_queue_bytes: usize = 0,
    stream_outbound_queue_bytes: usize = 0,
    stream_inbound_quantum_bytes: usize = 0,
    stream_outbound_quantum_bytes: usize = 0,

    // -- actor bookkeeping --
    /// Spawned via `std.Io.concurrent` so the runtime owns the per-fiber
    /// tracking allocation. The wrapper here is just a pointer + result
    /// slot; the runtime touches its own heap allocation, never `self`.
    /// `Future.cancel` blocks until the runtime is fully done with the
    /// allocation, so freeing `self` after both `cancel`s is safe — no
    /// post-call atomic UAF possible.
    main_future: ?std.Io.Future(std.Io.Cancelable!void) = null,
    running: Atomic(bool) = .init(false),
    shutdown_requested: Atomic(bool) = .init(false),
    lifecycle: stage_mod.ActorLifecycle = .{},
    pending_ready: waitset_mod.Ready = .{},
    write: WriteState = .{},

    // -- stats accumulator (actor publishes via shared.stats) --
    stats_snapshot: ConnectionStats = .{},
    handshake_started_mono_ns: u64 = 0,

    // ----- init / spawn / deinit -----------------------------------------

    pub const SpawnParams = struct {
        allocator: Allocator,
        io: std.Io,
        conn: *quiche.quiche_conn,
        transport: NetworkTransport,
        shared: *SharedState,
        pending_accept_cap: usize,
        stream_inbound_queue_bytes: usize = 0,
        stream_outbound_queue_bytes: usize = 0,
        stream_inbound_quantum_bytes: usize = 0,
        stream_outbound_quantum_bytes: usize = 0,
    };

    /// Allocate a ConnectionActor on the heap and initialise its fields.
    /// Caller hands over `transport` ownership and one `shared` ref. The
    /// returned actor has not yet started its fiber — call `spawn` next.
    pub fn create(params: SpawnParams) Allocator.Error!*ConnectionActor {
        const self = try params.allocator.create(ConnectionActor);
        errdefer params.allocator.destroy(self);

        const initial_stats = ConnectionStats.fromQuiche(params.conn, 0);

        self.* = .{
            .allocator = params.allocator,
            .io = params.io,
            .shared = params.shared,
            .conn = params.conn,
            .transport = params.transport,
            .streams = std.AutoHashMap(u64, *StreamRecord).init(params.allocator),
            .next_bidi_stream_id = if (quiche.quiche_conn_is_server(params.conn)) 1 else 0,
            .pending_accept_cap = params.pending_accept_cap,
            .stream_inbound_queue_bytes = params.stream_inbound_queue_bytes,
            .stream_outbound_queue_bytes = params.stream_outbound_queue_bytes,
            .stream_inbound_quantum_bytes = params.stream_inbound_quantum_bytes,
            .stream_outbound_quantum_bytes = params.stream_outbound_quantum_bytes,
            .stats_snapshot = initial_stats,
        };
        self.transport.?.retainResources();
        return self;
    }

    /// Pre-spawn cleanup: releases the shared-state ref that the actor
    /// would have held and frees actor-owned resources without going
    /// through the fiber-exit path. Used when setup partially fails (or
    /// when an InitialConnection is dropped before `spawn` is called).
    pub fn destroyPrespawn(self: *ConnectionActor) void {
        const shared = self.shared;
        self.destroyLogical();
        if (self.incoming_packets) |packets| {
            packets.release();
            self.incoming_packets = null;
        }
        self.allocator.destroy(self);
        shared.release();
    }

    /// Logical teardown: free actor-private resources but leave the actor
    /// struct memory intact. The connection handle still needs the actor
    /// pointer so it can cancel/await spawned futures and free the allocation
    /// after the main loop returns.
    fn destroyLogical(self: *ConnectionActor) void {
        const allocator = self.allocator;
        if (self.incoming_packets) |packets| {
            packets.close(self.io);
        }

        self.deinitPendingAcceptStreams();
        self.detachOpenStreams();
        if (self.transport) |*transport| {
            transport.addStat("connections_closed", 1);
            transport.subStat("active_actors", 1);
            self.unregisterAllCids(transport);
            transport.deinit();
            self.transport = null;
        }
        self.cids.deinit(allocator);
        self.streams.deinit();
        self.ready_outbound_streams.deinit(allocator);
        self.pending_accept_streams.deinit(allocator);
        self.write.deinit();
        if (self.conn) |conn| {
            quiche.quiche_conn_free(conn);
            self.conn = null;
        }
    }

    /// Cancel any in-flight fibers and free the actor allocation. Safe
    /// to call from any fiber other than the actor's own (calling from
    /// inside `mainLoop` would deadlock on `Future.cancel`).
    ///
    /// `Future.cancel` blocks until the runtime has fully torn down its
    /// internal Future allocation; after both cancels return, the
    /// runtime is no longer touching anything reachable from `self`,
    /// so freeing the actor allocation is safe.
    pub fn destroySpawned(self: *ConnectionActor) void {
        self.shutdown_requested.store(true, .release);

        // Wake the main loop via the waitset so it can exit through its
        // ordinary loop condition and run shutdownAndCleanup; cancel as
        // a backstop in case the loop is spinning rather than parked.
        self.notifyShutdown();
        if (self.main_future) |*f| f.cancel(self.io) catch {};
        self.main_future = null;
        if (self.incoming_packets) |packets| {
            packets.close(self.io);
            packets.release();
            self.incoming_packets = null;
        }
        self.allocator.destroy(self);
    }

    fn deinitPendingAcceptStreams(self: *ConnectionActor) void {
        for (self.pending_accept_streams.items[self.pending_accept_head..]) |handle| {
            handle.deinit();
        }
        self.pending_accept_streams.clearRetainingCapacity();
        self.pending_accept_head = 0;
    }

    fn detachOpenStreams(self: *ConnectionActor) void {
        const io = self.io;
        // Snapshot the records first so we can free them without iterating
        // while the hash map's pointer_stability is observed by valueIterator.
        var records_buf: [16]*StreamRecord = undefined;
        var records_list = std.ArrayList(*StreamRecord).initBuffer(&records_buf);
        var dynamic_records = std.ArrayList(*StreamRecord).empty;
        defer dynamic_records.deinit(self.allocator);

        var iter = self.streams.valueIterator();
        while (iter.next()) |slot| {
            if (records_list.items.len < records_buf.len) {
                records_list.appendAssumeCapacity(slot.*);
            } else {
                dynamic_records.append(self.allocator, slot.*) catch {
                    // Best-effort on OOM during teardown: free the record
                    // we couldn't queue, before its inbound queue drops.
                    slot.*.shared.markClosedLocal();
                    slot.*.deinit(self.allocator);
                    self.allocator.destroy(slot.*);
                };
            }
        }

        for (records_list.items) |record| {
            self.releasePeerStreamCreditReservation(record);
            record.detachCloseWriteWaiter(io);
            record.shared.markClosedLocal();
            record.deinit(self.allocator);
            self.allocator.destroy(record);
        }
        for (dynamic_records.items) |record| {
            self.releasePeerStreamCreditReservation(record);
            record.detachCloseWriteWaiter(io);
            record.shared.markClosedLocal();
            record.deinit(self.allocator);
            self.allocator.destroy(record);
        }
        self.streams.clearRetainingCapacity();
        self.reserved_peer_bidi_streams = 0;
        self.ready_outbound_streams.clearRetainingCapacity();
        self.ready_outbound_head = 0;
        self.force_outbound_stream_scan = false;
    }

    fn releasePeerStreamCreditReservation(self: *ConnectionActor, record: *StreamRecord) void {
        if (!record.releasePeerStreamCreditReservation()) return;
        self.reserved_peer_bidi_streams -|= 1;
    }

    // ----- packet inbox bridge -------------------------------------------

    pub fn initPacketInbox(self: *ConnectionActor, capacity_bytes: usize, capacity_packets: usize) Allocator.Error!void {
        if (self.incoming_packets != null) return;
        self.incoming_packets = try packet_route.IncomingPacketChannel.init(self.allocator, self.io, capacity_bytes, capacity_packets);
    }

    pub fn attachIncomingPacketWaitSet(self: *ConnectionActor) void {
        if (self.incoming_packets) |packets| packets.setWaitSet(self.io, &self.shared.waitset);
    }

    pub fn enqueueInboundPacket(self: *ConnectionActor, packet: *RoutedPacket) PacketEnqueueResult {
        if (self.incoming_packets) |packets| return packets.sender().enqueue(self.io, packet);
        packet.deinit();
        return .dropped;
    }

    fn popInboundPacket(self: *ConnectionActor, packet: *RoutedPacket) bool {
        if (self.incoming_packets) |packets| {
            return packets.receiver().tryRecv(self.io, packet);
        }
        return false;
    }

    // ----- main loop -----------------------------------------------------

    pub fn spawn(self: *ConnectionActor, stage: stage_mod.HandshakeStage, deadline_ns: ?i96) std.Io.ConcurrentError!void {
        _ = self.transport orelse return;
        if (self.running.swap(true, .acq_rel)) return;
        self.shutdown_requested.store(false, .release);
        self.lifecycle.begin(stage);
        self.shared.handshake.begin(stage, deadline_ns);

        errdefer {
            if (self.main_future) |*f| f.cancel(self.io) catch {};
            self.main_future = null;
            self.running.store(false, .release);
            self.lifecycle.fail();
            if (stage == .handshaking) self.failHandshake();
        }

        self.main_future = try std.Io.concurrent(self.io, mainLoopEntry, .{self});
    }

    fn mainLoopEntry(self: *ConnectionActor) std.Io.Cancelable!void {
        return self.mainLoop();
    }

    fn mainLoop(self: *ConnectionActor) std.Io.Cancelable!void {
        defer self.shutdownAndCleanup();

        var inline_ready_rounds: usize = 0;
        while (!self.shutdown_requested.load(.acquire) and !self.shared.isClosed()) {
            const deadline_work_remains = self.processDueDeadline() catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => {
                    self.closeFromActorError(err);
                    return;
                },
            };
            if (deadline_work_remains and !self.shared.isClosed()) {
                inline_ready_rounds += 1;
                if (inline_ready_rounds >= max_inline_ready_rounds) {
                    inline_ready_rounds = 0;
                    try std.Io.sleep(self.io, .zero, .awake);
                }
                continue;
            }

            const ready = self.takeReady();
            const ready_work_remains = self.drainReady(ready) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => {
                    self.closeFromActorError(err);
                    return;
                },
            };

            if (self.shutdown_requested.load(.acquire) or self.shared.isClosed()) break;
            if (ready_work_remains) {
                inline_ready_rounds += 1;
                if (inline_ready_rounds >= max_inline_ready_rounds) {
                    inline_ready_rounds = 0;
                    try std.Io.sleep(self.io, .zero, .awake);
                }
                continue;
            }

            inline_ready_rounds = 0;
            const observed_epoch = self.shared.waitset.observe();
            self.park(observed_epoch) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => {
                    self.closeFromActorError(err);
                    return;
                },
            };
        }
    }

    fn shutdownAndCleanup(self: *ConnectionActor) void {
        if (self.shutdown_requested.load(.acquire) or self.shared.isClosed()) {
            self.closeApplicationQueues();
        }
        if (self.conn) |conn| {
            if (self.transport) |transport| {
                if (quiche.quiche_conn_is_closed(conn) or self.shutdown_requested.load(.acquire)) {
                    _ = self.flushScheduled(conn, &transport, std.math.maxInt(usize)) catch {};
                }
            }
        }

        const prior_result = self.lifecycle.closeResult();
        const result: stage_mod.ActorLifecycle.CloseResult = if (self.shutdown_requested.load(.acquire))
            .canceled
        else if (prior_result != .none)
            prior_result
        else
            .graceful;
        self.lifecycle.closed(result);
        self.write.deinit();
        self.running.store(false, .release);

        self.shared.markClosed();

        // Logical cleanup only: the actual actor allocation is freed by the
        // connection handle after spawned futures have been canceled/awaited.
        const shared = self.shared;
        self.destroyLogical();
        shared.release();
    }

    fn closeApplicationQueues(self: *ConnectionActor) void {
        const io = self.io;
        _ = self.shared.closeControlInbox();
        self.shared.accept_queue.close(io);
        const accepted_drained = self.shared.drainAcceptedStreams(io);
        if (accepted_drained > 0) {
            self.stats_snapshot.pending_accept_streams -|= @intCast(accepted_drained);
        }
        if (self.shared.datagram_queue) |q| {
            q.discardQueued(io);
            q.close(io);
        }
    }

    // ----- ready / parking ----------------------------------------------

    fn takeReady(self: *ConnectionActor) waitset_mod.Ready {
        const pending = self.pending_ready;
        self.pending_ready = .{};
        return waitset_mod.Ready.merge(pending, self.shared.waitset.take());
    }

    fn carryReady(self: *ConnectionActor, ready: waitset_mod.Ready) void {
        self.pending_ready = waitset_mod.Ready.merge(self.pending_ready, ready);
    }

    fn park(self: *ConnectionActor, observed_epoch: u32) NetworkError!void {
        const transport = self.transport orelse return error.QuicheRecvFailed;
        const io = transport.io;
        const timer = try self.computeWaitDeadline();

        if (timer == .none) {
            try self.shared.waitset.wait(io, observed_epoch);
            return;
        }

        const deadline = timer.toDeadline(io);
        try self.shared.waitset.waitTimeout(io, observed_epoch, deadline);
        if (self.shared.waitset.unchanged(observed_epoch) and io_time.timeoutExpired(io, deadline)) {
            try self.handleDeadline();
        }
    }

    fn computeWaitDeadline(self: *ConnectionActor) NetworkError!std.Io.Timeout {
        const transport = self.transport orelse return error.QuicheRecvFailed;
        const conn = self.conn orelse return error.QuicheRecvFailed;
        self.stats_snapshot.actor_loop_iterations += 1;
        return self.stageWaitDeadline(conn, &transport);
    }

    fn processDueDeadline(self: *ConnectionActor) NetworkError!bool {
        const conn = self.conn orelse return error.QuicheRecvFailed;
        const transport = self.transport orelse return error.QuicheRecvFailed;
        const timer = try self.stageWaitDeadline(conn, &transport);
        self.stats_snapshot.actor_deadline_checks += 1;
        if (!self.deadlineDue(transport.io, timer)) return false;
        self.recordDeadlineLate(transport.io, timer);
        self.stats_snapshot.actor_deadlines_processed += 1;
        try self.stagePostWait(conn, &transport);
        return !self.shared.isClosed();
    }

    fn handleDeadline(self: *ConnectionActor) NetworkError!void {
        const conn = self.conn orelse return error.QuicheRecvFailed;
        const transport = self.transport orelse return error.QuicheRecvFailed;
        try self.stagePostWait(conn, &transport);
    }

    // ----- ready drain ---------------------------------------------------

    fn drainReady(self: *ConnectionActor, ready: waitset_mod.Ready) NetworkError!bool {
        const conn = self.conn orelse return error.QuicheRecvFailed;
        const transport = self.transport orelse return error.QuicheRecvFailed;

        self.recordReady(ready);
        const read_result = try self.stageOnRead(conn, &transport, ready);
        const write_work_remains = try self.stageOnFlush(conn, &transport, ready);
        // quiche can transition the server side to established while producing
        // the final handshake flight, not only while consuming inbound packets.
        // Re-check after flush so `accept()` waiters do not depend on a later
        // client packet to observe handshake completion.
        self.updateHandshake(transport.io);

        var pending: waitset_mod.Ready = .{};
        if (read_result.packet_budget_exhausted) pending.inbound_packets = true;
        if (read_result.stream_work_remains) pending.stream_inbound = true;
        if (pending.any()) self.carryReady(pending);

        return (pending.any() or write_work_remains or self.shared.waitset.peek().any()) and !self.shared.isClosed();
    }

    fn recordReady(self: *ConnectionActor, ready: waitset_mod.Ready) void {
        if (ready.inbound_packets) self.stats_snapshot.actor_ready_inbound_packets += 1;
        if (ready.control_commands) self.stats_snapshot.actor_ready_control_commands += 1;
        if (ready.stream_inbound) self.stats_snapshot.actor_ready_stream_inbound += 1;
        if (ready.stream_outbound) self.stats_snapshot.actor_ready_stream_outbound += 1;
        if (ready.accepted_stream_pop) self.stats_snapshot.actor_ready_accepted_stream_pop += 1;
        if (ready.shutdown) self.stats_snapshot.actor_ready_shutdown += 1;
    }

    // ----- read/flush stages --------------------------------------------

    const ReadDrainResult = struct {
        packets_drained: usize = 0,
        packet_budget_exhausted: bool = false,
        stream_work_remains: bool = false,
    };

    fn stageOnRead(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        transport: *const NetworkTransport,
        ready: waitset_mod.Ready,
    ) NetworkError!ReadDrainResult {
        var packet: RoutedPacket = undefined;
        var drained: usize = 0;
        if (ready.inbound_packets) {
            while (drained < actor_recv_budget and self.popInboundPacket(&packet)) : (drained += 1) {
                const recv_result = recvRoutedPacket(conn, &packet);
                packet.deinit();
                try recv_result;
                if (self.shared.isClosed()) break;
            }
        }
        var stream_work_remains = false;
        if (drained > 0 and !self.shared.isClosed()) {
            stream_work_remains = try self.postRecvBatch(conn, transport);
        }
        if (ready.stream_inbound and !self.shared.isClosed()) {
            stream_work_remains = (try self.drainReadableStreamData(conn, transport.io)) or stream_work_remains;
        }
        if (ready.control_commands) _ = self.handleControlMessages();
        if (ready.accepted_stream_pop) {
            self.flushAcceptedStreamPops(transport.io);
            self.flushPendingAcceptStreams(transport.io);
        }
        self.updateHandshake(transport.io);
        if (drained > 0 or ready.control_commands or ready.stream_inbound) self.refreshStats(transport.io);
        return .{
            .packets_drained = drained,
            .packet_budget_exhausted = ready.inbound_packets and drained == actor_recv_budget and !self.shared.isClosed(),
            .stream_work_remains = stream_work_remains,
        };
    }

    fn stageOnFlush(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        transport: *const NetworkTransport,
        ready: waitset_mod.Ready,
    ) NetworkError!bool {
        if (ready.stream_outbound) {
            // Overflow on the producer side: a handle's push to the pending
            // queue failed, so it asked us to sweep every stream instead.
            if (self.shared.fallback_full_scan.swap(false, .acq_rel)) {
                self.force_outbound_stream_scan = true;
            }
            // Drain the per-stream signals regardless of fallback so each
            // stream's `outbound_signaled` flag is cleared (otherwise the
            // next handle write would skip its push thinking the actor
            // still has it queued).
            self.consumeOutboundPendingSignals(transport.io);
        }
        const stream_work_remains = try self.drainOutboundStreams(conn, transport.io);
        const write_work_remains = try self.flushScheduled(conn, transport, actor_send_budget);
        return stream_work_remains or write_work_remains;
    }

    /// Pop stream IDs that handles have pushed to the connection's
    /// outbound-pending queue and mark each corresponding `StreamRecord`
    /// ready for `drainOutboundStreams`. The flag is cleared *before*
    /// `markStreamOutboundReady` so a concurrent handle write detects
    /// "actor has popped" and pushes a fresh signal — the byte queue is
    /// the source of truth and `drainOneStreamOutbound` re-reads it on
    /// each iteration of its inner loop, so no data is lost.
    fn consumeOutboundPendingSignals(self: *ConnectionActor, io: std.Io) void {
        var buf: [16]u64 = undefined;
        while (true) {
            const n = self.shared.outbound_pending_ids.getUncancelable(io, &buf, 0) catch return;
            if (n == 0) break;
            for (buf[0..n]) |sid| {
                const record = self.streams.get(sid) orelse continue;
                record.shared.outbound_signaled.store(false, .release);
                self.markStreamOutboundReady(record) catch {
                    // OOM appending to ready_outbound_streams; full scan
                    // will pick it up via needsOutboundDrain.
                    self.force_outbound_stream_scan = true;
                };
            }
        }
    }

    fn flushAcceptedStreamPops(self: *ConnectionActor, io: std.Io) void {
        const popped = self.shared.accepted_stream_pops.swap(0, .acq_rel);
        if (popped == 0) return;
        self.stats_snapshot.pending_accept_streams -|= @intCast(popped);
        self.stats_snapshot.last_updated_mono_ns = io_time.monotonicNs(io);
        self.publishStats();
    }

    fn stageWaitDeadline(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        transport: *const NetworkTransport,
    ) NetworkError!std.Io.Timeout {
        var deadline = minTimeout(
            transport.io,
            quicheTimeout(conn),
            self.pacedReleaseTimeout(),
        );
        if (self.lifecycle.isHandshake()) {
            if (self.shared.handshake.deadline()) |deadline_ns| {
                deadline = minTimeout(transport.io, deadline, deadlineFromNs(deadline_ns));
            }
        }
        return deadline;
    }

    fn stagePostWait(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        transport: *const NetworkTransport,
    ) NetworkError!void {
        if (self.lifecycle.isHandshake() and self.handshakeTimedOut(transport.io)) {
            const empty: [0]u8 = .{};
            _ = quiche.quiche_conn_close(conn, false, 0, empty[0..].ptr, 0);
            self.failHandshakeAndStamp(transport.io, .handshake_timeout);
            return;
        }
        if (quicheTimeoutExpired(conn)) {
            self.write.next_release_time_ns = null;
            quiche.quiche_conn_on_timeout(conn);
            self.updateHandshake(transport.io);
            self.refreshStats(transport.io);
            if (self.shared.isClosed()) return;
        }
        _ = try self.stageOnFlush(conn, transport, .{});
        self.updateHandshake(transport.io);
    }

    fn deadlineDue(self: *const ConnectionActor, io: std.Io, timer: std.Io.Timeout) bool {
        const deadline_ns = io_time.timeoutDeadlineNs(io, timer) orelse return false;
        const now_ns = io_time.monotonicNsSigned(io);
        if (deadline_ns <= now_ns) return true;
        if (self.write.next_release_time_ns) |release_at_ns| {
            return release_at_ns == deadline_ns and release_at_ns <= now_ns + pacing_release_slack_ns;
        }
        return false;
    }

    fn recordDeadlineLate(self: *ConnectionActor, io: std.Io, timer: std.Io.Timeout) void {
        const deadline_ns = io_time.timeoutDeadlineNs(io, timer) orelse return;
        const now_ns = io_time.monotonicNsSigned(io);
        if (now_ns <= deadline_ns) return;
        const late_ns: u64 = @intCast(now_ns - deadline_ns);
        self.stats_snapshot.actor_deadline_late_ns_max = @max(self.stats_snapshot.actor_deadline_late_ns_max, late_ns);
    }

    fn handshakeTimedOut(self: *const ConnectionActor, io: std.Io) bool {
        if (self.handshake_started_mono_ns == 0) return false;
        const deadline_ns = self.shared.handshake.deadline() orelse return false;
        return io_time.monotonicNsSigned(io) >= deadline_ns;
    }

    fn pacedReleaseTimeout(self: *const ConnectionActor) std.Io.Timeout {
        const release_at_ns = self.write.next_release_time_ns orelse return .none;
        return deadlineFromNs(release_at_ns);
    }

    // ----- packet flush -----

    pub fn flushScheduled(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        transport: *const NetworkTransport,
        max_packets: usize,
    ) NetworkError!bool {
        if (max_packets == 0) return true;

        var packets_released: usize = 0;
        while (packets_released < max_packets) {
            const batch_cap = @min(
                @min(@max(transport.outbound_batch_size, @as(usize, 1)), max_outbound_batch_size),
                max_packets - packets_released,
            );
            var payloads: [max_outbound_batch_size][max_flush_packet_len]u8 = undefined;
            var destinations: [max_outbound_batch_size]std.Io.net.IpAddress = undefined;
            var messages: [max_outbound_batch_size]std.Io.net.OutgoingMessage = undefined;
            var controls: [max_outbound_batch_size][socket_control.send_control_buffer_len]u8 align(socket_control.control_buffer_align) = undefined;
            var batch_len: usize = 0;
            var quiche_done = false;
            var paced_pending = false;

            if (self.write.bytes_written > 0) {
                if (self.write.next_release_time_ns) |release_at_ns| {
                    if (!releaseTimeDue(transport.io, release_at_ns)) return false;
                    self.write.next_release_time_ns = null;
                }

                destinations[batch_len] = self.write.destination orelse return error.AddressInvalid;
                @memcpy(payloads[batch_len][0..self.write.bytes_written], self.write.written());
                messages[batch_len] = .{
                    .address = &destinations[batch_len],
                    .data_ptr = payloads[batch_len][0..].ptr,
                    .data_len = self.write.bytes_written,
                };
                applyOutgoingControl(
                    &messages[batch_len],
                    controls[batch_len][0..],
                    transport.socket.caps,
                    self.write.selected_path,
                );
                batch_len += 1;
                packets_released += 1;
                self.write.reset();
            }

            while (batch_len < batch_cap and packets_released < max_packets) {
                var send_info: quiche.quiche_send_info = undefined;
                const sent = try self.quicheConnSendOnSelectedPath(conn, transport, payloads[batch_len][0..].ptr, max_flush_packet_len, &send_info);
                if (sent == quiche.QUICHE_ERR_DONE) {
                    self.write.selected_path = null;
                    if (self.write.pending_paths != null) continue;
                    quiche_done = true;
                    break;
                }
                if (sent < 0) return error.QuicheSendFailed;

                const len: usize = @intCast(sent);
                const destination = address.fromSockaddrStorage(@ptrCast(@alignCast(&send_info.to))) catch return error.AddressInvalid;
                if (sendInfoReleaseNs(&send_info)) |release_at_ns| {
                    if (!releaseTimeDue(transport.io, release_at_ns)) {
                        self.storePendingWrite(payloads[batch_len][0..len], destination, release_at_ns);
                        paced_pending = true;
                        break;
                    }
                }

                destinations[batch_len] = destination;
                messages[batch_len] = .{
                    .address = &destinations[batch_len],
                    .data_ptr = payloads[batch_len][0..].ptr,
                    .data_len = len,
                };
                applyOutgoingControl(
                    &messages[batch_len],
                    controls[batch_len][0..],
                    transport.socket.caps,
                    self.write.selected_path,
                );
                batch_len += 1;
                packets_released += 1;
            }

            if (batch_len > 0) {
                transport.socket.sendMany(transport.io, messages[0..batch_len], .{}) catch |err| {
                    self.stats_snapshot.write_errors += 1;
                    return err;
                };
                self.stats_snapshot.write_batches += 1;
                self.stats_snapshot.write_packets += @intCast(batch_len);
                for (messages[0..batch_len]) |message| self.stats_snapshot.write_bytes += message.data_len;
                if (!paced_pending) self.write.selected_path = null;
            }

            if (quiche_done or paced_pending) return false;
            if (batch_len == 0) return false;
        }

        return true;
    }

    fn storePendingWrite(
        self: *ConnectionActor,
        payload: []const u8,
        destination: std.Io.net.IpAddress,
        release_at_ns: i96,
    ) void {
        std.debug.assert(payload.len <= self.write.data.len);
        @memcpy(self.write.data[0..payload.len], payload);
        self.write.bytes_written = payload.len;
        self.write.next_release_time_ns = release_at_ns;
        self.write.destination = destination;
        self.write.selected_path = self.selectedPathFromSendDestination(destination);
        self.stats_snapshot.pacing_deferred += 1;
    }

    fn quicheConnSendOnSelectedPath(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        transport: *const NetworkTransport,
        out: [*]u8,
        out_len: usize,
        send_info: *quiche.quiche_send_info,
    ) NetworkError!isize {
        const path = (try self.selectSendPath(conn, transport)) orelse return quiche.QUICHE_ERR_DONE;
        var from_storage: address.PosixAddress = undefined;
        var to_storage: address.PosixAddress = undefined;
        const from_len = address.addressToPosix(&path.from, &from_storage);
        const to_len = address.addressToPosix(&path.to, &to_storage);
        const from_ptr: [*c]const quiche.struct_sockaddr = @ptrCast(&from_storage.any);
        const to_ptr: [*c]const quiche.struct_sockaddr = @ptrCast(&to_storage.any);

        self.stats_snapshot.send_on_path_calls += 1;
        const sent = quiche.quiche_conn_send_on_path(
            conn,
            out,
            out_len,
            from_ptr,
            from_len,
            to_ptr,
            to_len,
            send_info,
        );
        if (sent >= 0) {
            const from = address.fromSockaddrStorage(@ptrCast(@alignCast(&send_info.from))) catch path.from;
            const to = address.fromSockaddrStorage(@ptrCast(@alignCast(&send_info.to))) catch path.to;
            self.write.selected_path = .{ .from = from, .to = to };
        } else if (sent == quiche.QUICHE_ERR_DONE) {
            self.stats_snapshot.send_on_path_done += 1;
        } else {
            self.stats_snapshot.write_errors += 1;
        }
        return sent;
    }

    fn selectSendPath(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        transport: *const NetworkTransport,
    ) NetworkError!?WriteState.Path {
        if (self.write.selected_path) |path| return path;

        var from_storage: address.PosixAddress = undefined;
        const from_len = address.addressToPosix(&transport.local, &from_storage);
        const from_ptr: [*c]const quiche.struct_sockaddr = @ptrCast(&from_storage.any);

        if (self.write.pending_paths == null) {
            self.write.pending_paths = quiche.quiche_conn_paths_iter(conn, from_ptr, from_len);
        }

        const iter = self.write.pending_paths orelse return null;
        var peer_storage: quiche.sockaddr_storage = undefined;
        var peer_len: usize = @sizeOf(quiche.sockaddr_storage);
        if (quiche.quiche_socket_addr_iter_next(iter, &peer_storage, &peer_len)) {
            const to = address.fromSockaddrStorage(@ptrCast(@alignCast(&peer_storage))) catch return error.AddressInvalid;
            const path = WriteState.Path{ .from = transport.local, .to = to };
            self.write.selected_path = path;
            return path;
        }

        quiche.quiche_socket_addr_iter_free(iter);
        self.write.pending_paths = null;
        return null;
    }

    fn selectedPathFromSendDestination(self: *const ConnectionActor, destination: std.Io.net.IpAddress) ?WriteState.Path {
        const existing = self.write.selected_path orelse return null;
        return .{ .from = existing.from, .to = destination };
    }

    // ----- close-from-error -----------------------------------------------

    fn closeFromActorError(self: *ConnectionActor, err: anyerror) void {
        self.closeFromActorErrorCode(conn_stats.ActorError.fromError(err));
    }

    fn closeFromActorErrorCode(self: *ConnectionActor, actor_error: conn_stats.ActorError) void {
        if (self.lifecycle.closeResult() == .none) self.lifecycle.closing(.failed);
        self.stats_snapshot.actor_fatal_errors += 1;
        self.stats_snapshot.last_actor_error = actor_error;
        if (self.stats_snapshot.close_reason == .none or self.stats_snapshot.close_reason == .closed) {
            self.stats_snapshot.close_reason = .actor_error;
        }
        // Publish the fully-stamped fatal-error stats BEFORE the wake. markClosed()
        // below calls handshake.close() -> done.set, waking a dialer parked in
        // waitHandshake; its failReason()/classifyHandshakeFailure must see
        // last_actor_error + close_reason=.actor_error or it reports an opaque
        // error. (This publish previously sat behind `if (isHandshaking())` AFTER
        // markClosed had already flipped the stage to .closed — it was dead code
        // and never ran, so the fatal-error cause was silently dropped.)
        if (self.shared.handshake.isHandshaking()) self.publishStats();
        if (!self.shared.isClosed()) {
            if (self.conn) |conn| {
                const empty: [0]u8 = .{};
                _ = quiche.quiche_conn_close(conn, false, 0x1, empty[0..].ptr, 0);
            }
            self.shared.markClosed();
        }
        self.closeApplicationQueues();
        self.notifyShutdown();
    }

    fn failHandshake(self: *ConnectionActor) void {
        self.shared.handshake.fail(self.io);
    }

    fn notifyShutdown(self: *ConnectionActor) void {
        self.shared.waitset.notify(self.io, .{ .shutdown = true });
    }

    // ----- stats / publishing -------------------------------------------

    fn publishStats(self: *ConnectionActor) void {
        self.shared.stats.publish(self.io, self.stats_snapshot);
    }

    fn refreshStats(self: *ConnectionActor, io: std.Io) void {
        const conn = self.conn orelse return;
        ConnectionStats.applyQuicheFields(&self.stats_snapshot, conn, io_time.monotonicNs(io));
        self.shared.max_datagram_payload.store(conn_limits.maxDatagramPayload(conn), .release);
        refreshCloseErrorStats(conn, &self.stats_snapshot);
        self.publishStats();
        if (quiche.quiche_conn_is_closed(conn) or quiche.quiche_conn_is_draining(conn)) {
            self.shared.markClosed();
        }
    }

    fn noteRouteCommandFailure(self: *ConnectionActor, io: std.Io) void {
        self.stats_snapshot.cid_map_command_failures += 1;
        self.stats_snapshot.last_updated_mono_ns = io_time.monotonicNs(io);
        if (self.transport) |transport| transport.addStat("cid_map_command_drops", 1);
    }

    fn noteCloseFlushError(self: *ConnectionActor, io: std.Io, err: anyerror) void {
        self.stats_snapshot.close_flush_errors += 1;
        self.stats_snapshot.last_actor_error = conn_stats.ActorError.fromError(err);
        self.stats_snapshot.last_updated_mono_ns = io_time.monotonicNs(io);
        self.publishStats();
    }

    fn noteStreamCloseError(self: *ConnectionActor, io: std.Io, err: anyerror) void {
        self.stats_snapshot.stream_close_errors += 1;
        self.stats_snapshot.last_actor_error = conn_stats.ActorError.fromError(err);
        self.stats_snapshot.last_updated_mono_ns = io_time.monotonicNs(io);
        self.publishStats();
    }

    // ----- handshake update ----------------------------------------------

    fn updateHandshake(self: *ConnectionActor, io: std.Io) void {
        const conn = self.conn orelse return;
        if (!self.shared.handshake.isHandshaking()) return;

        if (self.handshakeTimedOut(io)) {
            const empty: [0]u8 = .{};
            _ = quiche.quiche_conn_close(conn, false, 0, empty[0..].ptr, 0);
            self.failHandshakeAndStamp(io, .handshake_timeout);
            return;
        }

        if (quiche.quiche_conn_is_draining(conn)) {
            // Defer to quiche's CONNECTION_CLOSE codes (peer/local error).
            self.failHandshakeAndStamp(io, null);
            return;
        }

        if (quiche.quiche_conn_is_closed(conn)) {
            const empty: [0]u8 = .{};
            _ = quiche.quiche_conn_close(conn, false, 0, empty[0..].ptr, 0);
            self.failHandshakeAndStamp(io, null);
            return;
        }

        if (quiche.quiche_conn_is_established(conn) or quiche.quiche_conn_is_in_early_data(conn)) {
            // Record the specific cause (e.g. PeerVerifyFailed) before failing so
            // the dialer can surface it instead of an opaque HandshakeFailed.
            self.completeHandshake(io) catch |err| {
                self.stats_snapshot.last_actor_error = conn_stats.ActorError.fromError(err);
                // The connection is already established at the quiche/TLS layer, so
                // this is our own libp2p-identity rejection (or a local fault while
                // completing). Tell the peer with a CONNECTION_CLOSE instead of
                // letting it wait out its idle timeout — the timeout/closed branches
                // above already close; this one must too.
                const empty: [0]u8 = .{};
                _ = quiche.quiche_conn_close(conn, false, 0x1, empty[0..].ptr, 0);
                self.failHandshakeAndStamp(io, null);
            };
        }
    }

    fn completeHandshake(self: *ConnectionActor, io: std.Io) !void {
        const conn = self.conn orelse return error.ConnectionClosed;

        if (self.shared.public_key.load(.acquire) == null) {
            const pub_key = try tls.extractPublicKey(self.allocator, conn);
            try self.shared.publishPublicKey(pub_key);
        }
        if (self.shared.remote_addr.load(.acquire) == null) {
            const transport = self.transport orelse return error.ConnectionClosed;
            try self.shared.publishRemoteAddr(transport.peer);
        }
        if (self.stats_snapshot.handshake_duration_ns == 0 and self.handshake_started_mono_ns > 0) {
            self.stats_snapshot.handshake_duration_ns = io_time.monotonicNs(io) -| self.handshake_started_mono_ns;
        }
        self.shared.handshake.establish(io);
        self.lifecycle.running();
    }

    /// Fail the handshake, recording why. `explicit_reason`, when given, is the
    /// caller's definitive cause (e.g. `.handshake_timeout`) and overrides any
    /// quiche-derived reason; `null` defers to quiche's CONNECTION_CLOSE codes
    /// (falling back to `.handshake_failed`).
    fn failHandshakeAndStamp(self: *ConnectionActor, io: std.Io, explicit_reason: ?conn_stats.CloseReason) void {
        // Capture quiche's peer/local CONNECTION_CLOSE codes so the dialer can
        // tell a TLS alert (CRYPTO_ERROR) from a transport-parameter error.
        // NOTE: refreshCloseErrorStats can set close_reason itself (e.g.
        // .idle_timeout UNCONDITIONALLY on a timed-out conn, plus draining/closed),
        // so the explicit caller reason below MUST run after it to win — do not
        // reorder them or add an early return.
        if (self.conn) |conn| refreshCloseErrorStats(conn, &self.stats_snapshot);
        if (explicit_reason) |reason| {
            self.stats_snapshot.close_reason = reason;
        } else if (self.stats_snapshot.close_reason == .none) {
            self.stats_snapshot.close_reason = .handshake_failed;
        }
        self.stats_snapshot.last_updated_mono_ns = io_time.monotonicNs(io);
        self.lifecycle.fail();
        // Publish BEFORE any wake. Both markClosed() (handshake.close -> done.set)
        // and handshake.fail() wake a dialer parked in waitHandshake, which reads
        // conn.stats() immediately. markClosed() previously ran at the TOP of this
        // function, so a multi-executor dialer could wake on its done.set (stage
        // .closed) and read the snapshot before this publish. Publish first, then
        // markClosed + fail (happens-before via the stats mutex + handshake event).
        self.publishStats();
        self.shared.markClosed();
        self.shared.handshake.fail(io);
    }

    // ----- CID lifecycle -------------------------------------------------

    fn registerCid(self: *ConnectionActor, cid: []const u8) Allocator.Error!bool {
        return self.cids.register(self.allocator, cid);
    }

    fn unregisterCid(self: *ConnectionActor, cid: []const u8) bool {
        const key = CidKey.init(cid) orelse return false;
        if (!self.cids.unregisterKey(key)) return false;
        if (self.transport) |transport| self.queueCidUnmap(&transport, key);
        return true;
    }

    fn unregisterAllCids(self: *ConnectionActor, transport: *const NetworkTransport) void {
        if (self.cids.count() == 0) return;
        var i: usize = 0;
        while (i < self.cids.count()) : (i += 1) {
            if (self.cids.key(i)) |key| self.queueCidUnmap(transport, key);
        }
        self.cids.clearRetainingCapacity();
    }

    fn queueCidUnmap(self: *ConnectionActor, transport: *const NetworkTransport, cid: CidKey) void {
        const queued = transport.route_updates.sender().trySend(transport.io, .{ .unmap_cid = cid }) catch {
            self.noteRouteCommandFailure(transport.io);
            return;
        };
        if (!queued) self.noteRouteCommandFailure(transport.io);
    }

    /// Free function (no actor state): reads quiche's current source CID for `conn`.
    fn currentSourceCidKey(conn: *quiche.quiche_conn) ?CidKey {
        var source_id_ptr: [*c]const u8 = null;
        var source_id_len: usize = 0;
        quiche.quiche_conn_source_id(conn, &source_id_ptr, &source_id_len);
        if (source_id_ptr == null) return null;
        return CidKey.init(source_id_ptr[0..source_id_len]);
    }

    pub fn registerInitialSourceIds(self: *ConnectionActor) Allocator.Error!usize {
        const conn = self.conn orelse return 0;
        var added: usize = 0;

        var source_id_ptr: [*c]const u8 = null;
        var source_id_len: usize = 0;
        quiche.quiche_conn_source_id(conn, &source_id_ptr, &source_id_len);
        if (source_id_ptr != null and source_id_len == local_cid_len) {
            if (try self.registerCid(source_id_ptr[0..source_id_len])) added += 1;
        }

        const iter = quiche.quiche_conn_source_ids(conn) orelse return added;
        defer quiche.quiche_connection_id_iter_free(iter);

        var cid_ptr: [*c]const u8 = null;
        var cid_len: usize = 0;
        while (quiche.quiche_connection_id_iter_next(iter, &cid_ptr, &cid_len)) {
            if (cid_ptr == null or cid_len != local_cid_len) continue;
            if (try self.registerCid(cid_ptr[0..cid_len])) added += 1;
        }
        return added;
    }

    const CidRefreshError = error{ ConnectionClosed, QuicheCidFailed } || Allocator.Error;

    fn refreshSourceCids(self: *ConnectionActor) CidRefreshError!usize {
        const conn = self.conn orelse return error.ConnectionClosed;
        const transport = self.transport orelse return 0;
        const existing_cid = currentSourceCidKey(conn) orelse return error.QuicheCidFailed;
        var issued: usize = 0;
        while (quiche.quiche_conn_scids_left(conn) > 0) {
            const cid = cid_gen.randomLocalCid() catch return error.QuicheCidFailed;
            const reset_token = cid_gen.randomResetToken() catch return error.QuicheCidFailed;
            const new_cid = CidKey.init(&cid) orelse return error.QuicheCidFailed;
            var seq: u64 = 0;
            const rc = quiche.quiche_conn_new_scid(conn, &cid, cid.len, &reset_token, false, &seq);
            if (rc < 0) return error.QuicheCidFailed;
            if (try self.registerCid(&cid)) issued += 1;
            const queued = try transport.route_updates.sender().trySend(transport.io, .{
                .map_cid = .{ .existing_cid = existing_cid, .new_cid = new_cid },
            });
            if (!queued) {
                self.noteRouteCommandFailure(transport.io);
                return error.QuicheCidFailed;
            }
        }
        _ = self.drainRetiredSourceCids();
        return issued;
    }

    fn drainRetiredSourceCids(self: *ConnectionActor) usize {
        const conn = self.conn orelse return 0;
        var removed: usize = 0;
        var cid_ptr: [*c]const u8 = null;
        var cid_len: usize = 0;
        while (quiche.quiche_conn_retired_scid_next(conn, &cid_ptr, &cid_len)) {
            if (cid_ptr == null or cid_len != local_cid_len) continue;
            if (self.unregisterCid(cid_ptr[0..cid_len])) removed += 1;
        }
        return removed;
    }

    fn drainPathEvents(self: *ConnectionActor) usize {
        const conn = self.conn orelse return 0;
        const count = drainQuichePathEvents(conn);
        self.stats_snapshot.path_events_drained += @intCast(count);
        return count;
    }

    // ----- post-recv batch ----------------------------------------------

    /// Drain everything that may have become observable after a batch of
    /// `quiche_conn_recv` calls: queued path events, freshly-issued source
    /// CIDs that need router registration, newly-readable streams,
    /// inbound stream bytes, and inbound datagrams.
    fn postRecvBatch(self: *ConnectionActor, conn: *quiche.quiche_conn, transport: *const NetworkTransport) NetworkError!bool {
        _ = self.drainPathEvents();
        _ = self.refreshSourceCids() catch return error.QuicheRecvFailed;
        self.queueReadableStreams(conn, transport.io) catch return error.QuicheRecvFailed;
        const stream_work_remains = try self.drainReadableStreamData(conn, transport.io);
        _ = self.drainInboundDatagrams() catch return error.QuicheRecvFailed;
        return stream_work_remains;
    }

    const DrainDatagramError = error{ ConnectionClosed, DgramNotEnabled };

    fn drainInboundDatagrams(self: *ConnectionActor) DrainDatagramError!usize {
        const conn = self.conn orelse return error.ConnectionClosed;
        const pool = self.shared.datagram_pool orelse return error.DgramNotEnabled;
        const dgram_queue = self.shared.datagram_queue orelse return error.DgramNotEnabled;
        var drained: usize = 0;
        var discard: [2048]u8 = undefined;
        const io = self.io;

        while (true) {
            const len = quiche.quiche_conn_dgram_recv_front_len(conn);
            if (len == quiche.QUICHE_ERR_DONE) return drained;
            if (len < 0) {
                if (drained > 0) return drained;
                return error.DgramNotEnabled;
            }

            const payload_len: usize = @intCast(len);
            if (payload_len > pool.slotSize()) {
                const read = quiche.quiche_conn_dgram_recv(conn, &discard, @min(payload_len, discard.len));
                if (read < 0 and read != quiche.QUICHE_ERR_BUFFER_TOO_SHORT) return error.ConnectionClosed;
                self.stats_snapshot.datagrams_dropped += 1;
                self.stats_snapshot.datagrams_oversized += 1;
                drained += 1;
                continue;
            }

            const dgram = pool.acquire(payload_len) orelse {
                const read_len = @min(payload_len, discard.len);
                const read = quiche.quiche_conn_dgram_recv(conn, &discard, read_len);
                if (read < 0 and read != quiche.QUICHE_ERR_BUFFER_TOO_SHORT) return error.ConnectionClosed;
                self.stats_snapshot.datagrams_dropped += 1;
                self.stats_snapshot.datagrams_pool_full += 1;
                drained += 1;
                continue;
            };
            errdefer dgram.release();

            const dgram_buf = dgram.mutableBytes();
            const read = quiche.quiche_conn_dgram_recv(conn, dgram_buf.ptr, dgram_buf.len);
            if (read < 0) return error.ConnectionClosed;
            dgram.setLen(@intCast(read));

            if (!dgram_queue.sender().trySend(io, dgram)) {
                self.stats_snapshot.datagrams_dropped += 1;
                self.stats_snapshot.datagrams_queue_full += 1;
            }
            drained += 1;
        }
    }

    // ----- command processing -------------------------------------------

    fn handleControlMessages(self: *ConnectionActor) usize {
        const conn = self.conn orelse return 0;
        const io = self.io;
        var handled: usize = 0;
        while (true) {
            var cmd_buf: [1]Command = undefined;
            const n = self.shared.inbox.getUncancelable(io, &cmd_buf, 0) catch break;
            if (n == 0) break;
            const cmd = cmd_buf[0];
            handled += 1;
            switch (cmd) {
                .open_stream => |reply| reply.complete(io, self.openStreamLocal(conn)),
                .close_stream => |args| {
                    self.handleCloseStream(conn, args.stream_id, io) catch |err| {
                        self.noteStreamCloseError(io, err);
                    };
                },
                .close_read_stream => |args| {
                    self.handleCloseReadStream(conn, args.stream_id);
                    args.reply.complete(io);
                },
                .close_write_stream => |args| {
                    self.handleCloseWriteStream(conn, args.stream_id, args.reply, io) catch |err| self.noteStreamCloseError(io, err);
                },
                .reset_stream => |args| {
                    self.handleResetStream(conn, args.stream_id, args.code);
                    args.reply.complete(io);
                },
                .drop_stream => |stream_id| {
                    self.handleResetStream(conn, stream_id, 0);
                },
                .send_datagram => |args| {
                    args.reply.complete(io, self.handleSendDatagram(conn, args.payload));
                },
                .close_connection => |args| {
                    args.reply.complete(io, self.handleCloseConnection(conn, args.code, args.reason[0..args.reason_len], io));
                },
                .shutdown => self.shutdown_requested.store(true, .release),
            }
        }
        return handled;
    }

    fn handleCloseConnection(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        code: u64,
        reason: []const u8,
        io: std.Io,
    ) commands.CloseConnectionError!void {
        if (self.shared.isClosed()) return error.AlreadyClosed;
        _ = quiche.quiche_conn_close(conn, true, code, reason.ptr, reason.len);
        self.lifecycle.closing(.graceful);
        self.stats_snapshot.close_reason = .application_close_requested;
        self.stats_snapshot.local_close_is_app = true;
        self.stats_snapshot.local_close_error_code = code;
        self.write.reset();
        if (self.transport) |transport| {
            _ = self.flushScheduled(conn, &transport, std.math.maxInt(usize)) catch |err| blk: {
                self.noteCloseFlushError(transport.io, err);
                break :blk false;
            };
            self.refreshStats(transport.io);
        } else {
            self.refreshStats(io);
        }
        self.shared.markClosed();
    }

    fn handleSendDatagram(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        payload: []const u8,
    ) commands.SendDatagramError!void {
        if (self.shared.isClosed()) return error.ConnectionClosed;
        const max_len = quiche.quiche_conn_dgram_max_writable_len(conn);
        if (max_len < 0) return error.DgramNotEnabled;
        if (payload.len > @as(usize, @intCast(max_len))) return error.DatagramTooLarge;

        const rc = quiche.quiche_conn_dgram_send(conn, payload.ptr, payload.len);
        if (rc == quiche.QUICHE_ERR_DONE) return error.SendQueueFull;
        if (rc == quiche.QUICHE_ERR_BUFFER_TOO_SHORT) return error.DatagramTooLarge;
        if (rc < 0) return error.ConnectionClosed;
        if (self.transport) |transport| self.refreshStats(transport.io);
    }

    /// Graceful bidi close. Request STOP_SENDING(0) on the read side, then
    /// keep the record alive until buffered outbound bytes drain and FIN is
    /// accepted by quiche.
    fn handleCloseStream(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        stream_id: u64,
        io: std.Io,
    ) NetworkError!void {
        const record = self.streams.get(stream_id) orelse return;
        _ = quiche.quiche_conn_stream_shutdown(conn, stream_id, @intCast(quiche.QUICHE_SHUTDOWN_READ), 0);
        try self.requestStreamFin(conn, record, null, io);
        self.collectFinishedStream(conn, stream_id);
    }

    fn handleCloseReadStream(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        stream_id: u64,
    ) void {
        if (self.streams.get(stream_id)) |record| record.shared.markReadShutdownLocal();
        _ = quiche.quiche_conn_stream_shutdown(conn, stream_id, @intCast(quiche.QUICHE_SHUTDOWN_READ), 0);
    }

    /// Drain buffered bytes and send FIN. Reply fires only when FIN has been
    /// accepted by quiche; if flow control blocks us, the reply stays attached
    /// to the record and is completed by a later outbound drain.
    fn handleCloseWriteStream(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        stream_id: u64,
        reply: *commands.VoidReply,
        io: std.Io,
    ) NetworkError!void {
        const record = self.streams.get(stream_id) orelse {
            reply.complete(io);
            return;
        };
        try self.requestStreamFin(conn, record, reply, io);
    }

    fn requestStreamFin(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        record: *StreamRecord,
        reply: ?*commands.VoidReply,
        io: std.Io,
    ) NetworkError!void {
        record.requestFin(reply, io);
        if (record.fin_sent or record.shared.isOutboundResetByPeer()) return;
        errdefer if (reply != null) record.detachCloseWriteWaiter(io);

        _ = try self.drainOneStreamOutbound(conn, record, io);
        if (record.needsOutboundDrain(io)) {
            self.markStreamOutboundReady(record) catch {
                self.force_outbound_stream_scan = true;
                self.stats_snapshot.outbound_ready_queue_drops += 1;
            };
        }
    }

    fn handleResetStream(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        stream_id: u64,
        code: u64,
    ) void {
        _ = quiche.quiche_conn_stream_shutdown(conn, stream_id, @intCast(quiche.QUICHE_SHUTDOWN_READ), code);
        _ = quiche.quiche_conn_stream_shutdown(conn, stream_id, @intCast(quiche.QUICHE_SHUTDOWN_WRITE), code);
        _ = self.removeStream(stream_id, true);
    }

    fn removeStream(self: *ConnectionActor, stream_id: u64, close_shared: bool) bool {
        const removed = self.streams.fetchRemove(stream_id) orelse return false;
        const record = removed.value;
        self.releasePeerStreamCreditReservation(record);
        self.removeStreamOutboundReady(record);
        record.detachCloseWriteWaiter(self.io);
        if (close_shared) {
            record.shared.markClosedLocal();
        } else {
            record.shared.markFinishedByActor();
        }
        record.deinit(self.allocator);
        self.allocator.destroy(record);
        self.stats_snapshot.open_streams -|= 1;
        return true;
    }

    // ----- stream open / accept ------------------------------------------

    fn openStreamLocal(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
    ) commands.OpenStreamError!*StreamHandle {
        if (self.shared.isClosed()) return error.ConnectionClosed;
        const peer_streams_left: u64 = @intCast(quiche.quiche_conn_peer_streams_left_bidi(conn));
        if (self.reserved_peer_bidi_streams >= peer_streams_left) return error.StreamLimitReached;

        const stream_id = self.next_bidi_stream_id;
        const is_server: u64 = @intFromBool(quiche.quiche_conn_is_server(conn));
        std.debug.assert(stream_id & 0x1 == is_server);
        std.debug.assert(stream_id & 0x2 == 0);

        const stream_state = try StreamShared.create(self.allocator, self.io, self.shared, .{
            .stream_id = stream_id,
            .inbound_queue_bytes = self.stream_inbound_queue_bytes,
            .outbound_queue_bytes = self.stream_outbound_queue_bytes,
        });
        // SharedState.create returns refs=1 (the handle ref). Record.init
        // retains for itself, so each side owns its ref on the success path.
        errdefer stream_state.release();

        const record = try self.allocator.create(StreamRecord);
        var record_initialized = false;
        errdefer if (!record_initialized) self.allocator.destroy(record);
        record.* = try StreamRecord.init(self.allocator, stream_state, self.stream_outbound_queue_bytes);
        record.peer_stream_credit_reserved = true;
        record_initialized = true;
        errdefer {
            record.deinit(self.allocator);
            self.allocator.destroy(record);
        }

        try self.streams.put(stream_id, record);
        errdefer _ = self.streams.remove(stream_id);

        const handle = try stream_handle.create(self.allocator, stream_state);
        // stream_handle.create does NOT retain — the create call above gave
        // us refs=2 (one for handle, one for record). Successful allocation
        // here transfers handle-side ownership to the returned *Stream.
        self.reserved_peer_bidi_streams += 1;
        self.next_bidi_stream_id += 4;
        self.stats_snapshot.opened_streams_local += 1;
        self.stats_snapshot.open_streams += 1;
        if (self.transport) |transport| {
            self.stats_snapshot.last_updated_mono_ns = io_time.monotonicNs(transport.io);
        }
        return handle;
    }

    fn createRemoteStream(self: *ConnectionActor, stream_id: u64) Allocator.Error!*StreamHandle {
        const stream_state = try StreamShared.create(self.allocator, self.io, self.shared, .{
            .stream_id = stream_id,
            .inbound_queue_bytes = self.stream_inbound_queue_bytes,
            .outbound_queue_bytes = self.stream_outbound_queue_bytes,
        });
        errdefer stream_state.release();

        const record = try self.allocator.create(StreamRecord);
        var record_initialized = false;
        errdefer if (!record_initialized) self.allocator.destroy(record);
        record.* = try StreamRecord.init(self.allocator, stream_state, self.stream_outbound_queue_bytes);
        record_initialized = true;
        errdefer {
            record.deinit(self.allocator);
            self.allocator.destroy(record);
        }

        try self.streams.put(stream_id, record);
        errdefer _ = self.streams.remove(stream_id);

        const handle = try stream_handle.create(self.allocator, stream_state);
        self.stats_snapshot.opened_streams_remote += 1;
        self.stats_snapshot.open_streams += 1;
        if (self.transport) |transport| {
            self.stats_snapshot.last_updated_mono_ns = io_time.monotonicNs(transport.io);
        }
        return handle;
    }

    // ----- stream-side outbound / inbound drains -------------------------

    const OutboundDrainResult = enum { idle, blocked, work_remains };

    fn markStreamOutboundReady(self: *ConnectionActor, record: *StreamRecord) Allocator.Error!void {
        if (self.shared.isClosed()) return;
        if (!record.needsOutboundDrain(self.io)) return;
        if (!record.markOutboundReadyQueued()) return;
        try self.ready_outbound_streams.append(self.allocator, record);
    }

    fn removeStreamOutboundReady(self: *ConnectionActor, record: *StreamRecord) void {
        if (!record.outboundReadyQueued() and self.ready_outbound_streams.items.len == 0) return;
        var write: usize = 0;
        for (self.ready_outbound_streams.items) |queued| {
            if (queued == record) continue;
            self.ready_outbound_streams.items[write] = queued;
            write += 1;
        }
        self.ready_outbound_streams.items.len = write;
        if (self.ready_outbound_head > write) self.ready_outbound_head = write;
        record.clearOutboundReadyQueued();
    }

    fn compactOutboundReadyQueue(self: *ConnectionActor) void {
        if (self.ready_outbound_head == 0) return;
        if (self.ready_outbound_head >= self.ready_outbound_streams.items.len) {
            self.ready_outbound_streams.clearRetainingCapacity();
            self.ready_outbound_head = 0;
            return;
        }
        const remaining = self.ready_outbound_streams.items.len - self.ready_outbound_head;
        std.mem.copyForwards(
            *StreamRecord,
            self.ready_outbound_streams.items[0..remaining],
            self.ready_outbound_streams.items[self.ready_outbound_head..],
        );
        self.ready_outbound_streams.items.len = remaining;
        self.ready_outbound_head = 0;
    }

    fn drainOutboundStreams(self: *ConnectionActor, conn: *quiche.quiche_conn, io: std.Io) NetworkError!bool {
        if (self.force_outbound_stream_scan) {
            self.force_outbound_stream_scan = false;
            for (self.ready_outbound_streams.items) |record| record.clearOutboundReadyQueued();
            self.ready_outbound_streams.clearRetainingCapacity();
            self.ready_outbound_head = 0;
            var work_remains = false;
            var stream_ids_buf: [256]u64 = undefined;
            var ids_len: usize = 0;
            var collect_pass = false;
            var iter = self.streams.valueIterator();
            while (iter.next()) |slot| {
                const record = slot.*;
                if (ids_len < stream_ids_buf.len) {
                    stream_ids_buf[ids_len] = record.streamId();
                    ids_len += 1;
                } else {
                    collect_pass = true;
                }
                const result = if (record.needsOutboundDrain(io))
                    try self.drainOneStreamOutbound(conn, record, io)
                else
                    OutboundDrainResult.idle;
                if (record.needsOutboundDrain(io)) {
                    work_remains = work_remains or result == .work_remains;
                    self.markStreamOutboundReady(record) catch {
                        self.force_outbound_stream_scan = true;
                        self.stats_snapshot.outbound_ready_queue_drops += 1;
                    };
                }
            }
            if (!collect_pass) {
                for (stream_ids_buf[0..ids_len]) |stream_id| self.collectFinishedStream(conn, stream_id);
            }
            return work_remains;
        }

        const end = self.ready_outbound_streams.items.len;
        var return_work_remains = false;
        while (self.ready_outbound_head < end) : (self.ready_outbound_head += 1) {
            const record = self.ready_outbound_streams.items[self.ready_outbound_head];
            record.clearOutboundReadyQueued();
            if (record.shared.isClosed() and !record.needsOutboundDrain(io)) continue;
            const stream_id = record.streamId();
            const result = try self.drainOneStreamOutbound(conn, record, io);
            if (record.needsOutboundDrain(io)) {
                self.markStreamOutboundReady(record) catch {
                    self.force_outbound_stream_scan = true;
                    self.stats_snapshot.outbound_ready_queue_drops += 1;
                };
                if (result == .work_remains) return_work_remains = true;
            }
            self.collectFinishedStream(conn, stream_id);
        }
        self.compactOutboundReadyQueue();
        return return_work_remains;
    }

    fn drainOneStreamOutbound(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        record: *StreamRecord,
        io: std.Io,
    ) NetworkError!OutboundDrainResult {
        const max_bytes = if (self.stream_outbound_quantum_bytes == 0) std.math.maxInt(usize) else self.stream_outbound_quantum_bytes;
        var sent_bytes: usize = 0;

        while (sent_bytes < max_bytes) {
            if (record.pendingOutboundSlice().len == 0 and !record.refillPendingOutbound(io)) {
                return try self.sendFinIfReady(conn, record, io);
            }

            var err_code: u64 = 0;
            const pending_full = record.pendingOutboundSlice();
            const pending = pending_full[0..@min(pending_full.len, max_bytes - sent_bytes)];
            const rc = quiche.quiche_conn_stream_send(conn, record.streamId(), pending.ptr, pending.len, false, &err_code);
            if (rc == quiche.QUICHE_ERR_DONE) return .blocked;
            if (rc == quiche.QUICHE_ERR_STREAM_STOPPED or rc == quiche.QUICHE_ERR_STREAM_RESET) {
                self.markStreamWriteStoppedByPeer(record);
                return .idle;
            }
            if (rc < 0) return error.QuicheSendFailed;
            if (rc == 0) return .blocked;

            const n: usize = @intCast(rc);
            self.releasePeerStreamCreditReservation(record);
            record.advancePendingOutbound(n);
            sent_bytes += n;
            self.stats_snapshot.last_updated_mono_ns = io_time.monotonicNs(io);
        }
        return if (record.needsOutboundDrain(io)) .work_remains else .idle;
    }

    fn markStreamWriteStoppedByPeer(self: *ConnectionActor, record: *StreamRecord) void {
        self.releasePeerStreamCreditReservation(record);
        record.markWriteStoppedByPeer(self.io);
    }

    fn sendFinIfReady(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        record: *StreamRecord,
        io: std.Io,
    ) NetworkError!OutboundDrainResult {
        if (!record.finReady(io)) return .idle;
        var err_code: u64 = 0;
        const empty: [0]u8 = .{};
        const rc = quiche.quiche_conn_stream_send(conn, record.streamId(), empty[0..].ptr, 0, true, &err_code);
        if (rc == quiche.QUICHE_ERR_DONE) return .blocked;
        if (rc == quiche.QUICHE_ERR_STREAM_STOPPED or rc == quiche.QUICHE_ERR_STREAM_RESET) {
            self.markStreamWriteStoppedByPeer(record);
            return .idle;
        }
        if (rc < 0) return error.QuicheSendFailed;
        self.releasePeerStreamCreditReservation(record);
        record.markFinSent(io);
        return .idle;
    }

    fn drainOneStreamInbound(
        self: *ConnectionActor,
        conn: *quiche.quiche_conn,
        record: *StreamRecord,
        io: std.Io,
    ) NetworkError!bool {
        const shared_state = record.shared;
        var inbound_queue = if (shared_state.inbound_queue) |*q| q else return false;

        const max_bytes = if (self.stream_inbound_quantum_bytes == 0) std.math.maxInt(usize) else self.stream_inbound_quantum_bytes;
        var read_bytes: usize = 0;
        var scratch: [16 * 1024]u8 = undefined;
        while (read_bytes < max_bytes) {
            const capacity = @min(inbound_queue.available(io), scratch.len);
            if (capacity == 0) {
                self.stats_snapshot.inbound_stream_queue_full += 1;
                return false;
            }

            var fin = false;
            var err_code: u64 = 0;
            const read_len = @min(capacity, max_bytes - read_bytes);
            const rc = quiche.quiche_conn_stream_recv(conn, shared_state.stream_id, scratch[0..read_len].ptr, read_len, &fin, &err_code);
            if (rc == quiche.QUICHE_ERR_DONE) return false;
            if (rc == quiche.QUICHE_ERR_STREAM_RESET) {
                shared_state.markInboundResetByPeer();
                return false;
            }
            if (rc < 0) return error.QuicheRecvFailed;

            if (rc > 0) {
                if (!inbound_queue.tryPutAll(io, scratch[0..@intCast(rc)])) {
                    self.stats_snapshot.inbound_stream_queue_full += 1;
                    if (fin) shared_state.closeInbound();
                    return false;
                }
            }
            if (fin) {
                shared_state.closeInbound();
                return false;
            }
            if (rc == 0) return false;
            read_bytes += @intCast(rc);
        }
        return true;
    }

    fn drainReadableStreamData(self: *ConnectionActor, conn: *quiche.quiche_conn, io: std.Io) NetworkError!bool {
        const iter = quiche.quiche_conn_readable(conn) orelse return false;
        defer quiche.quiche_stream_iter_free(iter);

        var work_remains = false;
        var stream_id: u64 = 0;
        while (quiche.quiche_stream_iter_next(iter, &stream_id)) {
            const record = self.streams.get(stream_id) orelse continue;
            work_remains = (try self.drainOneStreamInbound(conn, record, io)) or work_remains;
            self.collectFinishedStream(conn, stream_id);
        }
        return work_remains;
    }

    fn collectFinishedStream(self: *ConnectionActor, conn: *quiche.quiche_conn, stream_id: u64) void {
        if (!quiche.quiche_conn_stream_finished(conn, stream_id)) return;
        if (self.removeStream(stream_id, false)) {
            self.stats_snapshot.streams_collected_after_fin += 1;
        }
    }

    /// Walk quiche's readable iterator looking for streams the peer has
    /// opened that we haven't seen yet, and push them to the accept queue.
    /// Overflow goes to a bounded `pending_accept_streams` buffer so the
    /// peer is rate-limited via QUIC's own stream-credit mechanism.
    fn queueReadableStreams(self: *ConnectionActor, conn: *quiche.quiche_conn, io: std.Io) Allocator.Error!void {
        self.flushPendingAcceptStreams(io);

        const iter = quiche.quiche_conn_readable(conn) orelse return;
        defer quiche.quiche_stream_iter_free(iter);

        var stream_id: u64 = 0;
        while (quiche.quiche_stream_iter_next(iter, &stream_id)) {
            if (self.streams.contains(stream_id)) continue;

            const handle = try self.createRemoteStream(stream_id);
            if (self.shared.tryPutAcceptedStream(io, handle)) {
                self.stats_snapshot.pending_accept_streams += 1;
                continue;
            }

            // Accept queue full: spill to bounded overflow.
            const pending_live = self.pending_accept_streams.items.len -| self.pending_accept_head;
            if (self.pending_accept_cap > 0 and pending_live >= self.pending_accept_cap) {
                _ = quiche.quiche_conn_stream_shutdown(conn, stream_id, @intCast(quiche.QUICHE_SHUTDOWN_READ), 0);
                _ = quiche.quiche_conn_stream_shutdown(conn, stream_id, @intCast(quiche.QUICHE_SHUTDOWN_WRITE), 0);
                _ = self.removeStream(stream_id, true);
                self.stats_snapshot.opened_streams_remote -|= 1;
                handle.deinit();
                self.stats_snapshot.pending_accept_overflow += 1;
                continue;
            }
            self.pending_accept_streams.append(self.allocator, handle) catch |err| {
                _ = self.removeStream(stream_id, true);
                handle.deinit();
                self.stats_snapshot.opened_streams_remote -|= 1;
                return err;
            };
            self.stats_snapshot.pending_accept_streams += 1;
            self.stats_snapshot.inbound_stream_queue_full += 1;
            if (self.transport) |transport| self.stats_snapshot.last_updated_mono_ns = io_time.monotonicNs(transport.io);
        }
    }

    fn flushPendingAcceptStreams(self: *ConnectionActor, io: std.Io) void {
        while (self.pending_accept_head < self.pending_accept_streams.items.len) {
            const handle = self.pending_accept_streams.items[self.pending_accept_head];
            if (!self.shared.tryPutAcceptedStream(io, handle)) return;
            self.pending_accept_head += 1;
        }
        self.compactPendingAcceptStreams();
    }

    fn compactPendingAcceptStreams(self: *ConnectionActor) void {
        if (self.pending_accept_head == 0) return;
        if (self.pending_accept_head >= self.pending_accept_streams.items.len) {
            self.pending_accept_streams.clearRetainingCapacity();
            self.pending_accept_head = 0;
            return;
        }
        if (self.pending_accept_head < 64 and self.pending_accept_head * 2 < self.pending_accept_streams.items.len) return;

        const remaining = self.pending_accept_streams.items.len - self.pending_accept_head;
        std.mem.copyForwards(
            *StreamHandle,
            self.pending_accept_streams.items[0..remaining],
            self.pending_accept_streams.items[self.pending_accept_head..],
        );
        self.pending_accept_streams.items.len = remaining;
        self.pending_accept_head = 0;
    }
};

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Hand a single inbound packet (or GRO super-packet) to quiche. Side
/// effects beyond `quiche_conn_recv` — path event drain, source-CID
/// refresh, readable-stream discovery, datagram delivery — are deferred to
/// `postRecvBatch` (Cut 3) so they run once per actor tick instead of
/// once per packet.
pub fn recvRoutedPacket(conn: *quiche.quiche_conn, packet: *RoutedPacket) NetworkError!void {
    var peer_storage: address.PosixAddress = undefined;
    var local_storage: address.PosixAddress = undefined;
    const peer_len = address.addressToPosix(&packet.from, &peer_storage);
    const local_len = address.addressToPosix(&packet.to, &local_storage);
    var recv_info = quiche.quiche_recv_info{
        .from = @ptrCast(&peer_storage.any),
        .from_len = peer_len,
        .to = @ptrCast(&local_storage.any),
        .to_len = local_len,
    };

    const data = packet.bytes();
    if (packet.gro_segment_size) |segment_size| {
        if (segment_size == 0) return error.QuicheRecvFailed;
        var offset: usize = 0;
        while (offset < data.len) {
            const end = @min(offset + @as(usize, segment_size), data.len);
            const chunk = data[offset..end];
            const rc = quiche.quiche_conn_recv(conn, chunk.ptr, chunk.len, &recv_info);
            if (rc < 0 and rc != quiche.QUICHE_ERR_DONE) return error.QuicheRecvFailed;
            offset = end;
        }
    } else {
        const rc = quiche.quiche_conn_recv(conn, data.ptr, data.len, &recv_info);
        if (rc < 0 and rc != quiche.QUICHE_ERR_DONE) return error.QuicheRecvFailed;
    }
}

pub fn sendInfoReleaseNs(send_info: *const quiche.quiche_send_info) ?i96 {
    const sec: i96 = @intCast(send_info.at.tv_sec);
    const nsec: i96 = @intCast(send_info.at.tv_nsec);
    if (sec <= 0 and nsec <= 0) return null;
    return sec * std.time.ns_per_s + nsec;
}

fn releaseTimeDue(io: std.Io, release_at_ns: i96) bool {
    return release_at_ns <= io_time.monotonicNsSigned(io) + pacing_release_slack_ns;
}

fn deadlineFromNs(ns: i96) std.Io.Timeout {
    return .{ .deadline = .{ .raw = .fromNanoseconds(ns), .clock = .awake } };
}

fn minTimeout(io: std.Io, lhs: std.Io.Timeout, rhs: std.Io.Timeout) std.Io.Timeout {
    const lhs_ns = io_time.timeoutDeadlineNs(io, lhs) orelse return rhs;
    const rhs_ns = io_time.timeoutDeadlineNs(io, rhs) orelse return lhs;
    return deadlineFromNs(@min(lhs_ns, rhs_ns));
}

fn quicheTimeout(conn: *quiche.quiche_conn) std.Io.Timeout {
    const timeout_ns = quiche.quiche_conn_timeout_as_nanos(conn);
    if (timeout_ns == std.math.maxInt(u64)) return .none;
    return io_time.receiveTimeout(@intCast(timeout_ns));
}

fn quicheTimeoutExpired(conn: *quiche.quiche_conn) bool {
    return quiche.quiche_conn_timeout_as_nanos(conn) == 0;
}

fn drainQuichePathEvents(conn: *quiche.quiche_conn) usize {
    var count: usize = 0;
    while (quiche.quiche_conn_path_event_next(conn)) |event| {
        quiche.quiche_path_event_free(event);
        count += 1;
    }
    return count;
}

fn refreshCloseErrorStats(conn: *quiche.quiche_conn, refreshed: *ConnectionStats) void {
    var is_app = false;
    var error_code: u64 = 0;
    var reason: [*c]const u8 = null;
    var reason_len: usize = 0;

    if (quiche.quiche_conn_peer_error(conn, &is_app, &error_code, &reason, &reason_len)) {
        refreshed.peer_close_is_app = is_app;
        refreshed.peer_close_error_code = error_code;
        if (refreshed.close_reason == .none or refreshed.close_reason == .closed) refreshed.close_reason = .peer_error;
    }

    is_app = false;
    error_code = 0;
    reason = null;
    reason_len = 0;
    if (quiche.quiche_conn_local_error(conn, &is_app, &error_code, &reason, &reason_len)) {
        refreshed.local_close_is_app = is_app;
        refreshed.local_close_error_code = error_code;
        if (refreshed.close_reason == .none or refreshed.close_reason == .closed) refreshed.close_reason = .local_error;
    }

    if (quiche.quiche_conn_is_timed_out(conn)) refreshed.close_reason = .idle_timeout;
    if (quiche.quiche_conn_is_draining(conn) and refreshed.close_reason == .none) refreshed.close_reason = .draining;
    if (quiche.quiche_conn_is_closed(conn) and refreshed.close_reason == .none) refreshed.close_reason = .closed;
}

test "ConnectionActor methods compile" {
    _ = ConnectionActor.create;
    _ = ConnectionActor.spawn;
    _ = ConnectionActor.flushScheduled;
    _ = ConnectionActor.handleControlMessages;
    _ = ConnectionActor.handleCloseConnection;
    _ = ConnectionActor.handleSendDatagram;
    _ = ConnectionActor.handleCloseStream;
    _ = ConnectionActor.handleCloseReadStream;
    _ = ConnectionActor.handleCloseWriteStream;
    _ = ConnectionActor.handleResetStream;
    _ = ConnectionActor.removeStream;
    _ = ConnectionActor.openStreamLocal;
    _ = ConnectionActor.createRemoteStream;
    _ = ConnectionActor.markStreamOutboundReady;
    _ = ConnectionActor.removeStreamOutboundReady;
    _ = ConnectionActor.compactOutboundReadyQueue;
    _ = ConnectionActor.drainOutboundStreams;
    _ = ConnectionActor.drainOneStreamOutbound;
    _ = ConnectionActor.drainOneStreamInbound;
    _ = ConnectionActor.drainReadableStreamData;
    _ = ConnectionActor.queueReadableStreams;
    _ = ConnectionActor.flushPendingAcceptStreams;
    _ = ConnectionActor.compactPendingAcceptStreams;
    _ = ConnectionActor.collectFinishedStream;
    _ = ConnectionActor.markStreamWriteStoppedByPeer;
    _ = ConnectionActor.sendFinIfReady;
    _ = ConnectionActor.postRecvBatch;
    _ = ConnectionActor.refreshSourceCids;
    _ = ConnectionActor.drainPathEvents;
    _ = ConnectionActor.drainInboundDatagrams;
    _ = ConnectionActor.refreshStats;
    _ = ConnectionActor.publishStats;
    _ = ConnectionActor.updateHandshake;
    _ = ConnectionActor.completeHandshake;
    _ = ConnectionActor.registerInitialSourceIds;
    _ = ConnectionActor.initPacketInbox;
    _ = ConnectionActor.attachIncomingPacketWaitSet;
    _ = ConnectionActor.enqueueInboundPacket;
    _ = ConnectionActor.destroySpawned;
}

test "destroyPrespawn releases prespawn actor ownership" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const shared = try SharedState.create(allocator, io, .{
        .inbox_capacity = 1,
        .accept_capacity = 1,
    });

    const actor = try allocator.create(ConnectionActor);
    actor.* = .{
        .allocator = allocator,
        .io = io,
        .shared = shared,
        .streams = std.AutoHashMap(u64, *StreamRecord).init(allocator),
    };

    actor.destroyPrespawn();
    shared.release();
}

test "free helpers compile" {
    _ = recvRoutedPacket;
    _ = sendInfoReleaseNs;
    _ = releaseTimeDue;
    _ = deadlineFromNs;
    _ = minTimeout;
    _ = quicheTimeout;
    _ = quicheTimeoutExpired;
    _ = drainQuichePathEvents;
    _ = refreshCloseErrorStats;
}

test "applyOutgoingControl respects source-control capabilities" {
    var destination = std.Io.net.IpAddress{ .ip4 = .loopback(9001) };
    var payload: [1]u8 = .{0};
    var message = std.Io.net.OutgoingMessage{
        .address = &destination,
        .data_ptr = payload[0..].ptr,
        .data_len = payload.len,
    };
    var control: [socket_control.send_control_buffer_len]u8 align(socket_control.control_buffer_align) = undefined;

    applyOutgoingControl(&message, control[0..], .{ .pktinfo_v4 = true }, .{
        .from = .{ .ip4 = .loopback(9000) },
        .to = destination,
    });

    if (builtin.os.tag == .linux) {
        try std.testing.expect(message.control.len > 0);
    } else {
        try std.testing.expectEqual(@as(usize, 0), message.control.len);
    }
}
