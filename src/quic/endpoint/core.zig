const std = @import("std");
const route_commands_mod = @import("../router/route_commands.zig");
const retry_token = @import("../router/retry_token.zig");

pub const EndpointStats = struct {
    connections_started: u64 = 0,
    connections_established: u64 = 0,
    connections_closed: u64 = 0,
    failed_handshakes: u64 = 0,
    accept_backlog_full: u64 = 0,
    router_packets_recv: u64 = 0,
    router_recv_errors: u64 = 0,
    router_payload_truncated: u64 = 0,
    router_control_truncated: u64 = 0,
    router_packets_dispatched: u64 = 0,
    router_packet_drops: u64 = 0,
    router_unknown_dcid_packets: u64 = 0,
    router_rejected_initial_packets: u64 = 0,
    router_version_negotiation_sent: u64 = 0,
    router_version_negotiation_send_failures: u64 = 0,
    router_retry_sent: u64 = 0,
    router_retry_send_failures: u64 = 0,
    router_retry_token_invalid: u64 = 0,
    router_retry_token_validated: u64 = 0,
    cid_map_entries: u64 = 0,
    cid_map_command_drops: u64 = 0,
    cid_map_unknown_existing: u64 = 0,
    active_actors: u64 = 0,
};

pub const EndpointCore = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    refs: std.atomic.Value(usize) = .init(1),
    route_commands: *route_commands_mod.Queue.State,
    retry_tokens: retry_token.Store,
    /// Live, lock-free stat counters. Best-effort observability: written with
    /// monotonic atomics from any executor (router / connection fibers) and read
    /// via `stats()`. Monotonic ordering suffices — these counters are not a
    /// synchronization mechanism, so a reader may observe a slightly stale value.
    stats_snapshot: EndpointStats = .{},

    pub const InitError = error{RandomFailed} || std.mem.Allocator.Error;

    pub fn init(allocator: std.mem.Allocator, io: std.Io) InitError!*EndpointCore {
        const core = try allocator.create(EndpointCore);
        errdefer allocator.destroy(core);
        core.* = .{
            .allocator = allocator,
            .io = io,
            .route_commands = try route_commands_mod.Queue.State.init(allocator, io),
            .retry_tokens = try retry_token.Store.init(),
        };
        return core;
    }

    pub fn retain(core: *EndpointCore) void {
        const previous = core.refs.fetchAdd(1, .acq_rel);
        std.debug.assert(previous > 0);
    }

    pub fn release(core: *EndpointCore) void {
        const previous = core.refs.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;

        const allocator = core.allocator;
        core.route_commands.close(core.io);
        core.route_commands.discardQueued(core.io);
        core.route_commands.release();
        allocator.destroy(core);
    }

    pub fn stats(core: *EndpointCore) EndpointStats {
        var snapshot: EndpointStats = .{};
        inline for (std.meta.fields(EndpointStats)) |f| {
            @field(snapshot, f.name) = @atomicLoad(u64, &@field(core.stats_snapshot, f.name), .monotonic);
        }
        return snapshot;
    }

    pub fn addStat(core: *EndpointCore, comptime field: []const u8, value: u64) void {
        _ = @atomicRmw(u64, &@field(core.stats_snapshot, field), .Add, value, .monotonic);
    }

    pub fn subStat(core: *EndpointCore, comptime field: []const u8, value: u64) void {
        // Saturating at zero, lock-free: CAS the floored difference so a stray
        // over-decrement cannot wrap a counter to ~2^64 (matches the old `-|=`).
        const ptr = &@field(core.stats_snapshot, field);
        var current = @atomicLoad(u64, ptr, .monotonic);
        while (true) {
            const next = current -| value;
            current = @cmpxchgWeak(u64, ptr, current, next, .monotonic, .monotonic) orelse break;
        }
    }

    pub fn setStat(core: *EndpointCore, comptime field: []const u8, value: u64) void {
        @atomicStore(u64, &@field(core.stats_snapshot, field), value, .monotonic);
    }
};
