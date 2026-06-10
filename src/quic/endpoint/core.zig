const std = @import("std");
const AtomicRc = @import("ref_count").AtomicRc;
const route_table_mod = @import("../router/route_table.zig");
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
    rc: AtomicRc = .{},
    /// The shared CID → packet-inbox routing table. Written directly by
    /// connection actors / the dialer / the server accept path, read by the
    /// router's recv fiber — see route_table.zig for the locking model. Lives
    /// here because every actor retains the core for its whole life, so a
    /// writer can never outlive the table.
    route_table: route_table_mod.RouteTable,
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
            .route_table = route_table_mod.RouteTable.init(allocator),
            .retry_tokens = try retry_token.Store.init(),
        };
        return core;
    }

    pub fn retain(core: *EndpointCore) void {
        core.rc.retainChecked();
    }

    pub fn release(core: *EndpointCore) void {
        if (!core.rc.releaseChecked()) return;

        const allocator = core.allocator;
        // Backstop: the recv loop's exit already cleared the table; this frees
        // any entry registered after that (e.g. a dial racing endpoint close).
        core.route_table.deinit(core.io);
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
