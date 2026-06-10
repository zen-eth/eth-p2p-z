//! P0 QUIC loopback benchmark (`zig build bench-quic`).
//!
//! Two real endpoints (full TLS handshake) over loopback UDP, one connection,
//! one stream: the client blasts a fixed byte volume, the server drains it.
//! Reported per scenario: stream goodput (Gbit/s), the packet rate the
//! endpoint router actually sustained (from the live endpoint stats), and the
//! router's drop counters — the baseline the P2 (send batching/GSO) and P3
//! (recv-path) work must move.
//!
//! Two chunk sizes bound the shape: 64 KiB writes (bulk transfer, packets at
//! max size) and 1200 B writes (small-message traffic, one packet per write at
//! the QUIC layer's mercy). Run with -Doptimize=ReleaseFast. Runtime is zio
//! (production parity), executors = clamp(cpus, 2, 64).

const std = @import("std");
const zio = @import("zio");
const support = @import("quic/endpoint/test_support.zig");
const io_time = @import("quic/io/time.zig");

const TwoEndpoints = support.TwoEndpoints;
const AcceptCtx = support.AcceptCtx;

/// Wraps an allocator and tracks NET live heap bytes (allocs minus frees),
/// so the footprint scenario can attribute per-connection heap cost. Thread-
/// safe (atomics): zio executors allocate concurrently.
const ByteCounting = struct {
    parent: std.mem.Allocator,
    live: std.atomic.Value(isize) = .init(0),

    fn allocator(self: *ByteCounting) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    fn allocFn(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *ByteCounting = @ptrCast(@alignCast(ctx));
        const p = self.parent.vtable.alloc(self.parent.ptr, len, alignment, ret_addr) orelse return null;
        _ = self.live.fetchAdd(@intCast(len), .monotonic);
        return p;
    }

    fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *ByteCounting = @ptrCast(@alignCast(ctx));
        if (!self.parent.vtable.resize(self.parent.ptr, memory, alignment, new_len, ret_addr)) return false;
        _ = self.live.fetchAdd(@as(isize, @intCast(new_len)) - @as(isize, @intCast(memory.len)), .monotonic);
        return true;
    }

    fn remapFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *ByteCounting = @ptrCast(@alignCast(ctx));
        const p = self.parent.vtable.remap(self.parent.ptr, memory, alignment, new_len, ret_addr) orelse return null;
        _ = self.live.fetchAdd(@as(isize, @intCast(new_len)) - @as(isize, @intCast(memory.len)), .monotonic);
        return p;
    }

    fn freeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *ByteCounting = @ptrCast(@alignCast(ctx));
        self.parent.vtable.free(self.parent.ptr, memory, alignment, ret_addr);
        _ = self.live.fetchSub(@intCast(memory.len), .monotonic);
    }
};

fn maxRssBytes() usize {
    const ru = std.posix.getrusage(std.posix.rusage.SELF);
    // macOS reports bytes; Linux reports KiB.
    const raw: usize = @intCast(ru.maxrss);
    return if (@import("builtin").os.tag == .linux) raw * 1024 else raw;
}

const MultiAccept = struct {
    endpoint: *support.QuicEndpoint,
    conns: []?*support.Connection,
    err: ?anyerror = null,

    fn run(self: *MultiAccept) void {
        for (self.conns) |*slot| {
            slot.* = self.endpoint.accept() catch |err| {
                self.err = err;
                return;
            };
        }
    }
};

/// P5 footprint probe: open N idle connections and attribute the per-conn
/// cost two ways — net live HEAP bytes (via the counting allocator wrapping
/// both the endpoints AND the zio runtime) and peak-RSS growth (catches what
/// the allocator does not, e.g. mmap'd fiber stacks).
fn runFootprint(counting: *ByteCounting, io: std.Io, conn_count: usize) !void {
    const allocator = counting.allocator();
    var fixture = try TwoEndpoints.init(allocator, io, .{}, .{});
    defer fixture.deinit();
    const server_addr = try fixture.bindServerLoopback();
    _ = try fixture.bindClientLoopback();

    const server_conns = try allocator.alloc(?*support.Connection, conn_count);
    defer allocator.free(server_conns);
    @memset(server_conns, null);
    const client_conns = try allocator.alloc(?*support.Connection, conn_count);
    defer allocator.free(client_conns);
    @memset(client_conns, null);

    var acceptor = MultiAccept{ .endpoint = fixture.server, .conns = server_conns };
    var accept_future = try std.Io.concurrent(io, MultiAccept.run, .{&acceptor});

    // Baseline AFTER the endpoints are up, BEFORE any connection.
    io_time.ms(100).sleep(io) catch {};
    const heap_before = counting.live.load(.monotonic);
    const rss_before = maxRssBytes();

    for (client_conns) |*slot| {
        slot.* = try fixture.client.dial(server_addr, .{
            .timeout = support.receiveTimeout(support.default_handshake_timeout_ns),
        });
    }
    accept_future.await(io);
    if (acceptor.err) |err| return err;

    io_time.ms(200).sleep(io) catch {}; // settle: handshake fibers unwind
    const heap_after = counting.live.load(.monotonic);
    const rss_after = maxRssBytes();

    const pairs_f = @as(f64, @floatFromInt(conn_count));
    std.debug.print(
        "footprint {d} conns   heap {d:.1} KiB/conn-pair ({d:.1} MiB total)   peak-rss {d:.1} KiB/conn-pair ({d:.1} MiB total)\n",
        .{
            conn_count,
            @as(f64, @floatFromInt(heap_after - heap_before)) / 1024.0 / pairs_f,
            @as(f64, @floatFromInt(heap_after - heap_before)) / 1024.0 / 1024.0,
            @as(f64, @floatFromInt(rss_after -| rss_before)) / 1024.0 / pairs_f,
            @as(f64, @floatFromInt(rss_after -| rss_before)) / 1024.0 / 1024.0,
        },
    );

    for (client_conns) |slot| if (slot) |c| {
        c.close(io, 0, "bench done") catch {};
        c.deinit();
    };
    for (server_conns) |slot| if (slot) |c| c.deinit();
}

const bulk_total: usize = 256 * 1024 * 1024;
const bulk_chunk: usize = 64 * 1024;
const small_total: usize = 32 * 1024 * 1024;
const small_chunk: usize = 1200;

const ReadDrain = struct {
    stream: *support.Stream,
    io: std.Io,
    total: usize,
    err: ?anyerror = null,

    fn run(self: *ReadDrain) void {
        var buf: [64 * 1024]u8 = undefined;
        var got: usize = 0;
        while (got < self.total) {
            const want = @min(buf.len, self.total - got);
            self.stream.readAll(self.io, buf[0..want], .{}) catch |err| {
                self.err = err;
                return;
            };
            got += want;
        }
    }
};

fn runScenario(
    allocator: std.mem.Allocator,
    io: std.Io,
    name: []const u8,
    total: usize,
    chunk_len: usize,
) !void {
    var fixture = try TwoEndpoints.init(allocator, io, .{}, .{});
    defer fixture.deinit();
    const server_addr = try fixture.bindServerLoopback();
    _ = try fixture.bindClientLoopback();

    var accept_ctx = AcceptCtx{ .endpoint = fixture.server };
    var accept_future = try std.Io.concurrent(io, AcceptCtx.run, .{&accept_ctx});
    const client_conn = fixture.client.dial(server_addr, .{
        .timeout = support.receiveTimeout(support.default_handshake_timeout_ns),
    }) catch |err| {
        accept_future.cancel(io);
        accept_future.await(io);
        return err;
    };
    defer client_conn.deinit();
    accept_future.await(io);
    if (accept_ctx.err) |err| return err;
    const server_conn = accept_ctx.conn orelse return error.AcceptFailed;
    defer server_conn.deinit();

    const outbound = try client_conn.openStream(io);
    defer outbound.deinit();
    // First byte opens the stream server-side.
    try outbound.writeAll(io, "x", .{});
    const inbound = try server_conn.acceptStream(io, .{ .timeout = support.timeout_s(5) });
    defer inbound.deinit();
    var first: [1]u8 = undefined;
    try inbound.readAll(io, &first, .{});

    const chunk = try allocator.alloc(u8, chunk_len);
    defer allocator.free(chunk);
    @memset(chunk, 0x5a);

    const server_stats_before = fixture.server.stats();
    const client_stats_before = fixture.client.stats();

    var drain = ReadDrain{ .stream = inbound, .io = io, .total = total };
    var drain_future = try std.Io.concurrent(io, ReadDrain.run, .{&drain});

    const t0 = io_time.monotonicNs(io);
    var sent: usize = 0;
    while (sent < total) {
        const n = @min(chunk_len, total - sent);
        try outbound.writeAll(io, chunk[0..n], .{});
        sent += n;
    }
    drain_future.await(io);
    const t1 = io_time.monotonicNs(io);
    if (drain.err) |err| return err;

    const server_stats = fixture.server.stats();
    const client_stats = fixture.client.stats();

    const secs = @as(f64, @floatFromInt(t1 - t0)) / 1e9;
    const gbps = (@as(f64, @floatFromInt(total)) * 8.0) / 1e9 / secs;
    const srv_pkts = server_stats.router_packets_recv - server_stats_before.router_packets_recv;
    const cli_pkts = client_stats.router_packets_recv - client_stats_before.router_packets_recv;
    const srv_drops = server_stats.router_packet_drops - server_stats_before.router_packet_drops;
    const cli_drops = client_stats.router_packet_drops - client_stats_before.router_packet_drops;
    std.debug.print(
        "{s:<18} {d:>7.3} Gbit/s  {d:>6.2} s  rx-pps srv {d:>7.0} cli {d:>7.0}  drops srv {d} cli {d}\n",
        .{
            name,
            gbps,
            secs,
            @as(f64, @floatFromInt(srv_pkts)) / secs,
            @as(f64, @floatFromInt(cli_pkts)) / secs,
            srv_drops,
            cli_drops,
        },
    );

    outbound.close(io) catch {};
    inbound.close(io) catch {};
    client_conn.close(io, 0, "bench done") catch {};
}

pub fn main(init: std.process.Init) !void {
    var counting = ByteCounting{ .parent = init.gpa };
    const allocator = counting.allocator();
    const cpu_count = std.Thread.getCpuCount() catch 2;
    const executor_count: u8 = @intCast(std.math.clamp(cpu_count, 2, 64));
    const runtime = try zio.Runtime.init(allocator, .{ .executors = .exact(executor_count) });
    defer runtime.deinit();
    const io = runtime.io();

    std.debug.print("quic loopback bench: executors={d}\n\n", .{executor_count});
    // BENCH_FOOTPRINT_ONLY: run just the footprint probe. Peak-RSS is a
    // process-lifetime high-water mark, so the RSS column is only meaningful
    // in this mode (a prior throughput scenario inflates it past relevance).
    // In the default combined run the footprint goes LAST and only its
    // heap-live column is meaningful. (Running it FIRST is not an option:
    // 50 churned connection pairs reproducibly degrade the subsequent
    // loopback scenarios ~30x with packet drops — recorded as a lead in the
    // bench-bimodality investigation, see docs/benchmarks.)
    if (std.c.getenv("BENCH_FOOTPRINT_ONLY") != null) {
        try runFootprint(&counting, io, 50);
        return;
    }
    try runScenario(allocator, io, "bulk 64KiB chunks", bulk_total, bulk_chunk);
    try runScenario(allocator, io, "small 1200B chunks", small_total, small_chunk);
    try runFootprint(&counting, io, 50);
}
