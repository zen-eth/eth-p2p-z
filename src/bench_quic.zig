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
    const allocator = init.gpa;
    const cpu_count = std.Thread.getCpuCount() catch 2;
    const executor_count: u8 = @intCast(std.math.clamp(cpu_count, 2, 64));
    const runtime = try zio.Runtime.init(allocator, .{ .executors = .exact(executor_count) });
    defer runtime.deinit();
    const io = runtime.io();

    std.debug.print("quic loopback bench: executors={d}\n\n", .{executor_count});
    try runScenario(allocator, io, "bulk 64KiB chunks", bulk_total, bulk_chunk);
    try runScenario(allocator, io, "small 1200B chunks", small_total, small_chunk);
}
