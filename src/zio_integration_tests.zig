//! End-to-end multi-executor integration tests (Layer 2-6 net).
//!
//! Drives two real QUIC endpoints over a loopback UDP socket on a REAL
//! `zio.Runtime` with >=2 executors: handshake + bidirectional stream echo,
//! with the server `accept()` running on a concurrent fiber (worker executor)
//! rather than the OS thread the std.Io.Threaded unit tests are forced to use.
//!
//! This exercises the full multi-executor stack the upper-layer refactors touch
//! — router fiber, per-connection actors, stream byte queues, TLS handshake,
//! teardown — under genuine cross-executor scheduling (the path that starved
//! under the historical .exact(1) bug). It is the regression net for refactor
//! layers L2-L6 (docs/quiche-refactor-plan.md).
//!
//! Pulls BoringSSL (via the quiche dep), so it is a separate, slower-compiling
//! target from the Layer-0 zio-io-test. Run: `zig build zio-integ-test`.
//!
//! PLATFORM NOTE: these tests SKIP on macOS. The QUIC data path (handshake +
//! stream echo + per-connection teardown) works on the macOS kqueue backend, but
//! `endpoint.deinit()` -> `router.closeListener()` -> `router_future.cancel()`
//! LIVELOCKS there: zio's kqueue multi-executor backend does not wake/interrupt a
//! fiber blocked in a socket `receive` when its Future is cancelled, so the
//! cancelling fiber spins forever (verified by staged logging: both tests reach
//! the final connection-deinit, then hang in fixture.deinit). The identical
//! teardown path tears down cleanly on Linux (epoll / io_uring), where the QUIC
//! interop suite already exercises it. So the end-to-end net runs on Linux
//! (Docker / CI); the Layer-0 primitive net (zio-io-test) runs anywhere.
//! Tracked alongside E7 as a zio kqueue-backend upstream issue.

const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const support = @import("quic/endpoint/test_support.zig");

const testing = std.testing;
const AcceptCtx = support.AcceptCtx;
const TwoEndpoints = support.TwoEndpoints;
const closeStreamForTest = support.closeStreamForTest;
const receiveTimeout = support.receiveTimeout;
const default_handshake_timeout_ns = support.default_handshake_timeout_ns;

/// Linux-only: see the PLATFORM NOTE above. Returns `error.SkipZigTest` on macOS
/// (and any non-Linux host) so `zig build zio-integ-test` is safe to run locally.
fn requireLinux() !void {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
}

/// Spin a multi-executor zio runtime and run `root(io)` to completion on the
/// main executor; worker fibers (the server accept loop, the connection actors,
/// the router) run on the other executor(s). Propagates the root fiber's error.
fn runRoot(comptime executors: u8, comptime root: anytype) !void {
    const rt = try zio.Runtime.init(testing.allocator, .{ .executors = .exact(executors) });
    defer rt.deinit();
    var handle = try rt.spawn(root, .{rt.io()});
    defer handle.cancel();
    return handle.join();
}

/// Full loopback: dial + handshake (server accepts on a concurrent fiber),
/// then a bidirectional stream echo. Shared by the exact(2) and exact(4) tests.
fn loopbackHandshakeAndEcho(io: std.Io) !void {
    const allocator = testing.allocator;

    var fixture = try TwoEndpoints.init(
        allocator,
        io,
        .{ .endpoint = .{ .connection_accept_queue_len = 1 } },
        .{},
    );
    defer fixture.deinit();

    const server_addr = try fixture.bindServerLoopback();
    _ = try fixture.bindClientLoopback();

    var server_pk = try fixture.server_host.publicKey(allocator);
    defer if (server_pk.data) |data| allocator.free(data);

    // Server accept runs on a concurrent fiber (a worker executor), so dial and
    // accept make progress simultaneously across executors — no OS thread.
    var accept_ctx = AcceptCtx{ .endpoint = fixture.server };
    var accept_future = try std.Io.concurrent(io, AcceptCtx.run, .{&accept_ctx});

    const client_conn = try fixture.client.dial(server_addr, .{
        .timeout = receiveTimeout(default_handshake_timeout_ns),
        .expected_peer_key = &server_pk,
    });
    defer client_conn.deinit();

    accept_future.await(io);
    if (accept_ctx.err) |err| return err;
    const server_conn = accept_ctx.conn orelse return error.TestExpectedConn;
    defer server_conn.deinit();

    try testing.expect(client_conn.stats().packets_sent > 0);
    try testing.expect(server_conn.stats().packets_recv > 0);

    // Bidirectional stream echo: client opens + writes, server accepts + reads +
    // replies, client reads the reply. Exercises stream byte queues + the
    // outbound-signal / accept-queue waitset paths across executors.
    const outbound = try client_conn.openStream(io);
    defer outbound.deinit();
    defer closeStreamForTest(io, outbound);

    const payload = "multiexecutor-loopback-ping";
    try outbound.writeAll(io, payload, .{});

    const inbound = try server_conn.acceptStream(io, .{});
    defer inbound.deinit();
    defer closeStreamForTest(io, inbound);

    var recv_buf: [64]u8 = undefined;
    try inbound.readAll(io, recv_buf[0..payload.len], .{});
    try testing.expectEqualStrings(payload, recv_buf[0..payload.len]);

    const reply = "pong";
    try inbound.writeAll(io, reply, .{});

    var reply_buf: [16]u8 = undefined;
    try outbound.readAll(io, reply_buf[0..reply.len], .{});
    try testing.expectEqualStrings(reply, reply_buf[0..reply.len]);
}

test "endpoint loopback: handshake + stream echo on exact(2)" {
    try requireLinux();
    try runRoot(2, struct {
        fn root(io: std.Io) !void {
            try loopbackHandshakeAndEcho(io);
        }
    }.root);
}

test "endpoint loopback: handshake + stream echo on exact(4)" {
    try requireLinux();
    try runRoot(4, struct {
        fn root(io: std.Io) !void {
            try loopbackHandshakeAndEcho(io);
        }
    }.root);
}
