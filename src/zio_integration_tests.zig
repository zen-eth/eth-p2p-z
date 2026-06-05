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
//! under the historical .exact(1) bug). It is the regression net for the
//! multi-executor refactor of the upper layers.
//!
//! Pulls BoringSSL (via the quiche dep), so it is a separate, slower-compiling
//! target from the Layer-0 zio-io-test. Run: `zig build zio-integ-test`.
//!
//! Runs on every backend (macOS kqueue, Linux epoll / io_uring), including the
//! teardown path (`endpoint.deinit()` -> `router.closeListener()`). Teardown is
//! cooperative (a stop flag + closing `route_commands` to wake the router's select
//! arm), not `router_future.cancel()`: cancel-based teardown of the router's
//! multi-arm `Select` deadlocks on macOS kqueue. See `router.closeListener`.

const std = @import("std");
const zio = @import("zio");
const support = @import("quic/endpoint/test_support.zig");
const switch_mod = @import("switch.zig");
const identity = @import("identity.zig");
const protocols = @import("protocols.zig");
const quic = @import("quic.zig");
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;

const testing = std.testing;
const AcceptCtx = support.AcceptCtx;
const TwoEndpoints = support.TwoEndpoints;
const closeStreamForTest = support.closeStreamForTest;
const receiveTimeout = support.receiveTimeout;
const default_handshake_timeout_ns = support.default_handshake_timeout_ns;
const Switch = switch_mod.Switch;
const SwitchConnection = switch_mod.SwitchConnection;

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
    try runRoot(2, struct {
        fn root(io: std.Io) !void {
            try loopbackHandshakeAndEcho(io);
        }
    }.root);
}

test "endpoint loopback: handshake + stream echo on exact(4)" {
    try runRoot(4, struct {
        fn root(io: std.Io) !void {
            try loopbackHandshakeAndEcho(io);
        }
    }.root);
}

// ---------------------------------------------------------------------------
// Switch connection teardown under multi-executor scheduling.
//
// Regression guard for a teardown deadlock: a Switch connection's inbound
// dispatcher parks inside acceptStream (futexWait on the connection's accept
// waitset) waiting for a stream that never arrives. Tearing the connection down
// (SwitchConnection.deinit) used to rely on a cross-executor Group.cancel to
// unblock that parked dispatcher before joining it. On a real multi-executor
// runtime that cancel's wakeup could be lost, so the join blocked forever.
//
// The fix drives the dispatcher to exit via a PERSISTENT signal — closing the
// connection marks it closed and notifies the accept waitset (level-triggered),
// so the parked acceptStream wakes via notify, observes isClosed(), and returns
// ConnectionClosed; the dispatcher loop exits on its own and the join is then
// trivial. This test loops the bring-up/teardown many times across 2 and 4
// executors to surface the intermittent lost-wakeup race; a hang here is the
// deadlock, and a clean completion (leak-checked by the testing allocator)
// proves the persistent-signal teardown is reliable.

/// Minimal inbound handler so the server has a registered protocol (an empty
/// registry makes the dispatcher exit immediately with NoRegisteredProtocols,
/// which would defeat the "dispatcher parked in acceptStream" precondition).
const NoopHandler = struct {
    fn run(_: *@This(), handler_io: std.Io, stream: *quic.Stream) anyerror!void {
        var b: [1]u8 = undefined;
        stream.readAll(handler_io, &b, .{}) catch {};
    }
};

fn switchDispatcherTeardownLoop(io: std.Io, iterations: usize) !void {
    const allocator = testing.allocator;

    var server_key = try identity.KeyPair.generate(.ED25519);
    defer server_key.deinit();
    var client_key = try identity.KeyPair.generate(.ED25519);
    defer client_key.deinit();

    const server_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &server_key, .{});
    defer server_endpoint.deinit();
    const client_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &client_key, .{});
    defer client_endpoint.deinit();

    const server = try Switch.init(allocator, io, server_endpoint);
    defer server.deinit();
    const client = try Switch.init(allocator, io, client_endpoint);
    defer client.deinit();

    var noop = NoopHandler{};
    try server.addProtocolService(
        "/test/teardown/1.0.0",
        protocols.streamHandlerService(NoopHandler, NoopHandler.run, &noop),
    );

    var listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer listen_addr.deinit(allocator);
    try server.listen(listen_addr);
    var client_listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer client_listen_addr.deinit(allocator);
    try client.listen(client_listen_addr);

    var addrs = try server.listenMultiaddrs(allocator);
    defer {
        for (addrs.items) |addr| allocator.free(addr);
        addrs.deinit(allocator);
    }
    var dial_addr = try Multiaddr.fromString(allocator, addrs.items[0]);
    defer dial_addr.deinit(allocator);

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        // Accept on a concurrent fiber so dial + accept make progress across
        // executors (the multi-executor scheduling the deadlock needs).
        const AcceptSwitchCtx = struct {
            sw: *Switch,
            conn: ?*SwitchConnection = null,
            err: ?anyerror = null,

            fn run(ctx: *@This()) void {
                ctx.conn = ctx.sw.accept() catch |err| {
                    ctx.err = err;
                    return;
                };
            }
        };
        var accept_ctx = AcceptSwitchCtx{ .sw = server };
        var accept_future = try std.Io.concurrent(io, AcceptSwitchCtx.run, .{&accept_ctx});

        const client_conn = try client.dial(dial_addr, .{
            .timeout = receiveTimeout(default_handshake_timeout_ns),
        });
        defer client_conn.deinit();

        accept_future.await(io);
        if (accept_ctx.err) |err| return err;
        const server_conn = accept_ctx.conn orelse return error.TestExpectedConn;

        // Start the inbound dispatcher: with no inbound stream pending it blocks
        // inside acceptStream, parked on the connection's accept waitset. This is
        // the precondition for the deadlock — teardown must unblock it.
        try server_conn.startInboundDispatcher(.{});

        // Give the dispatcher a moment to actually reach the parked wait
        // (otherwise it might still be spawning and never exercise the race).
        try std.Io.sleep(io, .fromMilliseconds(2), .awake);

        // The operation under test: this drives shutdownAndDestroy -> cleanup,
        // which must wake the parked dispatcher and join it without hanging.
        server_conn.deinit();
    }
}

// 50 bring-up/teardown cycles per executor count. The race surfaced on the
// FIRST iteration before the fix, so this is ample as a permanent regression
// guard while keeping the default suite fast; it was validated at far higher
// counts (hundreds of cycles, zero hangs) during the fix.
const teardown_loop_iterations: usize = 50;

test "switch connection teardown: parked dispatcher unblocks on exact(2)" {
    try runRoot(2, struct {
        fn root(io: std.Io) !void {
            try switchDispatcherTeardownLoop(io, teardown_loop_iterations);
        }
    }.root);
}

test "switch connection teardown: parked dispatcher unblocks on exact(4)" {
    try runRoot(4, struct {
        fn root(io: std.Io) !void {
            try switchDispatcherTeardownLoop(io, teardown_loop_iterations);
        }
    }.root);
}

