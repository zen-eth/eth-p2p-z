//! A runnable gossipsub node for interop smoke testing. It stands up a real
//! QUIC endpoint + Switch + Gossipsub service and either listens for inbound
//! connections or dials one or more peers, then subscribes to a topic and (in
//! `pub` mode) publishes a payload repeatedly until a duration elapses.
//!
//! Connectivity is flexible: a listener accepts EVERY inbound connection (a
//! bootstrap node in a star is dialed by every other node), and a dialer can
//! dial MULTIPLE peers via `peers=<ma1>,<ma2>,...` (a lone `peer=<ma>` still
//! works). gossipsub forms a mesh over whatever connections exist, so the same
//! binary serves both the two-node smoke runs and a multi-node mixed mesh.
//!
//! Coordination between the out-of-process nodes is file-based, not Redis: the
//! listener writes its full `/p2p/<peer-id>` multiaddr to `addr_file` once
//! bound, and an orchestrator hands that string to the dialers' `peers` arg.
//!
//! This mirrors the transport interop binary's endpoint/Switch setup; the new
//! piece is the Gossipsub service plus a message handler that prints received
//! messages and records receipt so a smoke run can assert delivery.
//!
//! Mesh-formation note: gossipsub forwarding is mesh-based, so a publish only
//! reaches peers that are in the mesh for the topic. After connecting and
//! subscribing, the heartbeat needs ~1 tick (one second) to GRAFT and form the
//! mesh, and subscriptions need to propagate. Rather than guess a single delay,
//! the publisher republishes the same payload every 500ms for the whole run so
//! at least one publish lands after the mesh forms.

pub const std_options = @import("zig-libp2p").std_options;

const std = @import("std");
const libp2p = @import("zig-libp2p");
const zio = @import("zio");
const identity = libp2p.identity;
const gossipsub = libp2p.gossipsub;
const identify = libp2p.protocols.identify;
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;

/// How often the publisher republishes its payload (mesh-formation tolerant).
const publish_interval_ms: u64 = 500;

const Role = enum { listen, dial };
const Mode = enum { publish, subscribe };

const Args = struct {
    role: Role,
    mode: Mode,
    port: u16 = 4101,
    /// A single peer multiaddr to dial. Kept for backward compatibility with the
    /// two-node runs; `peers` (comma-separated) supersedes it for mesh runs.
    peer: ?[]const u8 = null,
    /// Comma-separated peer multiaddrs to dial. The node dials every entry and
    /// starts an inbound dispatcher per connection, so it meshes with each peer.
    peers: ?[]const u8 = null,
    addr_file: ?[]const u8 = null,
    topic: []const u8 = "interop-test",
    message: []const u8 = "hello-gossipsub",
    duration_ms: u64 = 5000,

    /// Iterate the dial targets: every entry of `peers` (split on commas) plus a
    /// lone `peer` if set. Blank entries are skipped so a trailing comma is fine.
    const PeerIterator = struct {
        peers_rest: []const u8,
        single: ?[]const u8,

        fn next(self: *PeerIterator) ?[]const u8 {
            while (self.peers_rest.len != 0) {
                const comma = std.mem.indexOfScalar(u8, self.peers_rest, ',');
                const raw = if (comma) |i| self.peers_rest[0..i] else self.peers_rest;
                self.peers_rest = if (comma) |i| self.peers_rest[i + 1 ..] else self.peers_rest[raw.len..];
                const trimmed = std.mem.trim(u8, raw, " \t\r\n");
                if (trimmed.len != 0) return trimmed;
            }
            if (self.single) |p| {
                self.single = null;
                const trimmed = std.mem.trim(u8, p, " \t\r\n");
                if (trimmed.len != 0) return trimmed;
            }
            return null;
        }
    };

    fn dialTargets(self: *const Args) PeerIterator {
        return .{ .peers_rest = self.peers orelse "", .single = self.peer };
    }

    /// Parse `key=value` arguments. `arena` owns the captured strings: it is the
    /// process arena, freed automatically on exit, so no per-arg frees here.
    fn parse(arena: std.mem.Allocator, args: std.process.Args) !Args {
        var out = Args{ .role = .listen, .mode = .subscribe };
        var saw_role = false;
        var saw_mode = false;

        var it = try std.process.Args.Iterator.initAllocator(args, arena);
        defer it.deinit();
        _ = it.skip(); // program name

        while (it.next()) |arg_z| {
            const arg: []const u8 = arg_z;
            const eq = std.mem.indexOfScalar(u8, arg, '=') orelse {
                std.log.err("expected key=value argument, got '{s}'", .{arg});
                return error.InvalidArgument;
            };
            // Args returned by the iterator point at its internal buffer (reused
            // on the next `next`), so own each captured string in the arena.
            const key = try arena.dupe(u8, arg[0..eq]);
            const value = try arena.dupe(u8, arg[eq + 1 ..]);

            if (std.mem.eql(u8, key, "role")) {
                out.role = std.meta.stringToEnum(Role, value) orelse return error.InvalidRole;
                saw_role = true;
            } else if (std.mem.eql(u8, key, "mode")) {
                if (std.mem.eql(u8, value, "pub")) {
                    out.mode = .publish;
                } else if (std.mem.eql(u8, value, "sub")) {
                    out.mode = .subscribe;
                } else return error.InvalidMode;
                saw_mode = true;
            } else if (std.mem.eql(u8, key, "port")) {
                out.port = try std.fmt.parseInt(u16, value, 10);
            } else if (std.mem.eql(u8, key, "peer")) {
                out.peer = value;
            } else if (std.mem.eql(u8, key, "peers")) {
                out.peers = value;
            } else if (std.mem.eql(u8, key, "addr_file")) {
                out.addr_file = value;
            } else if (std.mem.eql(u8, key, "topic")) {
                out.topic = value;
            } else if (std.mem.eql(u8, key, "message")) {
                out.message = value;
            } else if (std.mem.eql(u8, key, "duration_ms")) {
                out.duration_ms = try std.fmt.parseInt(u64, value, 10);
            } else {
                std.log.err("unknown argument key '{s}'", .{key});
                return error.UnknownArgument;
            }
        }

        if (!saw_role) {
            std.log.err("missing required arg role=listen|dial", .{});
            return error.MissingRole;
        }
        if (!saw_mode) {
            std.log.err("missing required arg mode=pub|sub", .{});
            return error.MissingMode;
        }
        if (out.role == .dial and out.peer == null and out.peers == null) {
            std.log.err("dial role requires peer=<multiaddr> or peers=<ma1>,<ma2>,...", .{});
            return error.MissingPeer;
        }
        return out;
    }
};

/// Records whether a message arrived and prints each received message. The
/// router fiber (a zio worker) calls `onMessage`; `main` reads `received`.
const Recorder = struct {
    io: std.Io,
    received: std.atomic.Value(bool) = .init(false),

    fn handler(self: *Recorder) gossipsub.MessageHandler {
        return .{ .ctx = self, .on_message = onMessage };
    }

    fn onMessage(ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) void {
        const self: *Recorder = @ptrCast(@alignCast(ctx));
        self.received.store(true, .release);
        // A clear, greppable line so the orchestrator can assert delivery. The
        // sender id is raw peer-id bytes; print as hex to stay printable.
        var line_buf: [640]u8 = undefined;
        const line = std.fmt.bufPrint(
            &line_buf,
            "RECV topic={s} from={x} data={s}\n",
            .{ topic, from[0..@min(from.len, 64)], data },
        ) catch return;
        printToStdout(self.io, line) catch {};
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    const cpu_count = std.Thread.getCpuCount() catch 2;
    const executor_count: u8 = @intCast(std.math.clamp(cpu_count, 2, 64));
    const runtime = try zio.Runtime.init(allocator, .{ .executors = .exact(executor_count) });
    defer runtime.deinit();
    const io = runtime.io();

    const args = try Args.parse(init.arena.allocator(), init.minimal.args);

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();
    const endpoint = try libp2p.QuicEndpoint.initWithIdentity(allocator, io, &host_key, .{});
    defer endpoint.deinit();
    const switcher = try libp2p.Switch.init(allocator, io, endpoint);
    defer switcher.deinit();

    const local_peer = try host_key.peerId(allocator);

    // Register the identify protocol responder. go-libp2p (and rust-libp2p)
    // learn which protocols a peer speaks via the `/ipfs/id/1.0.0` exchange, and
    // their pubsub only opens a `/meshsub` stream to a peer once identify reports
    // it supports meshsub. So the identify response must advertise identify
    // itself plus every `/meshsub` version we speak (1.2.0, 1.1.0, 1.0.0); the
    // peer then negotiates the best common one. Advertising 1.2.0 is what lets a
    // 1.2-capable go/rust peer pick 1.2.0 with us. Without this go never peers us
    // in gossipsub. The handler only reads its (immutable) options, so one shared
    // instance serves every inbound identify stream. It must outlive the run, so
    // it lives here on `main`'s stack (the service borrows it; deinit is a no-op).
    var id_handler = identify.IdentifyHandler.initWithOptions(allocator, .{
        .agent_version = "eth-p2p-z-gossipsub-interop/0.1.0",
        .protocols = &(.{identify.protocol_id} ++ gossipsub.supported_protocols),
    });
    try switcher.addProtocolService(
        identify.protocol_id,
        libp2p.protocols.streamHandlerService(identify.IdentifyHandler, identify.IdentifyHandler.run, &id_handler),
    );

    var recorder = Recorder{ .io = io };
    // Construct gossipsub BEFORE any dial/accept so its peer-event callback is
    // registered when the connection comes up. Pass the host key to enable
    // StrictSign (sign outbound, verify inbound) so go-libp2p — which defaults to
    // StrictSign — accepts our published messages and we accept its signed ones.
    // Scoring is left disabled here (null): the interop binary exercises the
    // base mesh/gossip behaviour against go/rust-libp2p without the score gates.
    const gs = try gossipsub.Gossipsub.init(allocator, io, switcher, local_peer, &host_key, recorder.handler(), null, .{});
    var gs_live = true;
    defer if (gs_live) gs.deinit();

    switch (args.role) {
        .listen => try runListener(allocator, io, switcher, &host_key, gs, &args),
        .dial => try runDialer(allocator, io, switcher, gs, &args),
    }

    // Tear down gossipsub before the deferred Switch/endpoint deinit. By now the
    // run duration has elapsed; closing the Switch (its deferred deinit) tears
    // down the connection, then gossipsub deinit clears the callback + frees the
    // router. Deiniting gossipsub here keeps the documented order.
    gs.deinit();
    gs_live = false;

    const received = recorder.received.load(.acquire);
    var json_buf: [256]u8 = undefined;
    const json = try std.fmt.bufPrint(
        &json_buf,
        "{{\"role\":\"{s}\",\"mode\":\"{s}\",\"received\":{s}}}\n",
        .{
            @tagName(args.role),
            if (args.mode == .publish) "pub" else "sub",
            if (received) "true" else "false",
        },
    );
    try printToStdout(io, json);
}

fn runListener(
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    host_key: *identity.KeyPair,
    gs: *gossipsub.Gossipsub,
    args: *const Args,
) !void {
    const listen_text = try std.fmt.allocPrint(allocator, "/ip4/0.0.0.0/udp/{d}/quic-v1", .{args.port});
    defer allocator.free(listen_text);
    var listen_addr = try Multiaddr.fromString(allocator, listen_text);
    defer listen_addr.deinit(allocator);
    try switcher.listen(listen_addr);
    std.log.info("listener bound {s}", .{listen_text});

    const published = try buildPublishedMultiaddr(allocator, switcher, host_key);
    defer allocator.free(published);
    std.log.info("listener advertising {s}", .{published});

    if (args.addr_file) |path| {
        try writeAddrFile(io, path, published);
    }

    // Accept inbound connections on a fiber so the run loop bounds the wait by
    // duration: a peer might never connect, and `accept()` blocks otherwise. The
    // bootstrap of a star is dialed by every other node, so the fiber loops and
    // accepts EACH inbound connection (not just the first), starting a dispatcher
    // per connection and stashing it for teardown.
    var accept_state = AcceptState{ .allocator = allocator, .switcher = switcher };
    var accept_future = try std.Io.concurrent(io, AcceptState.run, .{&accept_state});
    defer {
        // Stop the accept fiber (no-op if it already returned), then close every
        // accepted connection before gossipsub/Switch teardown.
        accept_future.cancel(io);
        accept_state.deinit();
    }

    // Always subscribe so the mesh can form (a sub-only node still needs the
    // subscription to receive; a pub node subscribes too so the peer learns it).
    try gs.subscribe(args.topic);

    try runForDuration(io, gs, args);
}

const AcceptState = struct {
    allocator: std.mem.Allocator,
    switcher: *libp2p.Switch,
    conns: std.ArrayList(*libp2p.SwitchConnection) = .empty,

    /// Accept inbound connections in a loop, starting an inbound stream
    /// dispatcher for each. Returns on accept failure or cancellation (the run
    /// loop bounds the wait). A bootstrap node is dialed by every leaf, so it
    /// must accept more than one connection for the mesh to span all peers.
    fn run(self: *AcceptState) void {
        while (true) {
            const conn = self.switcher.accept() catch |err| {
                switch (err) {
                    error.Canceled => {},
                    else => std.log.warn("accept failed: {any}", .{err}),
                }
                return;
            };
            // Start inbound dispatch so the `/meshsub` inbound handler runs for
            // this connection; without it the peers wire up but never exchange
            // RPCs (and the dialer's identify exchange times out).
            conn.startInboundDispatcher(.{}) catch |err| {
                std.log.warn("startInboundDispatcher failed: {any}", .{err});
                conn.deinit();
                continue;
            };
            // The accept fiber and the teardown both touch `conns`, but never
            // concurrently: teardown first cancels this fiber (joining it) and
            // only then calls deinit, so appends here are sequenced before reads
            // there. Drop the connection if we cannot record it for teardown.
            self.conns.append(self.allocator, conn) catch {
                conn.deinit();
                continue;
            };
            std.log.info("listener accepted inbound connection ({d} total)", .{self.conns.items.len});
        }
    }

    fn deinit(self: *AcceptState) void {
        for (self.conns.items) |conn| conn.deinit();
        self.conns.deinit(self.allocator);
    }
};

fn runDialer(
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    gs: *gossipsub.Gossipsub,
    args: *const Args,
) !void {
    // Dial every configured peer and start an inbound dispatcher per connection.
    // gossipsub forms a mesh over whatever connections exist, so a node that
    // dials several peers can graft into a mesh spanning all of them. Every
    // connection must stay alive for the whole run, so they are held in a list
    // and closed together on teardown.
    var conns: std.ArrayList(*libp2p.SwitchConnection) = .empty;
    defer {
        for (conns.items) |conn| conn.deinit();
        conns.deinit(allocator);
    }

    var it = args.dialTargets();
    var dialed: usize = 0;
    while (it.next()) |peer_text| {
        std.log.info("dialing {s}", .{peer_text});
        var remote_addr = Multiaddr.fromString(allocator, peer_text) catch |err| {
            std.log.warn("invalid peer multiaddr {s}: {any}", .{ peer_text, err });
            continue;
        };
        defer remote_addr.deinit(allocator);

        const conn = switcher.dial(remote_addr, .{ .timeout = timeoutFromMs(args.duration_ms) }) catch |err| {
            std.log.warn("dial {s} failed: {any}", .{ peer_text, err });
            continue;
        };
        // Start inbound dispatch so this side's `/meshsub` inbound handler runs
        // for this peer; without it the peers wire up but never exchange RPCs.
        conn.startInboundDispatcher(.{}) catch |err| {
            std.log.warn("startInboundDispatcher for {s} failed: {any}", .{ peer_text, err });
            conn.deinit();
            continue;
        };
        try conns.append(allocator, conn);
        dialed += 1;
    }
    std.log.info("dialer connected to {d} peer(s)", .{dialed});
    if (dialed == 0) return error.NoPeersDialed;

    // Always subscribe so the mesh can form on both ends.
    try gs.subscribe(args.topic);

    try runForDuration(io, gs, args);
}

/// Run until `duration_ms` elapses. In publish mode, republish the payload every
/// `publish_interval_ms` so at least one publish lands after the mesh forms; in
/// subscribe mode, just wait (deliveries arrive on the router fiber).
fn runForDuration(io: std.Io, gs: *gossipsub.Gossipsub, args: *const Args) !void {
    if (args.mode != .publish) {
        try timeoutFromMs(args.duration_ms).sleep(io);
        return;
    }

    var elapsed_ms: u64 = 0;
    while (elapsed_ms < args.duration_ms) : (elapsed_ms += publish_interval_ms) {
        gs.publish(args.topic, args.message) catch |err| {
            std.log.warn("publish failed: {any}", .{err});
        };
        const step = @min(publish_interval_ms, args.duration_ms - elapsed_ms);
        try timeoutFromMs(step).sleep(io);
    }
}

/// Build the full `/ip4/.../udp/<port>/quic-v1/p2p/<peer-id>` multiaddr from the
/// Switch's first bound listen address and this node's peer id.
fn buildPublishedMultiaddr(
    allocator: std.mem.Allocator,
    switcher: *libp2p.Switch,
    host_key: *identity.KeyPair,
) ![]u8 {
    var addrs = try switcher.listenMultiaddrs(allocator);
    defer {
        for (addrs.items) |addr| allocator.free(addr);
        addrs.deinit(allocator);
    }
    if (addrs.items.len == 0) return error.NoListenAddress;

    var local = try Multiaddr.fromString(allocator, addrs.items[0]);
    defer local.deinit(allocator);
    const parsed = try local.parseIpUdp(allocator);

    const local_peer = try host_key.peerId(allocator);
    const peer_text = try local_peer.toString(allocator);
    defer allocator.free(peer_text);

    // Bind on 0.0.0.0 but advertise 127.0.0.1 for the loopback smoke run.
    return std.fmt.allocPrint(
        allocator,
        "/ip4/127.0.0.1/udp/{d}/quic-v1/p2p/{s}",
        .{ parsed.address.getPort(), peer_text },
    );
}

fn writeAddrFile(io: std.Io, path: []const u8, contents: []const u8) !void {
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);
    var buf: [256]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(contents);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

/// Emit `bytes` to stdout. We write more than once (each `RECV` plus the final
/// JSON), and stdout is often redirected to a regular file by the orchestrator.
/// A positional writer would seek to offset 0 on every call and overwrite the
/// previous line, so use a STREAMING writer: it appends at the fd's current
/// position, which is correct for both pipes and redirected files.
fn printToStdout(io: std.Io, bytes: []const u8) !void {
    const stdout_file: std.Io.File = .{ .handle = std.posix.STDOUT_FILENO, .flags = .{ .nonblocking = false } };
    var buf: [640]u8 = undefined;
    var writer = stdout_file.writerStreaming(io, &buf);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

fn timeoutFromMs(ms: u64) std.Io.Timeout {
    const ns = std.math.mul(u64, ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
    return .{ .duration = .{ .raw = .fromNanoseconds(@intCast(ns)), .clock = .awake } };
}
