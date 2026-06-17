//! A runnable gossipsub node for interop smoke testing: stands up a QUIC
//! endpoint + Switch + Gossipsub, listens or dials one or more peers, subscribes
//! to a topic, and (in `pub` mode) republishes a payload until a duration
//! elapses. One binary serves two-node and multi-node meshes: a listener accepts
//! EVERY inbound connection (a star bootstrap is dialed by all), a dialer dials
//! many via `peers=<ma1>,<ma2>,...`. Node coordination is file-based — the
//! listener writes its `/p2p/<peer-id>` multiaddr to `addr_file` once bound.
//!
//! Shadow support (additive, OFF by default): `shadow=on` restricts the QUIC UDP
//! path to plain `recvmsg`/`sendmsg` (no GRO/GSO/`IP_PKTINFO`/timestamp cmsgs —
//! the only socket form the Shadow simulator supports), and `announce=<ip>` binds
//! and advertises a specific IP (Shadow hosts are single-homed, so the wildcard
//! `0.0.0.0` bind is not dialable).
//!
//! Mesh-formation: a publish only reaches peers in the topic mesh, and the
//! heartbeat needs ~1 tick (1s) after subscribe to GRAFT. Rather than guess one
//! delay, the publisher republishes every 500ms so a publish lands post-mesh.

pub const std_options = @import("zig-libp2p").std_options;

const std = @import("std");
const libp2p = @import("zig-libp2p");
const zio = @import("zio");
const identity = libp2p.identity;
const gossipsub = libp2p.gossipsub;
const identify = libp2p.protocols.identify;
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;
const testplans = @import("testplans.zig");

// Pull in testplans.zig's in-file tests under `zig build test`.
test {
    _ = testplans;
}

/// Scan args for `--params <path>` (the test-plans framework invocation) and
/// return the path, or null if absent (keeping the key=value smoke-test mode).
/// The value may be `--params=<path>` or a separate `--params <path>` token.
fn testPlansParamsPath(arena: std.mem.Allocator, args: std.process.Args) !?[]const u8 {
    const slice = try std.process.Args.toSlice(args, arena);
    var i: usize = 1; // skip program name
    while (i < slice.len) : (i += 1) {
        const arg: []const u8 = slice[i];
        if (std.mem.eql(u8, arg, "--params")) {
            if (i + 1 < slice.len) return slice[i + 1];
            return error.MissingParamsValue;
        }
        if (std.mem.startsWith(u8, arg, "--params=")) {
            return arg["--params=".len..];
        }
    }
    return null;
}

/// Parse the optional local-testing overrides (`--node-id <N>`,
/// `--connect-localhost`). The framework never passes these, so a real Shadow run
/// uses all defaults; they let a developer run a loopback mesh without Shadow.
fn testPlansOptions(arena: std.mem.Allocator, args: std.process.Args) !testplans.Options {
    var opts = testplans.Options{};
    const slice = try std.process.Args.toSlice(args, arena);
    var i: usize = 1;
    while (i < slice.len) : (i += 1) {
        const arg: []const u8 = slice[i];
        if (std.mem.eql(u8, arg, "--node-id")) {
            if (i + 1 >= slice.len) return error.MissingNodeIdValue;
            opts.node_id_override = try std.fmt.parseInt(u64, slice[i + 1], 10);
            i += 1;
        } else if (std.mem.startsWith(u8, arg, "--node-id=")) {
            opts.node_id_override = try std.fmt.parseInt(u64, arg["--node-id=".len..], 10);
        } else if (std.mem.eql(u8, arg, "--connect-localhost")) {
            opts.connect_localhost = true;
        }
    }
    return opts;
}

/// How often the publisher republishes its payload (mesh-formation tolerant).
const publish_interval_ms: u64 = 500;

/// Upper bound on a printed RECV line (and any single stdout write). Sized well
/// above the 1 KiB gossipsub v1.2 IDONTWANT threshold so the large-message
/// scenario's payload prints intact rather than truncated.
const max_print_len: usize = 8192;

const Role = enum { listen, dial };
const Mode = enum { publish, subscribe };

/// Signing scheme. `strict` signs+verifies (the go-libp2p default). `anonymous`
/// is StrictNoSign: no author/seqno, dedup keyed by content id
/// `sha256(topic ++ data)`, inbound unverified. Cross-impl contract: every node
/// in the topic must use the SAME id scheme or dedup breaks.
const Sign = enum { strict, anonymous };

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
    /// Read a single peer multiaddr to dial from this file: the read counterpart
    /// of the listener's `addr_file=` write, for Shadow coordination over the
    /// shared host filesystem. The dialer polls briefly for the file (the listener
    /// may not have bound yet). Additive to `peer`/`peers`; all targets are dialed.
    peer_file: ?[]const u8 = null,
    topic: []const u8 = "interop-test",
    message: []const u8 = "hello-gossipsub",
    duration_ms: u64 = 5000,
    /// Signing scheme: `strict` (default, signs+verifies) or `anonymous`
    /// (StrictNoSign, content-id dedup). See `Sign`.
    sign: Sign = .strict,
    /// Enable peer-exchange (PX) on PRUNE (go-libp2p `WithPeerExchange`), OFF by
    /// default. When ON, an over-degree heartbeat prune offers the surviving mesh
    /// peers' signed records, and an inbound PX'd PRUNE makes us dial the offered.
    px: bool = false,
    /// Override the per-topic mesh DEGREE targets; null keeps the go-libp2p
    /// baseline (6/5/12). A PX emitter sets a small `d_high` so a tiny topology
    /// goes over-degree and the heartbeat prunes WITH PX. Each set independently.
    d: ?usize = null,
    d_low: ?usize = null,
    d_high: ?usize = null,
    /// Run the QUIC endpoint in Shadow-compatible mode (`shadow=on`, OFF by
    /// default): see the module header. Sets `EndpointOptions.shadow_compatible`.
    shadow: bool = false,
    /// The IP this listener advertises (`announce=<ip>`). When set, it both BINDS
    /// to this IP (quiche needs a concrete local path address — Shadow has no
    /// `IP_PKTINFO`) and PUBLISHES it in the addr_file, identify listen addrs, and
    /// sealed peer record. When absent: wildcard `0.0.0.0` bind, advertise loopback
    /// `127.0.0.1` (on-host runs).
    announce: ?[]const u8 = null,

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
            } else if (std.mem.eql(u8, key, "peer_file")) {
                out.peer_file = value;
            } else if (std.mem.eql(u8, key, "topic")) {
                out.topic = value;
            } else if (std.mem.eql(u8, key, "message")) {
                out.message = value;
            } else if (std.mem.eql(u8, key, "duration_ms")) {
                out.duration_ms = try std.fmt.parseInt(u64, value, 10);
            } else if (std.mem.eql(u8, key, "sign")) {
                if (std.mem.eql(u8, value, "strict")) {
                    out.sign = .strict;
                } else if (std.mem.eql(u8, value, "anonymous") or std.mem.eql(u8, value, "nosign")) {
                    out.sign = .anonymous;
                } else return error.InvalidSign;
            } else if (std.mem.eql(u8, key, "px")) {
                if (std.mem.eql(u8, value, "on") or std.mem.eql(u8, value, "true")) {
                    out.px = true;
                } else if (std.mem.eql(u8, value, "off") or std.mem.eql(u8, value, "false")) {
                    out.px = false;
                } else return error.InvalidPx;
            } else if (std.mem.eql(u8, key, "shadow")) {
                if (std.mem.eql(u8, value, "on") or std.mem.eql(u8, value, "true")) {
                    out.shadow = true;
                } else if (std.mem.eql(u8, value, "off") or std.mem.eql(u8, value, "false")) {
                    out.shadow = false;
                } else return error.InvalidShadow;
            } else if (std.mem.eql(u8, key, "announce")) {
                out.announce = value;
            } else if (std.mem.eql(u8, key, "d")) {
                out.d = try std.fmt.parseInt(usize, value, 10);
            } else if (std.mem.eql(u8, key, "d_low")) {
                out.d_low = try std.fmt.parseInt(usize, value, 10);
            } else if (std.mem.eql(u8, key, "d_high")) {
                out.d_high = try std.fmt.parseInt(usize, value, 10);
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
        if (out.role == .dial and out.peer == null and out.peers == null and out.peer_file == null) {
            std.log.err("dial role requires peer=<multiaddr>, peers=<ma1>,<ma2>,..., or peer_file=<path>", .{});
            return error.MissingPeer;
        }
        return out;
    }
};

/// Build a mesh-degree override from the `d`/`d_low`/`d_high` args, or null when
/// none is set. Any unset field falls back to the 6/5/12 baseline. Values should
/// stay consistent (`d_low <= d <= d_high`) — the router does not validate them.
fn meshDegreeFromArgs(args: *const Args) ?gossipsub.MeshDegree {
    if (args.d == null and args.d_low == null and args.d_high == null) return null;
    return .{
        .d = args.d orelse 6,
        .d_low = args.d_low orelse 5,
        .d_high = args.d_high orelse 12,
    };
}

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
        // Greppable line for the orchestrator's delivery assert. `from` is the
        // author peer-id (hex). Under anonymous it is empty, so `author=` prints
        // blank — which the anonymous scenario asserts (no author on the wire).
        var line_buf: [max_print_len]u8 = undefined;
        const line = std.fmt.bufPrint(
            &line_buf,
            "RECV topic={s} author={x} data={s}\n",
            .{ topic, from[0..@min(from.len, 64)], data },
        ) catch return;
        printToStdout(self.io, line) catch {};
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    const cpu_count = libp2p.effectiveCpuCount(allocator); // cgroup-CFS-quota-aware
    const executor_count: u8 = @intCast(std.math.clamp(cpu_count, 2, 64));
    const runtime = try zio.Runtime.init(allocator, .{ .executors = .exact(executor_count) });
    defer runtime.deinit();
    const io = runtime.io();

    // test-plans mode: invoked as `<bin> --params <path>` (the libp2p
    // gossipsub-interop framework's invocation), run as a framework participant.
    // Any other invocation keeps the key=value smoke-test path below. See
    // testplans.zig.
    if (try testPlansParamsPath(init.arena.allocator(), init.minimal.args)) |params_path| {
        const tp_opts = try testPlansOptions(init.arena.allocator(), init.minimal.args);
        return testplans.run(allocator, io, params_path, tp_opts);
    }

    const args = try Args.parse(init.arena.allocator(), init.minimal.args);

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();
    // Shadow mode restricts the QUIC UDP path to plain `recvmsg`/`sendmsg` (see
    // module header); OFF by default leaves the full control-message path.
    const endpoint = try libp2p.QuicEndpoint.initWithIdentity(allocator, io, &host_key, .{
        .endpoint = .{ .shadow_compatible = args.shadow },
    });
    defer endpoint.deinit();
    const switcher = try libp2p.Switch.init(allocator, io, endpoint);
    defer switcher.deinit();

    const local_peer = try host_key.peerId(allocator);

    var recorder = Recorder{ .io = io };
    // Construct gossipsub BEFORE any dial/accept so its peer-event callback is
    // registered when the connection comes up. `strict` passes the host key (enable
    // StrictSign); `anonymous` passes none (StrictNoSign) — see `Sign`. Scoring is
    // left disabled (null) so the binary exercises base mesh/gossip without gates.
    const host_key_opt: ?*const identity.KeyPair = if (args.sign == .strict) &host_key else null;
    var gs_config: gossipsub.RouterConfig = switch (args.sign) {
        // Strict signing runs the ASYNC validation pipeline (verify off the router
        // fiber). The in-flight cap follows go-libp2p (worker count = NumCPU): one
        // validation per executor keeps every core usable while bounding held
        // message snapshots, and exercises the off-fiber verify path in interop.
        .strict => .{ .validation_concurrency = executor_count },
        .anonymous => .{ .signature_policy = .anonymous },
    };
    // Peer-exchange + optional mesh-degree override; see the `px`/`d` fields. The
    // small `d_high` lets a tiny topology go over-degree so the heartbeat prunes
    // WITH PX (e.g. a `px=on d_high=2` bootstrap dialed by 3 leaves prunes one).
    gs_config.peer_exchange_enabled = args.px;
    gs_config.mesh_degree = meshDegreeFromArgs(&args);
    const gs = try gossipsub.Gossipsub.init(allocator, io, switcher, local_peer, host_key_opt, recorder.handler(), null, gs_config);
    // Runs only on an error return from the run functions; the success path
    // hard-exits via `finishAndExit` (see its note).
    defer gs.deinit();

    // `runListener`/`runDialer` hard-exit via `finishAndExit` on success, so
    // control returns here only when the run fails to start — the error then
    // propagates out and the deferred teardown above runs.
    switch (args.role) {
        .listen => try runListener(allocator, io, switcher, &host_key, &recorder, gs, &args),
        .dial => try runDialer(allocator, io, switcher, &host_key, &recorder, gs, &args),
    }
}

/// Register the identify responder, advertising our protocols AND a signed peer
/// record (libp2p identify field 8) sealed over `listen_addrs` so a peer can
/// certify our addresses (and peer-exchange us). go-libp2p/rust-libp2p only open a
/// `/meshsub` stream once identify reports meshsub, so the response advertises
/// identify plus every `/meshsub` version we speak; the peer picks the best
/// common one. The sealed record uses the SAME `listen_addrs` bytes so its
/// addresses match. The handler is read-only, so one instance serves every
/// inbound stream; the caller keeps it alive for the whole run.
fn registerIdentify(
    allocator: std.mem.Allocator,
    switcher: *libp2p.Switch,
    host_key: *const identity.KeyPair,
    listen_addrs: []const []const u8,
    handler_out: *identify.IdentifyHandler,
) !void {
    handler_out.* = try identify.IdentifyHandler.initWithSignedRecord(
        allocator,
        .{
            .agent_version = "eth-p2p-z-gossipsub-interop/0.1.0",
            .protocols = &(.{identify.protocol_id} ++ gossipsub.supported_protocols),
            .listen_addrs = listen_addrs,
        },
        host_key,
        // One record per process, so a fixed seq is enough (the cert-address book
        // keeps the highest-seq record per peer). A node that re-published after an
        // address change would bump a monotonic counter here.
        1,
    );
    try switcher.addProtocolService(
        identify.protocol_id,
        libp2p.protocols.streamHandlerService(identify.IdentifyHandler, identify.IdentifyHandler.run, handler_out),
    );
}

/// Run the client side of identify against a freshly-connected `conn`: open
/// `/ipfs/id/1.0.0`, read the peer's identify, and if it carries a verified
/// `signedPeerRecord`, hand it to gossipsub's cert-record store so peer-exchange
/// can vouch for the peer (the consume is what populates PX end-to-end).
/// Best-effort: failures are logged and ignored, the connection is already up.
fn exchangeIdentify(
    allocator: std.mem.Allocator,
    io: std.Io,
    conn: *libp2p.SwitchConnection,
    gs: *gossipsub.Gossipsub,
) void {
    const stream = conn.openProtocolStream(identify.protocol_id, .{}) catch |err| {
        std.log.info("identify exchange: open stream failed: {any}", .{err});
        return;
    };
    var owned = identify.readIdentify(allocator, io, stream) catch |err| {
        std.log.info("identify exchange: read failed: {any}", .{err});
        stream.close(io) catch {};
        stream.deinit();
        return;
    };
    defer owned.deinit(allocator);
    stream.close(io) catch {};
    stream.deinit();

    const peer = conn.peerId();
    const consumed = identify.consumeSignedPeerRecord(allocator, &owned.reader, peer) catch |err| {
        std.log.info("identify exchange: signed peer record rejected: {any}", .{err});
        return;
    };
    if (consumed) |*rec| {
        var r = rec.*;
        r.deinit(allocator);
        // The original envelope bytes (verified above) go to the store verbatim.
        gs.consumeIdentifyRecord(peer, owned.reader.getSignedPeerRecord());
        std.log.info("identify exchange: stored signed peer record", .{});
    }
}

/// Emit the result JSON line that the orchestrator parses. Written with a DIRECT
/// write(2) syscall rather than through the cooperative io runtime, so it cannot
/// be delayed or lost if the runtime is momentarily busy at shutdown.
fn emitResult(recorder: *Recorder, args: *const Args) void {
    const received = recorder.received.load(.acquire);
    var json_buf: [256]u8 = undefined;
    const json = std.fmt.bufPrint(
        &json_buf,
        "{{\"role\":\"{s}\",\"mode\":\"{s}\",\"received\":{s}}}\n",
        .{
            @tagName(args.role),
            if (args.mode == .publish) "pub" else "sub",
            if (received) "true" else "false",
        },
    ) catch unreachable; // The fixed format + bounded fields always fit json_buf.
    var off: usize = 0;
    while (off < json.len) {
        const n = std.c.write(std.posix.STDOUT_FILENO, json.ptr + off, json.len - off);
        if (n <= 0) break; // EOF/error: nothing more we can usefully do.
        off += @intCast(n);
    }
}

/// Emit the result, then `exit(0)` rather than unwinding the fiber teardown: a
/// short-lived test/interop node has no reason to orchestrate the multi-layer
/// graceful teardown (gossipsub → connections → switch → endpoint), and a direct
/// exit keeps every node's shutdown prompt and robust.
fn finishAndExit(io: std.Io, recorder: *Recorder, args: *const Args) noreturn {
    _ = io;
    emitResult(recorder, args);
    std.process.exit(0);
}

fn runListener(
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    host_key: *identity.KeyPair,
    recorder: *Recorder,
    gs: *gossipsub.Gossipsub,
    args: *const Args,
) !void {
    // Bind the announce IP when given (gives quiche a concrete local path address
    // under Shadow, which lacks `IP_PKTINFO`), else the wildcard `0.0.0.0`.
    const bind_ip: []const u8 = args.announce orelse "0.0.0.0";
    const listen_text = try std.fmt.allocPrint(allocator, "/ip4/{s}/udp/{d}/quic-v1", .{ bind_ip, args.port });
    defer allocator.free(listen_text);
    var listen_addr = try Multiaddr.fromString(allocator, listen_text);
    defer listen_addr.deinit(allocator);
    try switcher.listen(listen_addr);
    std.log.info("listener bound {s}", .{listen_text});

    // The IP a peer must DIAL to reach us: the wildcard `0.0.0.0` bind is not
    // dialable, so advertise the announce IP when given, else loopback 127.0.0.1.
    const advertise_ip: []const u8 = args.announce orelse "127.0.0.1";

    // Register identify now the socket is bound, so the sealed record advertises
    // our (dialable-IP-rewritten) listen multiaddrs. The handler outlives the run
    // on this fiber's stack (the service borrows it); free its record on return.
    var listen_addrs = try advertisedListenAddrs(allocator, switcher, advertise_ip);
    defer {
        for (listen_addrs.items) |addr| allocator.free(addr);
        listen_addrs.deinit(allocator);
    }
    var id_handler: identify.IdentifyHandler = undefined;
    try registerIdentify(allocator, switcher, host_key, listen_addrs.items, &id_handler);
    defer id_handler.deinit();

    const published = try buildPublishedMultiaddr(allocator, switcher, host_key, advertise_ip);
    defer allocator.free(published);
    std.log.info("listener advertising {s}", .{published});

    if (args.addr_file) |path| {
        try writeAddrFile(io, path, published);
    }

    // Accept on a fiber so the run loop bounds the wait by duration (`accept()`
    // blocks and a peer might never connect). A star bootstrap is dialed by every
    // node, so the fiber loops and accepts EACH inbound connection, not just one.
    var accept_state = AcceptState{ .allocator = allocator, .io = io, .switcher = switcher, .gs = gs };
    var accept_future = try std.Io.concurrent(io, AcceptState.run, .{&accept_state});
    defer {
        // Error-path only (the success path hard-exits via `finishAndExit`).
        // `closeListener` already makes the accept fiber return, so cancel is a
        // no-op if it did.
        accept_future.cancel(io);
        accept_state.deinit();
    }

    // A listener may ALSO dial upstream peers (`peers=`), so it is both reachable
    // and a star member — what a PX-offered leaf needs (it must be dialable for the
    // pruner's offer target to reach it). Optional: a pure bootstrap dials nothing.
    var dial_conns: std.ArrayList(*libp2p.SwitchConnection) = .empty;
    defer {
        for (dial_conns.items) |conn| conn.deinit();
        dial_conns.deinit(allocator);
    }
    const dialed = try dialPeers(allocator, io, switcher, gs, args, &dial_conns);
    if (dialed != 0) std.log.info("listener also dialed {d} upstream peer(s)", .{dialed});

    // Always subscribe so the mesh can form (a sub-only node still needs the
    // subscription to receive; a pub node subscribes too so the peer learns it).
    try gs.subscribe(args.topic);

    // Watch for PX-driven peer growth (a peer we were OFFERED and dialed, beyond
    // the upstream peers we dialed ourselves). See `PeerCountMonitor`.
    var px_monitor = PeerCountMonitor{ .io = io, .gs = gs, .baseline = dialed };
    var px_future = try std.Io.concurrent(io, PeerCountMonitor.run, .{&px_monitor});
    defer px_future.cancel(io);

    try runForDuration(io, allocator, gs, args);

    // Quiesce inbound accepts: `closeListener` closes the accept queue so a parked
    // `accept()` wakes with `error.ListenerClosed` and returns cleanly. We do NOT
    // join the accept fiber — if it is mid-flight spawning a connection's
    // dispatcher, joining could wait on the per-connection actor teardown, which
    // can deadlock the runtime. Quiescing is enough; the hard-exit reclaims the
    // rest, so the teardown defers above are intentionally skipped.
    switcher.closeListener(io);

    finishAndExit(io, recorder, args);
}

// NOT `Switch.serve()`: this loop also runs identify as a CLIENT per inbound peer
// (`exchangeIdentify`, certifying records for PX), which needs per-connection
// access `serve()` does not expose and cannot move to the peer-event callback
// (gossipsub already owns it). So a manual accept loop is the right tool here.
const AcceptState = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    gs: *gossipsub.Gossipsub,
    conns: std.ArrayList(*libp2p.SwitchConnection) = .empty,

    /// Accept inbound connections in a loop, starting an inbound dispatcher +
    /// client-side identify for each. Returns on accept failure or cancellation
    /// (the run loop bounds the wait). A bootstrap must accept more than one
    /// connection for the mesh to span all peers.
    fn run(self: *AcceptState) void {
        while (true) {
            const conn = self.switcher.accept() catch |err| {
                switch (err) {
                    // Clean stops, not failures: `ListenerClosed` (graceful
                    // `closeListener`) and `Canceled` (direct fiber cancel).
                    error.ListenerClosed, error.Canceled => {},
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
            // Run identify as the CLIENT against inbound peers too: a PX emitter
            // can only vouch for a peer it holds a record for, so without this an
            // accept-only bootstrap would offer bare peer-ids (no dialable address)
            // and PX would not complete. go/rust run identify on every connection
            // regardless of dial direction. Best-effort — failures are logged.
            exchangeIdentify(self.allocator, self.io, conn, self.gs);
            // Accept fiber and teardown both touch `conns` but never concurrently:
            // teardown cancels (joins) this fiber before deinit, so appends are
            // sequenced before reads there. Drop the conn if we cannot record it.
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

/// Dial every configured peer (`peers`/`peer`/`peer_file`), starting an inbound
/// dispatcher + identify-as-client per connection and appending each to `conns`
/// (caller owns teardown). Returns the count dialed; a failed dial to one peer
/// does not abort the others. Shared by the dialer role and a listener that also
/// dials upstream.
fn dialPeers(
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    gs: *gossipsub.Gossipsub,
    args: *const Args,
    conns: *std.ArrayList(*libp2p.SwitchConnection),
) !usize {
    // A `peer_file` target: poll briefly for the file, then dial its contents (see
    // the `peer_file` field). Read once up front; `dialTargets` handles the direct
    // `peer`/`peers` entries.
    var file_peer_buf: [512]u8 = undefined;
    const file_peer: ?[]const u8 = if (args.peer_file) |path|
        readPeerFile(io, path, &file_peer_buf) catch |err| blk: {
            std.log.warn("peer_file {s} unreadable: {any}", .{ path, err });
            break :blk null;
        }
    else
        null;

    var it = args.dialTargets();
    var dialed: usize = 0;
    if (file_peer) |peer_text| {
        if (try dialOnePeer(allocator, io, switcher, gs, args, conns, peer_text)) dialed += 1;
    }
    while (it.next()) |peer_text| {
        if (try dialOnePeer(allocator, io, switcher, gs, args, conns, peer_text)) dialed += 1;
    }
    return dialed;
}

/// Dial one peer multiaddr: dial, start its inbound dispatcher, run
/// identify-as-client, append to `conns`. A bad multiaddr or failed dial/dispatch
/// is logged and returns false; only an OOM appending to `conns` propagates.
fn dialOnePeer(
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    gs: *gossipsub.Gossipsub,
    args: *const Args,
    conns: *std.ArrayList(*libp2p.SwitchConnection),
    peer_text: []const u8,
) !bool {
    std.log.info("dialing {s}", .{peer_text});
    var remote_addr = Multiaddr.fromString(allocator, peer_text) catch |err| {
        std.log.warn("invalid peer multiaddr {s}: {any}", .{ peer_text, err });
        return false;
    };
    defer remote_addr.deinit(allocator);

    const conn = switcher.dial(remote_addr, .{ .timeout = timeoutFromMs(args.duration_ms) }) catch |err| {
        std.log.warn("dial {s} failed: {any}", .{ peer_text, err });
        return false;
    };
    // Start inbound dispatch so this side's `/meshsub` inbound handler runs for
    // this peer; without it the peers wire up but never exchange RPCs.
    conn.startInboundDispatcher(.{}) catch |err| {
        std.log.warn("startInboundDispatcher for {s} failed: {any}", .{ peer_text, err });
        conn.deinit();
        return false;
    };
    // Run identify as the client against the just-dialed peer: read its identify
    // and certify its signed peer record into our gossipsub store (completing
    // peer-exchange). Best-effort — failures are logged, the connection stays up.
    exchangeIdentify(allocator, io, conn, gs);
    try conns.append(allocator, conn);
    return true;
}

/// Read a peer multiaddr written by a listener's `addr_file` into `buf`, returning
/// the trimmed first line. Polls for the file to appear and be non-empty (the
/// dialer may start before the listener has bound and written it). Bounds the wait
/// so a missing file fails rather than hanging the run.
fn readPeerFile(io: std.Io, path: []const u8, buf: []u8) ![]const u8 {
    const poll_interval_ms: u64 = 100;
    const max_wait_ms: u64 = 10_000;
    var waited_ms: u64 = 0;
    while (true) {
        if (readFileFirstLine(io, path, buf)) |line| {
            if (line.len != 0) return line;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }
        if (waited_ms >= max_wait_ms) return error.PeerFileTimeout;
        try timeoutFromMs(poll_interval_ms).sleep(io);
        waited_ms += poll_interval_ms;
    }
}

/// Read the first line of `path` into `buf`, trimmed of trailing whitespace. The
/// listener writes a single multiaddr line, so the read is bounded by `buf`.
fn readFileFirstLine(io: std.Io, path: []const u8, buf: []u8) ![]const u8 {
    const file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    // The reader needs its own buffer distinct from the read destination `buf`; a
    // small one suffices since a single short read drains the tiny address line.
    var reader_buf: [128]u8 = undefined;
    var reader = file.reader(io, &reader_buf);
    // A short read (fewer than `buf.len` bytes, including 0 at EOF) is expected —
    // the address line is small — so `readSliceShort` returns the byte count and
    // only surfaces a genuine `ReadFailed`.
    const n = try reader.interface.readSliceShort(buf);
    const contents = buf[0..n];
    const newline = std.mem.indexOfScalar(u8, contents, '\n') orelse contents.len;
    return std.mem.trim(u8, contents[0..newline], " \t\r\n");
}

/// Watches the gossipsub peer count for growth past the baseline (peers the node
/// dialed itself): a PX'd PRUNE makes the router dial a peer it was OFFERED, which
/// raises the count. Logs a single greppable `PX-CONNECT` line on the first rise
/// so a scenario can assert the PX dial happened. Own fiber; canceled on teardown.
const PeerCountMonitor = struct {
    io: std.Io,
    gs: *gossipsub.Gossipsub,
    /// Peers the node dialed itself; growth past this is a PX-offered connection.
    baseline: usize,

    fn run(self: *PeerCountMonitor) void {
        var announced = false;
        while (true) {
            const count = self.gs.peerCount();
            if (!announced and count > self.baseline) {
                announced = true;
                std.log.info("PX-CONNECT peer_count={d} baseline={d}", .{ count, self.baseline });
            }
            // Poll period well under a heartbeat; the run loop bounds the lifetime
            // and the fiber is canceled on teardown, so an unbounded loop is fine.
            timeoutFromMs(100).sleep(self.io) catch return;
        }
    }
};

fn runDialer(
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    host_key: *identity.KeyPair,
    recorder: *Recorder,
    gs: *gossipsub.Gossipsub,
    args: *const Args,
) !void {
    // Register identify (responder + sealed record). A dialer binds no socket, so
    // it advertises no listen addrs — its record names only its peer-id, still
    // letting a peer certify the (empty) set. The handler outlives the run here.
    var id_handler: identify.IdentifyHandler = undefined;
    try registerIdentify(allocator, switcher, host_key, &.{}, &id_handler);
    defer id_handler.deinit();

    // Every connection must stay alive for the whole run, so hold them in a list
    // and close them together on teardown.
    var conns: std.ArrayList(*libp2p.SwitchConnection) = .empty;
    defer {
        for (conns.items) |conn| conn.deinit();
        conns.deinit(allocator);
    }

    const dialed = try dialPeers(allocator, io, switcher, gs, args, &conns);
    std.log.info("dialer connected to {d} peer(s)", .{dialed});
    if (dialed == 0) return error.NoPeersDialed;

    // Always subscribe so the mesh can form on both ends.
    try gs.subscribe(args.topic);

    // Watch the gossipsub peer count for growth beyond what we explicitly dialed:
    // a PX-offered peer we dial because the bootstrap pruned us with PX shows up
    // as an EXTRA peer here, which is the observable proof of a cross-impl PX dial.
    var px_monitor = PeerCountMonitor{ .io = io, .gs = gs, .baseline = dialed };
    var px_future = try std.Io.concurrent(io, PeerCountMonitor.run, .{&px_monitor});
    defer px_future.cancel(io);

    try runForDuration(io, allocator, gs, args);

    // Hard-exit like the listener: a dialer has no accept fiber, but each dialed
    // connection runs an inbound dispatcher in the same uncancelable QUIC
    // `acceptStream`, so this keeps shutdown uniform and immune to that stall.
    finishAndExit(io, recorder, args);
}

/// Run until `duration_ms` elapses. In publish mode, republish every
/// `publish_interval_ms` so at least one publish lands after the mesh forms;
/// subscribe mode just waits (deliveries arrive on the router fiber).
///
/// Under anonymous the content-derived id means republishing an IDENTICAL payload
/// is suppressed by the seen-cache (which fires before the mesh grafts → nothing
/// propagates). So append an incrementing `#<n>` suffix for a fresh content-id per
/// publish; a subscriber asserts on the PREFIX. Signed publishes carry a fresh
/// seqno, so they keep the plain payload.
fn runForDuration(io: std.Io, allocator: std.mem.Allocator, gs: *gossipsub.Gossipsub, args: *const Args) !void {
    if (args.mode != .publish) {
        try timeoutFromMs(args.duration_ms).sleep(io);
        return;
    }

    // Size to the FULL payload plus the suffix: a fixed buffer would truncate a
    // large payload, so its content-id would never change and the seen-cache would
    // suppress every anonymous publish after the (pre-mesh) first → no propagation.
    const msg_buf = try allocator.alloc(u8, args.message.len + 32);
    defer allocator.free(msg_buf);
    var seq: u64 = 0;
    var elapsed_ms: u64 = 0;
    while (elapsed_ms < args.duration_ms) : (elapsed_ms += publish_interval_ms) {
        // `msg_buf` is sized past the suffix so bufPrint cannot overflow; `try`
        // surfaces a real failure rather than silently dropping the suffix.
        const payload: []const u8 = if (args.sign == .anonymous)
            try std.fmt.bufPrint(msg_buf, "{s}#{d}", .{ args.message, seq })
        else
            args.message;
        seq += 1;
        gs.publish(args.topic, payload) catch |err| {
            std.log.warn("publish failed: {any}", .{err});
        };
        const step = @min(publish_interval_ms, args.duration_ms - elapsed_ms);
        try timeoutFromMs(step).sleep(io);
    }
}

/// The Switch's listen multiaddrs with any wildcard `/ip4/0.0.0.0/` (not dialable)
/// rewritten to `/ip4/<advertise_ip>/`, so a peer that learns the address via a
/// signed peer record or identify can dial it. An address already bound to a
/// specific IP passes through unchanged. Caller owns each returned string.
fn advertisedListenAddrs(
    allocator: std.mem.Allocator,
    switcher: *libp2p.Switch,
    advertise_ip: []const u8,
) !std.ArrayList([]u8) {
    var bound = try switcher.listenMultiaddrs(allocator);
    defer {
        for (bound.items) |addr| allocator.free(addr);
        bound.deinit(allocator);
    }
    var out: std.ArrayList([]u8) = .empty;
    errdefer {
        for (out.items) |addr| allocator.free(addr);
        out.deinit(allocator);
    }
    const wildcard = "/ip4/0.0.0.0/";
    for (bound.items) |addr| {
        const rewritten = if (std.mem.indexOf(u8, addr, wildcard)) |_| blk: {
            const replacement = try std.fmt.allocPrint(allocator, "/ip4/{s}/", .{advertise_ip});
            defer allocator.free(replacement);
            break :blk try std.mem.replaceOwned(u8, allocator, addr, wildcard, replacement);
        } else try allocator.dupe(u8, addr);
        try out.append(allocator, rewritten);
    }
    return out;
}

/// Build the full `/ip4/<advertise_ip>/udp/<port>/quic-v1/p2p/<peer-id>` multiaddr
/// from the Switch's first bound listen port and this node's peer id. Names the
/// dialable `advertise_ip`, never the possibly-wildcard bound IP.
fn buildPublishedMultiaddr(
    allocator: std.mem.Allocator,
    switcher: *libp2p.Switch,
    host_key: *identity.KeyPair,
    advertise_ip: []const u8,
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

    return std.fmt.allocPrint(
        allocator,
        "/ip4/{s}/udp/{d}/quic-v1/p2p/{s}",
        .{ advertise_ip, parsed.address.getPort(), peer_text },
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

/// Emit `bytes` to stdout with a STREAMING writer: we write more than once and
/// stdout is often redirected to a file, where a positional writer would seek to
/// offset 0 each call and overwrite. Streaming appends at the fd's position,
/// correct for both pipes and files.
fn printToStdout(io: std.Io, bytes: []const u8) !void {
    const stdout_file: std.Io.File = .{ .handle = std.posix.STDOUT_FILENO, .flags = .{ .nonblocking = false } };
    var buf: [max_print_len]u8 = undefined;
    var writer = stdout_file.writerStreaming(io, &buf);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

fn timeoutFromMs(ms: u64) std.Io.Timeout {
    const ns = std.math.mul(u64, ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
    return .{ .duration = .{ .raw = .fromNanoseconds(@intCast(ns)), .clock = .awake } };
}
