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

/// Upper bound on a printed RECV line (and any single stdout write). Sized to
/// hold the topic, a hex author id, and a data payload well above the 1 KiB
/// gossipsub v1.2 IDONTWANT threshold, so the large-message interop scenario's
/// payload prints (and matches) intact rather than being truncated/dropped.
const max_print_len: usize = 8192;

const Role = enum { listen, dial };
const Mode = enum { publish, subscribe };

/// Which signing scheme this node runs. `strict` signs every outbound message
/// and verifies inbound (the go-libp2p default, and what the base mixed-mesh
/// runs use). `anonymous` is StrictNoSign: messages carry no author/seqno, are
/// keyed by a content-derived id (`sha256(topic ++ data)`), and inbound is not
/// verified — every node in the topic must agree on the SAME id scheme or dedup
/// breaks, which is exactly what the anonymous cross-impl scenario checks.
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
    topic: []const u8 = "interop-test",
    message: []const u8 = "hello-gossipsub",
    duration_ms: u64 = 5000,
    /// Signing scheme: `strict` (default, signs+verifies) or `anonymous`
    /// (StrictNoSign, content-id dedup). See `Sign`.
    sign: Sign = .strict,

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
            } else if (std.mem.eql(u8, key, "sign")) {
                if (std.mem.eql(u8, value, "strict")) {
                    out.sign = .strict;
                } else if (std.mem.eql(u8, value, "anonymous") or std.mem.eql(u8, value, "nosign")) {
                    out.sign = .anonymous;
                } else return error.InvalidSign;
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
        // A clear, greppable line so the orchestrator can assert delivery. `from`
        // is the message AUTHOR's peer-id (raw bytes, printed as hex to stay
        // printable). Under the anonymous policy it is empty, so `author=` prints
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

    var recorder = Recorder{ .io = io };
    // Construct gossipsub BEFORE any dial/accept so its peer-event callback is
    // registered when the connection comes up.
    //
    // In `strict` mode pass the host key to enable StrictSign (sign outbound,
    // verify inbound) so go-libp2p — which defaults to StrictSign — accepts our
    // published messages and we accept its signed ones. In `anonymous` mode pass
    // NO key and the anonymous (StrictNoSign) policy: messages carry no author,
    // inbound is not verified, and dedup keys on the content id
    // `sha256(topic ++ data)` (the policy's built-in derivation), which the go and
    // rust peers must match for cross-impl propagation to line up.
    //
    // Scoring is left disabled here (null): the interop binary exercises the
    // base mesh/gossip behaviour against go/rust-libp2p without the score gates.
    const host_key_opt: ?*const identity.KeyPair = if (args.sign == .strict) &host_key else null;
    const gs_config: gossipsub.RouterConfig = switch (args.sign) {
        .strict => .{},
        .anonymous => .{ .signature_policy = .anonymous },
    };
    const gs = try gossipsub.Gossipsub.init(allocator, io, switcher, local_peer, host_key_opt, recorder.handler(), null, gs_config);
    // Only runs on an error return from `runListener`/`runDialer` (the run never
    // started). The success path emits its result and hard-exits from within the
    // run functions — see the note on `finishAndExit` for why a test/interop node
    // hard-exits rather than unwinding the per-connection teardown.
    defer gs.deinit();

    // `runListener` / `runDialer` run until the duration elapses, then emit the
    // result JSON and hard-exit (via `finishAndExit`). Control therefore does NOT
    // return here on the success path; this switch only returns when the run fails
    // to start, in which case the error propagates out of `main` and the deferred
    // teardown above runs normally.
    switch (args.role) {
        .listen => try runListener(allocator, io, switcher, &host_key, &recorder, gs, &args),
        .dial => try runDialer(allocator, io, switcher, &host_key, &recorder, gs, &args),
    }
}

/// Register the identify responder, advertising our protocols AND a signed peer
/// record (libp2p identify field 8) sealed over our `listen_addrs` so go/rust can
/// certify our addresses (and peer-exchange us). go-libp2p (and rust-libp2p) learn
/// which protocols a peer speaks via the `/ipfs/id/1.0.0` exchange, and their
/// pubsub only opens a `/meshsub` stream once identify reports meshsub — so the
/// response advertises identify plus every `/meshsub` version we speak (1.2.0,
/// 1.1.0, 1.0.0); the peer negotiates the best common one. The handler only reads
/// its immutable options (and the sealed record it owns), so one instance serves
/// every inbound identify stream; the caller keeps it alive for the whole run.
/// `listen_addrs` are the string-form listen multiaddrs identify already sends —
/// the sealed record uses the SAME bytes so its addresses match `listenAddrs`.
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
        // This node seals exactly one record per process, so a fixed seq is enough
        // (the certified-address book keeps the highest-seq record per peer; a fresh
        // process always advertises seq 1). A long-lived node that re-published its
        // record after an address change would bump a monotonic counter here.
        1,
    );
    try switcher.addProtocolService(
        identify.protocol_id,
        libp2p.protocols.streamHandlerService(identify.IdentifyHandler, identify.IdentifyHandler.run, handler_out),
    );
}

/// Run the client side of identify against a freshly-connected `conn`: open
/// `/ipfs/id/1.0.0`, read the peer's identify, and if it carries a verified
/// `signedPeerRecord` for this peer, hand it to gossipsub's certified-record store
/// (so our peer-exchange can vouch for the peer). Best-effort: any failure is
/// logged and ignored — identify-as-responder still works, and the connection is
/// already up. This mirrors how go/rust run identify as the dialer after
/// connecting; the record consume is what populates PX end-to-end.
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

/// Emit the result, then terminate the process immediately rather than unwinding
/// the fiber teardown. The connection-teardown deadlock that once made shutdown
/// unreliable is fixed at the library level (Switch teardown no longer relies on
/// a cross-executor cancel), but a short-lived test/interop node has no reason to
/// orchestrate the multi-layer graceful teardown (gossipsub, then its
/// connections, then the switch, then the endpoint) — emit then exit(0) keeps
/// every node's shutdown prompt and robust.
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
    const listen_text = try std.fmt.allocPrint(allocator, "/ip4/0.0.0.0/udp/{d}/quic-v1", .{args.port});
    defer allocator.free(listen_text);
    var listen_addr = try Multiaddr.fromString(allocator, listen_text);
    defer listen_addr.deinit(allocator);
    try switcher.listen(listen_addr);
    std.log.info("listener bound {s}", .{listen_text});

    // Register identify now that the socket is bound, so the sealed peer record
    // advertises our real listen multiaddrs. The handler outlives the run on this
    // fiber's stack (the service borrows it); free its owned record on return.
    var listen_addrs = try switcher.listenMultiaddrs(allocator);
    defer {
        for (listen_addrs.items) |addr| allocator.free(addr);
        listen_addrs.deinit(allocator);
    }
    var id_handler: identify.IdentifyHandler = undefined;
    try registerIdentify(allocator, switcher, host_key, listen_addrs.items, &id_handler);
    defer id_handler.deinit();

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
        // Error-path teardown only. The success path hard-exits via
        // `finishAndExit` (noreturn), so this defer does not run there; it cleans
        // up when the run fails to start. `closeListener` makes the accept fiber
        // return, so cancel here is a no-op if it already did.
        accept_future.cancel(io);
        accept_state.deinit();
    }

    // Always subscribe so the mesh can form (a sub-only node still needs the
    // subscription to receive; a pub node subscribes too so the peer learns it).
    try gs.subscribe(args.topic);

    try runForDuration(io, gs, args);

    // Graceful quiesce of inbound accepts: `Switch.closeListener` closes the
    // accept queue, so an accept fiber parked in `accept()` wakes with
    // `error.ListenerClosed` and returns cleanly (no cancel of a blocked accept,
    // no lost-wake hang). We do NOT then block on joining the accept fiber: if it
    // is mid-flight handling a just-accepted connection (spawning that
    // connection's inbound dispatcher), joining could wait on the per-connection
    // actor teardown, which intermittently deadlocks the cooperative runtime — the
    // same orthogonal connection-lifecycle issue described on `finishAndExit`.
    // Quiescing is enough; the hard-exit reclaims the rest.
    switcher.closeListener(io);

    // Emit the result and hard-exit. `finishAndExit` never returns, so the
    // accept_state/connection teardown defers above are intentionally skipped.
    finishAndExit(io, recorder, args);
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
                    // Both are clean stops: `ListenerClosed` is the graceful
                    // shutdown signal from `Switch.closeListener`; `Canceled` is
                    // a direct fiber cancel. Neither is a failure to log.
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
    host_key: *identity.KeyPair,
    recorder: *Recorder,
    gs: *gossipsub.Gossipsub,
    args: *const Args,
) !void {
    // Register identify (responder + sealed peer record). A dialer does not bind a
    // listen socket, so it advertises no listen addrs — its record names only its
    // peer-id, still letting a peer certify the (empty) address set and confirming
    // identify works both ways. The handler outlives the run on this stack.
    var id_handler: identify.IdentifyHandler = undefined;
    try registerIdentify(allocator, switcher, host_key, &.{}, &id_handler);
    defer id_handler.deinit();

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
        // Run identify as the client against the just-dialed peer: read its
        // identify and certify its signed peer record into our gossipsub store
        // (completing peer-exchange). Best-effort — failures are logged, the
        // connection stays up.
        exchangeIdentify(allocator, io, conn, gs);
        try conns.append(allocator, conn);
        dialed += 1;
    }
    std.log.info("dialer connected to {d} peer(s)", .{dialed});
    if (dialed == 0) return error.NoPeersDialed;

    // Always subscribe so the mesh can form on both ends.
    try gs.subscribe(args.topic);

    try runForDuration(io, gs, args);

    // Emit the result and hard-exit, matching the listener path so every node
    // terminates promptly. A dialer has no blocking accept fiber, but hard-exiting
    // keeps shutdown uniform and immune to the per-connection actor teardown stall
    // (each dialed connection runs an inbound dispatcher in the same uncancelable
    // QUIC `acceptStream` as the listener's). `finishAndExit` never returns.
    finishAndExit(io, recorder, args);
}

/// Run until `duration_ms` elapses. In publish mode, republish every
/// `publish_interval_ms` so at least one publish lands after the mesh forms; in
/// subscribe mode, just wait (deliveries arrive on the router fiber).
///
/// Under the anonymous policy the message-id is content-derived
/// (`sha256(topic ++ data)`), so republishing the IDENTICAL payload yields the
/// same id every time and the router's seen-cache suppresses every publish after
/// the first — which fires before the mesh has grafted, so nothing propagates.
/// (go-libp2p's StrictNoSign behaves the same: it checks the seen-cache before
/// publishing.) To keep the republish-until-meshed strategy working under
/// anonymous, append an incrementing suffix so each publish has a UNIQUE payload
/// (hence a fresh content-id) and propagates once the mesh is up. The suffix is
/// `#<n>`, so a subscriber asserts on the message PREFIX. Signed publishes carry
/// a fresh seqno each time, so they keep the plain payload.
fn runForDuration(io: std.Io, gs: *gossipsub.Gossipsub, args: *const Args) !void {
    if (args.mode != .publish) {
        try timeoutFromMs(args.duration_ms).sleep(io);
        return;
    }

    var msg_buf: [max_print_len]u8 = undefined;
    var seq: u64 = 0;
    var elapsed_ms: u64 = 0;
    while (elapsed_ms < args.duration_ms) : (elapsed_ms += publish_interval_ms) {
        const payload: []const u8 = if (args.sign == .anonymous)
            std.fmt.bufPrint(&msg_buf, "{s}#{d}", .{ args.message, seq }) catch args.message
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
    var buf: [max_print_len]u8 = undefined;
    var writer = stdout_file.writerStreaming(io, &buf);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

fn timeoutFromMs(ms: u64) std.Io.Timeout {
    const ns = std.math.mul(u64, ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
    return .{ .duration = .{ .raw = .fromNanoseconds(@intCast(ns)), .clock = .awake } };
}
