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
//! Shadow support: `shadow=on` runs the QUIC endpoint in Shadow-compatible mode
//! (plain `recvmsg`/`sendmsg`, no GRO/GSO/`IP_PKTINFO`/timestamp cmsgs — the only
//! socket form the Shadow network simulator supports), and `announce=<ip>` makes
//! the listener bind and advertise a specific IP instead of the wildcard
//! `0.0.0.0`/loopback `127.0.0.1` (Shadow hosts are single-homed, and a dialer
//! must reach the listener's assigned IP). Both are additive and OFF by default,
//! so every non-Shadow scenario is unchanged.
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
const testplans = @import("testplans.zig");

// Pull in testplans.zig's in-file tests when this module is built as a test
// artifact (`zig build test`).
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
    /// Read a single peer multiaddr to dial from this file (`peer_file=<path>`),
    /// the read counterpart of the listener's `addr_file=` write. For Shadow
    /// coordination: the listener writes its published `/p2p/<peer-id>` multiaddr
    /// to a path on the shared host filesystem, and the dialer (started a little
    /// later) reads it — no out-of-band peer-id exchange needed. The dialer polls
    /// briefly for the file to appear and be non-empty, tolerating the listener's
    /// startup. Additive to `peer`/`peers`; all configured targets are dialed.
    peer_file: ?[]const u8 = null,
    topic: []const u8 = "interop-test",
    message: []const u8 = "hello-gossipsub",
    duration_ms: u64 = 5000,
    /// Signing scheme: `strict` (default, signs+verifies) or `anonymous`
    /// (StrictNoSign, content-id dedup). See `Sign`.
    sign: Sign = .strict,
    /// Enable peer-exchange (PX) on PRUNE (go-libp2p `WithPeerExchange`). OFF by
    /// default — the cross-impl PX scenario opts in with `px=on`. When ON, an
    /// over-degree heartbeat prune offers the surviving mesh peers' signed
    /// records, and an inbound PX'd PRUNE makes us dial the offered peers.
    px: bool = false,
    /// Override the per-topic mesh DEGREE targets (`d`/`d_low`/`d_high`). Null
    /// keeps the go-libp2p baseline (6/5/12). A PX emitter sets a small `d_high`
    /// (e.g. `d_high=2`) so a tiny topology goes over-degree and the heartbeat
    /// prunes a peer WITH PX. Each is set independently; defaults fill the rest.
    d: ?usize = null,
    d_low: ?usize = null,
    d_high: ?usize = null,
    /// Run the QUIC endpoint in Shadow-compatible mode (`shadow=on`). When set,
    /// the UDP path is restricted to plain `recvmsg`/`sendmsg` with no ancillary
    /// control data — no GRO/GSO, no `IP_PKTINFO`, no per-packet timestamps —
    /// which is the only socket form the Shadow network simulator supports. OFF
    /// by default, so every non-Shadow scenario is unchanged. See `EndpointOptions
    /// .shadow_compatible`.
    shadow: bool = false,
    /// The IP address this listener advertises (`announce=<ip>`). Under Shadow each
    /// host is single-homed with one assigned IP, and a wildcard `0.0.0.0` bind is
    /// not dialable, so the listener must publish the host's real IP for a dialer to
    /// reach it. When set, the listener BINDS to this IP (giving quiche a correct
    /// single-homed local path address — Shadow has no `IP_PKTINFO`) and PUBLISHES
    /// it in the addr_file, the identify listen addrs, and the sealed peer record.
    /// When absent, the listener keeps the wildcard `0.0.0.0` bind and advertises
    /// the loopback `127.0.0.1` (the on-host smoke-run behavior).
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
/// none is set (keep the go-libp2p baseline 6/5/12). Any unset field falls back
/// to that baseline so a scenario can lower just `d_high` to force an over-degree
/// prune. A PX emitter typically sets `d_high` small; the values should stay
/// consistent (`d_low <= d <= d_high`) — the router does not validate them.
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

    // test-plans mode: when invoked as `<bin> --params <path>` (the official
    // libp2p gossipsub-interop framework's exact invocation), run as a framework
    // participant — deterministic key from the Shadow hostname, params.json script,
    // analyzer-format tracer log. Every other invocation keeps the key=value
    // smoke-test behavior below unchanged. See testplans.zig.
    if (try testPlansParamsPath(init.arena.allocator(), init.minimal.args)) |params_path| {
        const tp_opts = try testPlansOptions(init.arena.allocator(), init.minimal.args);
        return testplans.run(allocator, io, params_path, tp_opts);
    }

    const args = try Args.parse(init.arena.allocator(), init.minimal.args);

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();
    // In Shadow mode restrict the QUIC UDP path to plain `recvmsg`/`sendmsg` with
    // no ancillary control data: the Shadow simulator supports neither GRO/GSO nor
    // `IP_PKTINFO`/timestamp cmsgs. OFF by default, so the non-Shadow scenarios use
    // the full control-message path unchanged.
    const endpoint = try libp2p.QuicEndpoint.initWithIdentity(allocator, io, &host_key, .{
        .endpoint = .{ .shadow_compatible = args.shadow },
    });
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
    var gs_config: gossipsub.RouterConfig = switch (args.sign) {
        .strict => .{},
        .anonymous => .{ .signature_policy = .anonymous },
    };
    // Peer-exchange opt-in (default OFF). When `px=on`, the router offers signed
    // records on an over-degree prune and dials peers offered to it. The optional
    // mesh-degree override lets a tiny topology go over-degree so the heartbeat
    // prunes WITH PX — e.g. a `px=on d_high=2` bootstrap dialed by 3 leaves prunes
    // one of them, offering the others' records.
    gs_config.peer_exchange_enabled = args.px;
    gs_config.mesh_degree = meshDegreeFromArgs(&args);
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
    // Bind on the announce IP when given (single-homed Shadow host), else the
    // wildcard `0.0.0.0` (the on-host smoke runs). Binding the specific IP gives
    // quiche a correct local path address under Shadow mode, where the per-packet
    // local-destination (`IP_PKTINFO`) is unavailable and the loop falls back to
    // the socket's bound address.
    const bind_ip: []const u8 = args.announce orelse "0.0.0.0";
    const listen_text = try std.fmt.allocPrint(allocator, "/ip4/{s}/udp/{d}/quic-v1", .{ bind_ip, args.port });
    defer allocator.free(listen_text);
    var listen_addr = try Multiaddr.fromString(allocator, listen_text);
    defer listen_addr.deinit(allocator);
    try switcher.listen(listen_addr);
    std.log.info("listener bound {s}", .{listen_text});

    // The IP a peer must DIAL to reach us. The Switch may bind the wildcard
    // 0.0.0.0 (not dialable) or a single-homed Shadow IP; either way the
    // advertised address must name a reachable IP. Use the announce IP when given
    // (the Shadow host's assigned IP), else the loopback 127.0.0.1 of the on-host
    // smoke runs.
    const advertise_ip: []const u8 = args.announce orelse "127.0.0.1";

    // Register identify now that the socket is bound, so the sealed peer record
    // advertises our listen multiaddrs. The bound address may be the wildcard
    // 0.0.0.0, but a peer that learns this record (PX) must be able to DIAL it,
    // and 0.0.0.0 is not dialable — rewrite each to the advertised IP (the same
    // address `buildPublishedMultiaddr` advertises). The handler outlives the run
    // on this fiber's stack (the service borrows it); free its owned record on
    // return.
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

    // Accept inbound connections on a fiber so the run loop bounds the wait by
    // duration: a peer might never connect, and `accept()` blocks otherwise. The
    // bootstrap of a star is dialed by every other node, so the fiber loops and
    // accepts EACH inbound connection (not just the first), starting a dispatcher
    // per connection and stashing it for teardown.
    var accept_state = AcceptState{ .allocator = allocator, .io = io, .switcher = switcher, .gs = gs };
    var accept_future = try std.Io.concurrent(io, AcceptState.run, .{&accept_state});
    defer {
        // Error-path teardown only. The success path hard-exits via
        // `finishAndExit` (noreturn), so this defer does not run there; it cleans
        // up when the run fails to start. `closeListener` makes the accept fiber
        // return, so cancel here is a no-op if it already did.
        accept_future.cancel(io);
        accept_state.deinit();
    }

    // A listener may ALSO dial upstream peers (`peers=`), making it a node that is
    // both reachable (bound + accepting) AND a member of a star — exactly what a
    // PX-offered leaf needs: it must be dialable so the peer the pruner offers it
    // to can reach it. Dials are optional here (a pure bootstrap dials nothing).
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
    io: std.Io,
    switcher: *libp2p.Switch,
    gs: *gossipsub.Gossipsub,
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
            // Run identify as the CLIENT against this inbound peer too, so a
            // listener certifies its inbound peers' signed records into the cert
            // store. A PX emitter (an over-degree listener) can only vouch for a
            // peer it holds a record for, so without this an accept-only bootstrap
            // would offer bare peer-ids (no dialable address) and PX would not
            // complete. go/rust run identify on every connection regardless of dial
            // direction; this matches that. Best-effort — failures are logged.
            exchangeIdentify(self.allocator, self.io, conn, self.gs);
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

/// Dial every configured peer (`peers`/`peer`), start an inbound dispatcher and
/// run identify-as-client against each, and append the live connection to `conns`
/// (the caller owns teardown). Returns how many peers were successfully dialed.
/// gossipsub forms a mesh over whatever connections exist, so a node that dials
/// several peers grafts into a mesh spanning all of them; a failed dial to one
/// peer does not abort the others. Shared by the dialer role and a listener that
/// also dials upstream (a leaf that is both reachable and joins a star).
fn dialPeers(
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    gs: *gossipsub.Gossipsub,
    args: *const Args,
    conns: *std.ArrayList(*libp2p.SwitchConnection),
) !usize {
    // A `peer_file` target (the read counterpart of the listener's `addr_file`):
    // poll briefly for the file to appear and be non-empty, then dial its contents.
    // Used for Shadow coordination where the listener publishes its multiaddr to
    // the shared host filesystem. Read once up front; `dialTargets` handles the
    // direct `peer`/`peers` entries.
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

/// Dial a single peer multiaddr: dial, start its inbound dispatcher, run
/// identify-as-client, and append the live connection to `conns`. Returns true on
/// success. A bad multiaddr or a failed dial/dispatch is logged and returns false
/// (so one failed peer does not abort the rest); only an OOM appending to `conns`
/// propagates.
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

/// Watches gossipsub's peer count for growth beyond a baseline (the number of
/// peers the node explicitly dialed). When a PX'd PRUNE makes the router dial a
/// peer it was OFFERED — one it never dialed itself — that peer connects and the
/// count rises above the baseline. The monitor logs a single greppable
/// `PX-CONNECT` line on the first such rise so a scenario can assert the
/// cross-impl PX dial happened. Runs on its own fiber for the whole run; the
/// caller cancels it on teardown.
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

/// The Switch's listen multiaddrs with any wildcard `/ip4/0.0.0.0/` rewritten to
/// the dialable `/ip4/<advertise_ip>/`. The Switch may bind the wildcard 0.0.0.0,
/// which is NOT a dialable address — a peer that learns this address through a
/// signed peer record (peer-exchange) or identify must be able to dial it. The
/// `advertise_ip` is 127.0.0.1 for on-host runs or the assigned Shadow IP under
/// the simulator (and matches `buildPublishedMultiaddr`). An address already bound
/// to a specific IP (the Shadow case binds the announce IP directly) is passed
/// through unchanged. Caller owns each returned string.
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
/// from the Switch's first bound listen port and this node's peer id. The Switch
/// may bind the wildcard 0.0.0.0 (not dialable) or a specific IP; either way the
/// advertised multiaddr names `advertise_ip` — 127.0.0.1 for on-host runs, the
/// assigned Shadow IP under the simulator — at the bound port.
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
