//! test-plans gossipsub-interop participant mode.
//!
//! This makes the gossipsub interop node a drop-in participant in the official
//! libp2p `test-plans/gossipsub-interop` framework, run under the Shadow network
//! simulator over QUIC alongside the go-libp2p and rust-libp2p reference nodes.
//! It is entered ONLY when the binary is invoked as `<bin> --params <path.json>`
//! (the exact invocation the framework uses); every other invocation keeps the
//! existing `key=value` smoke-test behavior in `main.zig` untouched.
//!
//! The framework gives each node NO out-of-band coordination — nodes find each
//! other by convention, so every implementation must agree byte-for-byte on:
//!
//!   * Identity: the host is named `node<N>` (Shadow hostname). The Ed25519 key
//!     is built from a 32-byte seed whose first 8 bytes are `little-endian(N)`
//!     and the rest zero (Go's `ed25519.NewKeyFromSeed`). The derived libp2p
//!     peer-id is therefore identical across implementations, so any node can
//!     compute any other node's peer-id from just its number.
//!
//!   * Connector: to reach node `i`, resolve the DNS name `node<i>` (Shadow's
//!     resolver returns its IP), compute node `i`'s peer-id from the seed above,
//!     and dial `/ip4/<ip>/udp/9000/quic-v1/p2p/<peerId>`.
//!
//!   * Pubsub: StrictNoSign + NoAuthor (anonymous), message-id = the first 8
//!     bytes of the message data (the framework's `CalcID`), max message size
//!     10 MiB. A published message is `big-endian(messageID)` zero-padded to the
//!     requested size, so its first 8 bytes are the message number.
//!
//!   * Tracer log: JSON lines on STDOUT that the framework's
//!     `analyze_message_deliveries.py` parses — a `PeerID` line mapping node-id
//!     to peer-id, and a `Received Message` line per delivered message carrying
//!     the topic and the message-id rendered as the DECIMAL of its big-endian
//!     u64 (the analyzer's `formatMessageID`).
//!
//! The node executes a `params.json` script: a list of instructions
//! (initGossipSub / setTopicValidationDelay / connect / subscribeToTopic /
//! waitUntil / publish, optionally wrapped in ifNodeIDEquals) that every
//! implementation runs identically so the experiment is reproducible.

const std = @import("std");
const libp2p = @import("zig-libp2p");
const zio = @import("zio");
const identity = libp2p.identity;
const gossipsub = libp2p.gossipsub;
const identify = libp2p.protocols.identify;
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;

/// The QUIC listen/dial port every participant uses (the framework convention).
const listen_port: u16 = 9000;
/// gossipsub max message size (go reference: `WithMaxMessageSize(10 * 1 << 20)`).
const max_message_size: usize = 10 * (1 << 20);
/// How many topic-message validations may run off the router fiber at once. The
/// scenario installs a small per-message validation delay; running it async (go's
/// validator-worker model, `WithValidateQueueSize(600)`) keeps the delay from
/// stalling the single router fiber.
const validation_concurrency: usize = 1024;

/// Build the deterministic Ed25519 key for `node_id`: a 32-byte seed with
/// `little-endian(node_id)` in the first 8 bytes, zero after. Matches Go's
/// `ed25519.NewKeyFromSeed(seed)`, so the derived peer-id is byte-identical
/// across implementations.
fn nodeKey(node_id: u64) !identity.KeyPair {
    var seed = [_]u8{0} ** 32;
    std.mem.writeInt(u64, seed[0..8], node_id, .little);
    return identity.KeyPair.fromEd25519Seed(&seed);
}

/// Compute the base58 peer-id string of `node_id` (its deterministic key's
/// peer-id). Caller owns the returned string.
fn nodePeerIdString(allocator: std.mem.Allocator, node_id: u64) ![]u8 {
    var kp = try nodeKey(node_id);
    defer kp.deinit();
    const pid = try kp.peerId(allocator);
    return pid.toString(allocator);
}

/// The framework message-id (`CalcID`): the first 8 bytes of the message data.
/// All nodes in a topic MUST use the same function or dedup/interop breaks. A
/// shorter-than-8-byte message yields its whole content as the id (matching the
/// "first 8 bytes" slice on a short buffer).
fn calcId(
    ctx: ?*anyopaque,
    topic: []const u8,
    from: []const u8,
    seqno: []const u8,
    data: []const u8,
    allocator: std.mem.Allocator,
) anyerror!gossipsub.MessageId {
    _ = ctx;
    _ = topic;
    _ = from;
    _ = seqno;
    const n = @min(data.len, 8);
    const bytes = try allocator.dupe(u8, data[0..n]);
    return .{ .bytes = bytes };
}

/// A per-topic validation delay (the scenario's `setTopicValidationDelay`): when
/// installed it accepts every message but only AFTER sleeping `delay`, mocking a
/// validation cost. Runs on an async validation fiber (a zio fiber), so the sleep
/// is a normal cooperative suspension and does not block the router.
const Validator = struct {
    io: std.Io,
    /// Validation delay in nanoseconds (0 = accept immediately).
    delay_ns: u64 = 0,

    fn messageValidator(self: *Validator) gossipsub.MessageValidator {
        return .{ .ctx = self, .validate = validate };
    }

    fn validate(ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) gossipsub.ValidationResult {
        const self: *Validator = @ptrCast(@alignCast(ctx));
        _ = topic;
        _ = from;
        _ = data;
        if (self.delay_ns != 0) {
            const timeout = std.Io.Timeout{ .duration = .{ .raw = .fromNanoseconds(@intCast(self.delay_ns)), .clock = .awake } };
            timeout.sleep(self.io) catch {};
        }
        return .accept;
    }
};

/// Emits the tracer JSON lines `analyze_message_deliveries.py` parses, and records
/// every delivered message so a run can be inspected. The router fiber calls
/// `onMessage`; the timestamp comes from the wall clock (Shadow virtualizes it).
const Tracer = struct {
    io: std.Io,
    node_id: u64,

    fn handler(self: *Tracer) gossipsub.MessageHandler {
        return .{ .ctx = self, .on_message = onMessage };
    }

    /// A delivered message. The analyzer keys on the message-id rendered as the
    /// decimal of its big-endian u64 (its `formatMessageID`), so emit exactly
    /// that. The id bytes are the first 8 bytes of the data (`CalcID`); a message
    /// shorter than 8 bytes is zero-extended on the left, matching how the analyzer
    /// would read `binary.BigEndian.Uint64` on the padded id.
    fn onMessage(ctx: *anyopaque, topic: []const u8, from: []const u8, data: []const u8) void {
        const self: *Tracer = @ptrCast(@alignCast(ctx));
        _ = from;
        var id_buf = [_]u8{0} ** 8;
        const n = @min(data.len, 8);
        // Right-align the id bytes so a short payload reads as the same u64 the
        // analyzer computes (big-endian).
        @memcpy(id_buf[8 - n ..], data[0..n]);
        const msg_id = std.mem.readInt(u64, &id_buf, .big);
        emitReceived(self.io, self.node_id, topic, msg_id);
    }
};

/// Write a single JSON log line to STDOUT with a direct `write(2)` so it cannot be
/// lost if the cooperative runtime is busy at shutdown. The framework reads each
/// host's `.stdout`, so STDOUT (not STDERR) is the contract.
fn emitJsonLine(line: []const u8) void {
    var off: usize = 0;
    while (off < line.len) {
        const n = std.c.write(std.posix.STDOUT_FILENO, line.ptr + off, line.len - off);
        if (n <= 0) break;
        off += @intCast(n);
    }
}

/// Emit the `PeerID` line mapping this node's number to its peer-id, which the
/// analyzer uses to count total nodes and map peer-ids back to node-ids.
fn emitPeerId(allocator: std.mem.Allocator, node_id: u64, peer_id_str: []const u8) void {
    var buf: [256]u8 = undefined;
    const line = std.fmt.bufPrint(
        &buf,
        "{{\"msg\":\"PeerID\",\"node_id\":{d},\"id\":\"{s}\"}}\n",
        .{ node_id, peer_id_str },
    ) catch return;
    _ = allocator;
    emitJsonLine(line);
}

/// Emit a `Received Message` line: the analyzer treats the FIRST occurrence of an
/// id at a node as a delivery and any repeat as a duplicate. `time` is RFC3339
/// UTC with nanoseconds (Python `datetime.fromisoformat` parses it); `id` is the
/// message-id as a decimal u64 (the analyzer's `formatMessageID`); `topic` lets it
/// scope deliveries per topic.
fn emitReceived(io: std.Io, node_id: u64, topic: []const u8, msg_id: u64) void {
    var ts_buf: [40]u8 = undefined;
    const ts = formatNowRfc3339(io, &ts_buf) catch return;
    var buf: [512]u8 = undefined;
    const line = std.fmt.bufPrint(
        &buf,
        "{{\"msg\":\"Received Message\",\"time\":\"{s}\",\"node_id\":{d},\"topic\":\"{s}\",\"id\":\"{d}\"}}\n",
        .{ ts, node_id, topic, msg_id },
    ) catch return;
    emitJsonLine(line);
}

/// Format the current wall-clock time as RFC3339 UTC with nanoseconds, e.g.
/// `2026-06-07T12:34:56.123456789Z`. Shadow virtualizes the real clock, so this
/// reflects simulation time. Returns a slice into `buf`.
fn formatNowRfc3339(io: std.Io, buf: []u8) ![]const u8 {
    const ns_total = std.Io.Clock.now(.real, io).nanoseconds;
    const ns_nonneg: u128 = if (ns_total < 0) 0 else @intCast(ns_total);
    const secs: u64 = @intCast(ns_nonneg / std.time.ns_per_s);
    const sub_ns: u64 = @intCast(ns_nonneg % std.time.ns_per_s);

    const epoch_secs = std.time.epoch.EpochSeconds{ .secs = secs };
    const day_secs = epoch_secs.getDaySeconds();
    const year_day = epoch_secs.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();

    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}Z", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_secs.getHoursIntoDay(),
        day_secs.getMinutesIntoHour(),
        day_secs.getSecondsIntoMinute(),
        sub_ns,
    });
}

/// Read the whole params file into an arena-owned buffer.
fn readParamsFile(allocator: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
    const file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader_buf: [4096]u8 = undefined;
    var reader = file.reader(io, &reader_buf);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    while (true) {
        var chunk: [4096]u8 = undefined;
        const n = try reader.interface.readSliceShort(&chunk);
        if (n == 0) break;
        try out.appendSlice(allocator, chunk[0..n]);
    }
    return out.toOwnedSlice(allocator);
}

/// The mesh-degree override (D/Dlo/Dhi) extracted from an `initGossipSub` for this
/// node; null keeps the go-libp2p baseline.
const Setup = struct {
    mesh_degree: ?gossipsub.MeshDegree = null,
    validation_delay_ns: u64 = 0,
};

/// Pre-scan the script for the configuration that must be applied at gossipsub
/// construction (this node's `initGossipSub` mesh degree and any topic validation
/// delay), since our router takes that config up front rather than lazily. The
/// connect/subscribe/publish/waitUntil instructions are executed later, in order.
fn scanSetup(script: []const std.json.Value, node_id: u64) Setup {
    var setup = Setup{};
    for (script) |ins| {
        const obj = ins.object;
        const ty = (obj.get("type") orelse continue).string;
        if (std.mem.eql(u8, ty, "ifNodeIDEquals")) {
            const target: i64 = switch (obj.get("nodeID") orelse continue) {
                .integer => |v| v,
                else => continue,
            };
            if (target != @as(i64, @intCast(node_id))) continue;
            const inner = obj.get("instruction") orelse continue;
            applySetupInstruction(&setup, inner);
        } else {
            applySetupInstruction(&setup, ins);
        }
    }
    return setup;
}

fn applySetupInstruction(setup: *Setup, ins: std.json.Value) void {
    const obj = ins.object;
    const ty = (obj.get("type") orelse return).string;
    if (std.mem.eql(u8, ty, "initGossipSub")) {
        const params = obj.get("gossipSubParams") orelse return;
        if (params != .object) return;
        const d = jsonInt(params.object.get("D"));
        const dlo = jsonInt(params.object.get("Dlo"));
        const dhi = jsonInt(params.object.get("Dhi"));
        if (d != null or dlo != null or dhi != null) {
            // Defaults match go-libp2p so an unset field keeps the baseline.
            setup.mesh_degree = .{
                .d = @intCast(d orelse 6),
                .d_low = @intCast(dlo orelse 5),
                .d_high = @intCast(dhi orelse 12),
            };
        }
    } else if (std.mem.eql(u8, ty, "setTopicValidationDelay")) {
        const secs = jsonFloat(obj.get("delaySeconds")) orelse return;
        if (secs > 0) {
            setup.validation_delay_ns = @intFromFloat(secs * @as(f64, std.time.ns_per_s));
        }
    }
}

fn jsonInt(v: ?std.json.Value) ?i64 {
    const val = v orelse return null;
    return switch (val) {
        .integer => |i| i,
        .float => |f| @intFromFloat(f),
        else => null,
    };
}

fn jsonFloat(v: ?std.json.Value) ?f64 {
    const val = v orelse return null;
    return switch (val) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        else => null,
    };
}

/// The running participant: holds everything the script instructions act on.
const Node = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    gs: *gossipsub.Gossipsub,
    node_id: u64,
    start: std.Io.Clock.Timestamp,
    opts: Options = .{},
    /// Live connections (dialed peers), kept alive for the whole run.
    conns: std.ArrayList(*libp2p.SwitchConnection) = .empty,
    /// Topics we have already joined+subscribed (idempotent subscribe).
    subscribed: std.StringHashMapUnmanaged(void) = .empty,

    fn deinit(self: *Node) void {
        for (self.conns.items) |conn| conn.deinit();
        self.conns.deinit(self.allocator);
        var it = self.subscribed.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.subscribed.deinit(self.allocator);
    }

    /// Execute one script instruction. `ifNodeIDEquals` is unwrapped to its inner
    /// instruction only when the node matches; `initGossipSub` /
    /// `setTopicValidationDelay` were already applied at construction and are
    /// no-ops here.
    fn runInstruction(self: *Node, ins: std.json.Value) !void {
        const obj = ins.object;
        const ty = (obj.get("type") orelse return).string;

        if (std.mem.eql(u8, ty, "ifNodeIDEquals")) {
            const target = jsonInt(obj.get("nodeID")) orelse return;
            if (target != @as(i64, @intCast(self.node_id))) return;
            const inner = obj.get("instruction") orelse return;
            return self.runInstruction(inner);
        } else if (std.mem.eql(u8, ty, "connect")) {
            const list = obj.get("connectTo") orelse return;
            if (list != .array) return;
            for (list.array.items) |entry| {
                const target = jsonInt(entry) orelse continue;
                if (target < 0) continue;
                self.connectTo(@intCast(target)) catch |err| {
                    std.log.warn("connect to node{d} failed: {any}", .{ target, err });
                };
            }
            std.log.info("node{d} now has {d} gossipsub peer(s)", .{ self.node_id, self.gs.peerCount() });
        } else if (std.mem.eql(u8, ty, "subscribeToTopic")) {
            const topic = (obj.get("topicID") orelse return).string;
            try self.subscribe(topic);
        } else if (std.mem.eql(u8, ty, "waitUntil")) {
            const elapsed = jsonInt(obj.get("elapsedSeconds")) orelse return;
            try self.waitUntil(@intCast(@max(elapsed, 0)));
        } else if (std.mem.eql(u8, ty, "publish")) {
            try self.publish(obj);
        } else if (std.mem.eql(u8, ty, "initGossipSub") or std.mem.eql(u8, ty, "setTopicValidationDelay")) {
            // Applied at construction (see scanSetup); nothing to do per-step.
        } else {
            std.log.warn("unknown instruction type '{s}'", .{ty});
        }
    }

    /// Resolve `node<id>` to an IP and dial it over QUIC at the deterministic
    /// peer-id for that node. Mirrors the go reference's ShadowConnector.
    fn connectTo(self: *Node, target_id: u64) !void {
        const peer_str = try nodePeerIdString(self.allocator, target_id);
        defer self.allocator.free(peer_str);

        // A /dns4/ multiaddr lets the Switch's dial path resolve the Shadow
        // hostname itself (it calls resolveIpUdp, which handles dns4). Local-test
        // mode dials loopback at the target's per-node port instead.
        const addr_text = if (self.opts.connect_localhost)
            try std.fmt.allocPrint(
                self.allocator,
                "/ip4/127.0.0.1/udp/{d}/quic-v1/p2p/{s}",
                .{ self.opts.base_port + @as(u16, @intCast(target_id)), peer_str },
            )
        else
            try std.fmt.allocPrint(
                self.allocator,
                "/dns4/node{d}/udp/{d}/quic-v1/p2p/{s}",
                .{ target_id, listen_port, peer_str },
            );
        defer self.allocator.free(addr_text);

        var remote_addr = try Multiaddr.fromString(self.allocator, addr_text);
        defer remote_addr.deinit(self.allocator);

        const conn = try self.switcher.dial(remote_addr, .{ .timeout = secondsTimeout(30) });
        conn.startInboundDispatcher(.{}) catch |err| {
            conn.deinit();
            return err;
        };
        try self.conns.append(self.allocator, conn);
    }

    fn subscribe(self: *Node, topic: []const u8) !void {
        if (self.subscribed.contains(topic)) return;
        const owned = try self.allocator.dupe(u8, topic);
        errdefer self.allocator.free(owned);
        try self.subscribed.put(self.allocator, owned, {});
        try self.gs.subscribe(topic);
    }

    /// Wait until `elapsed_seconds` have passed since the node's start. Like the
    /// go reference, deliveries keep flowing on the router fiber while this fiber
    /// sleeps the remaining delta.
    fn waitUntil(self: *Node, elapsed_seconds: u64) !void {
        const target = self.start.addDuration(.{ .raw = .fromNanoseconds(@intCast(elapsed_seconds * std.time.ns_per_s)), .clock = .awake });
        const now = std.Io.Clock.Timestamp.now(self.io, .awake);
        const remaining = now.durationTo(target);
        if (remaining.raw.nanoseconds > 0) {
            const t = std.Io.Timeout{ .deadline = target };
            try t.sleep(self.io);
        }
    }

    /// Publish a message: `big-endian(messageID)` zero-padded to `messageSizeBytes`,
    /// so its first 8 bytes (the `CalcID`) are the message number. With flood-publish
    /// (on by default) the message reaches every topic subscriber via the mesh (or a
    /// fanout set if this node has not subscribed); the scenario subscribes every node
    /// before the publish phase, so the publisher is already in the topic mesh.
    fn publish(self: *Node, obj: std.json.ObjectMap) !void {
        const msg_id = jsonInt(obj.get("messageID")) orelse return error.InvalidPublish;
        const topic = (obj.get("topicID") orelse return error.InvalidPublish).string;
        const size: usize = @intCast(@max(jsonInt(obj.get("messageSizeBytes")) orelse 8, 8));

        const data = try self.allocator.alloc(u8, size);
        defer self.allocator.free(data);
        @memset(data, 0);
        std.mem.writeInt(u64, data[0..8], @intCast(msg_id), .big);

        std.log.info("node{d} publishing message {d} ({d} bytes) to {s}", .{ self.node_id, msg_id, size, topic });
        try self.gs.publish(topic, data);
    }
};

fn secondsTimeout(secs: u64) std.Io.Timeout {
    return .{ .duration = .{ .raw = .fromNanoseconds(@intCast(secs * std.time.ns_per_s)), .clock = .awake } };
}

/// Accepts inbound connections in a loop, starting an inbound dispatcher per
/// connection so the `/meshsub` handler runs. A node is dialed by every peer that
/// lists it in its `connectTo`, so it must accept many connections.
const AcceptLoop = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    conns: std.ArrayList(*libp2p.SwitchConnection) = .empty,

    fn run(self: *AcceptLoop) void {
        while (true) {
            const conn = self.switcher.accept() catch |err| {
                switch (err) {
                    error.ListenerClosed, error.Canceled => {},
                    else => std.log.warn("accept failed: {any}", .{err}),
                }
                return;
            };
            conn.startInboundDispatcher(.{}) catch |err| {
                std.log.warn("startInboundDispatcher failed: {any}", .{err});
                conn.deinit();
                continue;
            };
            self.conns.append(self.allocator, conn) catch {
                conn.deinit();
                continue;
            };
        }
    }

    fn deinit(self: *AcceptLoop) void {
        for (self.conns.items) |conn| conn.deinit();
        self.conns.deinit(self.allocator);
    }
};

/// Optional local-testing overrides. The framework never sets these (it passes
/// only `--params`), so the Shadow path is unchanged; they exist so a developer
/// can run a multi-node mesh on loopback without Shadow to validate the wiring.
pub const Options = struct {
    /// Use this node id instead of parsing the hostname (Shadow sets the hostname;
    /// a dev box does not, so `--node-id <N>` lets a local run pick its identity).
    node_id_override: ?u64 = null,
    /// Dial peers on `127.0.0.1` at port `base_port + targetNodeId` instead of
    /// resolving the Shadow DNS name `node<i>`, and listen on `base_port + node_id`.
    /// Lets several nodes share one loopback host for local testing.
    connect_localhost: bool = false,
    /// Base port for `connect_localhost` (each node uses `base_port + node_id`).
    base_port: u16 = 9000,
};

/// Entry point for test-plans mode. `params_path` is the value of `--params`.
pub fn run(allocator: std.mem.Allocator, io: std.Io, params_path: []const u8, opts: Options) !void {
    // Identity: parse the Shadow hostname `node<N>` into our node id (or take the
    // local override).
    const node_id = if (opts.node_id_override) |id| id else blk: {
        var hostname_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
        const hostname = try std.posix.gethostname(&hostname_buf);
        break :blk parseNodeId(hostname) orelse {
            std.log.err("hostname '{s}' is not of the form node<N>", .{hostname});
            return error.InvalidHostname;
        };
    };

    const my_port: u16 = if (opts.connect_localhost) opts.base_port + @as(u16, @intCast(node_id)) else listen_port;

    var host_key = try nodeKey(node_id);
    defer host_key.deinit();

    const local_peer = try host_key.peerId(allocator);
    const peer_str = try local_peer.toString(allocator);
    defer allocator.free(peer_str);
    // Emit the PeerID mapping line FIRST so the analyzer can count this node even
    // if it never receives a message.
    emitPeerId(allocator, node_id, peer_str);

    // Parse the script.
    const params_bytes = try readParamsFile(allocator, io, params_path);
    defer allocator.free(params_bytes);
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, params_bytes, .{});
    defer parsed.deinit();
    const script = blk: {
        if (parsed.value != .object) return error.InvalidParams;
        const s = parsed.value.object.get("script") orelse return error.InvalidParams;
        if (s != .array) return error.InvalidParams;
        break :blk s.array.items;
    };

    const setup = scanSetup(script, node_id);

    // QUIC endpoint in Shadow-compatible mode (plain recvmsg/sendmsg).
    const endpoint = try libp2p.QuicEndpoint.initWithIdentity(allocator, io, &host_key, .{
        .endpoint = .{ .shadow_compatible = true },
    });
    defer endpoint.deinit();
    const switcher = try libp2p.Switch.init(allocator, io, endpoint);
    defer switcher.deinit();

    // gossipsub: anonymous (StrictNoSign + NoAuthor), CalcID message-id, the
    // scenario's mesh degree, and the validation delay validator.
    var tracer = Tracer{ .io = io, .node_id = node_id };
    var validator = Validator{ .io = io, .delay_ns = setup.validation_delay_ns };
    var gs_config = gossipsub.RouterConfig{
        .signature_policy = .anonymous,
        .message_id_fn = .{ .func = calcId },
        .mesh_degree = setup.mesh_degree,
        .idontwant_message_threshold = 1024,
    };
    if (setup.validation_delay_ns != 0) {
        gs_config.validator = validator.messageValidator();
        gs_config.validation_concurrency = validation_concurrency;
    }
    // Anonymous policy requires NO host key.
    const gs = try gossipsub.Gossipsub.init(allocator, io, switcher, local_peer, null, tracer.handler(), null, gs_config);
    defer gs.deinit();

    // Listen on the framework's QUIC port (wildcard; the host is single-homed in
    // Shadow so the bound address is the host's IP). Local-test mode listens on a
    // per-node loopback port so several nodes can share one host.
    var listen_text_buf: [64]u8 = undefined;
    const listen_text = try std.fmt.bufPrint(&listen_text_buf, "/ip4/0.0.0.0/udp/{d}/quic-v1", .{my_port});
    var listen_addr = try Multiaddr.fromString(allocator, listen_text);
    defer listen_addr.deinit(allocator);
    try switcher.listen(listen_addr);
    std.log.info("node{d} listening on {s} as {s}", .{ node_id, listen_text, peer_str });

    // Register identify so peers learn our /meshsub protocols (their pubsub opens a
    // meshsub stream only once identify reports it).
    var id_handler = identify.IdentifyHandler.initWithOptions(allocator, .{
        .agent_version = "eth-p2p-z-gossipsub-interop/0.1.0",
        .protocols = &(.{identify.protocol_id} ++ gossipsub.supported_protocols),
        .listen_addrs = &.{},
    });
    defer id_handler.deinit();
    try switcher.addProtocolService(
        identify.protocol_id,
        libp2p.protocols.streamHandlerService(identify.IdentifyHandler, identify.IdentifyHandler.run, &id_handler),
    );

    // Accept inbound connections on a fiber for the whole run.
    var accept_loop = AcceptLoop{ .allocator = allocator, .io = io, .switcher = switcher };
    var accept_future = try std.Io.concurrent(io, AcceptLoop.run, .{&accept_loop});
    defer {
        switcher.closeListener(io);
        accept_future.cancel(io);
        accept_loop.deinit();
    }

    var node = Node{
        .allocator = allocator,
        .io = io,
        .switcher = switcher,
        .gs = gs,
        .node_id = node_id,
        .start = std.Io.Clock.Timestamp.now(io, .awake),
        .opts = opts,
    };
    defer node.deinit();

    // Execute the script in order.
    for (script) |ins| {
        node.runInstruction(ins) catch |err| {
            std.log.err("instruction failed: {any}", .{err});
            return err;
        };
    }

    std.log.info("node{d} script complete", .{node_id});

    // Hard-exit rather than unwind the fiber teardown. Every node finishes its
    // script at about the same simulated time and all log lines are already on
    // STDOUT via direct write(2), so there is nothing to flush. A short-lived
    // interop node has no reason to orchestrate the multi-layer graceful teardown
    // (gossipsub -> its connections -> switch -> endpoint); the per-connection
    // actor teardown can stall the cooperative runtime, and Shadow would then wait
    // out the whole stop_time on a hung process. exit(0) keeps shutdown prompt and
    // robust, matching the smoke-test path's `finishAndExit`.
    std.process.exit(0);
}

/// Parse a Shadow hostname `node<N>` into N. Returns null if the prefix or number
/// is malformed.
fn parseNodeId(hostname: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, hostname, "node")) return null;
    const digits = hostname["node".len..];
    if (digits.len == 0) return null;
    return std.fmt.parseInt(u64, digits, 10) catch null;
}

test "calcId returns first 8 bytes of data" {
    const data = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 5, 9, 9, 9 };
    var id = try calcId(null, "t", "", "", &data, std.testing.allocator);
    defer id.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 5 }, id.bytes);
}

test "parseNodeId parses node<N>" {
    try std.testing.expectEqual(@as(?u64, 0), parseNodeId("node0"));
    try std.testing.expectEqual(@as(?u64, 42), parseNodeId("node42"));
    try std.testing.expectEqual(@as(?u64, null), parseNodeId("nodeX"));
    try std.testing.expectEqual(@as(?u64, null), parseNodeId("host7"));
}
