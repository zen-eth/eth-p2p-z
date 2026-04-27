/// Phase 2.3 — Full TCP + TLS + Yamux + GossipSub interop binary.
///
/// Reads --params <path>.json, derives deterministic PeerID from Shadow
/// hostname, starts a TCP listener on port 9000, then executes the
/// instruction list.

pub const std_options = @import("zig-libp2p").std_options;

const std = @import("std");
const libp2p = @import("zig-libp2p");
const io_loop = libp2p.thread_event_loop;
const quic_mod = libp2p.transport.quic;
const identity = libp2p.identity;
const swarm = libp2p.swarm;
const protocols = libp2p.protocols;
const keys_mod = @import("peer_id").keys;
const PeerId = @import("peer_id").PeerId;
const Multiaddr = @import("multiaddr").Multiaddr;
const tcp_mod = libp2p.transport.tcp;
const yamux_mod = libp2p.transport.yamux;
const tls_tcp_mod = libp2p.security.tls_tcp;
const tls_sec_mod = libp2p.security.tls;
const p2p_conn = libp2p.conn;
const proto_binding_mod = libp2p.multistream.proto_binding;
const multistream_mod = libp2p.multistream.multistream;
const gossipsub_mod = libp2p.protocols.pubsub.gossipsub;
const Gossipsub = gossipsub_mod.Gossipsub;
const PubSubPeerProtocolHandler = gossipsub_mod.PubSubPeerProtocolHandler;
const event_mod = libp2p.event;
const instr_mod = @import("instruction.zig");
const Instruction = instr_mod.Instruction;

const TCP_PORT: u16 = 9000;

// ============================================================
// ISO 8601 timestamp helper (for "Received Message" logs)
// ============================================================

/// Format nanoseconds-since-Unix-epoch as "YYYY-MM-DDTHH:MM:SS.ffffffZ".
/// buf must be at least 27 bytes.
fn fmtIso8601(buf: []u8, ns: i128) []const u8 {
    const epoch = std.time.epoch;
    const secs: u64 = @intCast(@max(0, @divFloor(ns, std.time.ns_per_s)));
    const us: u64 = @intCast(@divFloor(@mod(ns, std.time.ns_per_s), 1000));
    const es = epoch.EpochSeconds{ .secs = secs };
    const yd = es.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}Z", .{
        yd.year,
        md.month.numeric(),
        md.day_index + 1,
        ds.getHoursIntoDay(),
        ds.getMinutesIntoHour(),
        ds.getSecondsIntoMinute(),
        us,
    }) catch buf[0..0];
}

// ============================================================
// Gossipsub message event listener — logs "Received Message"
// ============================================================

const GossipEvent = gossipsub_mod.Event;

const MessageListener = struct {
    node_id: u64,

    const vtable = event_mod.EventListenerVTable(GossipEvent){
        .handleFn = onEvent,
    };

    fn onEvent(instance: *anyopaque, ev: GossipEvent) void {
        const self: *MessageListener = @ptrCast(@alignCast(instance));
        switch (ev) {
            .message => |m| {
                if (m.message.data.len < 8) return;
                const msg_id = std.mem.readInt(u64, m.message.data[0..8], .big);
                var time_buf: [32]u8 = undefined;
                const time_str = fmtIso8601(&time_buf, std.time.nanoTimestamp());
                const stdout_file: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
                var out_buf: [256]u8 = undefined;
                var w = stdout_file.writer(&out_buf);
                w.interface.print(
                    "{{\"time\":\"{s}\",\"level\":\"INFO\",\"msg\":\"Received Message\",\"id\":\"{d}\",\"node_id\":{d}}}\n",
                    .{ time_str, msg_id, self.node_id },
                ) catch return;
                w.interface.flush() catch return;
            },
            else => {},
        }
    }

    pub fn any(self: *MessageListener) event_mod.AnyEventListener(GossipEvent) {
        return .{ .instance = self, .vtable = &vtable };
    }
};

// ============================================================
// Outbound dial completion slot
// ============================================================

const OutboundSlot = struct {
    event: std.Thread.ResetEvent = .{},
    yamux: ?*yamux_mod.YamuxSession = null,
    err: ?anyerror = null,
};

// ============================================================
// TCP enhancer for OUTBOUND (dialer) connections
// Used once per dial; slot is heap-allocated by the caller.
// ============================================================

const DialerEnhancer = struct {
    tls_binding: proto_binding_mod.AnyProtocolBinding,
    bindings_buf: [1]proto_binding_mod.AnyProtocolBinding,
    slot: *OutboundSlot,
    allocator: std.mem.Allocator,

    const vtable = p2p_conn.ConnEnhancerVTable{
        .enhanceConnFn = enhanceConnImpl,
    };

    fn enhanceConnImpl(instance: *anyopaque, conn: p2p_conn.AnyConn) anyerror!void {
        const self: *DialerEnhancer = @ptrCast(@alignCast(instance));
        self.bindings_buf = .{self.tls_binding};

        const upgrade_ctx = conn.getPipeline().allocator.create(DialerUpgradeCtx) catch
            return error.OutOfMemory;
        upgrade_ctx.* = .{ .conn = conn, .enhancer = self };

        var ms: multistream_mod.Multistream = undefined;
        try ms.init(10_000, &self.bindings_buf);
        ms.initConn(conn, upgrade_ctx, DialerUpgradeCtx.callback);
    }

    pub fn any(self: *DialerEnhancer) p2p_conn.AnyConnEnhancer {
        return .{ .instance = self, .vtable = &vtable };
    }
};

const DialerUpgradeCtx = struct {
    conn: p2p_conn.AnyConn,
    enhancer: *DialerEnhancer,

    fn callback(instance: ?*anyopaque, res: anyerror!?*anyopaque) void {
        const self: *DialerUpgradeCtx = @ptrCast(@alignCast(instance.?));
        defer self.conn.getPipeline().allocator.destroy(self);

        const slot = self.enhancer.slot;

        if (res) |result| {
            const sec_ptr: *p2p_conn.SecuritySession = @ptrCast(@alignCast(result.?));
            self.conn.setSecuritySession(sec_ptr.*);
            self.conn.getPipeline().allocator.destroy(sec_ptr);

            const yamux = yamux_mod.YamuxSession.init(
                self.enhancer.allocator,
                self.conn,
                true, // is_client = true (dialer)
            ) catch |err| {
                slot.err = err;
                slot.event.set();
                return;
            };

            if (self.conn.securitySession()) |sec| {
                yamux.remote_peer_id = PeerId.fromBytes(sec.remote_id) catch null;
            }

            self.conn.getPipeline().addLast("yamux", yamux.handler()) catch |err| {
                yamux.deinit();
                slot.err = err;
                slot.event.set();
                return;
            };

            const yamux_hctx = self.conn.getPipeline().tail.prev_context.?;
            yamux_hctx.handler.onActive(yamux_hctx) catch |err| {
                yamux.deinit();
                slot.err = err;
                slot.event.set();
                return;
            };

            slot.yamux = yamux;
            slot.event.set();
        } else |err| {
            slot.err = err;
            slot.event.set();
        }
    }
};

// ============================================================
// TCP enhancer for INBOUND (listener) connections
// Registers each accepted connection as a yamux session with
// the switch under a unique "/inbound/<N>" key.
// ============================================================

const ListenerEnhancer = struct {
    tls_binding: proto_binding_mod.AnyProtocolBinding,
    bindings_buf: [1]proto_binding_mod.AnyProtocolBinding,
    allocator: std.mem.Allocator,
    network_switch: *swarm.Switch,
    gossipsub: *Gossipsub,
    inbound_counter: std.atomic.Value(u64),

    const vtable = p2p_conn.ConnEnhancerVTable{
        .enhanceConnFn = enhanceConnImpl,
    };

    fn enhanceConnImpl(instance: *anyopaque, conn: p2p_conn.AnyConn) anyerror!void {
        const self: *ListenerEnhancer = @ptrCast(@alignCast(instance));
        self.bindings_buf = .{self.tls_binding};

        const upgrade_ctx = conn.getPipeline().allocator.create(ListenerUpgradeCtx) catch
            return error.OutOfMemory;
        upgrade_ctx.* = .{ .conn = conn, .enhancer = self };

        var ms: multistream_mod.Multistream = undefined;
        try ms.init(10_000, &self.bindings_buf);
        ms.initConn(conn, upgrade_ctx, ListenerUpgradeCtx.callback);
    }

    pub fn any(self: *ListenerEnhancer) p2p_conn.AnyConnEnhancer {
        return .{ .instance = self, .vtable = &vtable };
    }
};

const ListenerUpgradeCtx = struct {
    conn: p2p_conn.AnyConn,
    enhancer: *ListenerEnhancer,

    fn callback(instance: ?*anyopaque, res: anyerror!?*anyopaque) void {
        const self: *ListenerUpgradeCtx = @ptrCast(@alignCast(instance.?));
        defer self.conn.getPipeline().allocator.destroy(self);

        const enh = self.enhancer;

        if (res) |result| {
            const sec_ptr: *p2p_conn.SecuritySession = @ptrCast(@alignCast(result.?));
            self.conn.setSecuritySession(sec_ptr.*);
            self.conn.getPipeline().allocator.destroy(sec_ptr);

            const yamux = yamux_mod.YamuxSession.init(
                enh.allocator,
                self.conn,
                false, // is_client = false (listener)
            ) catch |err| {
                std.log.warn("Inbound Yamux init failed: {}", .{err});
                return;
            };

            if (self.conn.securitySession()) |sec| {
                yamux.remote_peer_id = PeerId.fromBytes(sec.remote_id) catch null;
            }

            self.conn.getPipeline().addLast("yamux", yamux.handler()) catch |err| {
                std.log.warn("Inbound Yamux pipeline addLast failed: {}", .{err});
                yamux.deinit();
                return;
            };

            const yamux_hctx = self.conn.getPipeline().tail.prev_context.?;
            yamux_hctx.handler.onActive(yamux_hctx) catch |err| {
                std.log.warn("Inbound Yamux onActive failed: {}", .{err});
                yamux.deinit();
                return;
            };

            const counter = enh.inbound_counter.fetchAdd(1, .acq_rel);
            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "/inbound/{d}", .{counter}) catch {
                yamux.deinit();
                return;
            };

            enh.network_switch.registerTcpYamux(
                key,
                yamux,
                enh.gossipsub,
                Gossipsub.onIncomingNewStream,
            );
        } else |err| {
            std.log.warn("Inbound TLS failed: {}", .{err});
        }
    }
};

// ============================================================
// Helpers
// ============================================================

/// Resolve "node<N>" hostname to the first IPv4 address.
fn resolveNodeIp(allocator: std.mem.Allocator, node_id: u64) !std.net.Address {
    const hostname = try std.fmt.allocPrint(allocator, "node{d}", .{node_id});
    defer allocator.free(hostname);

    const list = std.net.getAddressList(allocator, hostname, TCP_PORT) catch |err| {
        std.log.warn("DNS resolution failed for {s}: {}", .{ hostname, err });
        return err;
    };
    defer list.deinit();

    for (list.addrs) |addr| {
        if (addr.any.family == std.posix.AF.INET) {
            return addr;
        }
    }
    return error.NoIpv4AddressFound;
}

/// Compute the deterministic ED25519 public key (and derived PeerId) for node N.
fn nodePublicKey(allocator: std.mem.Allocator, node_id: u64) !PeerId {
    const Ed25519 = std.crypto.sign.Ed25519;
    var seed: [Ed25519.KeyPair.seed_length]u8 = [_]u8{0} ** Ed25519.KeyPair.seed_length;
    std.mem.writeInt(u64, seed[0..8], node_id, .little);
    const ed_kp = try Ed25519.KeyPair.generateDeterministic(seed);

    var pub_key = @import("peer_id").keys.PublicKey{
        .type = .ED25519,
        .data = &ed_kp.public_key.bytes,
    };
    return PeerId.fromPublicKey(allocator, &pub_key);
}

/// Map GossipSubParams → Gossipsub.Options.
fn gossipSubOptions(params: instr_mod.GossipSubParams) Gossipsub.Options {
    var opts = Gossipsub.Options{};
    if (params.D) |v| opts.D = @intCast(@max(v, 0));
    if (params.Dlo) |v| opts.D_lo = @intCast(@max(v, 0));
    if (params.Dhi) |v| opts.D_hi = @intCast(@max(v, 0));
    if (params.Dlazy) |v| opts.D_lazy = @intCast(@max(v, 0));
    if (params.HeartbeatInterval) |v| opts.heartbeat_interval_ms = @intFromFloat(@max(v * 1000.0, 1.0));
    if (params.FanoutTTL) |v| opts.fanout_ttl_ms = @intFromFloat(@max(v * 1000.0, 0.0));
    if (params.GossipFactor) |v| opts.gossip_factor = @floatCast(v);
    if (params.HistoryLength) |v| opts.history_length = @intCast(@max(v, 1));
    if (params.HistoryGossip) |v| opts.history_gossip = @intCast(@max(v, 1));
    if (params.GossipRetransmission) |v| opts.gossip_retransmission = @intCast(@max(v, 0));
    if (params.PruneBackoff) |v| opts.prune_backoff_s = @intFromFloat(@max(v, 0.0));
    if (params.UnsubscribeBackoff) |v| opts.unsubscribe_backoff_s = @intFromFloat(@max(v, 0.0));
    if (params.MaxIHaveLength) |v| opts.max_ihave_len = @intCast(@max(v, 0));
    if (params.MaxIHaveMessages) |v| opts.max_ihave_messages = @intCast(@max(v, 0));
    return opts;
}

// ============================================================
// main
// ============================================================

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // ------------------------------------------------------------------ //
    // 1. Parse --params flag
    // ------------------------------------------------------------------ //
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var params_path: ?[]const u8 = null;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--params") and i + 1 < args.len) {
            params_path = args[i + 1];
            i += 1;
        }
    }
    if (params_path == null) {
        std.log.err("--params <path> flag is required", .{});
        std.process.exit(1);
    }

    // ------------------------------------------------------------------ //
    // 2. Hostname → node ID
    // ------------------------------------------------------------------ //
    var hostname_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
    const hostname = try std.posix.gethostname(hostname_buf[0..]);

    var node_id: u64 = 0;
    if (std.mem.startsWith(u8, hostname, "node")) {
        node_id = std.fmt.parseInt(u64, hostname[4..], 10) catch blk: {
            std.log.warn("could not parse node ID from '{s}', defaulting to 0", .{hostname});
            break :blk 0;
        };
    }

    // ------------------------------------------------------------------ //
    // 3. Deterministic ED25519 key pair
    // ------------------------------------------------------------------ //
    const Ed25519 = std.crypto.sign.Ed25519;
    var seed: [Ed25519.KeyPair.seed_length]u8 = [_]u8{0} ** Ed25519.KeyPair.seed_length;
    std.mem.writeInt(u64, seed[0..8], node_id, .little);

    var host_key = try identity.KeyPair.fromEd25519Seed(&seed);
    defer host_key.deinit();

    // ------------------------------------------------------------------ //
    // 4. Derive peer_id and log it
    // ------------------------------------------------------------------ //
    const peer_id = try host_key.peerId(allocator);

    const b58_len = peer_id.toBase58Len();
    const b58_buf = try allocator.alloc(u8, b58_len);
    defer allocator.free(b58_buf);
    const peer_id_str = try peer_id.toBase58(b58_buf);

    const now_s = std.time.timestamp();
    const stdout_file: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
    var stdout_buf: [512]u8 = undefined;
    var stdout_writer = stdout_file.writer(&stdout_buf);
    try stdout_writer.interface.print(
        "{{\"time\":\"{d}\",\"level\":\"INFO\",\"msg\":\"PeerID\",\"id\":\"{s}\",\"node_id\":{d}}}\n",
        .{ now_s, peer_id_str, node_id },
    );
    try stdout_writer.interface.flush();

    // ------------------------------------------------------------------ //
    // 5. Parse experiment params
    // ------------------------------------------------------------------ //
    var params = try instr_mod.readParams(allocator, params_path.?);
    defer params.deinit();

    std.log.info("loaded {d} instruction(s) from {s}", .{ params.instructions.len, params_path.? });

    // ------------------------------------------------------------------ //
    // 6. Pre-scan for initGossipSub → collect opts
    // ------------------------------------------------------------------ //
    var gs_opts = Gossipsub.Options{};
    for (params.instructions) |inst| {
        if (inst == .init_gossipsub) {
            gs_opts = gossipSubOptions(inst.init_gossipsub.params);
            break;
        }
    }

    // ------------------------------------------------------------------ //
    // 7. Init ThreadEventLoop
    // ------------------------------------------------------------------ //
    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(allocator);
    defer {
        loop.close();
        loop.deinit();
    }

    // ------------------------------------------------------------------ //
    // 8. Init QuicTransport (required by Switch; not used for dialing)
    // ------------------------------------------------------------------ //
    var quic_transport: quic_mod.QuicTransport = undefined;
    try quic_transport.init(&loop, &host_key, keys_mod.KeyType.ECDSA, allocator);

    // ------------------------------------------------------------------ //
    // 9. Init Switch
    // ------------------------------------------------------------------ //
    var sw: swarm.Switch = undefined;
    sw.init(allocator, &quic_transport);
    defer {
        sw.stop();
        sw.deinit();
    }

    // ------------------------------------------------------------------ //
    // 10. Build TLS channel (shared by both transports)
    // ------------------------------------------------------------------ //
    var host_pub_key = try host_key.publicKey(allocator);
    defer if (host_pub_key.data) |data| allocator.free(data);

    const sign_ctx: ?*anyopaque = host_key.storage.tls;

    var tls_channel: tls_tcp_mod.TlsTcpChannel = undefined;
    try tls_channel.init(allocator, &host_pub_key, sign_ctx, tls_sec_mod.signDataWithTlsKey);
    defer tls_channel.deinit();

    // ------------------------------------------------------------------ //
    // 11. Init Gossipsub
    // ------------------------------------------------------------------ //
    var listen_ma_str_buf: [64]u8 = undefined;
    const listen_ma_str = try std.fmt.bufPrint(&listen_ma_str_buf, "/ip4/0.0.0.0/tcp/{d}", .{TCP_PORT});
    var listen_ma = try Multiaddr.fromString(allocator, listen_ma_str);
    defer listen_ma.deinit();

    var gs: Gossipsub = undefined;
    try gs.init(allocator, listen_ma, peer_id, &host_key, &sw, gs_opts);
    defer gs.deinit();

    // Register message listener to emit "Received Message" log lines.
    var msg_listener = MessageListener{ .node_id = node_id };
    try gs.event_emitter.addListener(.message, msg_listener.any());

    // ------------------------------------------------------------------ //
    // 12. Register PubSubPeerProtocolHandler with switch
    // ------------------------------------------------------------------ //
    var ps_handler = PubSubPeerProtocolHandler.init(allocator);
    defer ps_handler.deinit();
    try sw.addProtocolHandler(gossipsub_mod.v1_id, ps_handler.any());
    try sw.addProtocolHandler(gossipsub_mod.v1_1_id, ps_handler.any());

    // ------------------------------------------------------------------ //
    // 13. Init LISTENER enhancer + XevTransport + start listening
    // ------------------------------------------------------------------ //
    var listener_enhancer = ListenerEnhancer{
        .tls_binding = tls_channel.any(),
        .bindings_buf = undefined,
        .allocator = allocator,
        .network_switch = &sw,
        .gossipsub = &gs,
        .inbound_counter = std.atomic.Value(u64).init(0),
    };

    var listen_transport: tcp_mod.XevTransport = undefined;
    try listen_transport.init(listener_enhancer.any(), &loop, allocator, .{ .backlog = 128 });

    const bind_addr = try std.net.Address.parseIp("0.0.0.0", TCP_PORT);
    const listener = try listen_transport.listen(bind_addr);

    // Pre-arm 16 accepts so inbound connections can queue up.
    const AcceptCtx = struct {
        fn callback(_: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void {
            _ = res catch |err| {
                std.log.warn("TCP accept failed: {}", .{err});
            };
            // TlsTcpEnhancer handles the rest asynchronously.
        }
    };
    var j: usize = 0;
    while (j < 16) : (j += 1) {
        listener.accept(null, AcceptCtx.callback);
    }

    std.log.info("Phase 2.3 started — node_id={d} peer_id={s} listening on port {d}", .{ node_id, peer_id_str, TCP_PORT });

    // ------------------------------------------------------------------ //
    // 14. Init DIALER enhancer + XevTransport (reuses same TLS channel)
    // ------------------------------------------------------------------ //
    // DialerEnhancer slot is set per-dial in the instruction loop.
    var dial_slot = OutboundSlot{};
    var dialer_enhancer = DialerEnhancer{
        .tls_binding = tls_channel.any(),
        .bindings_buf = undefined,
        .slot = &dial_slot,
        .allocator = allocator,
    };

    var dial_transport: tcp_mod.XevTransport = undefined;
    try dial_transport.init(dialer_enhancer.any(), &loop, allocator, .{ .backlog = 0 });

    // ------------------------------------------------------------------ //
    // 15. Execute instructions
    // ------------------------------------------------------------------ //
    const start_ns = std.time.nanoTimestamp();
    for (params.instructions) |inst| {
        try runInstruction(
            inst,
            node_id,
            start_ns,
            allocator,
            &sw,
            &gs,
            &dial_transport,
            &dialer_enhancer,
            &dial_slot,
        );
    }

    std.log.info("all instructions executed — node_id={d}", .{node_id});

    // Keep running so gossipsub can forward messages. Shadow will kill us.
    std.Thread.sleep(std.math.maxInt(u64));
}

// ============================================================
// Instruction executor
// ============================================================

fn runInstruction(
    inst: Instruction,
    node_id: u64,
    start_ns: i128,
    allocator: std.mem.Allocator,
    sw: *swarm.Switch,
    gs: *Gossipsub,
    dial_transport: *tcp_mod.XevTransport,
    dialer_enhancer: *DialerEnhancer,
    dial_slot: *OutboundSlot,
) !void {
    switch (inst) {
        .wait_until => |w| {
            const target_ns = start_ns + @as(i128, w.elapsed_seconds) * std.time.ns_per_s;
            const now_ns = std.time.nanoTimestamp();
            if (target_ns > now_ns) {
                const sleep_ns: u64 = @intCast(target_ns - now_ns);
                std.log.info("waitUntil: sleeping {d}ms", .{sleep_ns / std.time.ns_per_ms});
                std.Thread.sleep(sleep_ns);
            }
        },

        .if_node_id_equals => |cond| {
            if (@as(u64, @intCast(cond.node_id)) == node_id) {
                try runInstruction(cond.instruction.*, node_id, start_ns, allocator, sw, gs, dial_transport, dialer_enhancer, dial_slot);
            }
        },

        .connect => |c| {
            for (c.connect_to) |target_node_id| {
                connectToPeer(
                    @intCast(target_node_id),
                    allocator,
                    sw,
                    gs,
                    dial_transport,
                    dialer_enhancer,
                    dial_slot,
                ) catch |err| {
                    std.log.warn("connect to node{d} failed: {}", .{ target_node_id, err });
                };
            }
        },

        .subscribe_to_topic => |s| {
            const SubscribeCtx = struct {
                event: std.Thread.ResetEvent = .{},
                err: ?anyerror = null,

                fn cb(ctx: ?*anyopaque, res: anyerror!void) void {
                    const self: *@This() = @ptrCast(@alignCast(ctx.?));
                    self.err = if (res) |_| null else |err| err;
                    self.event.set();
                }
            };
            var sub_ctx = SubscribeCtx{};
            gs.subscribe(s.topic_id, &sub_ctx, SubscribeCtx.cb);
            sub_ctx.event.wait();
            if (sub_ctx.err) |err| {
                std.log.warn("subscribe to {s} failed: {}", .{ s.topic_id, err });
            } else {
                std.log.info("subscribed to topic {s}", .{s.topic_id});
            }
        },

        .publish => |p| {
            // Build a message of the requested size.
            // First 8 bytes = message_id as big-endian u64 (spec requirement).
            const payload = try allocator.alloc(u8, @intCast(p.message_size_bytes));
            defer allocator.free(payload);
            @memset(payload, 0);
            if (payload.len >= 8) {
                std.mem.writeInt(u64, payload[0..8], @intCast(p.message_id), .big);
            }

            const PublishCtx = struct {
                event: std.Thread.ResetEvent = .{},
                err: ?anyerror = null,

                fn cb(ctx: ?*anyopaque, res: anyerror![]PeerId) void {
                    const self: *@This() = @ptrCast(@alignCast(ctx.?));
                    self.err = if (res) |_| null else |err| err;
                    self.event.set();
                }
            };
            var pub_ctx = PublishCtx{};
            gs.publish(p.topic_id, payload, &pub_ctx, PublishCtx.cb);
            pub_ctx.event.wait();
            if (pub_ctx.err) |err| {
                std.log.warn("publish message_id={d} to {s} failed: {}", .{ p.message_id, p.topic_id, err });
            } else {
                std.log.info("published message_id={d} to {s}", .{ p.message_id, p.topic_id });
            }
        },

        .init_gossipsub => {
            // Already handled during pre-scan; Gossipsub is already initialized.
            std.log.info("initGossipSub: already applied during startup", .{});
        },

        .set_topic_validation_delay => |v| {
            std.log.warn("TODO setTopicValidationDelay: topic={s} delay={d}s", .{
                v.topic_id, v.delay_seconds,
            });
        },

        .add_partial_message => |a| {
            std.log.warn("TODO addPartialMessage: topic={s} group={d} parts={d}", .{
                a.topic_id, a.group_id, a.parts,
            });
        },

        .publish_partial => |pp| {
            std.log.warn("TODO publishPartial: topic={s} group={d}", .{
                pp.topic_id, pp.group_id,
            });
        },
    }
}

// ============================================================
// TCP dial → TLS → Yamux → registerTcpYamux → addPeer
// ============================================================

fn connectToPeer(
    target_node_id: u64,
    allocator: std.mem.Allocator,
    sw: *swarm.Switch,
    gs: *Gossipsub,
    dial_transport: *tcp_mod.XevTransport,
    dialer_enhancer: *DialerEnhancer,
    dial_slot: *OutboundSlot,
) !void {
    // Resolve the target's IP.
    const peer_addr = try resolveNodeIp(allocator, target_node_id);

    // Compute the target's peer ID from its deterministic seed.
    const target_peer_id = try nodePublicKey(allocator, target_node_id);

    // Build the target's multiaddr using the Multiaddr API.
    // Extract IPv4 octets from the network-byte-order u32.
    var ip_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &ip_bytes, peer_addr.in.sa.addr, .big);

    var peer_ma = Multiaddr.init(allocator);
    defer peer_ma.deinit();
    try peer_ma.push(.{ .Ip4 = std.net.Ip4Address.init(ip_bytes, 0) });
    try peer_ma.push(.{ .Tcp = TCP_PORT });
    try peer_ma.push(.{ .P2P = target_peer_id });

    const peer_ma_str = try peer_ma.toString(allocator);
    defer allocator.free(peer_ma_str);

    std.log.info("connecting to node{d} at {s}", .{ target_node_id, peer_ma_str });

    // Reset the dial slot for this connection.
    dial_slot.* = .{};
    dialer_enhancer.slot = dial_slot;

    // Dial TCP.
    const TcpDialCtx = struct {
        event: std.Thread.ResetEvent = .{},
        err: ?anyerror = null,

        fn callback(ctx: ?*anyopaque, res: anyerror!p2p_conn.AnyConn) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            _ = res catch |err| {
                self.err = err;
            };
            self.event.set();
        }
    };
    var tcp_ctx = TcpDialCtx{};
    dial_transport.dial(peer_addr, &tcp_ctx, TcpDialCtx.callback);
    tcp_ctx.event.wait();
    if (tcp_ctx.err) |err| return err;

    // Wait for TLS + Yamux.
    dial_slot.event.wait();
    if (dial_slot.err) |err| return err;

    const yamux = dial_slot.yamux.?;

    // Register the Yamux session so switch.newStream can find it by multiaddr.
    sw.registerTcpYamux(peer_ma_str, yamux, gs, Gossipsub.onIncomingNewStream);

    // Ask Gossipsub to add this peer (opens a gossipsub stream over yamux).
    const AddPeerCtx = struct {
        event: std.Thread.ResetEvent = .{},
        err: ?anyerror = null,

        fn cb(ctx: ?*anyopaque, res: anyerror!void) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.err = if (res) |_| null else |err| err;
            self.event.set();
        }
    };
    var ap_ctx = AddPeerCtx{};
    gs.addPeer(peer_ma, &ap_ctx, AddPeerCtx.cb);
    ap_ctx.event.wait();
    if (ap_ctx.err) |err| {
        std.log.warn("addPeer for node{d} failed: {}", .{ target_node_id, err });
    } else {
        std.log.info("peer node{d} added successfully", .{target_node_id});
    }
}
