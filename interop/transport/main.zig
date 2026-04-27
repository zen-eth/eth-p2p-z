pub const std_options = @import("zig-libp2p").std_options;

const std = @import("std");
const libp2p = @import("zig-libp2p");
const io_loop = libp2p.thread_event_loop;
const quic = libp2p.transport.quic;
const identity = libp2p.identity;
const swarm = libp2p.swarm;
const protocols = libp2p.protocols;
const ping = libp2p.protocols.ping;
const keys = @import("peer_id").keys;
const PeerId = @import("peer_id").PeerId;
const Multiaddr = @import("multiaddr").Multiaddr;

// TCP / TLS / Yamux imports
const tcp_mod = libp2p.transport.tcp;
const yamux_mod = libp2p.transport.yamux;
const tls_tcp_mod = libp2p.security.tls_tcp;
const tls_sec_mod = libp2p.security.tls;
const p2p_conn = libp2p.conn;
const proto_binding_mod = libp2p.multistream.proto_binding;
const multistream_mod = libp2p.multistream.multistream;
const stream_mss = libp2p.protocols.mss;

// ============================================================
// TCP helper types
// ============================================================

/// Routes YamuxStream data_handler bytes → proto_msg_handler.onMessage.
/// Must be heap-allocated (or stack-allocated in a scope that outlives
/// all async data delivery) and have its .stream field set before the
/// stream's data_handler is registered.
const ProtoMsgBridge = struct {
    stream: protocols.AnyStream,

    const vtable = yamux_mod.StreamDataHandlerVTable{
        .onDataFn = onDataImpl,
        .onCloseFn = onCloseImpl,
    };

    fn onDataImpl(instance: *anyopaque, data: []const u8) anyerror!void {
        const self: *ProtoMsgBridge = @ptrCast(@alignCast(instance));
        if (self.stream.getProtoMsgHandler()) |h| {
            var handler = h;
            try handler.onMessage(self.stream, data);
        }
    }

    // No-op: PingInitiator.onClose would access PingProtocolHandler (already
    // defer-deinitialized) by the time close fires.  The slots are transient
    // for a single-use interop binary so leaking them is acceptable.
    fn onCloseImpl(_: *anyopaque) void {}

    pub fn any(self: *ProtoMsgBridge) yamux_mod.AnyStreamDataHandler {
        return .{ .instance = self, .vtable = &vtable };
    }
};

/// Context that receives the Yamux session after TLS + pipeline setup.
/// Allocated on the main-thread stack; event fires when TLS is complete.
const TlsDoneCtx = struct {
    event: std.Thread.ResetEvent = .{},
    conn: ?p2p_conn.AnyConn = null,
    yamux: ?*yamux_mod.YamuxSession = null,
    err: ?anyerror = null,
    allocator: std.mem.Allocator,
    is_client: bool,
};

/// Custom AnyConnEnhancer: negotiates /tls/1.0.0 via multistream-select,
/// then adds a YamuxSession to the pipeline and signals TlsDoneCtx.
const TcpTlsEnhancer = struct {
    tls_binding: proto_binding_mod.AnyProtocolBinding,
    tls_done_ctx: *TlsDoneCtx,
    /// Storage for the single-element bindings slice passed to Multistream.
    bindings_buf: [1]proto_binding_mod.AnyProtocolBinding,

    const vtable = p2p_conn.ConnEnhancerVTable{
        .enhanceConnFn = enhanceConnImpl,
    };

    fn enhanceConnImpl(instance: *anyopaque, conn: p2p_conn.AnyConn) anyerror!void {
        const self: *TcpTlsEnhancer = @ptrCast(@alignCast(instance));
        self.bindings_buf = [1]proto_binding_mod.AnyProtocolBinding{self.tls_binding};

        const upgrade_ctx = conn.getPipeline().allocator.create(UpgradeCtx) catch
            return error.OutOfMemory;
        upgrade_ctx.* = .{
            .conn = conn,
            .tls_done = self.tls_done_ctx,
        };

        var ms: multistream_mod.Multistream = undefined;
        try ms.init(10_000, &self.bindings_buf);
        ms.initConn(conn, upgrade_ctx, UpgradeCtx.callback);
    }

    pub fn any(self: *TcpTlsEnhancer) p2p_conn.AnyConnEnhancer {
        return .{ .instance = self, .vtable = &vtable };
    }

    // --- Inner callback context -------------------------------------------

    const UpgradeCtx = struct {
        conn: p2p_conn.AnyConn,
        tls_done: *TlsDoneCtx,

        fn callback(instance: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *UpgradeCtx = @ptrCast(@alignCast(instance.?));
            defer self.conn.getPipeline().allocator.destroy(self);

            const td = self.tls_done;

            if (res) |result| {
                const session: *p2p_conn.SecuritySession = @ptrCast(@alignCast(result.?));
                self.conn.setSecuritySession(session.*);
                self.conn.getPipeline().allocator.destroy(session);

                // Add Yamux to the pipeline.
                const yamux = yamux_mod.YamuxSession.init(
                    td.allocator,
                    self.conn,
                    td.is_client,
                ) catch |err| {
                    td.err = err;
                    td.event.set();
                    return;
                };

                // Provide the remote peer identity so PingProtocolHandler can
                // acquire an outbound slot keyed by peer ID.
                if (self.conn.securitySession()) |sec| {
                    yamux.remote_peer_id = PeerId.fromBytes(sec.remote_id) catch null;
                }

                self.conn.getPipeline().addLast("yamux", yamux.handler()) catch |err| {
                    yamux.deinit();
                    td.err = err;
                    td.event.set();
                    return;
                };

                // Manually trigger onActive so YamuxSession.handler_ctx is set.
                const yamux_hctx = self.conn.getPipeline().tail.prev_context.?;
                yamux_hctx.handler.onActive(yamux_hctx) catch |err| {
                    yamux.deinit();
                    td.err = err;
                    td.event.set();
                    return;
                };

                td.conn = self.conn;
                td.yamux = yamux;
                td.event.set();
            } else |err| {
                td.err = err;
                td.event.set();
            }
        }
    };
};

// ============================================================
// Redis client
// ============================================================

const RedisClient = struct {
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    reader: std.net.Stream.Reader,
    writer: std.net.Stream.Writer,
    read_buf: [4096]u8,
    write_buf: [4096]u8,

    pub fn connect(allocator: std.mem.Allocator, host: []const u8, port: u16) !RedisClient {
        const stream = try std.net.tcpConnectToHost(allocator, host, port);
        errdefer stream.close();

        var client = RedisClient{
            .allocator = allocator,
            .stream = stream,
            .reader = undefined,
            .writer = undefined,
            .read_buf = undefined,
            .write_buf = undefined,
        };
        client.reader = stream.reader(&client.read_buf);
        client.writer = stream.writer(&client.write_buf);

        return client;
    }

    pub fn deinit(self: *RedisClient) void {
        self.writer.interface.flush() catch {};
        self.stream.close();
    }

    fn writeCommand(self: *RedisClient, parts: []const []const u8) !void {
        const writer = &self.writer.interface;
        try writer.print("*{d}\r\n", .{parts.len});
        for (parts) |part| {
            try writer.print("${d}\r\n", .{part.len});
            try writer.writeAll(part);
            try writer.writeAll("\r\n");
        }
        try self.writer.interface.flush();
    }

    fn readOneByte(self: *RedisClient) !u8 {
        var buf: [1]u8 = undefined;
        try self.reader.interface().readSliceAll(&buf);
        return buf[0];
    }

    fn readExact(self: *RedisClient, buffer: []u8) !void {
        try self.reader.interface().readSliceAll(buffer);
    }

    fn readLine(self: *RedisClient, buffer: *std.ArrayList(u8)) !void {
        buffer.clearRetainingCapacity();
        while (true) {
            const byte = try self.readOneByte();
            if (byte == '\r') {
                const next = try self.readOneByte();
                if (next != '\n') return error.InvalidResponse;
                break;
            }
            try buffer.append(self.allocator, byte);
        }
    }

    fn readInteger(self: *RedisClient, scratch: *std.ArrayList(u8)) !i64 {
        try self.readLine(scratch);
        return std.fmt.parseInt(i64, scratch.items, 10);
    }

    fn readBulkString(self: *RedisClient, scratch: *std.ArrayList(u8)) !?[]u8 {
        const prefix = try self.readOneByte();
        if (prefix != '$') return error.InvalidResponse;
        return self.readBulkStringBody(scratch);
    }

    fn readBulkStringBody(
        self: *RedisClient,
        scratch: *std.ArrayList(u8),
    ) !?[]u8 {
        scratch.clearRetainingCapacity();
        while (true) {
            const byte = try self.readOneByte();
            if (byte == '\r') {
                const next = try self.readOneByte();
                if (next != '\n') return error.InvalidResponse;
                break;
            }
            try scratch.append(self.allocator, byte);
        }
        const length = std.fmt.parseInt(i64, scratch.items, 10) catch {
            std.log.err("invalid bulk length line: {s}", .{scratch.items});
            return error.InvalidResponse;
        };
        if (length < 0) {
            return null;
        }
        const usize_len: usize = @intCast(length);
        const data = try self.allocator.alloc(u8, usize_len);
        errdefer self.allocator.free(data);

        try self.readExact(data);
        const cr = try self.readOneByte();
        const lf = try self.readOneByte();
        if (cr != '\r' or lf != '\n') {
            return error.InvalidResponse;
        }
        return data;
    }

    pub fn rpush(self: *RedisClient, key: []const u8, value: []const u8, scratch: *std.ArrayList(u8)) !void {
        const cmd = [_][]const u8{ "RPUSH", key, value };
        try self.writeCommand(&cmd);
        const prefix = try self.readOneByte();
        if (prefix != ':') return error.InvalidResponse;
        _ = try self.readInteger(scratch);
    }

    pub fn del(self: *RedisClient, key: []const u8, scratch: *std.ArrayList(u8)) !void {
        const cmd = [_][]const u8{ "DEL", key };
        try self.writeCommand(&cmd);
        const prefix = try self.readOneByte();
        if (prefix != ':') return error.InvalidResponse;
        _ = try self.readInteger(scratch);
    }

    pub fn blpop(self: *RedisClient, key: []const u8, timeout_secs: u32, scratch: *std.ArrayList(u8)) !?[]u8 {
        var timeout_buf: [16]u8 = undefined;
        const timeout_slice = try std.fmt.bufPrint(&timeout_buf, "{d}", .{timeout_secs});
        const cmd = [_][]const u8{ "BLPOP", key, timeout_slice };
        try self.writeCommand(&cmd);

        const prefix = try self.readOneByte();
        switch (prefix) {
            '*' => {
                const count = try self.readInteger(scratch);
                if (count == 0) return null;
                if (count != 2) return error.InvalidResponse;
                const key_value = try self.readBulkString(scratch) orelse return error.InvalidResponse;
                defer self.allocator.free(key_value);
                return self.readBulkString(scratch);
            },
            '$' => {
                const value = try self.readBulkString(scratch);
                return value;
            },
            '-' => {
                try self.readLine(scratch);
                return error.RedisError;
            },
            else => return error.InvalidResponse,
        }
    }
};

const Env = struct {
    transport: []const u8,
    is_dialer: bool,
    muxer: ?[]u8,
    security: ?[]u8,
    bind_ip: []const u8,
    publish_host: []const u8,
    redis_host: []const u8,
    redis_port: u16,
    timeout_seconds: u32,
    listen_port: u16,

    fn load(allocator: std.mem.Allocator) !Env {
        const transport_owned = try getRequiredOwned(allocator, "TRANSPORT");
        errdefer allocator.free(transport_owned);

        const is_dialer_raw = try getRequiredOwned(allocator, "IS_DIALER");
        defer allocator.free(is_dialer_raw);
        const is_dialer = try parseBool(is_dialer_raw);

        const bind_ip_owned = try getOptionalOwnedOrDefault(allocator, "IP", "0.0.0.0");
        errdefer allocator.free(bind_ip_owned);

        const default_publish = if (is_dialer) "dialer" else "listener";
        const publish_host_owned = try getOptionalOwnedOrDefault(allocator, "PUBLISH_HOST", default_publish);
        errdefer allocator.free(publish_host_owned);

        const muxer_owned = try getOptionalOwned(allocator, "MUXER");
        errdefer if (muxer_owned) |value| allocator.free(value);

        const security_owned = try getOptionalOwned(allocator, "SECURITY");
        errdefer if (security_owned) |value| allocator.free(value);

        const redis_addr_owned = try getOptionalOwnedOrDefault(allocator, "REDIS_ADDR", "redis:6379");
        errdefer allocator.free(redis_addr_owned);
        const parsed = try parseHostPort(allocator, redis_addr_owned);
        const redis_host_owned = parsed.host;
        allocator.free(redis_addr_owned);

        const timeout_secs = try getOptionalUint(allocator, "TEST_TIMEOUT_SECONDS", 180);
        const listen_port_value = try getOptionalUint(allocator, "LISTEN_PORT", 4001);
        if (listen_port_value > std.math.maxInt(u16)) return error.InvalidPort;
        const listen_port = @as(u16, @intCast(listen_port_value));

        return Env{
            .transport = transport_owned,
            .is_dialer = is_dialer,
            .muxer = muxer_owned,
            .security = security_owned,
            .bind_ip = bind_ip_owned,
            .publish_host = publish_host_owned,
            .redis_host = redis_host_owned,
            .redis_port = parsed.port,
            .timeout_seconds = timeout_secs,
            .listen_port = listen_port,
        };
    }

    fn deinit(self: *Env, allocator: std.mem.Allocator) void {
        allocator.free(self.transport);
        allocator.free(self.bind_ip);
        allocator.free(self.publish_host);
        allocator.free(self.redis_host);
        if (self.muxer) |value| allocator.free(value);
        if (self.security) |value| allocator.free(value);
    }
};

const ParsedHostPort = struct {
    host: []u8,
    port: u16,
};

fn parseHostPort(allocator: std.mem.Allocator, value: []const u8) !ParsedHostPort {
    const idx = std.mem.lastIndexOfScalar(u8, value, ':') orelse return error.InvalidAddressFormat;
    const host_part = value[0..idx];
    const port_part = value[idx + 1 ..];
    const port = try std.fmt.parseInt(u16, port_part, 10);
    const host_buf = try allocator.dupe(u8, host_part);
    return ParsedHostPort{ .host = host_buf, .port = port };
}

fn getRequiredOwned(allocator: std.mem.Allocator, key: []const u8) ![]u8 {
    return std.process.getEnvVarOwned(allocator, key) catch |err| {
        if (err == error.EnvironmentVariableNotFound) {
            std.log.err("missing required env var {s}", .{key});
        }
        return err;
    };
}

fn getOptionalOwnedOrDefault(allocator: std.mem.Allocator, key: []const u8, default_value: []const u8) ![]u8 {
    return std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => allocator.dupe(u8, default_value),
        else => err,
    };
}

fn getOptionalOwned(allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
    return std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => err,
    };
}

fn resolvePublishHost(allocator: std.mem.Allocator, is_dialer: bool, bind_ip: []const u8) ![]u8 {
    if (is_dialer) {
        return allocator.dupe(u8, "dialer");
    }

    std.log.info("listener bind_ip {s}", .{bind_ip});
    return allocator.dupe(u8, bind_ip);
}

fn logEnvironment(allocator: std.mem.Allocator) void {
    var env_map = std.process.getEnvMap(allocator) catch |err| {
        std.log.err("unable to load environment map: {any}", .{err});
        return;
    };
    defer env_map.deinit();

    var it = env_map.iterator();
    while (it.next()) |entry| {
        std.log.info("env {s}={s}", .{ entry.key_ptr.*, entry.value_ptr.* });
    }
}

fn getOptionalUint(allocator: std.mem.Allocator, key: []const u8, default_value: u32) !u32 {
    const owned = std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => return default_value,
        else => return err,
    };
    defer allocator.free(owned);
    return std.fmt.parseInt(u32, owned, 10);
}

fn parseBool(value: []const u8) !bool {
    if (std.ascii.eqlIgnoreCase(value, "true") or std.ascii.eqlIgnoreCase(value, "1")) return true;
    if (std.ascii.eqlIgnoreCase(value, "false") or std.ascii.eqlIgnoreCase(value, "0")) return false;
    return error.InvalidBoolean;
}

const PingResultCtx = struct {
    event: std.Thread.ResetEvent = .{},
    result_ns: ?u64 = null,
    err: ?anyerror = null,
    sender: ?*ping.PingStream = null,

    fn callback(ctx: ?*anyopaque, sender: ?*ping.PingStream, res: anyerror!u64) void {
        const self: *PingResultCtx = @ptrCast(@alignCast(ctx.?));
        self.result_ns = res catch |err| {
            self.err = err;
            self.sender = sender;
            self.event.set();
            return;
        };
        self.sender = sender;
        self.event.set();
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const check = gpa.deinit();
        if (check == .leak) std.log.warn("memory leaked from GPA", .{});
    }
    const allocator = gpa.allocator();

    logEnvironment(allocator);

    var env = try Env.load(allocator);
    defer env.deinit(allocator);

    {
        std.debug.print("env redis host: {s}:{d}\n", .{ env.redis_host, env.redis_port });
    }

    const is_tcp = std.mem.eql(u8, env.transport, "tcp");
    const is_quic = std.mem.eql(u8, env.transport, "quic") or std.mem.eql(u8, env.transport, "quic-v1");

    if (!is_tcp and !is_quic) {
        std.log.err("unsupported transport {s}", .{env.transport});
        return error.UnsupportedTransport;
    }

    if (env.muxer) |muxer| {
        if (is_quic and muxer.len != 0 and !std.mem.eql(u8, muxer, "none")) {
            std.log.err("unsupported muxer {s} for quic", .{muxer});
            return error.UnsupportedMuxer;
        }
        if (is_tcp and muxer.len != 0 and
            !std.mem.eql(u8, muxer, "none") and
            !std.mem.eql(u8, muxer, "yamux"))
        {
            std.log.err("unsupported muxer {s} for tcp", .{muxer});
            return error.UnsupportedMuxer;
        }
    }

    if (env.security) |security| {
        if (security.len != 0 and !std.mem.eql(u8, security, "tls")) {
            std.log.err("unsupported security {s}", .{security});
            return error.UnsupportedSecurity;
        }
    }

    var loop: io_loop.ThreadEventLoop = undefined;
    try loop.init(allocator);
    defer {
        loop.deinit();
    }

    var host_key = try identity.KeyPair.generate(keys.KeyType.ED25519);
    defer host_key.deinit();

    if (is_tcp) {
        // ---- TCP + TLS + Yamux path ----------------------------------------
        var host_pub_key = try host_key.publicKey(allocator);
        defer if (host_pub_key.data) |data| allocator.free(data);

        // EVP_PKEY backing the ED25519 host identity (stored in the .tls variant).
        const sign_ctx: ?*anyopaque = host_key.storage.tls;

        var tls_done_ctx = TlsDoneCtx{
            .allocator = allocator,
            .is_client = env.is_dialer,
        };

        var tls_channel: tls_tcp_mod.TlsTcpChannel = undefined;
        try tls_channel.init(
            allocator,
            &host_pub_key,
            sign_ctx,
            tls_sec_mod.signDataWithTlsKey,
        );
        defer tls_channel.deinit();

        var enhancer = TcpTlsEnhancer{
            .tls_binding = tls_channel.any(),
            .tls_done_ctx = &tls_done_ctx,
            .bindings_buf = undefined,
        };

        var tcp_transport: tcp_mod.XevTransport = undefined;
        try tcp_transport.init(enhancer.any(), &loop, allocator, .{ .backlog = 128 });

        var redis = try RedisClient.connect(allocator, env.redis_host, env.redis_port);
        defer redis.deinit();

        if (env.is_dialer) {
            try runTcpDialer(allocator, &loop, &tcp_transport, &redis, &env, &tls_done_ctx);
        } else {
            try runTcpListener(allocator, &loop, &tcp_transport, &redis, &env, &tls_done_ctx);
        }
    } else {
        // ---- QUIC path (original) ------------------------------------------
        var transport: quic.QuicTransport = undefined;
        try transport.init(&loop, &host_key, keys.KeyType.ECDSA, allocator);

        var switcher: swarm.Switch = undefined;
        switcher.init(allocator, &transport);
        defer {
            switcher.stop();
            loop.close();
            switcher.deinit();
        }

        var ping_handler = ping.PingProtocolHandler.init(allocator, &loop);
        defer ping_handler.deinit();
        try switcher.addProtocolHandler(ping.protocol_id, ping_handler.any());

        var redis = try RedisClient.connect(allocator, env.redis_host, env.redis_port);
        defer redis.deinit();

        if (env.is_dialer) {
            try runDialer(allocator, &switcher, &redis, &env, &transport);
        } else {
            try runListener(allocator, &switcher, &redis, &env, &transport);
        }
    }
}

// ============================================================
// TCP dialer
// ============================================================

fn runTcpDialer(
    allocator: std.mem.Allocator,
    loop: *io_loop.ThreadEventLoop,
    transport: *tcp_mod.XevTransport,
    redis: *RedisClient,
    env: *const Env,
    tls_done_ctx: *TlsDoneCtx,
) !void {
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);

    std.log.info("waiting for listener address (TCP/Yamux)", .{});
    const blpop_res = try redis.blpop("listenerAddr", env.timeout_seconds, &scratch);
    const addr_bytes = blpop_res orelse return error.ListenerAddressTimeout;
    defer allocator.free(addr_bytes);

    const trimmed = std.mem.trim(u8, addr_bytes, " \t\r\n");
    std.log.info("dialing (TCP) {s}", .{trimmed});

    // Parse /ip4/<ip>/tcp/<port>[/p2p/<peer>] from the multiaddr string.
    var ip4_str: ?[]const u8 = null;
    var tcp_port: ?u16 = null;
    {
        var parts = std.mem.splitScalar(u8, trimmed, '/');
        _ = parts.next(); // empty segment before leading '/'
        while (parts.next()) |seg| {
            if (std.mem.eql(u8, seg, "ip4")) {
                ip4_str = parts.next();
            } else if (std.mem.eql(u8, seg, "tcp")) {
                const port_seg = parts.next() orelse return error.InvalidMultiaddr;
                tcp_port = try std.fmt.parseInt(u16, port_seg, 10);
            }
        }
    }
    const ip4 = ip4_str orelse return error.MissingIp4InMultiaddr;
    const port = tcp_port orelse return error.MissingTcpPortInMultiaddr;
    const dial_addr = try std.net.Address.parseIp4(ip4, port);

    const timeout_ns = @as(u128, env.timeout_seconds) * std.time.ns_per_s;
    const ping_timeout_ns: u64 = if (timeout_ns > std.math.maxInt(u64))
        std.math.maxInt(u64)
    else
        @intCast(timeout_ns);

    var timer = try std.time.Timer.start();

    // Dial TCP — callback fires immediately upon TCP connection (before TLS).
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
    var tcp_dial_ctx = TcpDialCtx{};
    transport.dial(dial_addr, &tcp_dial_ctx, TcpDialCtx.callback);
    tcp_dial_ctx.event.wait();
    if (tcp_dial_ctx.err) |err| return err;

    // Wait for TLS handshake + Yamux session to be ready.
    tls_done_ctx.event.wait();
    if (tls_done_ctx.err) |err| return err;

    const yamux = tls_done_ctx.yamux.?;

    // Open a Yamux stream and set up stream-level MSS → ping.
    const ping_proto_id: protocols.ProtocolId = ping.protocol_id;
    var proposed_protos = [1]protocols.ProtocolId{ping_proto_id};

    const stream = try yamux.openStream();
    stream.proposed_protocols = &proposed_protos;

    var bridge = ProtoMsgBridge{ .stream = stream.any() };
    stream.setDataHandler(bridge.any());

    // Stream-level multistream-select handler with ping registered.
    var stream_mss_handler = stream_mss.MultistreamSelectHandler.init(allocator);
    defer stream_mss_handler.deinit();

    var ping_handler = ping.PingProtocolHandler.init(allocator, loop);
    defer ping_handler.deinit();
    try stream_mss_handler.addProtocolHandler(ping.protocol_id, ping_handler.any());

    // Context for receiving the PingStream after MSS selects /ipfs/ping/1.0.0.
    const PingStreamCtx = struct {
        event: std.Thread.ResetEvent = .{},
        sender: ?*ping.PingStream = null,
        err: ?anyerror = null,

        fn callback(ctx: ?*anyopaque, res: anyerror!?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            if (res) |ptr| {
                self.sender = @ptrCast(@alignCast(ptr.?));
            } else |err| {
                self.err = err;
            }
            self.event.set();
        }
    };
    var ping_stream_ctx = PingStreamCtx{};

    try stream_mss_handler.onInitiatorStart(stream.any(), &ping_stream_ctx, PingStreamCtx.callback);

    // Kick off MSS header exchange.
    {
        var handler = stream.any().getProtoMsgHandler().?;
        try handler.onActivated(stream.any());
    }

    // Wait for MSS completion + PingStream ready.
    ping_stream_ctx.event.wait();
    if (ping_stream_ctx.err) |err| return err;
    const ping_stream = ping_stream_ctx.sender.?;

    // Perform the ping.
    var ping_rtt_ctx = PingResultCtx{};
    try ping_stream.initiator.?.beginPing(ping_timeout_ns, &ping_rtt_ctx, PingResultCtx.callback);
    ping_rtt_ctx.event.wait();

    // Close the ping stream.
    if (ping_rtt_ctx.sender) |sender| {
        const CloseCtx = struct {
            event: std.Thread.ResetEvent = .{},

            fn callback(ctx: ?*anyopaque, _: anyerror!void) void {
                const self: *@This() = @ptrCast(@alignCast(ctx.?));
                self.event.set();
            }
        };
        var close_ctx = CloseCtx{};
        sender.close(&close_ctx, CloseCtx.callback);
        close_ctx.event.wait();
    }

    if (ping_rtt_ctx.err) |err| {
        std.debug.print("interop[tcp-dialer] -> ping failed: {any}\n", .{err});
        return err;
    }

    std.debug.print("interop[tcp-dialer] -> ping completed successfully\n", .{});
    const rtt_ns = ping_rtt_ctx.result_ns orelse return error.PingFailed;
    const total_ns = timer.read();

    const handshake_ms = @as(f64, @floatFromInt(total_ns)) / @as(f64, std.time.ns_per_ms);
    const ping_ms = @as(f64, @floatFromInt(rtt_ns)) / @as(f64, std.time.ns_per_ms);

    const metrics = struct {
        handshakePlusOneRTTMillis: f64,
        pingRTTMilllis: f64,
    }{ .handshakePlusOneRTTMillis = handshake_ms, .pingRTTMilllis = ping_ms };

    const stdout_file: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = stdout_file.writer(&stdout_buf);
    try std.json.Stringify.value(metrics, .{}, &stdout_writer.interface);
    try stdout_writer.interface.writeAll("\n");
    try stdout_writer.interface.flush();
}

// ============================================================
// TCP listener
// ============================================================

fn runTcpListener(
    allocator: std.mem.Allocator,
    loop: *io_loop.ThreadEventLoop,
    transport: *tcp_mod.XevTransport,
    redis: *RedisClient,
    env: *const Env,
    tls_done_ctx: *TlsDoneCtx,
) !void {
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);

    // Parse bind address.
    const bind_addr = try std.net.Address.parseIp(env.bind_ip, env.listen_port);
    const listener = try transport.listen(bind_addr);

    std.debug.print("interop[tcp-listener] -> listening on {s}:{d}\n", .{ env.bind_ip, env.listen_port });

    // Accept the first incoming connection in the background.
    // The TcpTlsEnhancer fires tls_done_ctx.event when TLS + Yamux are ready.
    const AcceptCtx = struct {
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
    var accept_ctx = AcceptCtx{};
    listener.accept(&accept_ctx, AcceptCtx.callback);

    // Publish address to Redis so the dialer can connect.
    const publish_base = try buildBaseMultiaddrString(allocator, env.publish_host, env.listen_port, env.transport);
    defer allocator.free(publish_base);

    // For TCP we need to publish the peer ID too.
    // Derive it from the public key stored in the TLS channel.
    // Since we don't have easy access to the peer ID here, we publish without /p2p/ suffix.
    // The dialer only needs the IP + port.
    std.log.info("listener publishing {s}", .{publish_base});
    redis.del("listenerAddr", &scratch) catch |err| {
        std.log.warn("failed to clear listenerAddr list: {any}", .{err});
    };
    try redis.rpush("listenerAddr", publish_base, &scratch);

    // Wait for a connection (TCP accept) — TLS + Yamux setup happens async.
    accept_ctx.event.wait();
    if (accept_ctx.err) |err| {
        std.log.err("accept error: {any}", .{err});
        return err;
    }

    // Wait for TLS + Yamux to complete.
    tls_done_ctx.event.wait();
    if (tls_done_ctx.err) |err| {
        std.log.err("TLS/Yamux error: {any}", .{err});
        return err;
    }

    const yamux = tls_done_ctx.yamux.?;

    // Set up a ping responder context on the heap so it outlives this function's
    // sleep period.  The event loop will call callbacks on these objects.
    const ListenerStreamCtx = struct {
        mss_handler: stream_mss.MultistreamSelectHandler,
        ping_handler: ping.PingProtocolHandler,
        bridge: ProtoMsgBridge,
        allocator: std.mem.Allocator,

        fn handleStream(ctx: ?*anyopaque, res: anyerror!*yamux_mod.YamuxStream) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            const raw_stream = res catch |err| {
                std.log.err("acceptStream error: {any}", .{err});
                return;
            };

            const any_stream = raw_stream.any();
            self.bridge.stream = any_stream;
            raw_stream.setDataHandler(self.bridge.any());

            self.mss_handler.onResponderStart(any_stream, null, noopControllerCb) catch |err| {
                std.log.err("onResponderStart error: {any}", .{err});
                return;
            };

            var handler = any_stream.getProtoMsgHandler().?;
            handler.onActivated(any_stream) catch |err| {
                std.log.err("onActivated (responder) error: {any}", .{err});
            };
        }
    };

    const ls_ctx = try allocator.create(ListenerStreamCtx);
    // Note: ls_ctx is intentionally NOT freed — it must outlive the sleep period
    // while the event loop handles incoming pings via callbacks.
    ls_ctx.allocator = allocator;
    ls_ctx.mss_handler = stream_mss.MultistreamSelectHandler.init(allocator);
    ls_ctx.ping_handler = ping.PingProtocolHandler.init(allocator, loop);
    ls_ctx.bridge = .{ .stream = undefined }; // .stream set in handleStream
    try ls_ctx.mss_handler.addProtocolHandler(ping.protocol_id, ls_ctx.ping_handler.any());

    yamux.acceptStream(ls_ctx, ListenerStreamCtx.handleStream);

    std.log.info("interop[tcp-listener] -> waiting for pings (timeout={d}s)", .{env.timeout_seconds});
    const sleep_ns = std.math.clamp(
        @as(u64, env.timeout_seconds) * std.time.ns_per_s,
        std.time.ns_per_s,
        std.math.maxInt(u64),
    );
    std.Thread.sleep(sleep_ns);
}

fn noopControllerCb(_: ?*anyopaque, _: anyerror!?*anyopaque) void {}

// ============================================================
// QUIC listener (original)
// ============================================================

fn runListener(
    allocator: std.mem.Allocator,
    switcher: *swarm.Switch,
    redis: *RedisClient,
    env: *const Env,
    transport: *quic.QuicTransport,
) !void {
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);

    const listen_addr_str = try std.fmt.allocPrint(allocator, "/ip4/{s}/udp/{d}/{s}", .{ env.bind_ip, env.listen_port, env.transport });
    defer allocator.free(listen_addr_str);
    std.debug.print("interop[listener] -> starting listener on {s}\n", .{listen_addr_str});

    var listen_addr = try Multiaddr.fromString(allocator, listen_addr_str);
    defer listen_addr.deinit();

    try switcher.listen(listen_addr, null, struct {
        fn onStream(_: ?*anyopaque, res: anyerror!?*anyopaque) void {
            _ = res catch |err| {
                std.log.warn("incoming stream error: {any}", .{err});
                return;
            };
        }
    }.onStream);

    const peer_buf = try allocator.alloc(u8, transport.local_peer_id.toBase58Len());
    defer allocator.free(peer_buf);
    const peer_slice = try transport.local_peer_id.toBase58(peer_buf);

    var listen_addrs: ?std.ArrayList([]u8) = null;
    var publish_base: []u8 = undefined;

    discovery: {
        const discovered = switcher.listenMultiaddrs(allocator) catch |err| {
            std.log.warn(
                "unable to enumerate listener addresses via switch: {any}; falling back to configured publish host {s}",
                .{ err, env.publish_host },
            );
            publish_base = try buildBaseMultiaddrString(allocator, env.publish_host, env.listen_port, env.transport);
            break :discovery;
        };
        listen_addrs = discovered;
        const list_view = listen_addrs.?;
        std.log.info("enumerated {d} listener address(es):", .{list_view.items.len});
        for (list_view.items, 0..) |addr, i| {
            std.log.info("  [{d}] {s}", .{ i, addr });
        }
        if (list_view.items.len > 0) {
            publish_base = try allocator.dupe(u8, list_view.items[0]);
        } else {
            std.log.warn(
                "no listen addresses produced by switch; using configured publish host {s}",
                .{env.publish_host},
            );
            publish_base = try buildBaseMultiaddrString(allocator, env.publish_host, env.listen_port, env.transport);
        }
    }

    defer allocator.free(publish_base);
    defer if (listen_addrs) |*list| swarm.Switch.freeListenMultiaddrs(allocator, list);

    const published_addr = try std.fmt.allocPrint(allocator, "{s}/p2p/{s}", .{ publish_base, peer_slice });
    defer allocator.free(published_addr);

    redis.del("listenerAddr", &scratch) catch |err| {
        std.log.warn("failed to clear listenerAddr list: {any}", .{err});
    };

    std.log.info("listener publishing {s}", .{published_addr});
    try redis.rpush("listenerAddr", published_addr, &scratch);

    const sleep_ns = std.math.clamp(@as(u64, env.timeout_seconds) * std.time.ns_per_s, std.time.ns_per_s, std.math.maxInt(u64));
    std.Thread.sleep(sleep_ns);
}

fn buildBaseMultiaddrString(allocator: std.mem.Allocator, host: []const u8, port: u16, transport_name: []const u8) ![]u8 {
    var ma = Multiaddr.init(allocator);
    errdefer ma.deinit();

    const addr = std.net.Address.parseIp(host, port) catch |err| switch (err) {
        error.InvalidIPAddressFormat => blk: {
            try ma.push(.{ .Dns4 = host });
            break :blk null;
        },
        else => return err,
    };

    if (addr) |parsed| {
        switch (parsed.any.family) {
            std.posix.AF.INET => {
                const ip_bytes = std.mem.toBytes(parsed.in.sa.addr);
                try ma.push(.{ .Ip4 = std.net.Ip4Address.init(ip_bytes, 0) });
            },
            std.posix.AF.INET6 => {
                const ip_bytes = std.mem.toBytes(parsed.in6.sa.addr);
                try ma.push(.{ .Ip6 = std.net.Ip6Address.init(ip_bytes, 0, 0, 0) });
            },
            else => {},
        }
    }

    try pushTransport(&ma, transport_name, port);

    const rendered = try ma.toString(allocator);
    ma.deinit();
    return rendered;
}

fn pushTransport(ma: *Multiaddr, transport_name: []const u8, port: u16) !void {
    if (std.mem.eql(u8, transport_name, "quic")) {
        try ma.push(.{ .Udp = port });
        try ma.push(.Quic);
    } else if (std.mem.eql(u8, transport_name, "quic-v1")) {
        try ma.push(.{ .Udp = port });
        try ma.push(.QuicV1);
    } else if (std.mem.eql(u8, transport_name, "tcp")) {
        try ma.push(.{ .Tcp = port });
    } else {
        return error.UnsupportedTransport;
    }
}

// ============================================================
// QUIC dialer (original)
// ============================================================

fn runDialer(
    allocator: std.mem.Allocator,
    switcher: *swarm.Switch,
    redis: *RedisClient,
    env: *const Env,
    transport: *quic.QuicTransport,
) !void {
    _ = transport;
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);

    std.log.info("waiting for listener address", .{});
    const blpop_res = try redis.blpop("listenerAddr", env.timeout_seconds, &scratch);
    const addr_bytes = blpop_res orelse return error.ListenerAddressTimeout;
    defer allocator.free(addr_bytes);

    const trimmed = std.mem.trim(u8, addr_bytes, " \t\r\n");
    std.log.info("dialing {s}", .{trimmed});

    var remote_addr = try Multiaddr.fromString(allocator, trimmed);
    defer remote_addr.deinit();

    var ping_ctx = PingResultCtx{};
    var timer = try std.time.Timer.start();
    const timeout_ns = @as(u128, env.timeout_seconds) * std.time.ns_per_s;
    const ping_timeout_ns: u64 = if (timeout_ns > std.math.maxInt(u64))
        std.math.maxInt(u64)
    else
        @intCast(timeout_ns);
    std.debug.print("interop[dialer] -> initiating ping\n", .{});
    var ping_service = ping.PingService.init(allocator, switcher);
    try ping_service.ping(remote_addr, .{ .timeout_ns = ping_timeout_ns }, &ping_ctx, PingResultCtx.callback);
    ping_ctx.event.wait();

    if (ping_ctx.sender) |sender| {
        const CloseCtx = struct {
            event: std.Thread.ResetEvent = .{},

            const Self = @This();

            fn callback(ctx: ?*anyopaque, _: anyerror!void) void {
                const self: *Self = @ptrCast(@alignCast(ctx.?));
                self.event.set();
            }
        };

        var close_ctx = CloseCtx{};
        sender.close(&close_ctx, CloseCtx.callback);
        close_ctx.event.wait();
    }

    if (ping_ctx.err) |err| {
        std.debug.print("interop[dialer] -> ping failed: {any}\n", .{err});
        return err;
    }
    std.debug.print("interop[dialer] -> ping completed successfully\n", .{});
    const rtt_ns = ping_ctx.result_ns orelse return error.PingFailed;
    const total_ns = timer.read();

    const handshake_ms = @as(f64, @floatFromInt(total_ns)) / @as(f64, std.time.ns_per_ms);
    const ping_ms = @as(f64, @floatFromInt(rtt_ns)) / @as(f64, std.time.ns_per_ms);

    const metrics = struct {
        handshakePlusOneRTTMillis: f64,
        pingRTTMilllis: f64,
    }{ .handshakePlusOneRTTMillis = handshake_ms, .pingRTTMilllis = ping_ms };

    const stdout_file: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = stdout_file.writer(&stdout_buf);
    try std.json.Stringify.value(metrics, .{}, &stdout_writer.interface);
    try stdout_writer.interface.writeAll("\n");
    try stdout_writer.interface.flush();
}
