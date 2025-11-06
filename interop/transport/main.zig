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
const multiaddr = @import("multiformats").multiaddr;
const Multiaddr = multiaddr.Multiaddr;

const RedisClient = struct {
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    reader: std.io.BufferedReader(4096, std.net.Stream.Reader),
    writer: std.io.BufferedWriter(4096, std.net.Stream.Writer),

    pub fn connect(allocator: std.mem.Allocator, host: []const u8, port: u16) !RedisClient {
        var stream = try std.net.tcpConnectToHost(allocator, host, port);
        errdefer stream.close();
        const reader = std.io.bufferedReader(stream.reader());
        const writer = std.io.bufferedWriter(stream.writer());

        return RedisClient{
            .allocator = allocator,
            .stream = stream,
            .reader = reader,
            .writer = writer,
        };
    }

    pub fn deinit(self: *RedisClient) void {
        self.writer.flush() catch {};
        self.stream.close();
    }

    fn writeCommand(self: *RedisClient, parts: []const []const u8) !void {
        var writer = self.writer.writer();
        try writer.print("*{d}\r\n", .{parts.len});
        for (parts) |part| {
            try writer.print("${d}\r\n", .{part.len});
            try writer.writeAll(part);
            try writer.writeAll("\r\n");
        }
        try self.writer.flush();
    }

    fn readLine(self: *RedisClient, buffer: *std.ArrayList(u8)) !void {
        buffer.clearRetainingCapacity();
        var reader = self.reader.reader();
        while (true) {
            const byte = try reader.readByte();
            if (byte == '\r') {
                const next = try reader.readByte();
                if (next != '\n') return error.InvalidResponse;
                break;
            }
            try buffer.append(byte);
        }
    }

    fn readInteger(self: *RedisClient, scratch: *std.ArrayList(u8)) !i64 {
        try self.readLine(scratch);
        return std.fmt.parseInt(i64, scratch.items, 10);
    }

    fn readBulkString(self: *RedisClient, scratch: *std.ArrayList(u8)) !?[]u8 {
        var reader = self.reader.reader();
        const prefix = try reader.readByte();
        if (prefix != '$') return error.InvalidResponse;
        return self.readBulkStringAfterPrefix(scratch, reader);
    }

    fn readBulkStringAfterPrefix(
        self: *RedisClient,
        scratch: *std.ArrayList(u8),
        reader_any: anytype,
    ) !?[]u8 {
        var reader = reader_any;
        scratch.clearRetainingCapacity();
        while (true) {
            const byte = try reader.readByte();
            if (byte == '\r') {
                const next = try reader.readByte();
                if (next != '\n') return error.InvalidResponse;
                break;
            }
            try scratch.append(byte);
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

        try reader.readNoEof(data);
        const cr = try reader.readByte();
        const lf = try reader.readByte();
        if (cr != '\r' or lf != '\n') {
            return error.InvalidResponse;
        }
        return data;
    }

    pub fn rpush(self: *RedisClient, key: []const u8, value: []const u8, scratch: *std.ArrayList(u8)) !void {
        const cmd = [_][]const u8{ "RPUSH", key, value };
        try self.writeCommand(&cmd);
        const reader = self.reader.reader();
        const prefix = try reader.readByte();
        if (prefix != ':') return error.InvalidResponse;
        _ = try self.readInteger(scratch);
    }

    pub fn del(self: *RedisClient, key: []const u8, scratch: *std.ArrayList(u8)) !void {
        const cmd = [_][]const u8{ "DEL", key };
        try self.writeCommand(&cmd);
        const reader = self.reader.reader();
        const prefix = try reader.readByte();
        if (prefix != ':') return error.InvalidResponse;
        _ = try self.readInteger(scratch);
    }

    pub fn blpop(self: *RedisClient, key: []const u8, timeout_secs: u32, scratch: *std.ArrayList(u8)) !?[]u8 {
        var timeout_buf: [16]u8 = undefined;
        const timeout_slice = try std.fmt.bufPrint(&timeout_buf, "{d}", .{timeout_secs});
        const cmd = [_][]const u8{ "BLPOP", key, timeout_slice }; // lifetime tied to stack
        try self.writeCommand(&cmd);

        var reader = self.reader.reader();
        const prefix = try reader.readByte();
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
                const value = try self.readBulkStringAfterPrefix(scratch, reader);
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
        const transport_owned = try getRequiredOwned(allocator, "transport");
        errdefer allocator.free(transport_owned);

        const is_dialer_raw = try getRequiredOwned(allocator, "is_dialer");
        defer allocator.free(is_dialer_raw);
        const is_dialer = try parseBool(is_dialer_raw);

        const bind_ip_owned = try getOptionalOwnedOrDefault(allocator, "ip", "0.0.0.0");
        errdefer allocator.free(bind_ip_owned);

        const default_publish = if (is_dialer) "dialer" else "listener";
        const publish_host_owned = try getOptionalOwnedOrDefault(allocator, "PUBLISH_HOST", default_publish);
        errdefer allocator.free(publish_host_owned);

        const muxer_owned = try getOptionalOwned(allocator, "muxer");
        errdefer if (muxer_owned) |value| allocator.free(value);

        const security_owned = try getOptionalOwned(allocator, "security");
        errdefer if (security_owned) |value| allocator.free(value);

        const redis_addr_owned = try getOptionalOwnedOrDefault(allocator, "redis_addr", "redis:6379");
        errdefer allocator.free(redis_addr_owned);
        const parsed = try parseHostPort(allocator, redis_addr_owned);
        const redis_host_owned = parsed.host;
        allocator.free(redis_addr_owned);

        const timeout_secs = try getOptionalUint(allocator, "test_timeout_seconds", 180);
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
        var stderr_writer = std.io.getStdErr().writer();
        stderr_writer.print("env redis host: {s}:{d}\n", .{ env.redis_host, env.redis_port }) catch {};
    }

    if (!std.mem.eql(u8, env.transport, "quic") and !std.mem.eql(u8, env.transport, "quic-v1")) {
        std.log.err("unsupported transport {s}", .{env.transport});
        return error.UnsupportedTransport;
    }

    if (env.muxer) |muxer| {
        if (muxer.len != 0 and !std.mem.eql(u8, muxer, "none")) {
            std.log.err("unsupported muxer {s}", .{muxer});
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

    var transport: quic.QuicTransport = undefined;
    try transport.init(&loop, &host_key, keys.KeyType.ECDSA, allocator);

    var switcher: swarm.Switch = undefined;
    switcher.init(allocator, &transport);
    defer {
        switcher.stop();
        loop.close();
        switcher.deinit();
    }

    var ping_handler = ping.PingProtocolHandler.init(allocator, &switcher);
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

fn runListener(
    allocator: std.mem.Allocator,
    switcher: *swarm.Switch,
    redis: *RedisClient,
    env: *const Env,
    transport: *quic.QuicTransport,
) !void {
    var scratch = std.ArrayList(u8).init(allocator);
    defer scratch.deinit();

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
    std.time.sleep(sleep_ns);
}

fn buildBaseMultiaddrString(allocator: std.mem.Allocator, host: []const u8, port: u16, transport: []const u8) ![]u8 {
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

    try ma.push(.{ .Udp = port });
    try pushTransport(&ma, transport);

    const rendered = try ma.toString(allocator);
    ma.deinit();
    return rendered;
}

fn pushTransport(ma: *Multiaddr, transport: []const u8) !void {
    if (std.mem.eql(u8, transport, "quic")) {
        try ma.push(.Quic);
    } else if (std.mem.eql(u8, transport, "quic-v1")) {
        try ma.push(.QuicV1);
    } else {
        return error.UnsupportedTransport;
    }
}

fn runDialer(
    allocator: std.mem.Allocator,
    switcher: *swarm.Switch,
    redis: *RedisClient,
    env: *const Env,
    transport: *quic.QuicTransport,
) !void {
    _ = transport;
    var scratch = std.ArrayList(u8).init(allocator);
    defer scratch.deinit();

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

            fn callback(ctx: ?*anyopaque, _: anyerror!*quic.QuicStream) void {
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

    var stdout = std.io.getStdOut().writer();
    try std.json.stringify(metrics, .{}, stdout);
    try stdout.writeByte('\n');
}
