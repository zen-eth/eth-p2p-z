const std = @import("std");
const Io = std.Io;
const net = Io.net;
const libp2p = @import("zig-libp2p");
const ping_mod = libp2p.ping;
const identify_mod = libp2p.identify;
const quic_mod = libp2p.quic_transport;
const engine_mod = libp2p.quic_engine;
const identity = libp2p.identity;
const tls_mod = libp2p.tls;
const multiaddr = @import("multiaddr");
const Multiaddr = multiaddr.Multiaddr;
const Ip4Addr = multiaddr.Ip4Addr;
const ssl = @import("ssl");
const PeerId = @import("peer_id").PeerId;
const keys = @import("peer_id").keys;

const log = std.log.scoped(.interop);

// ── Env ──────────────────────────────────────────────────────────────────

const Env = struct {
    transport: []const u8,
    is_dialer: bool,
    bind_ip: []const u8,
    redis_host: []const u8,
    redis_port: u16,
    timeout_seconds: u32,
    listen_port: u16,

    fn load() Env {
        const transport = getEnvOrDefault("TRANSPORT", "quic-v1");
        const is_dialer = parseBool(getEnvOrDefault("IS_DIALER", "false"));
        const bind_ip = getEnvOrDefault("IP", "0.0.0.0");
        const redis_addr = getEnvOrDefault("REDIS_ADDR", "redis:6379");
        const timeout_seconds = parseU32(getEnvOrDefault("TEST_TIMEOUT_SECONDS", "180"));
        const listen_port = parseU16(getEnvOrDefault("LISTEN_PORT", "4001"));

        const parsed = parseHostPort(redis_addr);

        return .{
            .transport = transport,
            .is_dialer = is_dialer,
            .bind_ip = bind_ip,
            .redis_host = parsed.host,
            .redis_port = parsed.port,
            .timeout_seconds = timeout_seconds,
            .listen_port = listen_port,
        };
    }
};

fn getEnvOrDefault(key: [*:0]const u8, default: []const u8) []const u8 {
    const val = std.c.getenv(key);
    if (val) |v| {
        return std.mem.span(v);
    }
    return default;
}

fn parseBool(value: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(value, "true") or std.mem.eql(u8, value, "1")) return true;
    return false;
}

fn parseU32(value: []const u8) u32 {
    return std.fmt.parseInt(u32, value, 10) catch 180;
}

fn parseU16(value: []const u8) u16 {
    return std.fmt.parseInt(u16, value, 10) catch 4001;
}

const ParsedHostPort = struct {
    host: []const u8,
    port: u16,
};

fn parseHostPort(value: []const u8) ParsedHostPort {
    const idx = std.mem.lastIndexOfScalar(u8, value, ':') orelse return .{ .host = value, .port = 6379 };
    const host_part = value[0..idx];
    const port_part = value[idx + 1 ..];
    const port = std.fmt.parseInt(u16, port_part, 10) catch 6379;
    return .{ .host = host_part, .port = port };
}

// ── Redis client ─────────────────────────────────────────────────────────

const RedisClient = struct {
    stream: net.Stream,
    reader: net.Stream.Reader,
    writer: net.Stream.Writer,
    read_buf: [4096]u8,
    write_buf: [4096]u8,

    fn connect(io: Io, host: []const u8, port: u16) !RedisClient {
        // Parse host IP
        const ip = net.Ip4Address.parse(host, port) catch {
            // Try loopback for "localhost"
            return RedisClient.connectAddr(io, .{ .ip4 = net.Ip4Address.loopback(port) });
        };
        return RedisClient.connectAddr(io, .{ .ip4 = ip });
    }

    fn connectAddr(io: Io, addr: net.IpAddress) !RedisClient {
        const stream = try net.IpAddress.connect(addr, io, .{ .mode = .stream });
        var client = RedisClient{
            .stream = stream,
            .reader = undefined,
            .writer = undefined,
            .read_buf = undefined,
            .write_buf = undefined,
        };
        client.reader = stream.reader(io, &client.read_buf);
        client.writer = stream.writer(io, &client.write_buf);
        return client;
    }

    fn close(self: *RedisClient, io: Io) void {
        self.writer.interface.flush() catch {};
        self.stream.close(io);
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
        self.reader.interface.readSliceAll(&buf) catch return error.RedisReadError;
        return buf[0];
    }

    fn readLine(self: *RedisClient, buf: []u8) ![]const u8 {
        var len: usize = 0;
        while (len < buf.len) {
            const byte = try self.readOneByte();
            if (byte == '\r') {
                const next = try self.readOneByte();
                if (next != '\n') return error.InvalidResponse;
                return buf[0..len];
            }
            buf[len] = byte;
            len += 1;
        }
        return error.LineTooLong;
    }

    fn readInteger(self: *RedisClient) !i64 {
        var buf: [64]u8 = undefined;
        const line = try self.readLine(&buf);
        return std.fmt.parseInt(i64, line, 10);
    }

    fn readBulkString(self: *RedisClient, allocator: std.mem.Allocator) !?[]u8 {
        const prefix = try self.readOneByte();
        if (prefix != '$') return error.InvalidResponse;
        return self.readBulkStringBody(allocator);
    }

    fn readBulkStringBody(self: *RedisClient, allocator: std.mem.Allocator) !?[]u8 {
        var len_buf: [64]u8 = undefined;
        const len_line = try self.readLine(&len_buf);
        const length = std.fmt.parseInt(i64, len_line, 10) catch return error.InvalidResponse;
        if (length < 0) return null;

        const usize_len: usize = @intCast(length);
        const data = try allocator.alloc(u8, usize_len);
        errdefer allocator.free(data);

        // Read exact bytes
        var read: usize = 0;
        while (read < usize_len) {
            const byte = try self.readOneByte();
            data[read] = byte;
            read += 1;
        }

        // Read trailing \r\n
        const cr = try self.readOneByte();
        const lf = try self.readOneByte();
        if (cr != '\r' or lf != '\n') return error.InvalidResponse;
        return data;
    }

    fn rpush(self: *RedisClient, key: []const u8, value: []const u8) !void {
        const cmd = [_][]const u8{ "RPUSH", key, value };
        try self.writeCommand(&cmd);
        const prefix = try self.readOneByte();
        if (prefix != ':') return error.InvalidResponse;
        _ = try self.readInteger();
    }

    fn del(self: *RedisClient, key: []const u8) !void {
        const cmd = [_][]const u8{ "DEL", key };
        try self.writeCommand(&cmd);
        const prefix = try self.readOneByte();
        if (prefix != ':') return error.InvalidResponse;
        _ = try self.readInteger();
    }

    fn blpop(self: *RedisClient, allocator: std.mem.Allocator, key: []const u8, timeout_secs: u32) !?[]u8 {
        var timeout_buf: [16]u8 = undefined;
        const timeout_slice = std.fmt.bufPrint(&timeout_buf, "{d}", .{timeout_secs}) catch return error.FormatError;
        const cmd = [_][]const u8{ "BLPOP", key, timeout_slice };
        try self.writeCommand(&cmd);

        const prefix = try self.readOneByte();
        switch (prefix) {
            '*' => {
                const count = try self.readInteger();
                if (count == 0) return null;
                if (count != 2) return error.InvalidResponse;
                const key_value = try self.readBulkString(allocator) orelse return error.InvalidResponse;
                defer allocator.free(key_value);
                return self.readBulkString(allocator);
            },
            '$' => {
                return self.readBulkStringBody(allocator);
            },
            '-' => {
                var err_buf: [256]u8 = undefined;
                _ = try self.readLine(&err_buf);
                return error.RedisError;
            },
            else => return error.InvalidResponse,
        }
    }
};

// ── Timing ───────────────────────────────────────────────────────────────

fn timestampNs() u64 {
    return std.c.mach_absolute_time();
}

// ── Switch type ──────────────────────────────────────────────────────────

const AppSwitch = libp2p.Switch(.{
    .transports = &.{quic_mod.QuicTransport},
    .protocols = &.{ ping_mod.Handler, identify_mod.Handler },
});

// ── Entry point ──────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const check = gpa.deinit();
        if (check == .leak) log.warn("memory leaked from GPA", .{});
    }
    const allocator = gpa.allocator();

    var threaded: Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const env = Env.load();

    log.info("transport={s} is_dialer={} redis={s}:{d}", .{
        env.transport, env.is_dialer, env.redis_host, env.redis_port,
    });

    if (!std.mem.eql(u8, env.transport, "quic") and !std.mem.eql(u8, env.transport, "quic-v1")) {
        log.err("unsupported transport: {s}", .{env.transport});
        return error.UnsupportedTransport;
    }

    // Generate ECDSA host key
    const host_key = tls_mod.generateKeyPair(.ECDSA) catch |err| {
        log.err("failed to generate host key: {}", .{err});
        return err;
    };
    defer ssl.EVP_PKEY_free(host_key);

    // Derive local peer ID for listener address publishing
    var kp = identity.KeyPair{
        .key_type = .ECDSA,
        .backend = .tls,
        .storage = .{ .tls = host_key },
    };
    const local_peer_id = kp.peerId(allocator) catch |err| {
        log.err("failed to derive peer ID: {}", .{err});
        return err;
    };

    var peer_b58_buf: [128]u8 = undefined;
    const local_peer_b58 = local_peer_id.toBase58(&peer_b58_buf) catch |err| {
        log.err("failed to encode peer ID: {}", .{err});
        return err;
    };
    log.info("local peer ID: {s}", .{local_peer_b58});

    // Init Switch
    var sw = AppSwitch.init(allocator, .{ .host_key = host_key }, .{
        ping_mod.Handler{},
        identify_mod.Handler{
            .allocator = allocator,
            .config = .{},
            .peer_results = std.StringHashMap(identify_mod.IdentifyResult).init(allocator),
        },
    });
    defer sw.deinit(io);

    // Connect to Redis
    var redis = RedisClient.connect(io, env.redis_host, env.redis_port) catch |err| {
        log.err("failed to connect to redis {s}:{d}: {}", .{ env.redis_host, env.redis_port, err });
        return err;
    };
    defer redis.close(io);

    if (env.is_dialer) {
        try runDialer(allocator, &sw, &redis, &env, io);
    } else {
        try runListener(allocator, &sw, &redis, &env, io, local_peer_b58);
    }
}

// ── Listener ─────────────────────────────────────────────────────────────

fn runListener(
    allocator: std.mem.Allocator,
    sw: *AppSwitch,
    redis: *RedisClient,
    env: *const Env,
    io: Io,
    local_peer_b58: []const u8,
) !void {
    // Build listen multiaddr: /ip4/<ip>/udp/<port>/quic-v1
    var listen_addr_buf: [256]u8 = undefined;
    const listen_addr_str = std.fmt.bufPrint(&listen_addr_buf, "/ip4/{s}/udp/{d}/quic-v1", .{
        env.bind_ip, env.listen_port,
    }) catch |err| {
        log.err("failed to format listen addr: {}", .{err});
        return err;
    };

    var listen_addr = Multiaddr.fromString(allocator, listen_addr_str) catch |err| {
        log.err("failed to parse listen multiaddr: {}", .{err});
        return err;
    };
    defer listen_addr.deinit();

    sw.listen(io, listen_addr) catch |err| {
        log.err("failed to listen: {}", .{err});
        return err;
    };

    // Get bound address (for port 0 auto-assignment)
    const bound = sw.listenAddrs() orelse {
        log.err("no listen address after listen()", .{});
        return error.NoListenAddress;
    };
    const bound_port = switch (bound) {
        .ip4 => |a| a.port,
        .ip6 => |a| a.port,
    };

    // Build published multiaddr string
    var publish_addr_buf: [256]u8 = undefined;
    const publish_addr = std.fmt.bufPrint(&publish_addr_buf, "/ip4/{s}/udp/{d}/quic-v1/p2p/{s}", .{
        env.bind_ip, bound_port, local_peer_b58,
    }) catch |err| {
        log.err("failed to format publish addr: {}", .{err});
        return err;
    };

    log.info("publishing address: {s}", .{publish_addr});

    // Clear old key and publish
    redis.del("listenerAddr") catch |err| {
        log.warn("failed to clear listenerAddr: {}", .{err});
    };
    try redis.rpush("listenerAddr", publish_addr);

    // Sleep until timeout
    const sleep_ms: i64 = @intCast(@as(u64, env.timeout_seconds) * 1000);
    const t: Io.Timeout = .{ .duration = .{
        .raw = Io.Duration.fromMilliseconds(sleep_ms),
        .clock = .awake,
    } };
    t.sleep(io) catch {};
}

// ── Dialer ───────────────────────────────────────────────────────────────

fn runDialer(
    allocator: std.mem.Allocator,
    sw: *AppSwitch,
    redis: *RedisClient,
    env: *const Env,
    io: Io,
) !void {
    log.info("waiting for listener address...", .{});

    const addr_bytes = try redis.blpop(allocator, "listenerAddr", env.timeout_seconds) orelse {
        log.err("timed out waiting for listener address", .{});
        return error.ListenerAddressTimeout;
    };
    defer allocator.free(addr_bytes);

    const trimmed = std.mem.trim(u8, addr_bytes, " \t\r\n");
    log.info("dialing: {s}", .{trimmed});

    var remote_addr = Multiaddr.fromString(allocator, trimmed) catch |err| {
        log.err("failed to parse remote addr: {}", .{err});
        return err;
    };
    defer remote_addr.deinit();

    // Dial and measure handshake time
    const handshake_start = timestampNs();
    const peer_id = sw.dial(io, remote_addr) catch |err| {
        log.err("dial failed: {}", .{err});
        return err;
    };

    // Ping via newStream — measures RTT internally
    sw.newStream(io, peer_id, ping_mod.Handler) catch |err| {
        log.err("ping failed: {}", .{err});
        return err;
    };
    const handshake_end = timestampNs();

    // Get ping RTT from handler
    const ping_handler = sw.getHandler(ping_mod.Handler);
    const ping_rtt_ns = ping_handler.last_rtt_ns;

    const total_ns = handshake_end - handshake_start;
    const handshake_ms = @as(f64, @floatFromInt(total_ns)) / 1_000_000.0;
    const ping_ms = @as(f64, @floatFromInt(ping_rtt_ns)) / 1_000_000.0;

    log.info("handshake+ping: {d:.3}ms, ping RTT: {d:.3}ms", .{ handshake_ms, ping_ms });

    // Output JSON metrics to stdout
    const metrics = .{
        .handshakePlusOneRTTMillis = handshake_ms,
        .pingRTTMilllis = ping_ms,
    };

    const json_bytes = std.json.Stringify.valueAlloc(allocator, metrics, .{}) catch |err| {
        log.err("failed to serialize JSON: {}", .{err});
        return err;
    };
    defer allocator.free(json_bytes);

    // Write to stdout via posix
    const stdout_fd = std.posix.STDOUT_FILENO;
    var written: usize = 0;
    while (written < json_bytes.len) {
        const result = std.c.write(stdout_fd, json_bytes.ptr + written, json_bytes.len - written);
        if (result < 0) break;
        written += @intCast(result);
    }
    _ = std.c.write(stdout_fd, "\n", 1);

    sw.close(io);
}
