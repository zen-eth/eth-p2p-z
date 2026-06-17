pub const std_options = @import("zig-libp2p").std_options;

const std = @import("std");
const libp2p = @import("zig-libp2p");
const zio = @import("zio");
const identity = libp2p.identity;
const protocols = libp2p.protocols;
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;

const RedisClient = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    stream: std.Io.net.Stream,
    reader: std.Io.net.Stream.Reader = undefined,
    writer: std.Io.net.Stream.Writer = undefined,
    read_buf: [4096]u8 = undefined,
    write_buf: [4096]u8 = undefined,
    initialized: bool = false,

    pub fn connect(allocator: std.mem.Allocator, io: std.Io, host: []const u8, port: u16) !RedisClient {
        const stream = try connectTcp(io, host, port);
        return .{
            .allocator = allocator,
            .io = io,
            .stream = stream,
        };
    }

    pub fn initBuffers(self: *RedisClient) void {
        self.reader = self.stream.reader(self.io, &self.read_buf);
        self.writer = self.stream.writer(self.io, &self.write_buf);
        self.initialized = true;
    }

    pub fn deinit(self: *RedisClient) void {
        if (self.initialized) self.writer.interface.flush() catch {};
        self.stream.close(self.io);
    }

    fn writeCommand(self: *RedisClient, parts: []const []const u8) !void {
        const writer = &self.writer.interface;
        try writer.print("*{d}\r\n", .{parts.len});
        for (parts) |part| {
            try writer.print("${d}\r\n", .{part.len});
            try writer.writeAll(part);
            try writer.writeAll("\r\n");
        }
        try writer.flush();
    }

    fn readOneByte(self: *RedisClient) !u8 {
        var buf: [1]u8 = undefined;
        try self.reader.interface.readSliceAll(&buf);
        return buf[0];
    }

    fn readExact(self: *RedisClient, buffer: []u8) !void {
        try self.reader.interface.readSliceAll(buffer);
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

    fn readBulkStringBody(self: *RedisClient, scratch: *std.ArrayList(u8)) !?[]u8 {
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
        const length = std.fmt.parseInt(i64, scratch.items, 10) catch return error.InvalidResponse;
        if (length < 0) return null;

        const out = try self.allocator.alloc(u8, @intCast(length));
        errdefer self.allocator.free(out);
        try self.readExact(out);
        if (try self.readOneByte() != '\r') return error.InvalidResponse;
        if (try self.readOneByte() != '\n') return error.InvalidResponse;
        return out;
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
                if (count < 0) return null;
                if (count != 2) return error.InvalidResponse;
                const key_value = try self.readBulkString(scratch) orelse return error.InvalidResponse;
                defer self.allocator.free(key_value);
                return self.readBulkString(scratch);
            },
            '$' => return self.readBulkStringBody(scratch),
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
    debug: bool,
    muxer: ?[]u8,
    security: ?[]u8,
    bind_ip: []const u8,
    publish_host: []const u8,
    redis_host: []const u8,
    redis_port: u16,
    timeout_seconds: u32,
    listen_port: u16,
    test_key: []const u8,

    fn load(allocator: std.mem.Allocator, env_map: *const std.process.Environ.Map) !Env {
        const transport_owned = try getRequiredOwned(allocator, env_map, "TRANSPORT");
        errdefer allocator.free(transport_owned);

        const is_dialer_raw = try getRequiredOwned(allocator, env_map, "IS_DIALER");
        defer allocator.free(is_dialer_raw);
        const is_dialer = try parseBool(is_dialer_raw);

        const debug_owned = try getOptionalOwnedOrDefault(allocator, env_map, "DEBUG", "false");
        defer allocator.free(debug_owned);
        const debug = try parseBool(debug_owned);

        const bind_ip_owned = try getOptionalOwnedOrDefault(allocator, env_map, "LISTENER_IP", "0.0.0.0");
        errdefer allocator.free(bind_ip_owned);

        const default_publish = if (is_dialer) "dialer" else "listener";
        const publish_host_owned = try getOptionalOwnedOrDefault(allocator, env_map, "PUBLISH_HOST", default_publish);
        errdefer allocator.free(publish_host_owned);

        const muxer_owned = try getOptionalOwned(allocator, env_map, "MUXER");
        errdefer if (muxer_owned) |value| allocator.free(value);

        const security_owned = try getOptionalOwned(allocator, env_map, "SECURITY");
        errdefer if (security_owned) |value| allocator.free(value);

        const redis_addr_owned = try getOptionalOwnedOrDefault(allocator, env_map, "REDIS_ADDR", "redis:6379");
        defer allocator.free(redis_addr_owned);
        const parsed_redis = try parseHostPort(allocator, redis_addr_owned);
        errdefer allocator.free(parsed_redis.host);

        const test_key_owned = try getOptionalOwnedOrDefault(allocator, env_map, "TEST_KEY", "transport");
        errdefer allocator.free(test_key_owned);

        const timeout_secs = try getOptionalUint(env_map, "TEST_TIMEOUT_SECONDS", 180);
        const listen_port_value = try getOptionalUint(env_map, "LISTEN_PORT", 4001);
        if (listen_port_value > std.math.maxInt(u16)) return error.InvalidPort;

        return .{
            .transport = transport_owned,
            .is_dialer = is_dialer,
            .debug = debug,
            .muxer = muxer_owned,
            .security = security_owned,
            .bind_ip = bind_ip_owned,
            .publish_host = publish_host_owned,
            .redis_host = parsed_redis.host,
            .redis_port = parsed_redis.port,
            .timeout_seconds = timeout_secs,
            .listen_port = @intCast(listen_port_value),
            .test_key = test_key_owned,
        };
    }

    /// Redis key for the listener multiaddr, namespaced by TEST_KEY so parallel
    /// tests sharing one redis instance don't collide.
    fn listenerAddrKey(self: *const Env, allocator: std.mem.Allocator) ![]u8 {
        return std.fmt.allocPrint(allocator, "{s}_listener_multiaddr", .{self.test_key});
    }

    fn deinit(self: *Env, allocator: std.mem.Allocator) void {
        allocator.free(self.transport);
        allocator.free(self.bind_ip);
        allocator.free(self.publish_host);
        allocator.free(self.redis_host);
        allocator.free(self.test_key);
        if (self.muxer) |value| allocator.free(value);
        if (self.security) |value| allocator.free(value);
    }
};

const ParsedHostPort = struct {
    host: []u8,
    port: u16,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const cpu_count = libp2p.effectiveCpuCount(allocator); // cgroup-CFS-quota-aware
    const executor_count: u8 = @intCast(std.math.clamp(cpu_count, 2, 64));
    const runtime = try zio.Runtime.init(allocator, .{ .executors = .exact(executor_count) });
    defer runtime.deinit();
    const io = runtime.io();

    var env = try Env.load(allocator, init.environ_map);
    defer env.deinit(allocator);
    if (env.debug) logEnvironment(init.environ_map);
    try validateInteropEnv(&env);

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();
    const endpoint = try libp2p.QuicEndpoint.initWithIdentity(allocator, io, &host_key, .{});
    defer endpoint.deinit();
    const switcher = try libp2p.Switch.init(allocator, io, endpoint);
    defer switcher.deinit();

    var ping_handler = protocols.ping.PingHandler.init(allocator);
    try switcher.addProtocolService(
        protocols.ping.protocol_id,
        protocols.streamHandlerService(protocols.ping.PingHandler, protocols.ping.PingHandler.run, &ping_handler),
    );

    // Interop peers open an identify stream right after connecting; without a
    // handler we answer `na` and they log a noisy (non-fatal) error.
    var identify_handler = protocols.identify.IdentifyHandler.init(allocator);
    try switcher.addProtocolService(
        protocols.identify.protocol_id,
        protocols.streamHandlerService(protocols.identify.IdentifyHandler, protocols.identify.IdentifyHandler.run, &identify_handler),
    );

    var redis = try RedisClient.connect(allocator, io, env.redis_host, env.redis_port);
    redis.initBuffers();
    defer redis.deinit();

    if (env.is_dialer) {
        try runDialer(allocator, io, switcher, &redis, &env);
    } else {
        try runListener(allocator, io, switcher, &host_key, &redis, &env);
    }
}

fn validateInteropEnv(env: *const Env) !void {
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
}

fn runListener(
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    host_key: *identity.KeyPair,
    redis: *RedisClient,
    env: *const Env,
) !void {
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);

    const listener_addr_key = try env.listenerAddrKey(allocator);
    defer allocator.free(listener_addr_key);

    const listen_addr_text = try buildListenMultiaddr(allocator, env.bind_ip, env.listen_port);
    defer allocator.free(listen_addr_text);
    std.log.info("listener binding {s}", .{listen_addr_text});

    var listen_addr = try Multiaddr.fromString(allocator, listen_addr_text);
    defer listen_addr.deinit(allocator);
    try switcher.listen(listen_addr);

    const published_addr = try buildPublishedMultiaddr(allocator, io, switcher, host_key, env);
    defer allocator.free(published_addr);

    redis.del(listener_addr_key, &scratch) catch |err| {
        std.log.warn("failed to clear {s}: {any}", .{ listener_addr_key, err });
    };
    try redis.rpush(listener_addr_key, published_addr, &scratch);
    std.log.info("listener published {s}", .{published_addr});

    // Serve every inbound connection: py-libp2p opens a SECOND QUIC connection
    // after identify, so serving only the first stalls a ping stream opened on the
    // later one. `Switch.serve` owns the accept loop + connection lifetimes.
    try switcher.serve(io);

    try timeoutFromSeconds(env.timeout_seconds).sleep(io);
}

fn runDialer(
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    redis: *RedisClient,
    env: *const Env,
) !void {
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(allocator);

    const listener_addr_key = try env.listenerAddrKey(allocator);
    defer allocator.free(listener_addr_key);

    std.log.info("waiting for listener address in Redis key {s}", .{listener_addr_key});
    const addr_bytes = try waitForListenerMultiaddr(redis, listener_addr_key, env.timeout_seconds, &scratch);
    defer allocator.free(addr_bytes);

    const trimmed = std.mem.trim(u8, addr_bytes, " \t\r\n");
    std.log.info("dialing {s}", .{trimmed});
    var remote_addr = try Multiaddr.fromString(allocator, trimmed);
    defer remote_addr.deinit(allocator);

    const total_start_ns = monotonicNs(io);
    const conn = try switcher.dial(remote_addr, .{ .timeout = timeoutFromSeconds(env.timeout_seconds) });
    defer conn.deinit();

    const rtt_ns = try runPing(allocator, io, conn);
    const total_ns = monotonicNs(io) -| total_start_ns;

    // stdout is a pipe under the interop harness; the positional writer falls back
    // to streaming on a non-seekable fd (ESPIPE -> Unseekable).
    const stdout_file: std.Io.File = .{ .handle = std.posix.STDOUT_FILENO, .flags = .{ .nonblocking = false } };
    var stdout_buf: [256]u8 = undefined;
    var stdout_writer = stdout_file.writer(io, &stdout_buf);
    try stdout_writer.interface.print(
        "{{\"handshakePlusOneRTTMillis\":{d:.3},\"pingRTTMilllis\":{d:.3}}}\n",
        .{ nsToMillis(total_ns), nsToMillis(rtt_ns) },
    );
    try stdout_writer.interface.flush();
}

fn waitForListenerMultiaddr(
    redis: *RedisClient,
    key: []const u8,
    timeout_seconds: u32,
    scratch: *std.ArrayList(u8),
) ![]u8 {
    return (try redis.blpop(key, timeout_seconds, scratch)) orelse error.ListenerAddressTimeout;
}

fn runPing(allocator: std.mem.Allocator, io: std.Io, conn: *libp2p.ManagedConnection) !u64 {
    _ = allocator;
    const stream = try conn.openProtocolStream(protocols.ping.protocol_id, .{});
    defer stream.deinit();
    defer stream.close(io) catch {};

    var payload: [32]u8 = undefined;
    for (&payload, 0..) |*byte, i| byte.* = @intCast(i);

    const start_ns = monotonicNs(io);
    try stream.writeAll(io, &payload, .{});

    var reply: [32]u8 = undefined;
    try stream.readAll(io, &reply, .{});
    if (!std.mem.eql(u8, &payload, &reply)) return error.PingMismatch;
    return monotonicNs(io) -| start_ns;
}

fn buildListenMultiaddr(allocator: std.mem.Allocator, bind_ip: []const u8, port: u16) ![]u8 {
    const proto = if (std.Io.net.IpAddress.parseIp4(bind_ip, port)) |_|
        "ip4"
    else |_| if (std.Io.net.IpAddress.parseIp6(bind_ip, port)) |_|
        "ip6"
    else |_|
        return error.InvalidBindAddress;

    return std.fmt.allocPrint(allocator, "/{s}/{s}/udp/{d}/quic-v1", .{ proto, bind_ip, port });
}

fn buildPublishedMultiaddr(
    allocator: std.mem.Allocator,
    io: std.Io,
    switcher: *libp2p.Switch,
    host_key: *identity.KeyPair,
    env: *const Env,
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

    const published_host = try resolvePublishedHost(allocator, io, env, parsed.address.getPort());
    defer allocator.free(published_host.text);

    const transport = if (std.mem.eql(u8, env.transport, "quic")) "quic" else "quic-v1";
    const local_peer_id = try host_key.peerId(allocator);
    const peer_text = try local_peer_id.toString(allocator);
    defer allocator.free(peer_text);

    return std.fmt.allocPrint(
        allocator,
        "/{s}/{s}/udp/{d}/{s}/p2p/{s}",
        .{ published_host.proto, published_host.text, parsed.address.getPort(), transport, peer_text },
    );
}

const PublishedHost = struct {
    proto: []const u8,
    text: []u8,
};

fn resolvePublishedHost(
    allocator: std.mem.Allocator,
    io: std.Io,
    env: *const Env,
    port: u16,
) !PublishedHost {
    const direct_proto = hostProtocol(env.publish_host);
    if (!std.mem.eql(u8, direct_proto, "dns4")) {
        return .{
            .proto = direct_proto,
            .text = try allocator.dupe(u8, env.publish_host),
        };
    }

    if (resolveHostAddress(io, env.publish_host, port)) |address| {
        return ipAddressHostText(allocator, address);
    } else |err| {
        std.log.warn("failed to resolve publish host {s}: {any}", .{ env.publish_host, err });
    }

    return .{
        .proto = direct_proto,
        .text = try allocator.dupe(u8, env.publish_host),
    };
}

fn ipAddressHostText(allocator: std.mem.Allocator, address: std.Io.net.IpAddress) !PublishedHost {
    return switch (address) {
        .ip4 => |ip4| .{
            .proto = "ip4",
            .text = try std.fmt.allocPrint(
                allocator,
                "{d}.{d}.{d}.{d}",
                .{ ip4.bytes[0], ip4.bytes[1], ip4.bytes[2], ip4.bytes[3] },
            ),
        },
        .ip6 => |ip6| .{
            .proto = "ip6",
            .text = try std.fmt.allocPrint(
                allocator,
                "{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}",
                .{
                    ip6.bytes[0],
                    ip6.bytes[1],
                    ip6.bytes[2],
                    ip6.bytes[3],
                    ip6.bytes[4],
                    ip6.bytes[5],
                    ip6.bytes[6],
                    ip6.bytes[7],
                    ip6.bytes[8],
                    ip6.bytes[9],
                    ip6.bytes[10],
                    ip6.bytes[11],
                    ip6.bytes[12],
                    ip6.bytes[13],
                    ip6.bytes[14],
                    ip6.bytes[15],
                },
            ),
        },
    };
}

fn hostProtocol(host: []const u8) []const u8 {
    if (std.Io.net.IpAddress.parseIp4(host, 0)) |_| return "ip4" else |_| {}
    if (std.Io.net.IpAddress.parseIp6(host, 0)) |_| return "ip6" else |_| {}
    return "dns4";
}

fn connectTcp(io: std.Io, host: []const u8, port: u16) !std.Io.net.Stream {
    if (std.Io.net.IpAddress.resolve(io, host, port)) |address| {
        return std.Io.net.IpAddress.connect(&address, io, .{ .mode = .stream });
    } else |_| {}

    const host_name = try std.Io.net.HostName.init(host);
    var result_storage: [16]std.Io.net.HostName.LookupResult = undefined;
    var results = std.Io.Queue(std.Io.net.HostName.LookupResult).init(&result_storage);
    try std.Io.net.HostName.lookup(host_name, io, &results, .{ .port = port });

    var first_connect_err: ?anyerror = null;
    while (true) {
        const result = results.getOne(io) catch |err| switch (err) {
            error.Closed => break,
            else => |e| return e,
        };
        switch (result) {
            .address => |address| {
                return std.Io.net.IpAddress.connect(&address, io, .{ .mode = .stream }) catch |err| {
                    if (first_connect_err == null) first_connect_err = err;
                    continue;
                };
            },
            .canonical_name => {},
        }
    }

    if (first_connect_err) |err| return err;
    return error.NoAddressReturned;
}

fn resolveHostAddress(io: std.Io, host: []const u8, port: u16) !std.Io.net.IpAddress {
    if (std.Io.net.IpAddress.resolve(io, host, port)) |address| return address else |_| {}

    const host_name = try std.Io.net.HostName.init(host);
    var result_storage: [16]std.Io.net.HostName.LookupResult = undefined;
    var results = std.Io.Queue(std.Io.net.HostName.LookupResult).init(&result_storage);
    try std.Io.net.HostName.lookup(host_name, io, &results, .{ .port = port });

    while (true) {
        const result = results.getOne(io) catch |err| switch (err) {
            error.Closed => break,
            else => |e| return e,
        };
        switch (result) {
            .address => |address| return address,
            .canonical_name => {},
        }
    }

    return error.NoAddressReturned;
}

fn parseHostPort(allocator: std.mem.Allocator, value: []const u8) !ParsedHostPort {
    const idx = std.mem.lastIndexOfScalar(u8, value, ':') orelse return error.InvalidAddressFormat;
    const host_part = value[0..idx];
    const port_part = value[idx + 1 ..];
    const port = try std.fmt.parseInt(u16, port_part, 10);
    return .{
        .host = try allocator.dupe(u8, host_part),
        .port = port,
    };
}

fn getRequiredOwned(allocator: std.mem.Allocator, env_map: *const std.process.Environ.Map, key: []const u8) ![]u8 {
    if (env_map.get(key)) |value| return allocator.dupe(u8, value);
    std.log.err("missing required env var {s}", .{key});
    return error.EnvironmentVariableMissing;
}

fn getOptionalOwnedOrDefault(
    allocator: std.mem.Allocator,
    env_map: *const std.process.Environ.Map,
    key: []const u8,
    default_value: []const u8,
) ![]u8 {
    if (env_map.get(key)) |value| return allocator.dupe(u8, value);
    return allocator.dupe(u8, default_value);
}

fn getOptionalOwned(allocator: std.mem.Allocator, env_map: *const std.process.Environ.Map, key: []const u8) !?[]u8 {
    if (env_map.get(key)) |value| return try allocator.dupe(u8, value);
    return null;
}

fn getOptionalUint(env_map: *const std.process.Environ.Map, key: []const u8, default_value: u32) !u32 {
    if (env_map.get(key)) |value| return std.fmt.parseInt(u32, value, 10);
    return default_value;
}

fn parseBool(value: []const u8) !bool {
    if (std.ascii.eqlIgnoreCase(value, "true") or std.ascii.eqlIgnoreCase(value, "1")) return true;
    if (std.ascii.eqlIgnoreCase(value, "false") or std.ascii.eqlIgnoreCase(value, "0")) return false;
    return error.InvalidBoolean;
}

fn logEnvironment(env_map: *const std.process.Environ.Map) void {
    var it = env_map.iterator();
    while (it.next()) |entry| {
        std.log.info("env {s}={s}", .{ entry.key_ptr.*, entry.value_ptr.* });
    }
}

fn timeoutFromSeconds(seconds: u32) std.Io.Timeout {
    const ns_u64 = std.math.mul(u64, seconds, std.time.ns_per_s) catch std.math.maxInt(u64);
    return .{ .duration = .{ .raw = .fromNanoseconds(@intCast(ns_u64)), .clock = .awake } };
}

fn nsToMillis(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_ms);
}

fn monotonicNs(io: std.Io) u64 {
    const ns = std.Io.Clock.awake.now(io).toNanoseconds();
    return if (ns <= 0) 0 else @intCast(ns);
}
