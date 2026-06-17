//! Perf app for the libp2p `unified-testing` framework (`perf/` suite).
//!
//! One binary is both listener and dialer (selected by `IS_DIALER`). The dialer
//! measures upload/download throughput and latency over QUIC, printing YAML on
//! stdout (logs go to stderr). Peers rendezvous via the framework's Redis using
//! UPPERCASE env vars and a `TEST_KEY`-namespaced redis key.

pub const std_options = @import("zig-libp2p").std_options;

const std = @import("std");
const libp2p = @import("zig-libp2p");
const zio = @import("zio");
const identity = libp2p.identity;
const protocols = libp2p.protocols;
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;
const Stream = libp2p.quic.Stream;

/// Wire format must match the libp2p `unified-testing` reference codec: dialer
/// writes a 16-byte big-endian header (upload_len ++ download_len), streams
/// `upload_len` bytes, then half-closes (FIN); listener drains the upload to
/// EOF, writes `download_len` bytes, FINs; dialer reads the response to EOF so
/// timing is end-to-end both ways (upload-only: the listener's FIN is the
/// "fully received" signal). Requires graceful half-close: read-EOF must not
/// shut the peer's write side.
const perf_protocol_id = "/perf/1.0.0";

/// Transfer chunk for upload/download loops.
const chunk_len = 256 * 1024;

const PerfHandler = struct {
    pub fn run(_: *PerfHandler, io: std.Io, stream: *Stream) !void {
        var header: [16]u8 = undefined;
        try stream.readAll(io, &header, .{});
        const upload_len = std.mem.readInt(u64, header[0..8], .big);
        const download_len = std.mem.readInt(u64, header[8..16], .big);

        // Drain to EOF to consume the dialer's FIN, so the stream is collected
        // promptly instead of lingering per request.
        var buf: [chunk_len]u8 = undefined;
        var received: u64 = 0;
        while (true) {
            const n = stream.read(io, &buf, .{}) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            received += n;
        }
        if (received != upload_len) {
            std.log.warn("perf listener: expected {d} upload bytes, got {d}", .{ upload_len, received });
        }

        // Send the requested download, then return so the Switch closes the
        // stream (FIN), which the dialer reads to EOF.
        @memset(&buf, 0);
        var to_write = download_len;
        while (to_write > 0) {
            const k: usize = @intCast(@min(to_write, @as(u64, buf.len)));
            try stream.writeAll(io, buf[0..k], .{});
            to_write -= k;
        }
    }
};

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
        return .{ .allocator = allocator, .io = io, .stream = stream };
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
                if (try self.readOneByte() != '\n') return error.InvalidResponse;
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
        if (try self.readOneByte() != '$') return error.InvalidResponse;
        return self.readBulkStringBody(scratch);
    }

    fn readBulkStringBody(self: *RedisClient, scratch: *std.ArrayList(u8)) !?[]u8 {
        scratch.clearRetainingCapacity();
        while (true) {
            const byte = try self.readOneByte();
            if (byte == '\r') {
                if (try self.readOneByte() != '\n') return error.InvalidResponse;
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
        try self.writeCommand(&.{ "RPUSH", key, value });
        if (try self.readOneByte() != ':') return error.InvalidResponse;
        _ = try self.readInteger(scratch);
    }

    pub fn del(self: *RedisClient, key: []const u8, scratch: *std.ArrayList(u8)) !void {
        try self.writeCommand(&.{ "DEL", key });
        if (try self.readOneByte() != ':') return error.InvalidResponse;
        _ = try self.readInteger(scratch);
    }

    pub fn blpop(self: *RedisClient, key: []const u8, timeout_secs: u32, scratch: *std.ArrayList(u8)) !?[]u8 {
        var timeout_buf: [16]u8 = undefined;
        const timeout_slice = try std.fmt.bufPrint(&timeout_buf, "{d}", .{timeout_secs});
        try self.writeCommand(&.{ "BLPOP", key, timeout_slice });

        switch (try self.readOneByte()) {
            '*' => {
                const count = try self.readInteger(scratch);
                if (count < 0) return null;
                if (count != 2) return error.InvalidResponse;
                const key_value = try self.readBulkString(scratch) orelse return error.InvalidResponse;
                self.allocator.free(key_value);
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

const default_bytes: u64 = 1024 * 1024 * 1024; // 1 GiB

const Env = struct {
    transport: []const u8,
    is_dialer: bool,
    debug: bool,
    test_key: []const u8,
    bind_ip: []const u8,
    publish_host: []const u8,
    redis_host: []const u8,
    redis_port: u16,
    listen_port: u16,
    timeout_seconds: u32,
    // Dialer-only knobs.
    upload_bytes: u64,
    download_bytes: u64,
    upload_iterations: ?u32,
    download_iterations: ?u32,
    latency_iterations: u32,
    duration_seconds: u32,

    fn load(allocator: std.mem.Allocator, env_map: *const std.process.Environ.Map) !Env {
        const transport_owned = try getRequiredOwned(allocator, env_map, "TRANSPORT");
        errdefer allocator.free(transport_owned);

        const is_dialer_raw = try getRequiredOwned(allocator, env_map, "IS_DIALER");
        defer allocator.free(is_dialer_raw);
        const is_dialer = try parseBool(is_dialer_raw);

        const test_key_owned = try getOptionalOwnedOrDefault(allocator, env_map, "TEST_KEY", "perf");
        errdefer allocator.free(test_key_owned);

        const debug_owned = try getOptionalOwnedOrDefault(allocator, env_map, "DEBUG", "false");
        defer allocator.free(debug_owned);
        const debug = try parseBool(debug_owned);

        const bind_ip_owned = try getOptionalOwnedOrDefault(allocator, env_map, "LISTENER_IP", "0.0.0.0");
        errdefer allocator.free(bind_ip_owned);

        // The listener publishes a dialable address; default to the compose
        // service name "listener", which docker DNS resolves to its container IP.
        const default_publish = if (is_dialer) "dialer" else "listener";
        const publish_host_owned = try getOptionalOwnedOrDefault(allocator, env_map, "PUBLISH_HOST", default_publish);
        errdefer allocator.free(publish_host_owned);

        const redis_addr_owned = try getOptionalOwnedOrDefault(allocator, env_map, "REDIS_ADDR", "redis:6379");
        defer allocator.free(redis_addr_owned);
        const parsed_redis = try parseHostPort(allocator, redis_addr_owned);
        errdefer allocator.free(parsed_redis.host);

        const listen_port_value = try getOptionalUint(env_map, "LISTEN_PORT", 4001);
        if (listen_port_value > std.math.maxInt(u16)) return error.InvalidPort;

        return .{
            .transport = transport_owned,
            .is_dialer = is_dialer,
            .debug = debug,
            .test_key = test_key_owned,
            .bind_ip = bind_ip_owned,
            .publish_host = publish_host_owned,
            .redis_host = parsed_redis.host,
            .redis_port = parsed_redis.port,
            .listen_port = @intCast(listen_port_value),
            .timeout_seconds = try getOptionalUint(env_map, "TEST_TIMEOUT_SECONDS", 1800),
            .upload_bytes = try getOptionalU64(env_map, "UPLOAD_BYTES", default_bytes),
            .download_bytes = try getOptionalU64(env_map, "DOWNLOAD_BYTES", default_bytes),
            .upload_iterations = try getOptionalIterations(env_map, "UPLOAD_ITERATIONS"),
            .download_iterations = try getOptionalIterations(env_map, "DOWNLOAD_ITERATIONS"),
            .latency_iterations = try getOptionalUint(env_map, "LATENCY_ITERATIONS", 100),
            .duration_seconds = try getOptionalUint(env_map, "DURATION", 20),
        };
    }

    fn deinit(self: *Env, allocator: std.mem.Allocator) void {
        allocator.free(self.transport);
        allocator.free(self.test_key);
        allocator.free(self.bind_ip);
        allocator.free(self.publish_host);
        allocator.free(self.redis_host);
    }

    /// Redis key for the listener multiaddr, namespaced by TEST_KEY so parallel
    /// tests on the shared redis don't collide.
    fn listenerAddrKey(self: *const Env, allocator: std.mem.Allocator) ![]u8 {
        return std.fmt.allocPrint(allocator, "{s}_listener_multiaddr", .{self.test_key});
    }
};

const ParsedHostPort = struct { host: []u8, port: u16 };

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
    // Default QUIC config so the perf number is a fair default-vs-default
    // comparison with the other impls' stock perf binaries.
    const endpoint = try libp2p.QuicEndpoint.initWithIdentity(allocator, io, &host_key, .{});
    defer endpoint.deinit();
    const switcher = try libp2p.Switch.init(allocator, io, endpoint);
    defer switcher.deinit();

    var perf_handler = PerfHandler{};
    try switcher.addProtocolService(
        perf_protocol_id,
        protocols.streamHandlerService(PerfHandler, PerfHandler.run, &perf_handler),
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

    const addr_key = try env.listenerAddrKey(allocator);
    defer allocator.free(addr_key);

    const listen_addr_text = try buildListenMultiaddr(allocator, env.bind_ip, env.listen_port);
    defer allocator.free(listen_addr_text);
    std.log.info("listener binding {s}", .{listen_addr_text});

    var listen_addr = try Multiaddr.fromString(allocator, listen_addr_text);
    defer listen_addr.deinit(allocator);
    try switcher.listen(listen_addr);

    const published_addr = try buildPublishedMultiaddr(allocator, io, switcher, host_key, env);
    defer allocator.free(published_addr);

    redis.del(addr_key, &scratch) catch |err| {
        std.log.warn("failed to clear {s}: {any}", .{ addr_key, err });
    };
    try redis.rpush(addr_key, published_addr, &scratch);
    std.log.info("listener published {s}", .{published_addr});

    // Serve inbound in the background; the Switch owns the accept loop and
    // connection lifetimes, torn down by `switcher.deinit`.
    try switcher.serve(io);

    // Stay up until the dialer finishes; docker shuts the container down when
    // the dialer exits, so a generous sleep is just an upper bound.
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

    const addr_key = try env.listenerAddrKey(allocator);
    defer allocator.free(addr_key);

    std.log.info("waiting for listener address in Redis key {s}", .{addr_key});
    const addr_bytes = (try redis.blpop(addr_key, env.timeout_seconds, &scratch)) orelse return error.ListenerAddressTimeout;
    defer allocator.free(addr_bytes);

    const trimmed = std.mem.trim(u8, addr_bytes, " \t\r\n");
    std.log.info("dialing {s}", .{trimmed});
    var remote_addr = try Multiaddr.fromString(allocator, trimmed);
    defer remote_addr.deinit(allocator);

    const conn = try switcher.dial(remote_addr, .{ .timeout = timeoutFromSeconds(120) });
    defer conn.deinit();

    var upload: Samples = .{};
    defer upload.deinit(allocator);
    var download: Samples = .{};
    defer download.deinit(allocator);
    var latency: Samples = .{};
    defer latency.deinit(allocator);

    // Upload: send UPLOAD_BYTES, receive a 1-byte ack (so timing ends when the
    // server has consumed the whole upload). Throughput in Gbps.
    try runThroughput(allocator, io, conn, env.upload_bytes, 1, env.upload_iterations, env, &upload);
    // Download: send 0, receive DOWNLOAD_BYTES.
    try runThroughput(allocator, io, conn, 0, env.download_bytes, env.download_iterations, env, &download);
    // Latency: 1 byte each way, measured as round-trip ms.
    {
        var i: u32 = 0;
        while (i < env.latency_iterations) : (i += 1) {
            const elapsed_ns = try perfRequest(io, conn, 1, 1);
            try latency.append(allocator, nsToMillis(elapsed_ns));
        }
    }

    try printResults(io, allocator, &upload, &download, &latency);
}

/// Run `iterations` (or, when null, as many as fit in DURATION seconds, at
/// least one) transfers of `(upload_bytes, download_bytes)`, recording each as
/// a Gbps sample of whichever direction is non-zero.
fn runThroughput(
    allocator: std.mem.Allocator,
    io: std.Io,
    conn: *libp2p.ManagedConnection,
    upload_bytes: u64,
    download_bytes: u64,
    iterations: ?u32,
    env: *const Env,
    out: *Samples,
) !void {
    const measured_bytes = if (upload_bytes != 0) upload_bytes else download_bytes;
    const deadline_ns = monotonicNs(io) + @as(u64, env.duration_seconds) * std.time.ns_per_s;

    var i: u32 = 0;
    while (true) : (i += 1) {
        if (iterations) |n| {
            if (i >= n) break;
        } else if (i > 0 and monotonicNs(io) >= deadline_ns) {
            break; // duration mode: always do at least one, then stop past DURATION
        }
        const elapsed_ns = try perfRequest(io, conn, upload_bytes, download_bytes);
        try out.append(allocator, bytesPerNsToGbps(measured_bytes, elapsed_ns));
        if (iterations == null and i + 1 >= 10_000) break; // safety bound
    }
}

/// One perf exchange on a fresh `/perf/1.0.0` stream; returns the elapsed
/// nanoseconds from the first byte written to the listener's response FIN.
fn perfRequest(io: std.Io, conn: *libp2p.ManagedConnection, upload_bytes: u64, download_bytes: u64) !u64 {
    const stream = try conn.openProtocolStream(perf_protocol_id, .{});
    defer stream.deinit();
    defer stream.close(io) catch {};

    var header: [16]u8 = undefined;
    std.mem.writeInt(u64, header[0..8], upload_bytes, .big);
    std.mem.writeInt(u64, header[8..16], download_bytes, .big);

    var buf: [chunk_len]u8 = undefined;
    @memset(&buf, 0);

    const start_ns = monotonicNs(io);
    try stream.writeAll(io, &header, .{});

    var to_write = upload_bytes;
    while (to_write > 0) {
        const k: usize = @intCast(@min(to_write, @as(u64, buf.len)));
        try stream.writeAll(io, buf[0..k], .{});
        to_write -= k;
    }
    // Half-close: FIN signals the request is complete (see perf_protocol_id).
    try stream.closeWrite(io);

    var received: u64 = 0;
    while (true) {
        const n = stream.read(io, &buf, .{}) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        received += n;
    }
    const elapsed = monotonicNs(io) -| start_ns;

    if (received != download_bytes) {
        std.log.warn("perf dialer: expected {d} download bytes, got {d}", .{ download_bytes, received });
        return error.ShortDownload;
    }
    return elapsed;
}

// ---------------------------------------------------------------------------
// Statistics (min/q1/median/q3/max + IQR outliers over a sample set)
// ---------------------------------------------------------------------------

const Samples = struct {
    values: std.ArrayList(f64) = .empty,

    fn deinit(self: *Samples, allocator: std.mem.Allocator) void {
        self.values.deinit(allocator);
    }
    fn append(self: *Samples, allocator: std.mem.Allocator, v: f64) !void {
        try self.values.append(allocator, v);
    }
};

const Summary = struct {
    iterations: usize,
    min: f64,
    q1: f64,
    median: f64,
    q3: f64,
    max: f64,
};

/// Linear-interpolation percentile over an ascending-sorted slice.
fn percentile(sorted: []const f64, p: f64) f64 {
    if (sorted.len == 1) return sorted[0];
    const pos = p * @as(f64, @floatFromInt(sorted.len - 1));
    const lo: usize = @intFromFloat(@floor(pos));
    const hi: usize = @min(lo + 1, sorted.len - 1);
    const frac = pos - @floor(pos);
    return sorted[lo] + frac * (sorted[hi] - sorted[lo]);
}

fn summarize(sorted: []const f64) Summary {
    return .{
        .iterations = sorted.len,
        .min = sorted[0],
        .q1 = percentile(sorted, 0.25),
        .median = percentile(sorted, 0.5),
        .q3 = percentile(sorted, 0.75),
        .max = sorted[sorted.len - 1],
    };
}

fn printResults(
    io: std.Io,
    allocator: std.mem.Allocator,
    upload: *Samples,
    download: *Samples,
    latency: *Samples,
) !void {
    // stdout is a pipe under the harness, so use a streaming writer.
    const stdout_file: std.Io.File = .{ .handle = std.posix.STDOUT_FILENO, .flags = .{ .nonblocking = false } };
    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = stdout_file.writer(io, &stdout_buf);
    const w = &stdout_writer.interface;

    try printBlock(w, allocator, "upload", upload, "Gbps");
    try printBlock(w, allocator, "download", download, "Gbps");
    try printBlock(w, allocator, "latency", latency, "ms");
    try w.flush();
}

fn printBlock(
    w: *std.Io.Writer,
    allocator: std.mem.Allocator,
    name: []const u8,
    samples: *Samples,
    unit: []const u8,
) !void {
    const items = samples.values.items;
    if (items.len == 0) return error.NoSamples;

    const sorted = try allocator.dupe(f64, items);
    defer allocator.free(sorted);
    std.mem.sort(f64, sorted, {}, std.sort.asc(f64));

    const s = summarize(sorted);
    const iqr = s.q3 - s.q1;
    const lo_fence = s.q1 - 1.5 * iqr;
    const hi_fence = s.q3 + 1.5 * iqr;

    try w.print("{s}:\n", .{name});
    try w.print("  iterations: {d}\n", .{s.iterations});
    try w.print("  min: {d:.4}\n", .{s.min});
    try w.print("  q1: {d:.4}\n", .{s.q1});
    try w.print("  median: {d:.4}\n", .{s.median});
    try w.print("  q3: {d:.4}\n", .{s.q3});
    try w.print("  max: {d:.4}\n", .{s.max});

    try w.writeAll("  outliers: [");
    var first = true;
    for (sorted) |v| {
        if (v < lo_fence or v > hi_fence) {
            if (!first) try w.writeAll(", ");
            try w.print("{d:.4}", .{v});
            first = false;
        }
    }
    try w.writeAll("]\n");

    try w.writeAll("  samples: [");
    for (sorted, 0..) |v, i| {
        if (i != 0) try w.writeAll(", ");
        try w.print("{d:.4}", .{v});
    }
    try w.writeAll("]\n");

    try w.print("  unit: {s}\n", .{unit});
}

fn bytesPerNsToGbps(bytes: u64, elapsed_ns: u64) f64 {
    if (elapsed_ns == 0) return 0;
    // bits / ns == Gbit/s exactly: (bytes*8) / (elapsed_ns * 1e-9) / 1e9.
    return (@as(f64, @floatFromInt(bytes)) * 8.0) / @as(f64, @floatFromInt(elapsed_ns));
}

// ---------------------------------------------------------------------------
// Shared scaffolding (env parsing, multiaddr build, TCP/DNS helpers)
// ---------------------------------------------------------------------------

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

    const local_peer_id = try host_key.peerId(allocator);
    const peer_text = try local_peer_id.toString(allocator);
    defer allocator.free(peer_text);

    return std.fmt.allocPrint(
        allocator,
        "/{s}/{s}/udp/{d}/quic-v1/p2p/{s}",
        .{ published_host.proto, published_host.text, parsed.address.getPort(), peer_text },
    );
}

const PublishedHost = struct { proto: []const u8, text: []u8 };

fn resolvePublishedHost(allocator: std.mem.Allocator, io: std.Io, env: *const Env, port: u16) !PublishedHost {
    const direct_proto = hostProtocol(env.publish_host);
    if (!std.mem.eql(u8, direct_proto, "dns4")) {
        return .{ .proto = direct_proto, .text = try allocator.dupe(u8, env.publish_host) };
    }
    if (resolveHostAddress(io, env.publish_host, port)) |address| {
        return ipAddressHostText(allocator, address);
    } else |err| {
        std.log.warn("failed to resolve publish host {s}: {any}", .{ env.publish_host, err });
    }
    return .{ .proto = direct_proto, .text = try allocator.dupe(u8, env.publish_host) };
}

fn ipAddressHostText(allocator: std.mem.Allocator, address: std.Io.net.IpAddress) !PublishedHost {
    return switch (address) {
        .ip4 => |ip4| .{
            .proto = "ip4",
            .text = try std.fmt.allocPrint(allocator, "{d}.{d}.{d}.{d}", .{
                ip4.bytes[0], ip4.bytes[1], ip4.bytes[2], ip4.bytes[3],
            }),
        },
        .ip6 => |ip6| .{
            .proto = "ip6",
            .text = try std.fmt.allocPrint(
                allocator,
                "{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}",
                .{
                    ip6.bytes[0],  ip6.bytes[1],  ip6.bytes[2],  ip6.bytes[3],
                    ip6.bytes[4],  ip6.bytes[5],  ip6.bytes[6],  ip6.bytes[7],
                    ip6.bytes[8],  ip6.bytes[9],  ip6.bytes[10], ip6.bytes[11],
                    ip6.bytes[12], ip6.bytes[13], ip6.bytes[14], ip6.bytes[15],
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
    return .{
        .host = try allocator.dupe(u8, value[0..idx]),
        .port = try std.fmt.parseInt(u16, value[idx + 1 ..], 10),
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

fn getOptionalUint(env_map: *const std.process.Environ.Map, key: []const u8, default_value: u32) !u32 {
    if (env_map.get(key)) |value| {
        if (value.len == 0 or std.mem.eql(u8, value, "null")) return default_value;
        return std.fmt.parseInt(u32, value, 10);
    }
    return default_value;
}

fn getOptionalU64(env_map: *const std.process.Environ.Map, key: []const u8, default_value: u64) !u64 {
    if (env_map.get(key)) |value| {
        if (value.len == 0 or std.mem.eql(u8, value, "null")) return default_value;
        return std.fmt.parseInt(u64, value, 10);
    }
    return default_value;
}

/// `null`/empty/absent -> run in DURATION mode (returns null); a number ->
/// that many iterations.
fn getOptionalIterations(env_map: *const std.process.Environ.Map, key: []const u8) !?u32 {
    if (env_map.get(key)) |value| {
        if (value.len == 0 or std.mem.eql(u8, value, "null")) return null;
        return try std.fmt.parseInt(u32, value, 10);
    }
    return null;
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
