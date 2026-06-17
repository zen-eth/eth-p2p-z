const std = @import("std");
const ssl = @import("ssl").c;
const quiche = @import("quiche").c;
const config = @import("../config.zig");
const connection_mod = @import("../connection/mod.zig");
const endpoint_core = @import("core.zig");
const raw = @import("raw.zig");
const dialer = @import("../dialer/mod.zig");
const router = @import("../router/mod.zig");
const transport_mod = @import("../io/transport.zig");
const tls = @import("../../security/tls.zig");
const identity = @import("../../identity.zig");
const Connection = connection_mod.Connection;
const log = @import("../log.zig");

pub const EndpointStats = endpoint_core.EndpointStats;

/// Opaque handle over a heap-allocated, stable-address `Impl`. Opaque (not a
/// plain struct) because the spawned router fiber borrows interior pointers into
/// `Impl` (see `routerContext`), so it must never be copied or moved — `opaque`
/// makes "pointer-only, never by value" a compile-time guarantee.
pub const QuicEndpoint = opaque {
    pub const Options = config.Options;
    pub const InitError = config.ConfigError || error{RandomFailed} || std.mem.Allocator.Error;
    pub const InitWithIdentityError = InitError || tls.ContextCreateError;
    pub const ListenError = router.ListenError;
    pub const AcceptError = router.AcceptError;
    pub const DialError = dialer.DialError;
    pub const DialOptions = dialer.DialOptions;

    /// Lower-level constructor: caller provides and owns the `*ssl.SSL_CTX`. For
    /// a libp2p endpoint built from a `KeyPair`, use `initWithIdentity` instead.
    pub fn init(allocator: std.mem.Allocator, io: std.Io, ssl_ctx: *ssl.SSL_CTX, opts: Options) InitError!*QuicEndpoint {
        log.enableFromEnv();
        const core = try endpoint_core.EndpointCore.init(allocator, io);
        errdefer core.release();

        const validated_options = try config.validateOptions(opts);
        const quiche_config = try config.buildQuicheConfig(validated_options);
        errdefer quiche.quiche_config_free(quiche_config);

        const endpoint = try allocator.create(Impl);
        errdefer allocator.destroy(endpoint);
        endpoint.* = .{
            .allocator = allocator,
            .io = io,
            .ssl_ctx = ssl_ctx,
            .quiche_config = quiche_config,
            .options = validated_options,
            .core = core,
        };
        return @ptrCast(endpoint);
    }

    /// Convenience constructor: builds a libp2p TLS context from `host_key`. The
    /// endpoint owns and deinits that TLS context, but NOT `host_key` — the caller
    /// must keep `host_key` alive for the endpoint's lifetime, as the TLS context
    /// calls back into it during cert generation and signing.
    pub fn initWithIdentity(
        allocator: std.mem.Allocator,
        io: std.Io,
        host_key: *identity.KeyPair,
        opts: Options,
    ) InitWithIdentityError!*QuicEndpoint {
        var owned_tls = try tls.Context.create(
            allocator,
            host_key,
            // ECDSA P-256 ephemeral cert key: its scheme ecdsa_secp256r1_sha256 is
            // mandatory-to-implement in TLS 1.3 (RFC 8446 §9.1) so every peer
            // advertises it. ed25519 is optional and unadvertised by some stacks
            // (jvm-libp2p / netty-quiche), which fails the handshake with
            // NO_COMMON_SIGNATURE_ALGORITHMS. The libp2p host key is unaffected —
            // it signs the libp2p extension, not the certificate.
            .ECDSA,
            @ptrCast(host_key),
            identity.signWithKeyPair,
        );
        var owned_tls_live = true;
        errdefer if (owned_tls_live) owned_tls.deinit();

        const endpoint = try init(allocator, io, owned_tls.ssl_ctx, opts);
        internal(endpoint).owned_tls = owned_tls;
        owned_tls_live = false;
        return endpoint;
    }

    /// Safe to call even if `bind()` was never invoked: `closeListener`
    /// tolerates an unbound endpoint (null socket / router_future / accept
    /// queue), so this also cleans up an endpoint that failed mid-setup.
    pub fn deinit(e: *QuicEndpoint) void {
        const endpoint = internal(e);
        router.closeListener(routerContext(e));
        quiche.quiche_config_free(endpoint.quiche_config);
        if (endpoint.owned_tls) |*owned| owned.deinit();
        endpoint.core.release();
        endpoint.allocator.destroy(endpoint);
    }

    /// Bind to `addr`. Returns the actual local address (the OS may pick a
    /// port if `addr` had port 0).
    pub fn bind(e: *QuicEndpoint, addr: std.Io.net.IpAddress) ListenError!std.Io.net.IpAddress {
        const ep = internal(e);
        ep.bind_lock.lockUncancelable(ep.io);
        defer ep.bind_lock.unlock(ep.io);
        return router.bind(routerContext(e), addr);
    }

    pub fn dial(e: *QuicEndpoint, addr: std.Io.net.IpAddress, opts: DialOptions) DialError!*Connection {
        const ep = internal(e);
        {
            // Auto-bind an ephemeral local socket for a dial-only endpoint.
            // ep.socket is plain (not atomic) and written under bind_lock, so read
            // it under the same lock: concurrent first-dials then see a consistent
            // value and only one runs router.bind, the rest find it already bound.
            ep.bind_lock.lockUncancelable(ep.io);
            defer ep.bind_lock.unlock(ep.io);
            if (ep.socket == null) {
                const ephemeral: std.Io.net.IpAddress = switch (addr) {
                    .ip4 => .{ .ip4 = .{ .bytes = .{ 0, 0, 0, 0 }, .port = 0 } },
                    .ip6 => .{ .ip6 = .{ .port = 0, .flow = 0, .bytes = [_]u8{0} ** 16, .interface = .{ .index = 0 } } },
                };
                _ = router.bind(routerContext(e), ephemeral) catch |err| switch (err) {
                    error.AlreadyBound => {},
                    // Forward the causes DialError shares; map genuine bind/config
                    // faults (AddressInUse, fd-quota, ...) to TransportError rather
                    // than the misleading "never bound" error.EndpointNotBound.
                    error.OutOfMemory => return error.OutOfMemory,
                    error.ConcurrencyUnavailable => return error.ConcurrencyUnavailable,
                    else => |bind_err| {
                        std.log.warn("dial: auto-bind of ephemeral endpoint failed: {}", .{bind_err});
                        return error.TransportError;
                    },
                };
            }
        }
        return dialer.dial(dialerContext(e), addr, opts);
    }

    pub fn accept(e: *QuicEndpoint) AcceptError!*Connection {
        return router.accept(routerContext(e));
    }

    /// Graceful-shutdown helper: unblock a fiber parked in `accept` (it then
    /// returns `error.ListenerClosed`, distinct from `error.Canceled`) WITHOUT
    /// tearing the listener down — the endpoint stays usable and `deinit` still
    /// does the full teardown. Idempotent and safe before `bind` and `deinit`.
    pub fn stopAccepting(e: *QuicEndpoint) void {
        router.stopAccepting(routerContext(e));
    }

    pub fn localAddr(e: *QuicEndpoint) ?std.Io.net.IpAddress {
        return router.localAddr(routerContext(e));
    }

    pub fn stats(e: *const QuicEndpoint) EndpointStats {
        return constInternal(e).core.stats();
    }
};

const Impl = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    ssl_ctx: *ssl.SSL_CTX,
    quiche_config: *quiche.quiche_config,
    options: config.Options,
    core: *endpoint_core.EndpointCore,
    /// Non-null only when built via `initWithIdentity`: the endpoint then owns
    /// and tears down this TLS context. Null under `init` (caller owns ssl_ctx).
    owned_tls: ?tls.Context = null,
    socket: ?*transport_mod.SharedUdpSocket = null,
    /// Serializes bind() (including dial()'s auto-bind). router.bind's
    /// null-check / socket-bind / slot-store is not atomic across its internal
    /// suspend, so concurrent first-binds would each spawn a router fiber and the
    /// second would orphan the first's socket + fiber (leak + teardown UAF).
    bind_lock: std.Io.Mutex = .init,
    accept_queue: ?*router.AcceptChannel.State = null,
    /// In-flight admission counter shared with the router fiber. See
    /// `router/accept_queue.zig:tryReserve`. Initialised to the configured
    /// accept queue capacity on `bind`; reset to 0 on `closeListener`.
    accept_available: std.atomic.Value(usize) = .init(0),
    /// Router main fiber lifetime. Uses `Future` (not `Group`) so the teardown
    /// `await` blocks until the runtime has fully torn down the fiber before we
    /// free surrounding state. Stopped cooperatively via `router_stopping`.
    router_future: ?std.Io.Future(router.RouterLoopError!void) = null,
    /// Tasks that own per-connection handshake state. Group is fine
    /// here because the tasks are self-cleaning and the surrounding
    /// `Impl` only goes away after `closeListener` has run a blocking
    /// `cancel` on this group, draining all in-flight tasks.
    handshake_waiters: std.Io.Group = .init,
    /// Cooperative teardown flag for the router fiber. See `router.closeListener`
    /// for why teardown sets this + sends the loopback wake rather than cancelling.
    router_stopping: std.atomic.Value(bool) = .init(false),
    /// Recv-fiber slab pool (see router.Context.slab_pool_slot). Filled on
    /// bind, endpoint reference dropped by closeListener.
    recv_slab_pool: ?*router.SlabPool = null,
};

fn rawContext(e: *QuicEndpoint) raw.Context {
    const endpoint = internal(e);
    return .{
        .allocator = endpoint.allocator,
        .io = endpoint.io,
        .ssl_ctx = endpoint.ssl_ctx,
        .quiche_config = endpoint.quiche_config,
    };
}

fn routerContext(e: *QuicEndpoint) router.Context {
    const endpoint = internal(e);
    return .{
        .allocator = endpoint.allocator,
        .io = endpoint.io,
        .options = endpoint.options,
        .core = endpoint.core,
        .socket_slot = &endpoint.socket,
        .accept_queue_slot = &endpoint.accept_queue,
        .accept_available = &endpoint.accept_available,
        .router_future_slot = &endpoint.router_future,
        .handshake_waiters = &endpoint.handshake_waiters,
        .stopping = &endpoint.router_stopping,
        .slab_pool_slot = &endpoint.recv_slab_pool,
        .raw = rawContext(e),
    };
}

fn dialerContext(e: *QuicEndpoint) dialer.Context {
    const endpoint = internal(e);
    return .{
        .allocator = endpoint.allocator,
        .io = endpoint.io,
        .options = endpoint.options,
        .core = endpoint.core,
        .raw = rawContext(e),
        .shared_socket = endpoint.socket,
        .route_registrar = if (endpoint.socket != null) routeRegistrar(e) else null,
    };
}

fn routeRegistrar(e: *QuicEndpoint) router.RouteRegistrar {
    const endpoint = internal(e);
    return .{
        .allocator = endpoint.allocator,
        .io = endpoint.io,
        .core = endpoint.core,
    };
}

fn internal(e: *QuicEndpoint) *Impl {
    return @ptrCast(@alignCast(e));
}

fn constInternal(e: *const QuicEndpoint) *const Impl {
    return @ptrCast(@alignCast(e));
}

test "production endpoint certificate uses the TLS-MTI ECDSA P-256 scheme" {
    // Interop regression guard: the ephemeral cert key must stay ECDSA P-256
    // (see initWithIdentity for the TLS-MTI signature-algorithm rationale).
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var host_key = try identity.KeyPair.generate(.ED25519);
    defer host_key.deinit();

    const endpoint = try QuicEndpoint.initWithIdentity(std.testing.allocator, io, &host_key, .{});
    defer endpoint.deinit();

    const cert_key = internal(endpoint).owned_tls.?.subject_keypair;
    try std.testing.expectEqual(ssl.EVP_PKEY_EC, ssl.EVP_PKEY_base_id(cert_key));
    const ec_key = ssl.EVP_PKEY_get0_EC_KEY(cert_key) orelse return error.TestUnexpectedResult;
    const group = ssl.EC_KEY_get0_group(ec_key) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(ssl.NID_X9_62_prime256v1, ssl.EC_GROUP_get_curve_name(group));
}
