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

pub const QuicEndpoint = opaque {
    pub const Options = config.Options;
    pub const InitError = config.ConfigError || error{RandomFailed} || std.mem.Allocator.Error;
    pub const InitWithIdentityError = InitError || tls.ContextCreateError;
    pub const ListenError = router.ListenError;
    pub const DialError = dialer.DialError;
    pub const DialOptions = dialer.DialOptions;

    /// Lower-level constructor: caller provides a configured `*ssl.SSL_CTX`
    /// and is responsible for its lifetime. For the common case where you
    /// just want a libp2p-flavored endpoint built from a `KeyPair`, use
    /// `initWithIdentity` instead.
    pub fn init(allocator: std.mem.Allocator, io: std.Io, ssl_ctx: *ssl.SSL_CTX, opts: Options) InitError!*QuicEndpoint {
        log.enableFromEnv();
        const core = try endpoint_core.EndpointCore.init(allocator, io);
        errdefer core.release();

        const validated_options = try config.validateOptions(opts);
        const quiche_config = try config.buildQuicheConfig(validated_options);
        errdefer quiche.quiche_config_free(quiche_config);

        const endpoint = try allocator.create(Impl);
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

    /// Convenience constructor: builds a libp2p TLS context from `host_key`
    /// internally and binds the endpoint's lifetime to it. The endpoint takes
    /// ownership of the constructed TLS context (deinit on `endpoint.deinit`),
    /// but does NOT take ownership of `host_key`. Caller must keep `host_key`
    /// alive for the endpoint's lifetime — the TLS context calls back into it
    /// during cert generation and signing.
    pub fn initWithIdentity(
        allocator: std.mem.Allocator,
        io: std.Io,
        host_key: *identity.KeyPair,
        opts: Options,
    ) InitWithIdentityError!*QuicEndpoint {
        var owned_tls = try tls.Context.create(
            allocator,
            host_key,
            .ED25519,
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
        return router.bind(routerContext(e), addr);
    }

    pub fn dial(e: *QuicEndpoint, addr: std.Io.net.IpAddress, opts: DialOptions) DialError!*Connection {
        if (internal(e).socket == null) {
            const ephemeral: std.Io.net.IpAddress = switch (addr) {
                .ip4 => .{ .ip4 = .{ .bytes = .{ 0, 0, 0, 0 }, .port = 0 } },
                .ip6 => .{ .ip6 = .{ .port = 0, .flow = 0, .bytes = [_]u8{0} ** 16, .interface = .{ .index = 0 } } },
            };
            _ = bind(e, ephemeral) catch |err| switch (err) {
                error.AlreadyBound => {},
                else => |bind_err| {
                    std.log.warn("dial: auto-bind of ephemeral endpoint failed: {}", .{bind_err});
                    return error.EndpointNotBound;
                },
            };
        }
        return dialer.dial(dialerContext(e), addr, opts);
    }

    pub fn accept(e: *QuicEndpoint) std.Io.Cancelable!*Connection {
        return router.accept(routerContext(e));
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
    /// Set when the endpoint was constructed via `initWithIdentity`. The
    /// endpoint owns and tears down this TLS context. When `init` is used
    /// with a caller-provided `ssl_ctx`, this stays null and the caller
    /// remains responsible for the TLS context lifetime.
    owned_tls: ?tls.Context = null,
    socket: ?*transport_mod.SharedUdpSocket = null,
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
    /// for why teardown sets this + closes `route_commands` rather than cancelling.
    router_stopping: std.atomic.Value(bool) = .init(false),
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
        .route_updates = endpoint.core.route_commands,
    };
}

fn internal(e: *QuicEndpoint) *Impl {
    return @ptrCast(@alignCast(e));
}

fn constInternal(e: *const QuicEndpoint) *const Impl {
    return @ptrCast(@alignCast(e));
}
