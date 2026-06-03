//! libp2p protocol multiplexer over a QUIC endpoint.
//!
//! The Switch is the libp2p layer over a `quic.QuicEndpoint`: it wraps the
//! endpoint's connection-level API with multiaddr-aware helpers, runs
//! multistream-select for inbound streams, and dispatches them to registered
//! protocol handlers. Identity / TLS / endpoint lifetime are NOT a Switch
//! concern — construct your `QuicEndpoint` (typically via
//! `quic.QuicEndpoint.initWithIdentity`) and hand it to `Switch.init`. The
//! Switch borrows it and owns the managed connections returned by `dial` and
//! `accept`. A managed connection may be deinited directly; any still-live
//! managed connections are canceled before the Switch frees its handler
//! registry. The endpoint can be deinited once Switch teardown is complete.

const std = @import("std");
const identity = @import("identity.zig");
const protocols = @import("protocols.zig");
const quic = @import("quic.zig");
const PeerId = @import("peer_id").PeerId;
const Multiaddr = @import("multiaddr").multiaddr.Multiaddr;
const Semaphore = @import("quic/io/semaphore.zig").Semaphore;
const io_time = @import("quic/io/time.zig");

const command_queue_capacity = 32;
/// E3 inbound stream-handler concurrency caps. Two layers, like go-libp2p
/// (system + peer scopes) and rust-libp2p (per-connection + ConnectionLimits):
/// a PER-CONNECTION cap (per-peer fairness) and an AGGREGATE cap (total
/// exhaustion across all connections). Both default conservative + tunable via
/// `Switch.Options`. Per-conn defaults below QUIC stream-credit (100) so it
/// actually binds.
const default_max_inflight_handlers_per_conn: usize = 32;
const default_max_inflight_handlers_total: usize = 256;
const default_negotiation_timeout: std.Io.Timeout = .{
    .duration = .{ .raw = .fromNanoseconds(10 * std.time.ns_per_s), .clock = .awake },
};

pub const Switch = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    /// Borrowed; must outlive the Switch.
    endpoint: *quic.QuicEndpoint,
    services: std.StringHashMap(protocols.AnyProtocolService),
    services_lock: std.Io.Mutex = .init,
    registry_frozen: bool = false,
    connections: std.ArrayList(*SwitchConnection) = .empty,
    connections_lock: std.Io.Mutex = .init,
    /// Per-connection handler-fiber cap; each actor seeds its own gate from this.
    max_inflight_handlers_per_conn: usize = default_max_inflight_handlers_per_conn,
    /// Aggregate handler-fiber gate, shared by every connection's dispatcher.
    handler_gate_total: Semaphore = Semaphore.init(default_max_inflight_handlers_total),

    pub const Options = struct {
        max_inflight_handlers_per_conn: usize = default_max_inflight_handlers_per_conn,
        max_inflight_handlers_total: usize = default_max_inflight_handlers_total,
    };
    pub const OptionsError = error{InvalidOptions};

    fn validateOptions(opts: Options) OptionsError!void {
        // Like every cross-fiber backpressure-budget knob, a zero cap would
        // deadlock the gate — reject it rather than silently clamp.
        if (opts.max_inflight_handlers_per_conn == 0) return error.InvalidOptions;
        if (opts.max_inflight_handlers_total == 0) return error.InvalidOptions;
    }

    pub const InitError = std.mem.Allocator.Error;
    pub const DispatchError = error{
        NoRegisteredProtocols,
        ConnectionClosed,
    } || std.mem.Allocator.Error || std.Io.ConcurrentError || quic.Connection.AcceptStreamError;
    pub const AddProtocolServiceError = error{RegistryFrozen} || std.mem.Allocator.Error;
    pub const DispatchOptions = struct {
        accept_timeout: std.Io.Timeout = .none,
        negotiation_timeout: std.Io.Timeout = default_negotiation_timeout,
    };
    pub const OpenProtocolStreamOptions = struct {
        negotiation_timeout: std.Io.Timeout = default_negotiation_timeout,
    };
    pub const OpenProtocolStreamError = error{
        ConnectionClosed,
        SelectedProtocolMismatch,
    } || quic.Connection.OpenStreamError || protocols.multistream.Error;
    pub const StartInboundDispatchError = error{ ConnectionClosed, AlreadyDispatching } || std.Io.Cancelable || std.Io.ConcurrentError;
    pub const CloseError = error{ConnectionClosed} || quic.Connection.CloseError;

    pub const ListenError = error{AddressInvalid} || quic.QuicEndpoint.ListenError;
    pub const DialOptions = struct { timeout: std.Io.Timeout = .none };
    pub const DialError = error{ AddressInvalid, PeerIdentityMismatch } || quic.QuicEndpoint.DialError || std.Io.ConcurrentError;
    pub const AcceptError = std.Io.Cancelable || std.Io.ConcurrentError || std.mem.Allocator.Error;

    pub fn init(allocator: std.mem.Allocator, io: std.Io, endpoint: *quic.QuicEndpoint) InitError!*Switch {
        const sw = try allocator.create(Switch);
        sw.* = .{
            .allocator = allocator,
            .io = io,
            .endpoint = endpoint,
            .services = std.StringHashMap(protocols.AnyProtocolService).init(allocator),
        };
        return sw;
    }

    /// Like `init`, but with custom E3 handler-concurrency caps.
    pub fn initWithOptions(
        allocator: std.mem.Allocator,
        io: std.Io,
        endpoint: *quic.QuicEndpoint,
        opts: Options,
    ) (InitError || OptionsError)!*Switch {
        try validateOptions(opts);
        const sw = try allocator.create(Switch);
        sw.* = .{
            .allocator = allocator,
            .io = io,
            .endpoint = endpoint,
            .services = std.StringHashMap(protocols.AnyProtocolService).init(allocator),
            .max_inflight_handlers_per_conn = opts.max_inflight_handlers_per_conn,
            .handler_gate_total = Semaphore.init(opts.max_inflight_handlers_total),
        };
        return sw;
    }

    pub fn deinit(sw: *Switch) void {
        while (true) {
            sw.connections_lock.lockUncancelable(sw.io);
            const conn = if (sw.connections.items.len > 0) sw.connections.pop().? else null;
            if (conn) |managed| managed.registered = false;
            sw.connections_lock.unlock(sw.io);

            if (conn) |managed| {
                managed.deinit();
            } else {
                break;
            }
        }
        sw.connections.deinit(sw.allocator);

        // All connections (hence their dispatchers/handlers) are torn down above,
        // so nobody is parked on the aggregate gate; close() is a harmless
        // belt-and-suspenders wake before the Switch (and its gate) are freed.
        sw.handler_gate_total.close(sw.io);

        sw.services_lock.lockUncancelable(sw.io);
        var it = sw.services.iterator();
        while (it.next()) |entry| {
            sw.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        sw.services.deinit();
        sw.services_lock.unlock(sw.io);

        sw.allocator.destroy(sw);
    }

    pub fn listen(sw: *Switch, addr: Multiaddr) ListenError!void {
        const parsed = addr.parseIpUdp(sw.allocator) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.AddressInvalid,
        };
        _ = try sw.endpoint.bind(parsed.address);
    }

    pub fn dial(sw: *Switch, addr: Multiaddr, opts: DialOptions) DialError!*SwitchConnection {
        const parsed = addr.resolveIpUdp(sw.allocator, sw.io) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.AddressInvalid,
        };
        const conn = try sw.endpoint.dial(parsed.address, .{ .timeout = opts.timeout });
        var conn_live = true;
        errdefer if (conn_live) conn.deinit();
        if (parsed.peer_id) |expected| {
            const actual = try connectionPeerId(sw.allocator, conn);
            if (!actual.eql(&expected)) return error.PeerIdentityMismatch;
        }
        conn_live = false;
        const managed = try sw.manageConnection(conn);
        return managed;
    }

    pub fn accept(sw: *Switch) AcceptError!*SwitchConnection {
        const conn = try sw.endpoint.accept();
        return sw.manageConnection(conn);
    }

    /// Returns the libp2p peer id of the remote, derived from the verified
    /// public key on the connection's TLS handshake.
    pub fn connectionPeerId(allocator: std.mem.Allocator, conn: *const quic.Connection) std.mem.Allocator.Error!PeerId {
        var pub_key = conn.remotePublicKey();
        return PeerId.fromPublicKey(allocator, &pub_key);
    }

    pub fn addProtocolService(sw: *Switch, id: protocols.ProtocolId, service: protocols.AnyProtocolService) AddProtocolServiceError!void {
        const owned_id = try sw.allocator.dupe(u8, id);
        var owned_id_live = true;
        errdefer if (owned_id_live) sw.allocator.free(owned_id);

        sw.services_lock.lockUncancelable(sw.io);
        defer sw.services_lock.unlock(sw.io);

        if (sw.registry_frozen) return error.RegistryFrozen;
        if (sw.services.fetchRemove(id)) |old| {
            sw.allocator.free(old.key);
            old.value.deinit();
        }
        try sw.services.put(owned_id, service);
        owned_id_live = false;
    }

    pub fn listenMultiaddrs(sw: *Switch, allocator: std.mem.Allocator) std.mem.Allocator.Error!std.ArrayList([]u8) {
        var out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out.items) |item| allocator.free(item);
            out.deinit(allocator);
        }
        if (sw.endpoint.localAddr()) |addr| {
            try out.append(allocator, try multiaddrTextFromIpUdp(allocator, addr));
        }
        return out;
    }

    fn manageConnection(sw: *Switch, conn: *quic.Connection) (std.mem.Allocator.Error || std.Io.ConcurrentError)!*SwitchConnection {
        const actor = SwitchConnectionActor.init(sw.allocator, sw.io, sw, conn) catch |err| {
            conn.deinit();
            return err;
        };
        // Exactly one actor-cleanup path may run on the error path. Both
        // `destroyUnspawned` and `shutdownAndDestroy` free `inbox_storage` and
        // destroy `actor`, so two stacked errdefers would double-free (and run
        // the second against freed memory). Guard on whether the main fiber was
        // spawned: before spawn -> `destroyUnspawned`; after -> `shutdownAndDestroy`.
        var actor_spawned = false;
        errdefer if (actor_spawned) actor.shutdownAndDestroy() else actor.destroyUnspawned();

        const managed = try sw.allocator.create(SwitchConnection);
        errdefer sw.allocator.destroy(managed);
        managed.* = .{
            .allocator = sw.allocator,
            .io = sw.io,
            .sw = sw,
            .actor = actor,
        };

        try actor.spawn();
        actor_spawned = true;

        sw.connections_lock.lockUncancelable(sw.io);
        defer sw.connections_lock.unlock(sw.io);
        try sw.connections.append(sw.allocator, managed);
        managed.registered = true;
        return managed;
    }

    fn unregisterConnection(sw: *Switch, conn: *SwitchConnection) void {
        sw.connections_lock.lockUncancelable(sw.io);
        defer sw.connections_lock.unlock(sw.io);

        if (!conn.registered) return;
        for (sw.connections.items, 0..) |item, index| {
            if (item == conn) {
                _ = sw.connections.swapRemove(index);
                conn.registered = false;
                return;
            }
        }
        conn.registered = false;
    }

    fn supportedProtocolIds(sw: *Switch) std.mem.Allocator.Error![][]const u8 {
        var ids: std.ArrayList([]const u8) = .empty;
        errdefer ids.deinit(sw.allocator);

        sw.services_lock.lockUncancelable(sw.io);
        defer sw.services_lock.unlock(sw.io);

        sw.registry_frozen = true;
        var it = sw.services.iterator();
        while (it.next()) |entry| try ids.append(sw.allocator, entry.key_ptr.*);
        return ids.toOwnedSlice(sw.allocator);
    }

    fn freezeProtocolRegistry(sw: *Switch) void {
        sw.services_lock.lockUncancelable(sw.io);
        defer sw.services_lock.unlock(sw.io);
        sw.registry_frozen = true;
    }

    fn protocolService(sw: *Switch, id: protocols.ProtocolId) ?protocols.AnyProtocolService {
        sw.services_lock.lockUncancelable(sw.io);
        defer sw.services_lock.unlock(sw.io);
        return sw.services.get(id);
    }
};

pub const SwitchConnection = struct {
    /// Handle to the libp2p session supervisor. The raw QUIC connection lives
    /// in the actor; this handle only posts libp2p-shaped commands.
    allocator: std.mem.Allocator,
    io: std.Io,
    sw: *Switch,
    actor: *SwitchConnectionActor,
    registered: bool = false,

    pub fn deinit(conn: *SwitchConnection) void {
        conn.sw.unregisterConnection(conn);
        conn.actor.shutdownAndDestroy();
        conn.allocator.destroy(conn);
    }

    pub fn openProtocolStream(
        conn: *SwitchConnection,
        protocol_id: protocols.ProtocolId,
        opts: Switch.OpenProtocolStreamOptions,
    ) Switch.OpenProtocolStreamError!*quic.Stream {
        var reply: OpenProtocolStreamReply = .{};
        try conn.post(.{ .open_protocol_stream = .{
            .protocol_id = protocol_id,
            .opts = opts,
            .reply = &reply,
        } });
        reply.event.waitUncancelable(conn.io);
        return reply.result;
    }

    pub fn dispatchInboundStream(conn: *SwitchConnection, opts: Switch.DispatchOptions) Switch.DispatchError!void {
        var reply: DispatchInboundStreamReply = .{};
        try conn.post(.{ .dispatch_inbound_stream = .{
            .opts = opts,
            .reply = &reply,
        } });
        reply.event.waitUncancelable(conn.io);
        return reply.result;
    }

    pub fn startInboundDispatch(conn: *SwitchConnection, opts: Switch.DispatchOptions) Switch.StartInboundDispatchError!void {
        var reply: StartInboundDispatchReply = .{};
        try conn.post(.{ .start_inbound_dispatch = .{
            .opts = opts,
            .reply = &reply,
        } });
        reply.event.waitUncancelable(conn.io);
        return reply.result;
    }

    pub fn startInboundDispatcher(conn: *SwitchConnection, opts: Switch.DispatchOptions) Switch.StartInboundDispatchError!void {
        return conn.startInboundDispatch(opts);
    }

    pub fn stopInboundDispatch(conn: *SwitchConnection) void {
        var reply: VoidReply = .{};
        conn.post(.{ .stop_inbound_dispatch = &reply }) catch return;
        reply.event.waitUncancelable(conn.io);
    }

    pub fn stopInboundDispatcher(conn: *SwitchConnection) void {
        conn.stopInboundDispatch();
    }

    pub fn close(conn: *SwitchConnection, app_error_code: u64, reason: []const u8) Switch.CloseError!void {
        var reply: CloseReply = .{};
        try conn.post(.{ .close = .{
            .code = app_error_code,
            .reason = reason,
            .reply = &reply,
        } });
        reply.event.waitUncancelable(conn.io);
        return reply.result;
    }

    pub fn stats(conn: *SwitchConnection) quic.ConnectionStats {
        var reply: StatsReply = .{};
        conn.post(.{ .stats = &reply }) catch return .{};
        reply.event.waitUncancelable(conn.io);
        return reply.stats;
    }

    pub fn peerId(conn: *const SwitchConnection) PeerId {
        return conn.actor.peer_id;
    }

    pub fn remoteAddress(conn: *const SwitchConnection) std.Io.net.IpAddress {
        return conn.actor.remote_addr;
    }

    fn post(conn: *SwitchConnection, command: Command) (error{ConnectionClosed} || std.Io.Cancelable)!void {
        conn.actor.inbox.putOne(conn.io, command) catch |err| switch (err) {
            error.Closed => return error.ConnectionClosed,
            error.Canceled => return error.Canceled,
        };
    }
};

pub const ManagedConnection = SwitchConnection;

const Command = union(enum) {
    open_protocol_stream: struct {
        protocol_id: protocols.ProtocolId,
        opts: Switch.OpenProtocolStreamOptions,
        reply: *OpenProtocolStreamReply,
    },
    dispatch_inbound_stream: struct {
        opts: Switch.DispatchOptions,
        reply: *DispatchInboundStreamReply,
    },
    start_inbound_dispatch: struct {
        opts: Switch.DispatchOptions,
        reply: *StartInboundDispatchReply,
    },
    stop_inbound_dispatch: *VoidReply,
    close: struct {
        code: u64,
        reason: []const u8,
        reply: *CloseReply,
    },
    stats: *StatsReply,
    shutdown: *VoidReply,
};

const VoidReply = struct {
    event: std.Io.Event = .unset,

    fn complete(reply: *VoidReply, io: std.Io) void {
        reply.event.set(io);
    }
};

const OpenProtocolStreamReply = struct {
    event: std.Io.Event = .unset,
    result: Switch.OpenProtocolStreamError!*quic.Stream = error.ConnectionClosed,

    fn complete(reply: *OpenProtocolStreamReply, io: std.Io, result: Switch.OpenProtocolStreamError!*quic.Stream) void {
        reply.result = result;
        reply.event.set(io);
    }
};

const DispatchInboundStreamReply = struct {
    event: std.Io.Event = .unset,
    result: Switch.DispatchError!void = error.ConnectionClosed,

    fn complete(reply: *DispatchInboundStreamReply, io: std.Io, result: Switch.DispatchError!void) void {
        reply.result = result;
        reply.event.set(io);
    }
};

const StartInboundDispatchReply = struct {
    event: std.Io.Event = .unset,
    result: Switch.StartInboundDispatchError!void = error.ConnectionClosed,

    fn complete(reply: *StartInboundDispatchReply, io: std.Io, result: Switch.StartInboundDispatchError!void) void {
        reply.result = result;
        reply.event.set(io);
    }
};

const CloseReply = struct {
    event: std.Io.Event = .unset,
    result: Switch.CloseError!void = error.ConnectionClosed,

    fn complete(reply: *CloseReply, io: std.Io, result: Switch.CloseError!void) void {
        reply.result = result;
        reply.event.set(io);
    }
};

const StatsReply = struct {
    event: std.Io.Event = .unset,
    stats: quic.ConnectionStats = .{},

    fn complete(reply: *StatsReply, io: std.Io, stats: quic.ConnectionStats) void {
        reply.stats = stats;
        reply.event.set(io);
    }
};

const SwitchConnectionActor = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    sw: *Switch,
    conn: ?*quic.Connection,
    inbox_storage: []Command,
    inbox: std.Io.Queue(Command),
    main_future: ?std.Io.Future(std.Io.Cancelable!void) = null,
    dispatcher_group: std.Io.Group = .init,
    handler_group: std.Io.Group = .init,
    /// Per-connection handler admission gate (E3 fairness layer), seeded from
    /// sw.max_inflight_handlers_per_conn. Acquired before spawning a handler,
    /// released in the handler's cleanup defer.
    handler_gate_conn: Semaphore,
    dispatcher_running: bool = false,
    closing: bool = false,
    peer_id: PeerId,
    remote_addr: std.Io.net.IpAddress,

    fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        sw: *Switch,
        conn: *quic.Connection,
    ) std.mem.Allocator.Error!*SwitchConnectionActor {
        const inbox_storage = try allocator.alloc(Command, command_queue_capacity);
        errdefer allocator.free(inbox_storage);

        const actor = try allocator.create(SwitchConnectionActor);
        errdefer allocator.destroy(actor);

        actor.* = .{
            .allocator = allocator,
            .io = io,
            .sw = sw,
            .conn = conn,
            .inbox_storage = inbox_storage,
            .inbox = std.Io.Queue(Command).init(inbox_storage),
            .handler_gate_conn = Semaphore.init(sw.max_inflight_handlers_per_conn),
            .peer_id = try Switch.connectionPeerId(allocator, conn),
            .remote_addr = conn.remoteAddress(),
        };
        return actor;
    }

    fn spawn(actor: *SwitchConnectionActor) std.Io.ConcurrentError!void {
        actor.main_future = try std.Io.concurrent(actor.io, actorMain, .{actor});
    }

    fn destroyUnspawned(actor: *SwitchConnectionActor) void {
        if (actor.conn) |conn| conn.deinit();
        actor.allocator.free(actor.inbox_storage);
        actor.allocator.destroy(actor);
    }

    fn shutdownAndDestroy(actor: *SwitchConnectionActor) void {
        if (actor.main_future) |*future| {
            var reply: VoidReply = .{};
            const sent = blk: {
                actor.inbox.putOneUncancelable(actor.io, .{ .shutdown = &reply }) catch break :blk false;
                break :blk true;
            };
            if (sent) reply.event.waitUncancelable(actor.io);
            _ = future.await(actor.io) catch {};
            actor.main_future = null;
        } else {
            actor.cleanup();
        }
        actor.allocator.free(actor.inbox_storage);
        actor.allocator.destroy(actor);
    }

    fn cleanup(actor: *SwitchConnectionActor) void {
        actor.closing = true;
        actor.inbox.close(actor.io);
        actor.dispatcher_running = false;
        actor.dispatcher_group.cancel(actor.io);
        actor.handler_group.cancel(actor.io);
        if (actor.conn) |conn| {
            const prev = actor.io.swapCancelProtection(.blocked);
            defer _ = actor.io.swapCancelProtection(prev);
            conn.close(actor.io, 0, "switch connection shutdown") catch {};
            conn.deinit();
            actor.conn = null;
        }
    }

    fn completePending(actor: *SwitchConnectionActor) void {
        var drained: [8]Command = undefined;
        while (true) {
            const count = actor.inbox.getUncancelable(actor.io, &drained, 0) catch return;
            if (count == 0) return;
            for (drained[0..count]) |command| completeCommandClosed(actor.io, command);
        }
    }

    fn openProtocolStream(
        actor: *SwitchConnectionActor,
        protocol_id: protocols.ProtocolId,
        opts: Switch.OpenProtocolStreamOptions,
    ) Switch.OpenProtocolStreamError!*quic.Stream {
        const conn = actor.liveConnection() orelse return error.ConnectionClosed;
        const stream = try conn.openStream(actor.io);
        var stream_live = true;
        errdefer if (stream_live) {
            closeStreamForCleanup(actor.io, stream);
            stream.deinit();
        };

        const selected = try protocols.multistream.negotiate(actor.io, stream, &.{protocol_id}, .{
            .role = .initiator,
            .timeout = opts.negotiation_timeout,
        });
        if (!std.mem.eql(u8, selected, protocol_id)) return error.SelectedProtocolMismatch;
        stream_live = false;
        return stream;
    }

    fn dispatchInboundStream(actor: *SwitchConnectionActor, opts: Switch.DispatchOptions) Switch.DispatchError!void {
        const conn = actor.liveConnection() orelse return error.ConnectionClosed;
        const supported = try actor.sw.supportedProtocolIds();
        errdefer actor.allocator.free(supported);
        if (supported.len == 0) return error.NoRegisteredProtocols;

        const stream = try conn.acceptStream(actor.io, .{ .timeout = opts.accept_timeout });
        var stream_live = true;
        errdefer if (stream_live) {
            closeStreamForCleanup(actor.io, stream);
            stream.deinit();
        };

        // Two-layer admission gate (E3): per-connection permit (fairness) THEN
        // aggregate permit (total). Consistent acquire order => deadlock-free.
        // Placed AFTER acceptStream so no earlier-return path holds a permit; the
        // dispatcher blocking here IS the back-pressure. A parked acquire is a
        // cancel point, so dispatcher_group.cancel unwinds it at teardown. The
        // handler releases both in its cleanup defers; the errdefers below only
        // cover the window before the spawn hands ownership to the handler.
        actor.handler_gate_conn.acquire(actor.io) catch |err| switch (err) {
            error.Closed => return error.ConnectionClosed,
            error.Canceled => return error.Canceled,
        };
        var conn_permit_held = true;
        errdefer if (conn_permit_held) actor.handler_gate_conn.release(actor.io);

        actor.sw.handler_gate_total.acquire(actor.io) catch |err| switch (err) {
            error.Closed => return error.ConnectionClosed,
            error.Canceled => return error.Canceled,
        };
        var total_permit_held = true;
        errdefer if (total_permit_held) actor.sw.handler_gate_total.release(actor.io);

        try actor.handler_group.concurrent(
            actor.io,
            runNegotiatedProtocolHandler,
            .{ actor.sw, actor.io, stream, supported, opts.negotiation_timeout, actor.peer_id, actor.remote_addr, &actor.handler_gate_conn },
        );
        // Spawn succeeded: the handler now owns the stream + both permits and
        // releases them in its defers, so disarm the pre-spawn cleanup here.
        stream_live = false;
        conn_permit_held = false;
        total_permit_held = false;
    }

    fn startInboundDispatch(actor: *SwitchConnectionActor, opts: Switch.DispatchOptions) Switch.StartInboundDispatchError!void {
        _ = actor.liveConnection() orelse return error.ConnectionClosed;
        if (actor.dispatcher_running) return error.AlreadyDispatching;
        actor.sw.freezeProtocolRegistry();
        try actor.dispatcher_group.concurrent(actor.io, inboundDispatcher, .{ actor, opts });
        actor.dispatcher_running = true;
    }

    fn stopInboundDispatch(actor: *SwitchConnectionActor) void {
        if (!actor.dispatcher_running) return;
        actor.dispatcher_running = false;
        actor.dispatcher_group.cancel(actor.io);
    }

    fn close(actor: *SwitchConnectionActor, code: u64, reason: []const u8) Switch.CloseError!void {
        const conn = actor.liveConnection() orelse return error.ConnectionClosed;
        actor.closing = true;
        actor.stopInboundDispatch();
        actor.handler_group.cancel(actor.io);
        return conn.close(actor.io, code, reason);
    }

    fn stats(actor: *SwitchConnectionActor) quic.ConnectionStats {
        const conn = actor.conn orelse return .{};
        return conn.stats();
    }

    fn liveConnection(actor: *SwitchConnectionActor) ?*quic.Connection {
        if (actor.closing) return null;
        return actor.conn;
    }
};

fn actorMain(actor: *SwitchConnectionActor) std.Io.Cancelable!void {
    defer {
        actor.cleanup();
        actor.completePending();
    }

    while (true) {
        const command = actor.inbox.getOne(actor.io) catch |err| switch (err) {
            error.Closed => return,
            error.Canceled => return error.Canceled,
        };
        switch (command) {
            .open_protocol_stream => |cmd| {
                const result = actor.openProtocolStream(cmd.protocol_id, cmd.opts) catch |err| {
                    cmd.reply.complete(actor.io, err);
                    if (err == error.Canceled) return error.Canceled;
                    continue;
                };
                cmd.reply.complete(actor.io, result);
            },
            .dispatch_inbound_stream => |cmd| {
                actor.dispatchInboundStream(cmd.opts) catch |err| {
                    cmd.reply.complete(actor.io, err);
                    if (err == error.Canceled) return error.Canceled;
                    continue;
                };
                cmd.reply.complete(actor.io, {});
            },
            .start_inbound_dispatch => |cmd| {
                actor.startInboundDispatch(cmd.opts) catch |err| {
                    cmd.reply.complete(actor.io, err);
                    if (err == error.Canceled) return error.Canceled;
                    continue;
                };
                cmd.reply.complete(actor.io, {});
            },
            .stop_inbound_dispatch => |reply| {
                actor.stopInboundDispatch();
                reply.complete(actor.io);
            },
            .close => |cmd| {
                actor.close(cmd.code, cmd.reason) catch |err| {
                    cmd.reply.complete(actor.io, err);
                    if (err == error.Canceled) return error.Canceled;
                    continue;
                };
                cmd.reply.complete(actor.io, {});
            },
            .stats => |reply| reply.complete(actor.io, actor.stats()),
            .shutdown => |reply| {
                actor.cleanup();
                reply.complete(actor.io);
                return;
            },
        }
    }
}

fn completeCommandClosed(io: std.Io, command: Command) void {
    switch (command) {
        .open_protocol_stream => |cmd| cmd.reply.complete(io, error.ConnectionClosed),
        .dispatch_inbound_stream => |cmd| cmd.reply.complete(io, error.ConnectionClosed),
        .start_inbound_dispatch => |cmd| cmd.reply.complete(io, error.ConnectionClosed),
        .stop_inbound_dispatch => |reply| reply.complete(io),
        .close => |cmd| cmd.reply.complete(io, error.ConnectionClosed),
        .stats => |reply| reply.complete(io, .{}),
        .shutdown => |reply| reply.complete(io),
    }
}

fn inboundDispatcher(actor: *SwitchConnectionActor, opts: Switch.DispatchOptions) std.Io.Cancelable!void {
    while (true) {
        actor.dispatchInboundStream(opts) catch |err| switch (err) {
            error.Canceled, error.ConnectionClosed => return,
            error.NoRegisteredProtocols => {
                // Configuration bug: dispatcher started before any handler was
                // registered. Looping would spin forever — surface and exit.
                std.log.warn("switch dispatcher exiting: no protocol handlers registered", .{});
                return;
            },
            error.OutOfMemory => return,
            else => |e| {
                std.log.debug("switch dispatcher: per-stream error: {}", .{e});
                continue;
            },
        };
    }
}

fn runNegotiatedProtocolHandler(
    sw: *Switch,
    io: std.Io,
    stream: *quic.Stream,
    supported: [][]const u8,
    timeout: std.Io.Timeout,
    peer_id: PeerId,
    remote_addr: std.Io.net.IpAddress,
    conn_gate: *Semaphore,
) std.Io.Cancelable!void {
    // Release both admission permits LAST — registered first so they run after
    // the stream is fully torn down, freeing a slot only once this handler is
    // done. release is non-blocking + allocation-free, safe during cancel
    // unwinding (no swapCancelProtection needed; it never parks).
    defer sw.handler_gate_total.release(io);
    defer conn_gate.release(io);
    defer sw.allocator.free(supported);
    defer {
        const prev = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(prev);
        closeStreamForCleanup(io, stream);
        stream.deinit();
    }

    const selected = protocols.multistream.negotiate(io, stream, supported, .{
        .timeout = timeout,
    }) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            std.log.debug("switch inbound stream negotiation failed: {}", .{err});
            return;
        },
    };
    const service = sw.protocolService(selected) orelse {
        std.log.debug("switch inbound stream selected unregistered protocol {s}", .{selected});
        return;
    };
    var handler = service.openInbound(sw.allocator, .{
        .protocol_id = selected,
        .peer_id = peer_id,
        .remote_addr = remote_addr,
    }) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            std.log.debug("switch protocol service failed to open stream handler: {}", .{err});
            return;
        },
    };
    defer handler.deinit(sw.allocator);

    handler.run(io, stream) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            std.log.debug("switch protocol handler failed: {}", .{err});
            return;
        },
    };
}

fn closeStreamForCleanup(io: std.Io, stream: *quic.Stream) void {
    stream.close(io) catch |err| std.log.debug("failed to close QUIC stream during cleanup: {}", .{err});
}

/// libp2p-format multiaddr text for an IPv4/IPv6 UDP/quic-v1 endpoint.
fn multiaddrTextFromIpUdp(allocator: std.mem.Allocator, addr: std.Io.net.IpAddress) std.mem.Allocator.Error![]u8 {
    return switch (addr) {
        .ip4 => |ip4| std.fmt.allocPrint(
            allocator,
            "/ip4/{d}.{d}.{d}.{d}/udp/{d}/quic-v1",
            .{ ip4.bytes[0], ip4.bytes[1], ip4.bytes[2], ip4.bytes[3], ip4.port },
        ),
        .ip6 => |ip6| if (mappedIpv4Bytes(ip6.bytes)) |ip4| std.fmt.allocPrint(
            allocator,
            "/ip4/{d}.{d}.{d}.{d}/udp/{d}/quic-v1",
            .{ ip4[0], ip4[1], ip4[2], ip4[3], ip6.port },
        ) else std.fmt.allocPrint(
            allocator,
            "/ip6/{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}:{x:0>2}{x:0>2}/udp/{d}/quic-v1",
            .{
                ip6.bytes[0],  ip6.bytes[1],  ip6.bytes[2],  ip6.bytes[3],
                ip6.bytes[4],  ip6.bytes[5],  ip6.bytes[6],  ip6.bytes[7],
                ip6.bytes[8],  ip6.bytes[9],  ip6.bytes[10], ip6.bytes[11],
                ip6.bytes[12], ip6.bytes[13], ip6.bytes[14], ip6.bytes[15],
                ip6.port,
            },
        ),
    };
}

fn mappedIpv4Bytes(bytes: [16]u8) ?[4]u8 {
    if (!std.mem.allEqual(u8, bytes[0..10], 0)) return null;
    if (bytes[10] != 0xff or bytes[11] != 0xff) return null;
    return .{ bytes[12], bytes[13], bytes[14], bytes[15] };
}

test "switch dial and accept use quic endpoint" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var server_key = try identity.KeyPair.generate(.ED25519);
    defer server_key.deinit();
    var client_key = try identity.KeyPair.generate(.ED25519);
    defer client_key.deinit();

    const server_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &server_key, .{});
    defer server_endpoint.deinit();
    const client_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &client_key, .{});
    defer client_endpoint.deinit();

    const server = try Switch.init(allocator, io, server_endpoint);
    defer server.deinit();
    const client = try Switch.init(allocator, io, client_endpoint);
    defer client.deinit();

    var listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer listen_addr.deinit(allocator);
    try server.listen(listen_addr);
    var client_listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer client_listen_addr.deinit(allocator);
    try client.listen(client_listen_addr);

    var addrs = try server.listenMultiaddrs(allocator);
    defer {
        for (addrs.items) |addr| allocator.free(addr);
        addrs.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 1), addrs.items.len);

    var dial_addr = try Multiaddr.fromString(allocator, addrs.items[0]);
    defer dial_addr.deinit(allocator);

    const client_conn = try client.dial(dial_addr, .{});
    defer client_conn.deinit();

    const server_conn = try server.accept();
    defer server_conn.deinit();

    try std.testing.expect(client_conn.stats().packets_sent > 0);
    try std.testing.expect(server_conn.stats().packets_recv > 0);
}

test "switch dispatches inbound streams to registered protocol handlers" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var server_key = try identity.KeyPair.generate(.ED25519);
    defer server_key.deinit();
    var client_key = try identity.KeyPair.generate(.ED25519);
    defer client_key.deinit();

    const server_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &server_key, .{});
    defer server_endpoint.deinit();
    const client_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &client_key, .{});
    defer client_endpoint.deinit();

    const server = try Switch.init(allocator, io, server_endpoint);
    defer server.deinit();
    const client = try Switch.init(allocator, io, client_endpoint);
    defer client.deinit();

    const HandlerEvent = struct {
        len: usize = 0,
        data: [64]u8 = undefined,
    };
    const RecordingHandler = struct {
        queue: *std.Io.Queue(HandlerEvent),
        expected_len: usize,

        fn run(self: *@This(), handler_io: std.Io, stream: *quic.Stream) anyerror!void {
            var event = HandlerEvent{};
            try stream.readAll(handler_io, event.data[0..self.expected_len], .{});
            event.len = self.expected_len;
            try self.queue.putOne(handler_io, event);
        }
    };

    var event_buffer: [1]HandlerEvent = undefined;
    var event_queue = std.Io.Queue(HandlerEvent).init(&event_buffer);
    var recording_handler = RecordingHandler{
        .queue = &event_queue,
        .expected_len = "handled by switch".len,
    };
    try server.addProtocolService(
        "/test/dispatch/1.0.0",
        protocols.streamHandlerService(RecordingHandler, RecordingHandler.run, &recording_handler),
    );
    const identify_protocols = [_][]const u8{
        protocols.identify.protocol_id,
        protocols.ping.protocol_id,
    };
    const identify_listen_addrs = [_][]const u8{
        "/ip4/127.0.0.1/udp/1/quic-v1",
    };
    var identify_handler = protocols.identify.IdentifyHandler.initWithOptions(allocator, .{
        .agent_version = "eth-p2p-z/test",
        .listen_addrs = &identify_listen_addrs,
        .protocols = &identify_protocols,
    });
    try server.addProtocolService(
        protocols.identify.protocol_id,
        protocols.streamHandlerService(protocols.identify.IdentifyHandler, protocols.identify.IdentifyHandler.run, &identify_handler),
    );
    var pubsub_buffer: [1]protocols.pubsub.OwnedRpc = undefined;
    var pubsub_queue = std.Io.Queue(protocols.pubsub.OwnedRpc).init(&pubsub_buffer);
    var pubsub_handler = protocols.pubsub.Gossipsub.init(allocator, &pubsub_queue);
    try server.addProtocolService(
        protocols.pubsub.protocol_id,
        protocols.streamHandlerService(protocols.pubsub.Gossipsub, protocols.pubsub.Gossipsub.run, &pubsub_handler),
    );

    var listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer listen_addr.deinit(allocator);
    try server.listen(listen_addr);
    var client_listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer client_listen_addr.deinit(allocator);
    try client.listen(client_listen_addr);

    var addrs = try server.listenMultiaddrs(allocator);
    defer {
        for (addrs.items) |addr| allocator.free(addr);
        addrs.deinit(allocator);
    }
    var dial_addr = try Multiaddr.fromString(allocator, addrs.items[0]);
    defer dial_addr.deinit(allocator);

    const client_conn = try client.dial(dial_addr, .{});
    var client_conn_live = true;
    errdefer if (client_conn_live) client_conn.deinit();

    const server_conn = try server.accept();
    var server_conn_live = true;
    errdefer if (server_conn_live) server_conn.deinit();

    {
        const payload = "handled by switch";

        const DispatchCtx = struct {
            conn: *SwitchConnection,
            err: ?anyerror = null,

            fn run(ctx: *@This()) void {
                ctx.conn.dispatchInboundStream(.{
                    .accept_timeout = .{ .duration = .{ .raw = .fromNanoseconds(std.time.ns_per_s), .clock = .awake } },
                }) catch |err| {
                    ctx.err = err;
                };
            }
        };

        var dispatch_ctx = DispatchCtx{ .conn = server_conn };
        const dispatch_thread = try std.Thread.spawn(.{}, DispatchCtx.run, .{&dispatch_ctx});
        const outbound = try client_conn.openProtocolStream("/test/dispatch/1.0.0", .{});
        defer outbound.deinit();
        defer closeStreamForCleanup(io, outbound);
        try outbound.writeAll(io, payload, .{});

        dispatch_thread.join();
        if (dispatch_ctx.err) |err| return err;
        const event = try event_queue.getOne(io);
        try std.testing.expectEqual(payload.len, event.len);
        try std.testing.expectEqualStrings(payload, event.data[0..event.len]);
    }

    {
        const DispatchCtx = struct {
            conn: *SwitchConnection,
            err: ?anyerror = null,

            fn run(ctx: *@This()) void {
                ctx.conn.dispatchInboundStream(.{
                    .accept_timeout = .{ .duration = .{ .raw = .fromNanoseconds(std.time.ns_per_s), .clock = .awake } },
                }) catch |err| {
                    ctx.err = err;
                };
            }
        };

        var dispatch_ctx = DispatchCtx{ .conn = server_conn };
        const dispatch_thread = try std.Thread.spawn(.{}, DispatchCtx.run, .{&dispatch_ctx});
        const outbound = try client_conn.openProtocolStream(protocols.identify.protocol_id, .{});
        defer outbound.deinit();
        defer closeStreamForCleanup(io, outbound);

        dispatch_thread.join();
        if (dispatch_ctx.err) |err| return err;

        var identify = try protocols.identify.readIdentify(allocator, io, outbound);
        defer identify.deinit(allocator);
        try std.testing.expectEqualStrings("ipfs/0.1.0", identify.reader.getProtocolVersion());
        try std.testing.expectEqualStrings("eth-p2p-z/test", identify.reader.getAgentVersion());
        try std.testing.expectEqual(@as(usize, 1), identify.reader.listenAddrsCount());
        try std.testing.expectEqualStrings(identify_listen_addrs[0], identify.reader.listenAddrsNext().?);
        try std.testing.expectEqual(@as(usize, 2), identify.reader.protocolsCount());
        try std.testing.expectEqualStrings(identify_protocols[0], identify.reader.protocolsNext().?);
        try std.testing.expectEqualStrings(identify_protocols[1], identify.reader.protocolsNext().?);
    }

    {
        const DispatchCtx = struct {
            conn: *SwitchConnection,
            err: ?anyerror = null,

            fn run(ctx: *@This()) void {
                ctx.conn.dispatchInboundStream(.{
                    .accept_timeout = .{ .duration = .{ .raw = .fromNanoseconds(std.time.ns_per_s), .clock = .awake } },
                }) catch |err| {
                    ctx.err = err;
                };
            }
        };

        var dispatch_ctx = DispatchCtx{ .conn = server_conn };
        const dispatch_thread = try std.Thread.spawn(.{}, DispatchCtx.run, .{&dispatch_ctx});
        const outbound = try client_conn.openProtocolStream(protocols.pubsub.protocol_id, .{});
        defer outbound.deinit();
        defer closeStreamForCleanup(io, outbound);

        const message = protocols.pubsub.PubSubMessage{
            .from = "peer-a",
            .data = "hello gossip",
            .seqno = "\x01",
            .topic = "/eth2/test",
        };
        try protocols.pubsub.writePublish(allocator, io, outbound, message);
        try outbound.closeWrite(io);

        dispatch_thread.join();
        if (dispatch_ctx.err) |err| return err;

        var rpc = try pubsub_queue.getOne(io);
        defer rpc.deinit(allocator);
        try std.testing.expectEqual(@as(usize, 1), rpc.reader.publishCount());
        var published = rpc.reader.publishNext().?;
        try std.testing.expectEqualStrings("/eth2/test", published.getTopic());
        try std.testing.expectEqualStrings("hello gossip", published.getData());
        try std.testing.expectEqualStrings("peer-a", published.getFrom());
    }

    {
        recording_handler.expected_len = "auto dispatched".len;
        try server_conn.startInboundDispatcher(.{});
        defer server_conn.stopInboundDispatcher();
        try std.testing.expectError(
            error.RegistryFrozen,
            server.addProtocolService(
                "/test/late/1.0.0",
                protocols.streamHandlerService(RecordingHandler, RecordingHandler.run, &recording_handler),
            ),
        );

        const outbound = try client_conn.openProtocolStream("/test/dispatch/1.0.0", .{});
        defer outbound.deinit();
        defer closeStreamForCleanup(io, outbound);
        const payload = "auto dispatched";

        try outbound.writeAll(io, payload, .{});

        const event = try event_queue.getOne(io);
        try std.testing.expectEqual(payload.len, event.len);
        try std.testing.expectEqualStrings(payload, event.data[0..event.len]);
    }

    client_conn.deinit();
    client_conn_live = false;
    server_conn.deinit();
    server_conn_live = false;
}

test "switch caps concurrent inbound handlers per connection (E3 gate)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var server_key = try identity.KeyPair.generate(.ED25519);
    defer server_key.deinit();
    var client_key = try identity.KeyPair.generate(.ED25519);
    defer client_key.deinit();

    const server_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &server_key, .{});
    defer server_endpoint.deinit();
    const client_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &client_key, .{});
    defer client_endpoint.deinit();

    // Per-connection cap = 2.
    const server = try Switch.initWithOptions(allocator, io, server_endpoint, .{ .max_inflight_handlers_per_conn = 2 });
    defer server.deinit();
    const client = try Switch.init(allocator, io, client_endpoint);
    defer client.deinit();

    const GateCtx = struct {
        inflight: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        completed: std.atomic.Value(usize) = .init(0),
        release: std.Io.Event = .unset,
    };
    const GatedHandler = struct {
        ctx: *GateCtx,
        // Records peak concurrency, then PARKS on `release` so the gate-saturated
        // state is stable to observe (no sleep/timing race). release is set by
        // the test once the gate has filled.
        fn run(self: *@This(), handler_io: std.Io, stream: *quic.Stream) anyerror!void {
            _ = stream;
            const now = self.ctx.inflight.fetchAdd(1, .acq_rel) + 1;
            var p = self.ctx.peak.load(.acquire);
            while (now > p) {
                if (self.ctx.peak.cmpxchgWeak(p, now, .acq_rel, .acquire)) |actual| p = actual else break;
            }
            self.ctx.release.wait(handler_io) catch {};
            _ = self.ctx.inflight.fetchSub(1, .acq_rel);
            _ = self.ctx.completed.fetchAdd(1, .acq_rel);
        }
    };

    var ctx = GateCtx{};
    var handler = GatedHandler{ .ctx = &ctx };
    try server.addProtocolService(
        "/test/gate/1.0.0",
        protocols.streamHandlerService(GatedHandler, GatedHandler.run, &handler),
    );

    var listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer listen_addr.deinit(allocator);
    try server.listen(listen_addr);

    var addrs = try server.listenMultiaddrs(allocator);
    defer {
        for (addrs.items) |a| allocator.free(a);
        addrs.deinit(allocator);
    }
    var dial_addr = try Multiaddr.fromString(allocator, addrs.items[0]);
    defer dial_addr.deinit(allocator);

    const client_conn = try client.dial(dial_addr, .{});
    defer client_conn.deinit();
    const server_conn = try server.accept();
    defer server_conn.deinit();
    try server_conn.startInboundDispatcher(.{});

    const stream_count = 6;
    const OpenCtx = struct {
        conn: *SwitchConnection,
        streams: [stream_count]?*quic.Stream = [_]?*quic.Stream{null} ** stream_count,
        // Each fiber writes its own slot (no contention). openProtocolStream
        // blocks until its handler negotiates, so the gated opens (3..) only
        // complete after `release` frees the first wave.
        fn open(self: *@This(), idx: usize) void {
            self.streams[idx] = self.conn.openProtocolStream("/test/gate/1.0.0", .{}) catch null;
        }
    };
    var open_ctx = OpenCtx{ .conn = client_conn };
    var group: std.Io.Group = .init;
    var i: usize = 0;
    while (i < stream_count) : (i += 1) {
        try group.concurrent(io, OpenCtx.open, .{ &open_ctx, i });
    }

    // Wait until the gate is saturated: exactly `cap` (=2) handlers have entered
    // and parked on `release`. A correct gate never exceeds 2; an uncapped gate
    // would let all 6 in (caught by the peak assertion below).
    var attempts: usize = 0;
    while (ctx.inflight.load(.acquire) < 2 and attempts < 600) : (attempts += 1) {
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 2), ctx.inflight.load(.acquire));

    // Release every handler; the gated opens now complete in waves of 2.
    ctx.release.set(io);
    group.await(io) catch {};

    attempts = 0;
    while (ctx.completed.load(.acquire) < stream_count and attempts < 600) : (attempts += 1) {
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, stream_count), ctx.completed.load(.acquire));
    // The gate NEVER let more than the per-connection cap run concurrently.
    try std.testing.expectEqual(@as(usize, 2), ctx.peak.load(.acquire));

    for (open_ctx.streams) |maybe_stream| {
        if (maybe_stream) |s| {
            closeStreamForCleanup(io, s);
            s.deinit();
        }
    }
}
