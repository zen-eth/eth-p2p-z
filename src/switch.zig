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
const channel = @import("quic/io/channel.zig");
const io_time = @import("quic/io/time.zig");

const command_queue_capacity = 32;
/// Aggregate cap on concurrent inbound stream-handler fibers across all
/// connections. The PER-CONNECTION limit is the QUIC stream credit itself
/// (`initial_max_streams_bidi`): one inbound stream is one handler, and the peer
/// cannot open more streams than its credit, so the transport bounds per-peer
/// concurrency with no extra gate — set that credit to the per-peer limit you
/// want. This aggregate cap is the cross-connection limit QUIC can't express; it
/// is claimed non-blockingly, and a full aggregate closes the inbound stream
/// rather than parking the dispatcher.
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
    /// Available slots in the aggregate handler cap, shared by every connection's
    /// dispatcher. Claimed non-blockingly via `channel.tryDecrementToFloor`.
    handler_slots_total: std.atomic.Value(usize) = .init(default_max_inflight_handlers_total),
    /// Optional observer of peer-level connect/disconnect. Set via
    /// `setPeerEventCallback`; null until then. The callbacks fire OUTSIDE
    /// `connections_lock` (see `manageConnection` / `unregisterConnection`).
    peer_event_callback: ?PeerEventCallback = null,

    /// Observer of peer-level lifecycle: a peer connected (handshake done, peer
    /// id known) or disconnected. The intended consumer is a gossipsub router
    /// that opens/closes its per-peer outbound stream + state.
    ///
    /// The callbacks run on the fiber that called `dial`/`accept` (connected) or
    /// `SwitchConnection.deinit` (disconnected), and ALWAYS outside the Switch's
    /// `connections_lock`. They must be cheap and non-blocking (the intended use
    /// just posts to a queue) and must not re-enter Switch connection management
    /// (dial/accept/deinit) in a way that could block or deadlock.
    pub const PeerEventCallback = struct {
        ctx: *anyopaque,
        on_connected: *const fn (ctx: *anyopaque, peer: PeerId, conn: *SwitchConnection, remote_addr: std.Io.net.IpAddress) void,
        on_disconnected: *const fn (ctx: *anyopaque, peer: PeerId) void,
    };

    pub const Options = struct {
        max_inflight_handlers_total: usize = default_max_inflight_handlers_total,
    };
    pub const OptionsError = error{InvalidOptions};

    fn validateOptions(opts: Options) OptionsError!void {
        // A zero cap would reject every inbound stream — reject the config rather
        // than silently clamp.
        if (opts.max_inflight_handlers_total == 0) return error.InvalidOptions;
    }

    pub const InitError = std.mem.Allocator.Error;
    pub const DispatchError = error{
        NoRegisteredProtocols,
        ConnectionClosed,
        /// The aggregate handler cap is full; the inbound stream was gracefully
        /// closed (non-blocking back-pressure). The dispatcher loop treats this as
        /// "skip and keep accepting" (after a short backoff), not a fatal error.
        HandlerLimitReached,
    } || std.mem.Allocator.Error || std.Io.ConcurrentError || quic.Connection.AcceptStreamError;
    pub const AddProtocolServiceError = error{RegistryFrozen} || std.mem.Allocator.Error;
    pub const DispatchOptions = struct {
        accept_timeout: std.Io.Timeout = .none,
        negotiation_timeout: std.Io.Timeout = default_negotiation_timeout,
    };
    pub const OpenProtocolStreamOptions = struct {
        negotiation_timeout: std.Io.Timeout = default_negotiation_timeout,
    };
    /// Result of a multi-protocol open: the negotiated stream plus the protocol
    /// id the peer accepted. `selected` aliases one element of the caller's
    /// proposed `protocols` slice (multistream returns the matched candidate, not
    /// a copy), so the caller must keep that slice alive as long as it reads
    /// `selected` — passing a static/comptime list (as gossipsub does) makes this
    /// trivially safe.
    pub const SelectedStream = struct {
        stream: *quic.Stream,
        selected: protocols.ProtocolId,
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
    pub const AcceptError = quic.QuicEndpoint.AcceptError || std.Io.ConcurrentError || std.mem.Allocator.Error;

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

    /// Like `init`, but with a custom aggregate handler cap.
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
            .handler_slots_total = .init(opts.max_inflight_handlers_total),
        };
        return sw;
    }

    /// Registers (or replaces) the peer connect/disconnect observer. Intended to
    /// be called once after construction, before any dial/accept, by the service
    /// that wants the events (e.g. a gossipsub router registering itself).
    pub fn setPeerEventCallback(sw: *Switch, cb: PeerEventCallback) void {
        sw.peer_event_callback = cb;
    }

    /// Unregisters the peer connect/disconnect observer. The observing service
    /// (e.g. a gossipsub router about to be freed) MUST call this before freeing
    /// the object the callback's `ctx` points at, so no later connect/disconnect
    /// fires into freed memory. Like `setPeerEventCallback`, this writes the
    /// `?PeerEventCallback` field without synchronization, so the caller must
    /// ensure no concurrent connect/disconnect (dial/accept/SwitchConnection.deinit
    /// firing the callback) is in flight when it runs.
    pub fn clearPeerEventCallback(sw: *Switch) void {
        sw.peer_event_callback = null;
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

        // The aggregate slot counter is just an atomic freed with the Switch
        // below. That is only safe because the loop above tore down every
        // connection first, which joins all handler fibers — so no handler can
        // still touch the counter. Don't change this to a fire-and-forget
        // teardown without preserving that ordering.

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

    /// Graceful-shutdown helper: unblock a fiber parked in `accept`. After this,
    /// a blocked `accept` returns `error.ListenerClosed` (distinct from
    /// `error.Canceled`), so an accept loop can detect the stop and exit cleanly
    /// instead of treating it as an unexpected failure. The underlying endpoint
    /// is NOT torn down — existing connections keep working — so a caller can
    /// quiesce inbound accepts before closing connections and running `deinit`.
    /// Idempotent and safe to call before `deinit`; the later listener teardown
    /// in `endpoint.deinit` won't double-close the accept channel. The `io`
    /// argument is accepted for call-site symmetry with the rest of the
    /// concurrency API; the Switch closes the accept channel via its own stored
    /// `io`, so the passed value is not required to differ.
    pub fn closeListener(sw: *Switch, io: std.Io) void {
        _ = io;
        sw.endpoint.stopAccepting();
    }

    /// Alias for `closeListener`, named for the operation it performs (stop
    /// accepting new inbound connections).
    pub fn stopAccepting(sw: *Switch, io: std.Io) void {
        sw.closeListener(io);
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

        // Register under the lock, then fire the connect event OUTSIDE it: the
        // observer (a router) may post to its own inbox, and holding
        // connections_lock across that risks lock-ordering / blocking issues.
        // Capture the peer id + remote address while registered so the values
        // are valid even though we fire after unlocking. Both peer_id and
        // remote_addr are set in the actor's init before it spawns, so they are
        // already valid here.
        {
            sw.connections_lock.lockUncancelable(sw.io);
            defer sw.connections_lock.unlock(sw.io);
            try sw.connections.append(sw.allocator, managed);
            managed.registered = true;
        }

        // Only a genuinely-registered connection reaches here: the append above
        // is the last fallible step, so there is no error path between
        // registration and firing.
        if (sw.peer_event_callback) |cb| {
            cb.on_connected(cb.ctx, managed.peerId(), managed, managed.remoteAddress());
        }
        return managed;
    }

    fn unregisterConnection(sw: *Switch, conn: *SwitchConnection) void {
        // Capture the peer id while still registered, perform the removal under
        // the lock, then fire the disconnect event OUTSIDE the lock (same reason
        // as the connect event in `manageConnection`). `did_unregister` guards
        // against a double-fire: this returns early without firing when called
        // on an already-unregistered connection.
        var did_unregister = false;
        const peer_id = conn.peerId();
        {
            sw.connections_lock.lockUncancelable(sw.io);
            defer sw.connections_lock.unlock(sw.io);

            if (!conn.registered) return;
            for (sw.connections.items, 0..) |item, index| {
                if (item == conn) {
                    _ = sw.connections.swapRemove(index);
                    break;
                }
            }
            conn.registered = false;
            did_unregister = true;
        }

        if (did_unregister) {
            if (sw.peer_event_callback) |cb| cb.on_disconnected(cb.ctx, peer_id);
        }
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

    /// Open an outbound stream proposing `protocol_ids` in preference order and
    /// return both the stream and the protocol the peer accepted. Unlike
    /// `openProtocolStream` (which proposes one id and fails on any mismatch),
    /// this negotiates the best common protocol from a list — the initiator
    /// proposes each id in turn until the responder accepts one. Used by
    /// gossipsub to speak the highest /meshsub version a peer supports while
    /// falling back cleanly to older ones.
    ///
    /// `protocol_ids` is borrowed; `result.selected` aliases one of its elements
    /// (see `SelectedStream`), so it must outlive the caller's use of `selected`.
    pub fn openProtocolStreamMulti(
        conn: *SwitchConnection,
        protocol_ids: []const protocols.ProtocolId,
        opts: Switch.OpenProtocolStreamOptions,
    ) Switch.OpenProtocolStreamError!Switch.SelectedStream {
        var reply: OpenProtocolStreamMultiReply = .{};
        try conn.post(.{ .open_protocol_stream_multi = .{
            .protocol_ids = protocol_ids,
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
    open_protocol_stream_multi: struct {
        protocol_ids: []const protocols.ProtocolId,
        opts: Switch.OpenProtocolStreamOptions,
        reply: *OpenProtocolStreamMultiReply,
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

const OpenProtocolStreamMultiReply = struct {
    event: std.Io.Event = .unset,
    result: Switch.OpenProtocolStreamError!Switch.SelectedStream = error.ConnectionClosed,

    fn complete(reply: *OpenProtocolStreamMultiReply, io: std.Io, result: Switch.OpenProtocolStreamError!Switch.SelectedStream) void {
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
            // The `.shutdown` command only gets seen between commands; while
            // actorMain is parked inside a command (e.g. an untimed acceptStream)
            // it never reaches it. Cancel the main future too: every blocking
            // point in actorMain is a cancel point that unwinds to fiber exit, so
            // this can't re-park. Cancel before waiting on the reply, since a
            // parked actorMain would never complete it otherwise (its defer's
            // completePending does, once cancel unparks it).
            future.cancel(actor.io) catch {};
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
        // Single-protocol open: propose just `protocol_id` and treat a selection
        // other than it as a mismatch (the responder echoing a candidate it was
        // not offered). With a one-element list `negotiate` can only return that
        // id, so the mismatch check is a belt-and-suspenders guard.
        const result = try actor.openProtocolStreamMulti(&.{protocol_id}, opts);
        if (!std.mem.eql(u8, result.selected, protocol_id)) {
            closeStreamForCleanup(actor.io, result.stream);
            result.stream.deinit();
            return error.SelectedProtocolMismatch;
        }
        return result.stream;
    }

    fn openProtocolStreamMulti(
        actor: *SwitchConnectionActor,
        protocol_ids: []const protocols.ProtocolId,
        opts: Switch.OpenProtocolStreamOptions,
    ) Switch.OpenProtocolStreamError!Switch.SelectedStream {
        const conn = actor.liveConnection() orelse return error.ConnectionClosed;
        const stream = try conn.openStream(actor.io);
        var stream_live = true;
        errdefer if (stream_live) {
            closeStreamForCleanup(actor.io, stream);
            stream.deinit();
        };

        // The initiator proposes each id in `protocol_ids` (preference order)
        // until the responder accepts one; `selected` aliases that element, so it
        // stays valid as long as the caller's slice does.
        const selected = try protocols.multistream.negotiate(actor.io, stream, protocol_ids, .{
            .role = .initiator,
            .timeout = opts.negotiation_timeout,
        });
        stream_live = false;
        return .{ .stream = stream, .selected = selected };
    }

    fn dispatchInboundStream(actor: *SwitchConnectionActor, opts: Switch.DispatchOptions) Switch.DispatchError!void {
        const conn = actor.liveConnection() orelse return error.ConnectionClosed;

        const stream = try conn.acceptStream(actor.io, .{ .timeout = opts.accept_timeout });
        var stream_live = true;
        errdefer if (stream_live) {
            closeStreamForCleanup(actor.io, stream);
            stream.deinit();
        };

        // Claim an aggregate handler slot (the per-connection limit is the QUIC
        // stream credit, enforced by the transport). A full aggregate is not an
        // error to retry on this stream: close it and return HandlerLimitReached.
        // The close is a graceful FIN/STOP_SENDING(0) — the peer's negotiation
        // fails but can't tell a limit from a completed handler.
        if (!channel.tryDecrementToFloor(&actor.sw.handler_slots_total)) return error.HandlerLimitReached;
        errdefer _ = actor.sw.handler_slots_total.fetchAdd(1, .release);

        // Build the supported-protocol list only after admission, so a refused
        // stream (the common path under load) never allocates or takes the
        // services lock. The errdefer above rolls the slot back on these paths.
        const supported = try actor.sw.supportedProtocolIds();
        errdefer actor.allocator.free(supported);
        if (supported.len == 0) return error.NoRegisteredProtocols;

        try actor.handler_group.concurrent(
            actor.io,
            runNegotiatedProtocolHandler,
            .{ actor.sw, actor.io, stream, supported, opts.negotiation_timeout, actor.peer_id, actor.remote_addr },
        );
        // Spawn is the last fallible step, so on success no errdefer fires: the
        // handler now owns the stream, the slot, and `supported`, and releases
        // them in its defers. Only the stream cleanup needs disarming.
        stream_live = false;
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
            .open_protocol_stream_multi => |cmd| {
                const result = actor.openProtocolStreamMulti(cmd.protocol_ids, cmd.opts) catch |err| {
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
        .open_protocol_stream_multi => |cmd| cmd.reply.complete(io, error.ConnectionClosed),
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
            error.ConcurrencyUnavailable => {
                // Out of fibers/threads — usually the same exhaustion as
                // OutOfMemory. Looping would tight-spin (accept and roll back
                // instantly), so exit like OutOfMemory; the connection stays up,
                // inbound dispatch stops.
                std.log.warn("switch dispatcher exiting: cannot spawn handler (concurrency unavailable)", .{});
                return;
            },
            error.HandlerLimitReached => {
                // The aggregate cap is full (only reachable when the whole process
                // is at capacity). Keep accepting, but back off so a flood of
                // queued streams can't spin the loop accepting-and-closing them at
                // full speed. A slot frees within a handler's lifetime, so a coarse
                // poll is fine. Cancel = teardown.
                io_time.ms(5).sleep(actor.io) catch return;
                continue;
            },
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
) std.Io.Cancelable!void {
    // Return the aggregate slot last (this defer runs after the stream is torn
    // down), so the slot only frees once this handler is fully done. A plain
    // atomic add — nobody waits on the counter, so no wake is needed.
    defer _ = sw.handler_slots_total.fetchAdd(1, .release);
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

test "openProtocolStreamMulti negotiates the best protocol the peer supports" {
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

    // A trivial inbound handler that just reads one byte, registered under the
    // SERVER's middle and low protocol ids but NOT the high one. The client
    // proposes [high, middle, low]; the responder rejects "high" (not
    // registered) and accepts "middle" (the first it supports), so the
    // negotiated protocol must be "middle".
    const high = "/test/multi/3.0.0";
    const middle = "/test/multi/2.0.0";
    const low = "/test/multi/1.0.0";

    const OneByteHandler = struct {
        fn run(_: *@This(), handler_io: std.Io, stream: *quic.Stream) anyerror!void {
            var b: [1]u8 = undefined;
            try stream.readAll(handler_io, &b, .{});
        }
    };
    var one_byte = OneByteHandler{};
    // Register middle and low (each a distinct service instance — registering one
    // instance under several keys would double-free on Switch teardown).
    try server.addProtocolService(middle, protocols.streamHandlerService(OneByteHandler, OneByteHandler.run, &one_byte));
    try server.addProtocolService(low, protocols.streamHandlerService(OneByteHandler, OneByteHandler.run, &one_byte));

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

    const proposed = [_]protocols.ProtocolId{ high, middle, low };
    const result = try client_conn.openProtocolStreamMulti(&proposed, .{});
    defer result.stream.deinit();
    defer closeStreamForCleanup(io, result.stream);
    // "high" is unregistered → rejected; "middle" is the first the peer accepts.
    try std.testing.expectEqualStrings(middle, result.selected);
    // Send the one byte the handler reads so it returns cleanly.
    try result.stream.writeAll(io, "x", .{});

    dispatch_thread.join();
    if (dispatch_ctx.err) |err| return err;

    client_conn.deinit();
    client_conn_live = false;
    server_conn.deinit();
    server_conn_live = false;
}

test "switch fires peer connect and disconnect events on both ends" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var server_key = try identity.KeyPair.generate(.ED25519);
    defer server_key.deinit();
    var client_key = try identity.KeyPair.generate(.ED25519);
    defer client_key.deinit();

    const server_peer_id = try server_key.peerId(allocator);
    const client_peer_id = try client_key.peerId(allocator);

    const server_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &server_key, .{});
    defer server_endpoint.deinit();
    const client_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &client_key, .{});
    defer client_endpoint.deinit();

    const server = try Switch.init(allocator, io, server_endpoint);
    defer server.deinit();
    const client = try Switch.init(allocator, io, client_endpoint);
    defer client.deinit();

    // Records every peer-event the Switch fires. A mutex keeps the recorder
    // sound even though connect/disconnect events run on whichever fiber called
    // dial/accept/deinit (which need not be the test fiber).
    const Recorder = struct {
        io: std.Io,
        lock: std.Io.Mutex = .init,
        connected: std.ArrayList(PeerId) = .empty,
        disconnected: std.ArrayList(PeerId) = .empty,

        fn onConnected(ctx: *anyopaque, peer: PeerId, conn: *SwitchConnection, remote_addr: std.Io.net.IpAddress) void {
            _ = conn;
            _ = remote_addr;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.lock.lockUncancelable(self.io);
            defer self.lock.unlock(self.io);
            self.connected.append(std.testing.allocator, peer) catch unreachable;
        }

        fn onDisconnected(ctx: *anyopaque, peer: PeerId) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.lock.lockUncancelable(self.io);
            defer self.lock.unlock(self.io);
            self.disconnected.append(std.testing.allocator, peer) catch unreachable;
        }

        fn callback(self: *@This()) Switch.PeerEventCallback {
            return .{
                .ctx = self,
                .on_connected = onConnected,
                .on_disconnected = onDisconnected,
            };
        }

        fn deinit(self: *@This()) void {
            self.connected.deinit(std.testing.allocator);
            self.disconnected.deinit(std.testing.allocator);
        }
    };

    var server_recorder = Recorder{ .io = io };
    defer server_recorder.deinit();
    var client_recorder = Recorder{ .io = io };
    defer client_recorder.deinit();

    server.setPeerEventCallback(server_recorder.callback());
    client.setPeerEventCallback(client_recorder.callback());

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

    // manageConnection fires on_connected synchronously before dial/accept
    // returns, so the records are visible now. The client learns the server's
    // peer id; the server learns the client's.
    try std.testing.expectEqual(@as(usize, 1), client_recorder.connected.items.len);
    try std.testing.expect(client_recorder.connected.items[0].eql(&server_peer_id));
    try std.testing.expectEqual(@as(usize, 1), server_recorder.connected.items.len);
    try std.testing.expect(server_recorder.connected.items[0].eql(&client_peer_id));

    try std.testing.expectEqual(@as(usize, 0), client_recorder.disconnected.items.len);
    try std.testing.expectEqual(@as(usize, 0), server_recorder.disconnected.items.len);

    // Tearing down the client connection fires on_disconnected with the
    // server's peer id on the client side.
    client_conn.deinit();
    client_conn_live = false;
    try std.testing.expectEqual(@as(usize, 1), client_recorder.disconnected.items.len);
    try std.testing.expect(client_recorder.disconnected.items[0].eql(&server_peer_id));

    server_conn.deinit();
    server_conn_live = false;
    try std.testing.expectEqual(@as(usize, 1), server_recorder.disconnected.items.len);
    try std.testing.expect(server_recorder.disconnected.items[0].eql(&client_peer_id));
}

test "switch rejects inbound handlers past the aggregate cap, recycling slots" {
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

    // Aggregate cap = 2.
    const server = try Switch.initWithOptions(allocator, io, server_endpoint, .{ .max_inflight_handlers_total = 2 });
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

    // Two streams fill the aggregate cap. openProtocolStream returns once the
    // server has negotiated, i.e. once each handler was admitted (slot claimed).
    const s1 = try client_conn.openProtocolStream("/test/gate/1.0.0", .{});
    defer {
        closeStreamForCleanup(io, s1);
        s1.deinit();
    }
    const s2 = try client_conn.openProtocolStream("/test/gate/1.0.0", .{});
    defer {
        closeStreamForCleanup(io, s2);
        s2.deinit();
    }

    // Wait until both handlers have entered run() and parked, so both slots are
    // definitively held.
    var attempts: usize = 0;
    while (ctx.inflight.load(.acquire) < 2) {
        if (attempts >= 600) {
            std.debug.print(
                "aggregate cap never reached within ~3s: inflight={d}, expected 2\n",
                .{ctx.inflight.load(.acquire)},
            );
            return error.SaturationTimeout;
        }
        attempts += 1;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 2), ctx.inflight.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), ctx.peak.load(.acquire));

    // Both slots are held. Two MORE inbound streams must be rejected (the server
    // gracefully closes them) rather than queued or blocking — so the client's
    // openProtocolStream fails. QUIC stream-credit (100) >> the cap (2), so these
    // streams do reach the switch and get rejected there. With a higher cap they
    // would be admitted and succeed, so this genuinely binds the cap.
    var rejected: usize = 0;
    for (0..2) |_| {
        if (client_conn.openProtocolStream("/test/gate/1.0.0", .{})) |extra| {
            closeStreamForCleanup(io, extra); // unexpected admission past the cap
            extra.deinit();
        } else |_| {
            rejected += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), rejected);
    // The rejects ran no handler: still exactly the cap in flight, peak never exceeded it.
    try std.testing.expectEqual(@as(usize, 2), ctx.inflight.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), ctx.peak.load(.acquire));

    // Release the two; they finish and RETURN their slots.
    ctx.release.set(io);
    attempts = 0;
    while (ctx.completed.load(.acquire) < 2) {
        if (attempts >= 600) {
            std.debug.print("admitted handlers did not complete within ~3s: completed={d}\n", .{ctx.completed.load(.acquire)});
            return error.CompletionTimeout;
        }
        attempts += 1;
        io_time.ms(5).sleep(io) catch {};
    }

    // Slots are recycled, not leaked: a new inbound stream is admitted again.
    // Retry to absorb the brief window between a handler's completed++ and its
    // slot-return defer running.
    attempts = 0;
    const s3 = blk: {
        while (true) {
            if (client_conn.openProtocolStream("/test/gate/1.0.0", .{})) |s| break :blk s else |_| {}
            if (attempts >= 600) return error.SlotsNotRecycled;
            attempts += 1;
            io_time.ms(5).sleep(io) catch {};
        }
    };
    defer {
        closeStreamForCleanup(io, s3);
        s3.deinit();
    }
    // Its handler runs (release already set) and completes; peak never exceeds the cap.
    attempts = 0;
    while (ctx.completed.load(.acquire) < 3) {
        if (attempts >= 600) return error.RecycledHandlerDidNotComplete;
        attempts += 1;
        io_time.ms(5).sleep(io) catch {};
    }
    try std.testing.expectEqual(@as(usize, 2), ctx.peak.load(.acquire));
}

test "closeListener unblocks a waiting accept for graceful shutdown" {
    // The core graceful-shutdown proof: a fiber parked in Switch.accept() (no
    // pending inbound) must be released cleanly by closeListener — returning the
    // distinct error.ListenerClosed (NOT error.Canceled) — so the accept loop
    // exits promptly, and a subsequent deinit completes with no hang or leak.
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var server_key = try identity.KeyPair.generate(.ED25519);
    defer server_key.deinit();

    const server_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &server_key, .{});
    defer server_endpoint.deinit();

    const server = try Switch.init(allocator, io, server_endpoint);
    defer server.deinit();

    var listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer listen_addr.deinit(allocator);
    try server.listen(listen_addr);

    // An accept loop that mirrors the interop binary's: it accepts in a loop and
    // treats ListenerClosed (and Canceled) as a clean stop. Records the terminal
    // error and the number of accepts so the test can assert what unblocked it.
    const AcceptLoop = struct {
        sw: *Switch,
        done: std.Io.Event = .unset,
        terminal: ?anyerror = null,
        accepts: usize = 0,

        fn run(self: *@This()) void {
            while (true) {
                const conn = self.sw.accept() catch |err| {
                    self.terminal = err;
                    self.done.set(self.sw.io);
                    return;
                };
                self.accepts += 1;
                conn.deinit();
            }
        }
    };

    var loop = AcceptLoop{ .sw = server };
    var loop_future = try std.Io.concurrent(io, AcceptLoop.run, .{&loop});

    // Give the fiber time to actually park inside accept() with nothing pending,
    // so we are exercising the blocked-accept wakeup (not a pre-close fast path).
    io_time.ms(50).sleep(io) catch {};
    try std.testing.expect(!loop.done.isSet());

    // Ask the listener to stop accepting. The parked accept() must wake.
    server.closeListener(io);

    // The accept fiber must exit promptly. Bounded wait so a regression (the old
    // lost-wake hang) fails the test instead of blocking the suite forever.
    var waited_ms: usize = 0;
    while (!loop.done.isSet()) {
        if (waited_ms >= 5000) return error.AcceptDidNotUnblock;
        io_time.ms(10).sleep(io) catch {};
        waited_ms += 10;
    }
    loop_future.await(io);

    // It unblocked with the clean closed error, distinct from a fiber cancel, and
    // never spuriously accepted a connection (none was dialed).
    try std.testing.expectEqual(@as(anyerror, error.ListenerClosed), loop.terminal.?);
    try std.testing.expectEqual(@as(usize, 0), loop.accepts);

    // closeListener is idempotent: a second call (and the implicit close inside
    // server.deinit() / endpoint.deinit() below) must not double-close or fault.
    server.closeListener(io);

    // A fresh accept after the listener is closed returns the clean error too,
    // rather than hanging.
    try std.testing.expectError(error.ListenerClosed, server.accept());
}

test "closeListener is idempotent across repeated calls and teardown" {
    // Closing the accept queue is guarded by the channel's own `closed` flag, so
    // calling closeListener many times (and again implicitly inside deinit) must
    // not double-close, fault, or leak. This exercises the idempotency directly,
    // without a parked accept fiber: just close repeatedly on a bound listener.
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var server_key = try identity.KeyPair.generate(.ED25519);
    defer server_key.deinit();

    const server_endpoint = try quic.QuicEndpoint.initWithIdentity(allocator, io, &server_key, .{});
    defer server_endpoint.deinit();

    const server = try Switch.init(allocator, io, server_endpoint);
    defer server.deinit();

    var listen_addr = try Multiaddr.fromString(allocator, "/ip4/127.0.0.1/udp/0/quic-v1");
    defer listen_addr.deinit(allocator);
    try server.listen(listen_addr);

    // Several closes in a row, each a no-op after the first.
    server.closeListener(io);
    server.closeListener(io);
    server.stopAccepting(io);

    // Every accept after closing reports the clean closed error, never hangs.
    try std.testing.expectError(error.ListenerClosed, server.accept());
    try std.testing.expectError(error.ListenerClosed, server.accept());

    // The deferred server.deinit() / endpoint.deinit() below run the listener's
    // full teardown, which closes the same accept channel once more — still safe.
}

test "closeListener drains a queued-but-unaccepted inbound connection without leak" {
    // When an inbound connection has been accepted by the router and buffered in
    // the accept queue but never handed to a Switch.accept() caller, closing the
    // listener (and the following endpoint teardown) must release that buffered
    // connection. The std.testing.allocator asserts no leak: the queued
    // connection is dropped/closed on teardown rather than orphaned.
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

    var addrs = try server.listenMultiaddrs(allocator);
    defer {
        for (addrs.items) |addr| allocator.free(addr);
        addrs.deinit(allocator);
    }
    var dial_addr = try Multiaddr.fromString(allocator, addrs.items[0]);
    defer dial_addr.deinit(allocator);

    // Dial in: the server's router completes the handshake and BUFFERS the
    // resulting connection in the accept queue. We deliberately do NOT call
    // server.accept(), so the inbound connection stays queued and unaccepted.
    const client_conn = try client.dial(dial_addr, .{});
    defer client_conn.deinit();

    // Give the server's router fiber time to publish the accepted connection into
    // the accept queue, so it is genuinely buffered before we stop accepting.
    var waited_ms: usize = 0;
    while (server_endpoint.stats().connections_established == 0) {
        if (waited_ms >= 5000) return error.InboundNeverQueued;
        io_time.ms(10).sleep(io) catch {};
        waited_ms += 10;
    }
    // A small extra settle so the publish into the queue has definitely landed.
    io_time.ms(50).sleep(io) catch {};

    // Stop accepting WITHOUT ever calling server.accept(): the inbound connection
    // stays buffered in the closed accept queue. The listener teardown
    // (server.deinit -> endpoint.deinit) must drain and release that buffered
    // connection rather than orphan it. The std.testing.allocator's leak check at
    // the end of the test is the assertion: a leaked queued connection fails here.
    server.closeListener(io);
    server.closeListener(io); // still idempotent with a buffered item present.
}
