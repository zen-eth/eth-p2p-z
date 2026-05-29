//! Two distinct lifecycle enums live here, each answering a different question:
//!
//! - `HandshakeStage` is the *handshake-waiter* gate. `Handshake.wait` blocks
//!   on this and returns once the state reaches a terminal value:
//!   `.established` (success) or `.failed` (handshake aborted). `.idle` is the
//!   pre-start state; `.handshaking` is the in-progress state. This is what
//!   `Connection.waitHandshake` exposes to callers.
//!
//! - `ActorStage` is the *actor lifecycle* and tracks the entire span of the
//!   per-connection actor fiber: `.initial` → `.handshake` → `.running` →
//!   `.closing` → `.closed`, plus `.failed`. This is what the actor itself uses
//!   to decide whether to close gracefully, run cleanup, or fail.
//!
//! The two overlap because `actor.spawn` seeds both from the same starting
//! handshake stage, but they answer different questions. Handshake success is
//! terminal for waiters (`.established`) but the actor continues running
//! (`.running`); the handshake gate has no concept of `.closing`/`.closed`.

const std = @import("std");
const io_time = @import("../io/time.zig");

/// Handshake-waiter gate states. `Handshake.wait` returns once the state
/// reaches `.established` or `.failed`.
pub const HandshakeStage = enum(u8) {
    idle,
    handshaking,
    established,
    failed,
    closed,
};

/// Full actor lifecycle: covers handshake, running, and shutdown phases.
pub const ActorStage = enum(u8) {
    initial,
    handshake,
    running,
    closing,
    closed,
    failed,
};

pub const ActorLifecycle = struct {
    stage: std.atomic.Value(ActorStage) = .init(.initial),
    close_result: std.atomic.Value(CloseResult) = .init(.none),

    pub const CloseResult = enum(u8) {
        none,
        graceful,
        canceled,
        failed,
    };

    pub fn begin(l: *ActorLifecycle, stage: HandshakeStage) void {
        l.close_result.store(.none, .release);
        l.stage.store(switch (stage) {
            .handshaking => .handshake,
            .idle, .established => .running,
            .failed, .closed => .failed,
        }, .release);
    }

    pub fn load(l: *const ActorLifecycle) ActorStage {
        return l.stage.load(.acquire);
    }

    pub fn isHandshake(l: *const ActorLifecycle) bool {
        return l.load() == .handshake;
    }

    pub fn running(l: *ActorLifecycle) void {
        l.stage.store(.running, .release);
    }

    pub fn closeResult(l: *const ActorLifecycle) CloseResult {
        return l.close_result.load(.acquire);
    }

    pub fn closing(l: *ActorLifecycle, result: CloseResult) void {
        l.close_result.store(result, .release);
        l.stage.store(.closing, .release);
    }

    pub fn closed(l: *ActorLifecycle, result: CloseResult) void {
        l.close_result.store(result, .release);
        l.stage.store(.closed, .release);
    }

    pub fn fail(l: *ActorLifecycle) void {
        l.close_result.store(.failed, .release);
        l.stage.store(.failed, .release);
    }
};

test "handshake close wakes waiters as connection closed" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var handshake: Handshake = .{};
    var closed: std.atomic.Value(bool) = .init(false);
    handshake.begin(.handshaking, null);

    handshake.close(io);
    try std.testing.expect(handshake.done.isSet());
    try std.testing.expectError(error.ConnectionClosed, handshake.wait(io, &closed));
}

pub const HandshakeWaitError = error{ HandshakeFailed, ConnectionClosed } || std.Io.Cancelable;

pub const Handshake = struct {
    /// Atomic: read by connection handles (via `load`) concurrently with the
    /// actor fiber's `establish`/`fail` stores.
    state: std.atomic.Value(HandshakeStage) = .init(.idle),
    done: std.Io.Event = .unset,
    /// Plain (non-atomic) on purpose: written once by `begin`, which `spawn`
    /// calls strictly before `std.Io.concurrent` creates the main-loop fiber
    /// (see actor.zig). After that the field is read only by the actor fiber
    /// (`deadline`/`timedOut`), so there is no concurrent access. If a future
    /// change ever calls `begin` after the fiber is running, make this atomic.
    deadline_ns: ?i96 = null,

    pub fn begin(h: *Handshake, stage: HandshakeStage, deadline_ns: ?i96) void {
        h.deadline_ns = deadline_ns;
        h.state.store(stage, .release);
    }

    pub fn load(h: *const Handshake) HandshakeStage {
        return h.state.load(.acquire);
    }

    pub fn isHandshaking(h: *const Handshake) bool {
        return h.load() == .handshaking;
    }

    pub fn deadline(h: *const Handshake) ?i96 {
        return h.deadline_ns;
    }

    pub fn timedOut(h: *const Handshake, io: std.Io) bool {
        const deadline_ns = h.deadline_ns orelse return false;
        return io_time.monotonicNsSigned(io) >= deadline_ns;
    }

    pub fn establish(h: *Handshake, io: std.Io) void {
        h.state.store(.established, .release);
        h.done.set(io);
    }

    pub fn fail(h: *Handshake, io: std.Io) void {
        h.state.store(.failed, .release);
        h.done.set(io);
    }

    pub fn close(h: *Handshake, io: std.Io) void {
        while (true) {
            const previous = h.load();
            switch (previous) {
                .idle, .handshaking => {
                    if (h.state.cmpxchgWeak(previous, .closed, .acq_rel, .acquire) == null) {
                        h.done.set(io);
                        return;
                    }
                },
                .established, .failed, .closed => return,
            }
        }
    }

    pub fn wait(h: *Handshake, io: std.Io, closed: *const std.atomic.Value(bool)) HandshakeWaitError!void {
        while (true) {
            switch (h.load()) {
                .established => return,
                .failed => return error.HandshakeFailed,
                .closed => return error.ConnectionClosed,
                .idle => if (closed.load(.acquire)) return error.ConnectionClosed,
                .handshaking => {},
            }
            h.done.wait(io) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
            };
        }
    }
};
