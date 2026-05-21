const std = @import("std");

pub fn monotonicNsSigned(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).toNanoseconds();
}

pub fn monotonicNs(io: std.Io) u64 {
    const value = monotonicNsSigned(io);
    if (value <= 0) return 0;
    return @intCast(value);
}

pub fn receiveTimeout(value: i96) std.Io.Timeout {
    return .{ .duration = .{ .raw = .fromNanoseconds(value), .clock = .awake } };
}

/// Shorthand timeout constructors. `.{ .timeout = io_time.ms(5000) }`
/// reads better than `.{ .timeout = receiveTimeout(5 * std.time.ns_per_s) }`.
pub fn ns(value: i96) std.Io.Timeout {
    return receiveTimeout(value);
}

pub fn us(value: u64) std.Io.Timeout {
    return receiveTimeout(@intCast(value * std.time.ns_per_us));
}

pub fn ms(value: u64) std.Io.Timeout {
    return receiveTimeout(@intCast(value * std.time.ns_per_ms));
}

pub fn s(value: u64) std.Io.Timeout {
    return receiveTimeout(@intCast(value * std.time.ns_per_s));
}

pub fn timeoutDeadlineNs(io: std.Io, timeout: std.Io.Timeout) ?i96 {
    const deadline = timeout.toDeadline(io);
    return switch (deadline) {
        .none => null,
        .duration => unreachable,
        .deadline => |d| d.raw.toNanoseconds(),
    };
}

pub fn timeoutExpired(io: std.Io, timeout: std.Io.Timeout) bool {
    return switch (timeout) {
        .none => false,
        .duration => |duration| duration.raw.toNanoseconds() <= 0,
        .deadline => |deadline| deadline.durationFromNow(io).raw.toNanoseconds() <= 0,
    };
}
