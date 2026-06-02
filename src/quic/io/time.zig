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

/// Shorthand relative-timeout constructors, e.g. `io_time.ms(10).sleep(io)` or
/// `.{ .timeout = io_time.ms(10) }`. `us`/`ms`/`s` widen to i96 before scaling so
/// large values never overflow u64.
pub fn ns(value: i96) std.Io.Timeout {
    return receiveTimeout(value);
}

pub fn us(value: u64) std.Io.Timeout {
    // Widen to i96 BEFORE multiplying so the product cannot overflow u64.
    return receiveTimeout(@as(i96, value) * std.time.ns_per_us);
}

pub fn ms(value: u64) std.Io.Timeout {
    return receiveTimeout(@as(i96, value) * std.time.ns_per_ms);
}

pub fn s(value: u64) std.Io.Timeout {
    return receiveTimeout(@as(i96, value) * std.time.ns_per_s);
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

test "shorthand timeout constructors do not overflow for large values" {
    // The product `value * ns_per_X` must not wrap in u64 before widening to
    // i96. We compare each shorthand against an explicitly-widened reference so
    // any Duration round-trip behavior cancels out and only the multiply is tested.
    {
        // ns_per_ms = 1e6; u64 wraps for value > ~1.8446e13.
        const value: u64 = 20_000_000_000_000; // 2e13 ms
        const want = receiveTimeout(@as(i96, value) * std.time.ns_per_ms).duration.raw.toNanoseconds();
        const got = ms(value).duration.raw.toNanoseconds();
        try std.testing.expectEqual(want, got);
    }
    {
        // ns_per_s = 1e9; u64 wraps for value > ~1.8446e10.
        const value: u64 = 20_000_000_000; // 2e10 s
        const want = receiveTimeout(@as(i96, value) * std.time.ns_per_s).duration.raw.toNanoseconds();
        const got = s(value).duration.raw.toNanoseconds();
        try std.testing.expectEqual(want, got);
    }
    {
        // ns_per_us = 1e3; u64 wraps for value > ~1.8446e16.
        const value: u64 = 20_000_000_000_000_000; // 2e16 us
        const want = receiveTimeout(@as(i96, value) * std.time.ns_per_us).duration.raw.toNanoseconds();
        const got = us(value).duration.raw.toNanoseconds();
        try std.testing.expectEqual(want, got);
    }
}
