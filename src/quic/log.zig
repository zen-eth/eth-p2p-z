const std = @import("std");
const quiche = @import("quiche").c;

const env_name = "LIBP2P_QUICHE_LOG";
var enabled: std.atomic.Value(bool) = .init(false);

pub fn enableFromEnv() void {
    const value_ptr = std.c.getenv(env_name) orelse return;
    const value = std.mem.span(value_ptr);
    if (!envValueEnabled(value)) return;
    enable();
}

pub fn enable() void {
    // One-time global registration. `swap(true)` returns the prior value, so only
    // the first caller across all executors proceeds — `quiche_enable_debug_logging`
    // installs a process-global callback and must not be called twice. The
    // `.acq_rel` ordering makes the winner's swap synchronize with later observers.
    if (enabled.swap(true, .acq_rel)) return;
    _ = quiche.quiche_enable_debug_logging(logCallback, null);
}

fn logCallback(line: [*c]const u8, _: ?*anyopaque) callconv(.c) void {
    std.log.debug("{s}", .{std.mem.span(line)});
}

fn envValueEnabled(value: []const u8) bool {
    if (value.len == 0) return false;
    if (std.mem.eql(u8, value, "0")) return false;
    if (std.ascii.eqlIgnoreCase(value, "false")) return false;
    if (std.ascii.eqlIgnoreCase(value, "off")) return false;
    if (std.ascii.eqlIgnoreCase(value, "no")) return false;
    return true;
}

test "quiche log env parsing" {
    try std.testing.expect(!envValueEnabled(""));
    try std.testing.expect(!envValueEnabled("0"));
    try std.testing.expect(!envValueEnabled("false"));
    try std.testing.expect(!envValueEnabled("OFF"));
    try std.testing.expect(!envValueEnabled("No"));
    try std.testing.expect(envValueEnabled("1"));
    try std.testing.expect(envValueEnabled("debug"));
}
