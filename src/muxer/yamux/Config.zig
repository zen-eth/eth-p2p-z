const std = @import("std");

pub const initial_stream_window: u32 = 256 * 1024;

const Config = @This();

/// AcceptBacklog is used to limit how many streams may be
/// waiting an accept.
accept_backlog: usize,

/// EnableKeepAlive is used to do periodic keep alive
/// messages using a ping.
enable_keep_alive: bool,

/// KeepAliveInterval is how often to perform the keep alive
keep_alive_interval: u64,

/// ConnectionWriteTimeout is meant to be a "safety valve" timeout after
/// which we will suspect a problem with the underlying connection and
/// close it. This is only applied to writes, where there's generally
/// an expectation that things will move along quickly.
connection_write_timeout: u64,

/// MaxStreamWindowSize is used to control the maximum
/// window size that we allow for a stream.
max_stream_window_size: u32,

/// StreamOpenTimeout is the maximum amount of time that a stream will
/// be allowed to remain in pending state while waiting for an ack from the peer.
/// Once the timeout is reached the session will be gracefully closed.
/// A zero value disables the StreamOpenTimeout allowing unbounded
/// blocking on OpenStream calls.
stream_open_timeout: u64,

/// StreamCloseTimeout is the maximum time that a stream will be allowed to
/// be in a half-closed state when `close` is called before forcibly
/// closing the connection. Forcibly closed connections will empty the
/// receive buffer, drop any future packets received for that stream,
/// and send a RST to the remote side.
stream_close_timeout: u64,

pub const Error = error{ BacklogMustBePositive, KeepAliveIntervalMustBePositive, MaxStreamWindowSizeTooSmall };

pub fn defaultConfig() Config {
    return .{
        .accept_backlog = 256,
        .enable_keep_alive = true,
        .keep_alive_interval = std.time.ns_per_s * 30,
        .connection_write_timeout = std.time.ns_per_s * 10,
        .max_stream_window_size = initial_stream_window, // Assuming this is defined elsewhere
        .stream_close_timeout = std.time.ns_per_s * 60 * 5, // 5 minutes
        .stream_open_timeout = std.time.ns_per_s * 75, // 75 seconds
    };
}

pub fn verifyConfig(config: *const Config) Error!void {
    if (config.accept_backlog <= 0) {
        return error.BacklogMustBePositive;
    }
    if (config.keep_alive_interval == 0) {
        return error.KeepAliveIntervalMustBePositive;
    }
    if (config.max_stream_window_size < initial_stream_window) {
        return error.MaxStreamWindowSizeTooSmall;
    }
}

test "Config.defaultConfig" {
    const config = defaultConfig();
    try std.testing.expectEqual(@as(usize, 256), config.accept_backlog);
    try std.testing.expect(config.enable_keep_alive);
    try std.testing.expectEqual(std.time.ns_per_s * 30, config.keep_alive_interval);
    try std.testing.expectEqual(std.time.ns_per_s * 10, config.connection_write_timeout);
    try std.testing.expectEqual(initial_stream_window, config.max_stream_window_size);
    try std.testing.expectEqual(std.time.ns_per_s * 60 * 5, config.stream_close_timeout);
    try std.testing.expectEqual(std.time.ns_per_s * 75, config.stream_open_timeout);
}

test "Config.verifyConfig success" {
    const config = defaultConfig();
    try verifyConfig(&config);
}

test "Config.verifyConfig errors" {
    var config = defaultConfig();

    // Test BacklogMustBePositive error
    config.accept_backlog = 0;
    try std.testing.expectError(error.BacklogMustBePositive, verifyConfig(&config));
    config.accept_backlog = 256; // reset

    // Test KeepAliveIntervalMustBePositive error
    config.keep_alive_interval = 0;
    try std.testing.expectError(error.KeepAliveIntervalMustBePositive, verifyConfig(&config));
    config.keep_alive_interval = std.time.ns_per_s * 30; // reset

    // Test MaxStreamWindowSizeTooSmall error
    config.max_stream_window_size = initial_stream_window - 1;
    try std.testing.expectError(error.MaxStreamWindowSizeTooSmall, verifyConfig(&config));
}

test "Config.updateConfig" {
    var config = defaultConfig();

    // Update multiple fields
    config.accept_backlog = 512;
    config.enable_keep_alive = false;
    config.keep_alive_interval = std.time.ns_per_s * 60; // 1 minute
    config.connection_write_timeout = std.time.ns_per_s * 20; // 20 seconds
    config.max_stream_window_size = initial_stream_window * 2; // double the window size

    // Verify updates
    try std.testing.expectEqual(@as(usize, 512), config.accept_backlog);
    try std.testing.expect(!config.enable_keep_alive);
    try std.testing.expectEqual(std.time.ns_per_s * 60, config.keep_alive_interval);
    try std.testing.expectEqual(std.time.ns_per_s * 20, config.connection_write_timeout);
    try std.testing.expectEqual(initial_stream_window * 2, config.max_stream_window_size);

    // Verify the updated config is still valid
    try verifyConfig(&config);
}
