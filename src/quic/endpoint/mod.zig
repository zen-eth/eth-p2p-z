const handle = @import("handle.zig");

pub const EndpointStats = handle.EndpointStats;
pub const QuicEndpoint = handle.QuicEndpoint;

test {
    _ = @import("handle.zig");
    _ = @import("handle_tests.zig");
}
