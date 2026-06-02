const connection = @import("quic/connection/mod.zig");
const datagram = @import("quic/datagram/mod.zig");
const endpoint = @import("quic/endpoint/mod.zig");
const stream = @import("quic/stream/mod.zig");
const config = @import("quic/config.zig");

pub const Connection = connection.Connection;
pub const ConnectionStats = connection.ConnectionStats;
pub const Datagram = datagram.Datagram;
pub const PathStats = connection.PathStats;
pub const QuicEndpoint = endpoint.QuicEndpoint;
pub const EndpointStats = endpoint.EndpointStats;
pub const Stream = stream.Stream;

/// Endpoint configuration surface, re-exported for discoverability (also
/// reachable as `QuicEndpoint.Options`).
pub const Options = config.Options;
pub const TransportOptions = config.TransportOptions;
pub const ActorOptions = config.ActorOptions;
pub const EndpointOptions = config.EndpointOptions;
