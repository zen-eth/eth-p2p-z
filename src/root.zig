const std = @import("std");

pub const std_options = @import("std_options.zig").options;
pub const identity = @import("identity.zig");
pub const security = @import("security.zig");
pub const quic = @import("quic.zig");
pub const protocols = @import("protocols.zig");
/// The identify protocol, surfaced at the root path.
pub const identify = @import("protocols/identify.zig");
pub const protobuf = @import("protobuf.zig");
pub const peer_record = @import("peer_record.zig");
pub const secp_context = @import("secp_context.zig");
pub const ref_count = @import("ref_count");
pub const RefCount = ref_count.RefCount;
pub const swarm = @import("switch.zig");
// Rooted here, not under protocols.zig: it imports switch.zig -> protocols.zig,
// so routing it back through that chain would risk an import cycle.
pub const gossipsub = @import("protocols/pubsub/gossipsub.zig");

pub const Connection = quic.Connection;
pub const ConnectionStats = quic.ConnectionStats;
pub const Datagram = quic.Datagram;
pub const EndpointStats = quic.EndpointStats;
pub const PathStats = quic.PathStats;
pub const QuicEndpoint = quic.QuicEndpoint;
pub const Stream = quic.Stream;
pub const Switch = swarm.Switch;
pub const SwitchConnection = swarm.SwitchConnection;
pub const ManagedConnection = swarm.ManagedConnection;
/// The libp2p peer id type, surfaced at the root for converting to/from raw
/// peer-id bytes (e.g. for the Switch peer-query API).
pub const PeerId = @import("peer_id").PeerId;

pub const PubSubMessage = protobuf.rpc.Message;

/// cgroup-CFS-quota-aware logical CPU count `min(cgroup quota, affinity)`: use
/// instead of `std.Thread.getCpuCount` for sizing a thread/executor pool, since
/// that is blind to the cgroup quota. Needs `io` (reads `/proc/self/*` + cgroup tree).
pub const getNumCpus = @import("cpu_count.zig").getNumCpus;

/// `getNumCpus` for sizing the zio executor pool BEFORE the real runtime exists:
/// spins a throwaway bootstrap io for the `/proc` reads, falling back to affinity
/// on any detection error. Logs the decision so containers surface oversubscription
/// (on `--cpus`/k8s `limits.cpu`, affinity over-reports and oversubscribes).
pub fn effectiveCpuCount(gpa: std.mem.Allocator) usize {
    const affinity = std.Thread.getCpuCount() catch 1;
    var bootstrap = std.Io.Threaded.init(gpa, .{});
    defer bootstrap.deinit();
    const n = getNumCpus(gpa, bootstrap.io()) catch |err| {
        std.log.info("[cpu] affinity={d} (cgroup cpu detection failed: {s}) -> using {d}", .{ affinity, @errorName(err), affinity });
        return @max(@as(usize, 1), affinity);
    };
    if (n < affinity) {
        std.log.info("[cpu] affinity={d} cgroup-cpu-quota -> using {d} (quota-limited; avoids oversubscription)", .{ affinity, n });
    } else {
        std.log.info("[cpu] affinity={d} cgroup-cpu={d} (no quota limit)", .{ affinity, n });
    }
    return n;
}

test {
    std.testing.refAllDecls(@This());
    _ = @import("cpu_count.zig");
    _ = @import("quic/config.zig");
    _ = @import("quic/endpoint/mod.zig");
    _ = @import("quic/io/socket_control.zig");
    _ = @import("quic/router/retry_token.zig");
    _ = @import("switch.zig");
    _ = @import("protocols/pubsub/pubsub.zig");
    _ = @import("protocols/pubsub/router.zig");
    _ = @import("protocols/pubsub/gossipsub.zig");
    _ = @import("protocols/pubsub/signing.zig");
    _ = @import("protocols/identify.zig");
    _ = @import("peer_record.zig");
}
