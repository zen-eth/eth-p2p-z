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
// gossipsub lives on the root import path (NOT routed through protocols.zig):
// it imports switch.zig, which imports protocols.zig, so exporting it through
// the protocols chain would risk an import cycle.
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

/// cgroup-CFS-quota-aware logical CPU count:
/// `min(cgroup quota, affinity)`, walking the full cgroup ancestor chain. Use
/// instead of `std.Thread.getCpuCount` when sizing a thread/executor pool. Needs
/// an `io` (it reads `/proc/self/{cgroup,mountinfo}` + the cgroup tree).
pub const getNumCpus = @import("cpu_count.zig").getNumCpus;

/// Convenience over `getNumCpus` for sizing the zio executor pool BEFORE the real
/// runtime exists: spins a throwaway bootstrap io for the `/proc` reads, logs the
/// affinity-vs-effective decision so containerized deployments surface any
/// oversubscription, and falls back to the affinity count on any detection error.
/// `std.Thread.getCpuCount` (sched_getaffinity) is blind to the cgroup CPU quota,
/// so on `--cpus`/k8s-`limits.cpu` it over-reports and oversubscribes executors.
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
