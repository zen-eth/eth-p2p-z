const libp2p = @import("../../root.zig");

const Switch = libp2p.swarm.Switch;

pub fn extend(comptime Loop: type) type {
    return struct {
        const SwitchCloseTask = struct {
            network_switch: *Switch,
        };

        fn runSwitchClose(_: *Loop, ctx: *SwitchCloseTask) void {
            ctx.network_switch.doClose();
        }

        pub fn queueSwitchClose(self: *Loop, network_switch: *Switch) !void {
            const task = try self.allocator.create(SwitchCloseTask);
            task.* = .{ .network_switch = network_switch };
            errdefer self.allocator.destroy(task);
            try self.queueCallWithDeinit(SwitchCloseTask, task, runSwitchClose, Loop.makeDestroyTask(SwitchCloseTask));
        }
    };
}
