const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const root_module = b.addModule("zig-libp2p", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const libp2p_lib = b.addLibrary(.{
        .name = "zig-libp2p",
        .root_module = root_module,
        .linkage = .static,
    });
    b.installArtifact(libp2p_lib);

    const filters = b.option([]const []const u8, "filter", "filter based on name");

    const libp2p_lib_unit_tests = b.addTest(.{
        .root_module = root_module,
        .filters = filters orelse &.{},
    });
    const run_libp2p_lib_unit_tests = b.addRunArtifact(libp2p_lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_libp2p_lib_unit_tests.step);
}
