const std = @import("std");
const ProtoGenStep = @import("gremlin").ProtoGenStep;

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    const libxev_dep = b.dependency("libxev", .{
        .target = target,
        .optimize = optimize,
    });
    const libxev_module = libxev_dep.module("xev");

    const zmultiformats_dep = b.dependency("zmultiformats", .{
        .target = target,
        .optimize = optimize,
    });
    const zmultiformats_module = zmultiformats_dep.module("multiformats-zig");

    const lsquic_dep = b.dependency("lsquic", .{
        .target = target,
        .optimize = optimize,
    });
    const lsquic_zig_dep = b.dependency("lsquic_zig", .{
        .target = target,
        .optimize = optimize,
    });
    const lsquic_module = lsquic_zig_dep.module("lsquic");

    const lsquic_zig_artifact = lsquic_zig_dep.artifact("lsquic");
    const lsquic_artifact = lsquic_dep.artifact("lsquic");
    const ssl_dep = lsquic_dep.builder.dependency("boringssl", .{
        .target = target,
        .optimize = optimize,
    });
    const ssl_module = ssl_dep.module("ssl");

    const gremlin_dep = b.dependency("gremlin", .{
        .target = target,
        .optimize = optimize,
    });
    const gremlin_module = gremlin_dep.module("gremlin");

    const protobuf = ProtoGenStep.create(
        b,
        .{
            .proto_sources = b.path("src/proto"),
            .target = b.path("src/proto"),
        },
    );

    const peer_id_dep = zmultiformats_dep.builder.dependency("peer_id", .{
        .target = target,
        .optimize = optimize,
    });
    const peer_id_module = peer_id_dep.module("peer-id");

    const root_module = b.addModule("zig-libp2p", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    root_module.addImport("xev", libxev_module);
    root_module.addImport("multiformats", zmultiformats_module);
    root_module.addImport("ssl", ssl_module);
    root_module.addImport("lsquic", lsquic_module);
    root_module.addIncludePath(lsquic_dep.path("include"));
    root_module.addImport("gremlin", gremlin_module);
    root_module.addImport("peer_id", peer_id_module);

    const libp2p_lib = b.addLibrary(.{
        .name = "zig-libp2p",
        // In this case the main source file is merely a path, however, in more
        // complicated build scripts, this could be a generated file.
        .root_module = root_module,
        .linkage = .static,
    });
    libp2p_lib.linkLibrary(lsquic_artifact);
    libp2p_lib.linkLibrary(lsquic_zig_artifact);
    libp2p_lib.linkSystemLibrary("zlib");

    b.installArtifact(libp2p_lib);

    const libp2p_exe = b.addExecutable(.{
        .name = "zig-libp2p",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    libp2p_exe.root_module.addIncludePath(lsquic_dep.path("include"));
    libp2p_exe.root_module.addImport("xev", libxev_module);
    libp2p_exe.root_module.addImport("multiformats", zmultiformats_module);
    libp2p_exe.root_module.addImport("ssl", ssl_module);
    libp2p_exe.root_module.addImport("gremlin", gremlin_module);
    libp2p_exe.root_module.addImport("peer_id", peer_id_module);
    libp2p_exe.step.dependOn(&protobuf.step);

    libp2p_exe.linkLibrary(lsquic_artifact);
    libp2p_exe.linkLibrary(lsquic_zig_artifact);
    libp2p_exe.linkSystemLibrary("zlib");
    b.installArtifact(libp2p_exe);

    // This *creates* a Run step in the build graph, to be executed when another
    // step is evaluated that depends on it. The next line below will establish
    // such a dependency.
    const run_cmd = b.addRunArtifact(libp2p_exe);

    // By making the run step depend on the install step, it will be run from the
    // installation directory rather than directly from within the cache directory.
    // This is not necessary, however, if the application depends on other installed
    // files, this ensures they will be present and in the expected location.
    run_cmd.step.dependOn(b.getInstallStep());

    // This allows the user to pass arguments to the application in the build
    // command itself, like this: `zig build run -- arg1 arg2 etc`
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // This creates a build step. It will be visible in the `zig build --help` menu,
    // and can be selected like this: `zig build run`
    // This will evaluate the `run` step rather than the default, which is "install".
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const filters = b.option([]const []const u8, "filter", "filter based on name");

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const libp2p_lib_unit_tests = b.addTest(.{
        .root_module = root_module,
        .target = target,
        .optimize = optimize,
        .filters = filters orelse &.{},
    });

    libp2p_lib_unit_tests.root_module.addIncludePath(lsquic_dep.path("include"));
    libp2p_lib_unit_tests.root_module.addImport("xev", libxev_module);
    libp2p_lib_unit_tests.root_module.addImport("multiformats", zmultiformats_module);
    libp2p_lib_unit_tests.root_module.addImport("ssl", ssl_module);
    libp2p_lib_unit_tests.root_module.addImport("lsquic", lsquic_module);

    libp2p_lib_unit_tests.linkLibrary(lsquic_artifact);
    libp2p_lib_unit_tests.linkLibrary(lsquic_zig_artifact);
    libp2p_lib_unit_tests.linkSystemLibrary("zlib");

    libp2p_lib_unit_tests.step.dependOn(&protobuf.step);
    const run_libp2p_lib_unit_tests = b.addRunArtifact(libp2p_lib_unit_tests);

    const libp2p_exe_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    libp2p_exe_unit_tests.root_module.addIncludePath(lsquic_dep.path("include"));
    libp2p_exe_unit_tests.root_module.addImport("xev", libxev_module);
    libp2p_exe_unit_tests.root_module.addImport("multiformats", zmultiformats_module);
    libp2p_exe_unit_tests.root_module.addImport("gremlin", gremlin_module);
    libp2p_exe_unit_tests.root_module.addImport("peer_id", peer_id_module);
    libp2p_exe_unit_tests.root_module.addImport("ssl", ssl_module);
    libp2p_exe_unit_tests.step.dependOn(&protobuf.step);

    libp2p_exe_unit_tests.linkLibrary(lsquic_artifact);
    libp2p_exe_unit_tests.linkLibrary(lsquic_zig_artifact);
    libp2p_exe_unit_tests.linkSystemLibrary("zlib");
    // // for exe, lib, tests, etc.
    // exe_unit_tests.root_module.addImport("aio", zig_aio_module);
    // // for coroutines api
    // exe_unit_tests.root_module.addImport("coro", zig_coro_module);
    const run_libp2p_exe_unit_tests = b.addRunArtifact(libp2p_exe_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");

    test_step.dependOn(&run_libp2p_lib_unit_tests.step);
    test_step.dependOn(&run_libp2p_exe_unit_tests.step);
}
