const std = @import("std");

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

    const noiz_dep = b.dependency("noiz", .{
        .target = target,
        .optimize = optimize,
    });
    const noiz_module = noiz_dep.module("noiz");

    const root_module = b.addModule("zig-libp2p", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    root_module.addImport("xev", libxev_module);
    root_module.addImport("noiz", noiz_module);

    const libp2p_lib = b.addLibrary(.{
        .name = "zig-libp2p",
        // In this case the main source file is merely a path, however, in more
        // complicated build scripts, this could be a generated file.
        .root_module = root_module,
        .linkage = .static,
    });

    b.installArtifact(libp2p_lib);

    const libp2p_exe = b.addExecutable(.{
        .name = "zig-libp2p",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    libp2p_exe.root_module.addImport("xev", libxev_module);
    libp2p_exe.root_module.addImport("noiz", noiz_module);

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

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const libp2p_lib_unit_tests = b.addTest(.{
        .root_module = root_module,
        .target = target,
        .optimize = optimize,
    });

    const run_libp2p_lib_unit_tests = b.addRunArtifact(libp2p_lib_unit_tests);

    const libp2p_exe_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // exe_unit_tests.root_module.addImport("libuv", libuv_module);
    // exe_unit_tests.root_module.addImport("multiformats-zig", multiformats_zig_module);
    libp2p_exe_unit_tests.root_module.addImport("xev", libxev_module);
    libp2p_exe_unit_tests.root_module.addImport("noiz", noiz_module);
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
