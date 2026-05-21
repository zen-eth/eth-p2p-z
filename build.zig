const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const log_level_opt = b.option([]const u8, "log-level", "Set std.log level (debug, info, warn, err)");
    const zio_backend = b.option(
        []const u8,
        "zio-backend",
        "Override zio event loop backend (io_uring, epoll, kqueue, iocp, poll)",
    );
    const build_options = b.addOptions();
    build_options.addOption(?[]const u8, "log_level", log_level_opt);

    const secp_dep = b.dependency("secp256k1", .{
        .target = target,
        .optimize = optimize,
    });
    const quiche_dep = b.dependency("quiche_zig", .{
        .target = target,
        .optimize = optimize,
    });
    const quiche_raw_mod = quiche_dep.module("quiche");
    const quiche_mod = b.addModule("quiche_compat", .{
        .root_source_file = b.path("src/quic/bindings/quiche.zig"),
        .target = target,
        .optimize = optimize,
    });
    quiche_mod.addImport("quiche_raw", quiche_raw_mod);
    const ssl_mod = b.addModule("ssl_compat", .{
        .root_source_file = b.path("src/quic/bindings/ssl.zig"),
        .target = target,
        .optimize = optimize,
    });
    ssl_mod.addImport("quiche_raw", quiche_raw_mod);
    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
        .backend = zio_backend,
    });

    const peer_id_mod = b.addModule("peer_id", .{
        .root_source_file = b.path("src/peer_id.zig"),
        .target = target,
        .optimize = optimize,
    });
    const multiaddr_mod = b.addModule("multiaddr", .{
        .root_source_file = b.path("src/multiaddr.zig"),
        .target = target,
        .optimize = optimize,
    });
    multiaddr_mod.addImport("peer_id", peer_id_mod);

    const deps = ModuleDeps{
        .build_options = build_options,
        .multiaddr = multiaddr_mod,
        .gremlin = b.addModule("gremlin", .{
            .root_source_file = b.path("src/gremlin.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .peer_id = peer_id_mod,
        .secp256k1 = secp_dep.module("secp256k1"),
        .quiche = quiche_mod,
        .ssl = ssl_mod,
    };

    const root_module = b.addModule("zig-libp2p", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    addImports(root_module, deps);

    const libp2p_lib = b.addLibrary(.{
        .name = "zig-libp2p",
        .root_module = root_module,
        .linkage = .static,
    });
    b.installArtifact(libp2p_lib);

    const exe_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    addImports(exe_module, deps);

    const libp2p_exe = b.addExecutable(.{
        .name = "zig-libp2p",
        .root_module = exe_module,
    });
    b.installArtifact(libp2p_exe);

    const interop_module = b.createModule(.{
        .root_source_file = b.path("interop/transport/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    addImports(interop_module, deps);
    interop_module.addImport("zig-libp2p", root_module);
    interop_module.addImport("zio", zio_dep.module("zio"));

    const transport_interop_exe = b.addExecutable(.{
        .name = "libp2p-transport-interop",
        .root_module = interop_module,
    });
    b.installArtifact(transport_interop_exe);

    const transport_interop_run_cmd = b.addRunArtifact(transport_interop_exe);
    transport_interop_run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        transport_interop_run_cmd.addArgs(args);
    }
    const transport_interop_step = b.step("transport-interop", "Run the transport interop binary");
    transport_interop_step.dependOn(&transport_interop_run_cmd.step);

    const run_cmd = b.addRunArtifact(libp2p_exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const filters = b.option([]const []const u8, "filter", "filter based on name");
    const libp2p_lib_unit_tests = b.addTest(.{
        .root_module = root_module,
        .filters = filters orelse &.{},
    });
    const run_libp2p_lib_unit_tests = b.addRunArtifact(libp2p_lib_unit_tests);

    const exe_test_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    addImports(exe_test_module, deps);

    const libp2p_exe_unit_tests = b.addTest(.{
        .root_module = exe_test_module,
    });
    const run_libp2p_exe_unit_tests = b.addRunArtifact(libp2p_exe_unit_tests);

    const interop_unit_tests = b.addTest(.{
        .root_module = interop_module,
        .filters = filters orelse &.{},
    });
    const run_interop_unit_tests = b.addRunArtifact(interop_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_libp2p_lib_unit_tests.step);
    test_step.dependOn(&run_libp2p_exe_unit_tests.step);
    test_step.dependOn(&run_interop_unit_tests.step);
}

const ModuleDeps = struct {
    build_options: *std.Build.Step.Options,
    multiaddr: *std.Build.Module,
    gremlin: *std.Build.Module,
    peer_id: *std.Build.Module,
    secp256k1: *std.Build.Module,
    quiche: *std.Build.Module,
    ssl: *std.Build.Module,
};

fn addImports(module: *std.Build.Module, deps: ModuleDeps) void {
    module.addOptions("build_options", deps.build_options);
    module.addImport("multiaddr", deps.multiaddr);
    module.addImport("gremlin", deps.gremlin);
    module.addImport("peer_id", deps.peer_id);
    module.addImport("secp256k1", deps.secp256k1);
    module.addImport("quiche", deps.quiche);
    module.addImport("ssl", deps.ssl);
}
