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

    // Shared refcount helpers (RefCount(T) wrapper + intrusive AtomicRc). A
    // standalone named module so it can be imported by name from any module
    // regardless of its root directory — in particular from the zio-io-test
    // module, which is rooted at src/quic/ and so cannot reach src/ref_count.zig
    // via a relative @import.
    const ref_count_mod = b.addModule("ref_count", .{
        .root_source_file = b.path("src/ref_count.zig"),
        .target = target,
        .optimize = optimize,
    });

    const deps = ModuleDeps{
        .build_options = build_options,
        .ref_count = ref_count_mod,
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

    const gossipsub_interop_module = b.createModule(.{
        .root_source_file = b.path("interop/gossipsub/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    addImports(gossipsub_interop_module, deps);
    gossipsub_interop_module.addImport("zig-libp2p", root_module);
    gossipsub_interop_module.addImport("zio", zio_dep.module("zio"));

    const gossipsub_interop_exe = b.addExecutable(.{
        .name = "libp2p-gossipsub-interop",
        .root_module = gossipsub_interop_module,
    });
    b.installArtifact(gossipsub_interop_exe);

    const gossipsub_interop_run_cmd = b.addRunArtifact(gossipsub_interop_exe);
    gossipsub_interop_run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        gossipsub_interop_run_cmd.addArgs(args);
    }
    const gossipsub_interop_step = b.step("gossipsub-interop", "Run the gossipsub interop binary");
    gossipsub_interop_step.dependOn(&gossipsub_interop_run_cmd.step);

    // P0 micro-benchmarks. Rooted in src/ so they can reach internals (the
    // Router generic, the endpoint test fixture) by relative import; run them
    // with -Doptimize=ReleaseFast for meaningful numbers.
    const bench_gossipsub_module = b.createModule(.{
        .root_source_file = b.path("src/bench_gossipsub.zig"),
        .target = target,
        .optimize = optimize,
    });
    addImports(bench_gossipsub_module, deps);
    bench_gossipsub_module.addImport("zio", zio_dep.module("zio"));
    const bench_gossipsub_exe = b.addExecutable(.{
        .name = "bench-gossipsub",
        .root_module = bench_gossipsub_module,
    });
    const bench_gossipsub_run = b.addRunArtifact(bench_gossipsub_exe);
    const bench_gossipsub_step = b.step("bench-gossipsub", "Run the gossipsub router micro-benchmark");
    bench_gossipsub_step.dependOn(&bench_gossipsub_run.step);

    const bench_quic_module = b.createModule(.{
        .root_source_file = b.path("src/bench_quic.zig"),
        .target = target,
        .optimize = optimize,
    });
    addImports(bench_quic_module, deps);
    bench_quic_module.addImport("zio", zio_dep.module("zio"));
    const bench_quic_exe = b.addExecutable(.{
        .name = "bench-quic",
        .root_module = bench_quic_module,
    });
    const bench_quic_run = b.addRunArtifact(bench_quic_exe);
    const bench_quic_step = b.step("bench-quic", "Run the QUIC loopback benchmark");
    bench_quic_step.dependOn(&bench_quic_run.step);

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

    const gossipsub_interop_unit_tests = b.addTest(.{
        .root_module = gossipsub_interop_module,
        .filters = filters orelse &.{},
    });
    const run_gossipsub_interop_unit_tests = b.addRunArtifact(gossipsub_interop_unit_tests);

    // The shared refcount helpers are their own standalone module, so their
    // in-file tests are not reached by refAllDecls on the root module; run them
    // as a dedicated test artifact under `zig build test`.
    const ref_count_unit_tests = b.addTest(.{
        .root_module = ref_count_mod,
        .filters = filters orelse &.{},
    });
    const run_ref_count_unit_tests = b.addRunArtifact(ref_count_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_ref_count_unit_tests.step);
    test_step.dependOn(&run_libp2p_lib_unit_tests.step);
    test_step.dependOn(&run_libp2p_exe_unit_tests.step);
    test_step.dependOn(&run_interop_unit_tests.step);
    test_step.dependOn(&run_gossipsub_interop_unit_tests.step);

    // Multi-executor integration tests for the Layer-0 IO primitives. Runs on a
    // real zio.Runtime (>=2 executors), unlike the in-file std.Io.Threaded unit
    // tests. BoringSSL-free: the module imports only `zio` + the io primitives
    // (reached via relative @import from src/quic/zio_io_tests.zig), so it builds
    // fast and does not pull the quiche/BoringSSL dependency.
    const zio_io_test_module = b.createModule(.{
        .root_source_file = b.path("src/quic/zio_io_tests.zig"),
        .target = target,
        .optimize = optimize,
    });
    zio_io_test_module.addImport("zio", zio_dep.module("zio"));
    zio_io_test_module.addImport("ref_count", ref_count_mod);
    const zio_io_unit_tests = b.addTest(.{
        .root_module = zio_io_test_module,
        .filters = filters orelse &.{},
    });
    const run_zio_io_unit_tests = b.addRunArtifact(zio_io_unit_tests);
    const zio_io_test_step = b.step("zio-io-test", "Run zio multi-executor IO-primitive integration tests");
    zio_io_test_step.dependOn(&run_zio_io_unit_tests.step);

    // End-to-end multi-executor integration tests (Layer 2-6 net): two QUIC
    // endpoints over loopback on a real zio.Runtime, server accept on a fiber.
    // Pulls the full deps incl. BoringSSL (reaches internals via relative import
    // of endpoint/test_support.zig, NOT via "zig-libp2p", to avoid a duplicate
    // module instance of the quic internals).
    // Rooted at src/ (not src/quic/) so the transitive internal imports that
    // escape upward (e.g. quic/endpoint/handle.zig -> ../../security/tls.zig)
    // stay within the module root path.
    const zio_integ_test_module = b.createModule(.{
        .root_source_file = b.path("src/zio_integration_tests.zig"),
        .target = target,
        .optimize = optimize,
    });
    addImports(zio_integ_test_module, deps);
    zio_integ_test_module.addImport("zio", zio_dep.module("zio"));
    // Focus this target on the end-to-end multi-executor tests: the loopback
    // handshake/echo tests and the connection-teardown regression tests (the
    // transitively reachable std.Io.Threaded unit tests are covered by
    // `zig build test`).
    const zio_integ_unit_tests = b.addTest(.{
        .root_module = zio_integ_test_module,
        .filters = filters orelse &.{ "loopback", "teardown" },
    });
    const run_zio_integ_unit_tests = b.addRunArtifact(zio_integ_unit_tests);
    const zio_integ_test_step = b.step("zio-integ-test", "Run zio multi-executor end-to-end integration tests");
    zio_integ_test_step.dependOn(&run_zio_integ_unit_tests.step);
}

const ModuleDeps = struct {
    build_options: *std.Build.Step.Options,
    ref_count: *std.Build.Module,
    multiaddr: *std.Build.Module,
    gremlin: *std.Build.Module,
    peer_id: *std.Build.Module,
    secp256k1: *std.Build.Module,
    quiche: *std.Build.Module,
    ssl: *std.Build.Module,
};

fn addImports(module: *std.Build.Module, deps: ModuleDeps) void {
    module.addOptions("build_options", deps.build_options);
    module.addImport("ref_count", deps.ref_count);
    module.addImport("multiaddr", deps.multiaddr);
    module.addImport("gremlin", deps.gremlin);
    module.addImport("peer_id", deps.peer_id);
    module.addImport("secp256k1", deps.secp256k1);
    module.addImport("quiche", deps.quiche);
    module.addImport("ssl", deps.ssl);
}
