const std = @import("std");
const ProtoGenStep = @import("gremlin").ProtoGenStep;

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const secp_dep = b.dependency("secp256k1", .{
        .target = target,
        .optimize = optimize,
    });
    const secp_module = secp_dep.module("secp256k1");

    // NOTE: lsquic dependency deferred until QUIC transport (Phase 2)
    // const lsquic_dep = b.dependency("lsquic", .{
    //     .target = target,
    //     .optimize = optimize,
    // });
    // NOTE: lsquic artifact and include path deferred until QUIC transport (Phase 2)
    // const lsquic_artifact = lsquic_dep.artifact("lsquic");
    // root_module.addIncludePath(lsquic_dep.path("include"));

    const multiaddr_dep = b.dependency("multiaddr", .{
        .target = target,
        .optimize = optimize,
    });
    const multiaddr_module = multiaddr_dep.module("multiaddr");
    const peer_id_dep = multiaddr_dep.builder.dependency("peer_id", .{
        .target = target,
        .optimize = optimize,
    });
    const peer_id_module = peer_id_dep.module("peer-id");

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

    const root_module = b.addModule("zig-libp2p", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    root_module.addImport("secp256k1", secp_module);
    // root_module.addImport("ssl", ssl_module);  // deferred until identity.zig is ported
    root_module.addImport("multiaddr", multiaddr_module);
    root_module.addImport("peer_id", peer_id_module);
    root_module.addImport("gremlin", gremlin_module);

    const libp2p_lib = b.addLibrary(.{
        .name = "zig-libp2p",
        .root_module = root_module,
        .linkage = .static,
    });
    // NOTE: lsquic/zlib linking deferred until QUIC transport is implemented (Phase 2)
    // libp2p_lib.root_module.linkLibrary(lsquic_artifact);
    // const zlib_system_name = switch (target.result.os.tag) {
    //     .windows => "zlib1",
    //     else => "z",
    // };
    // libp2p_lib.root_module.linkSystemLibrary(zlib_system_name, .{});
    b.installArtifact(libp2p_lib);

    const filters = b.option([]const []const u8, "filter", "filter based on name");

    const libp2p_lib_unit_tests = b.addTest(.{
        .root_module = root_module,
        .filters = filters orelse &.{},
    });
    // NOTE: lsquic/zlib linking deferred until QUIC transport is implemented (Phase 2)
    // libp2p_lib_unit_tests.root_module.linkLibrary(lsquic_artifact);
    // libp2p_lib_unit_tests.root_module.linkSystemLibrary(zlib_system_name, .{});
    libp2p_lib_unit_tests.step.dependOn(&protobuf.step);
    const run_libp2p_lib_unit_tests = b.addRunArtifact(libp2p_lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_libp2p_lib_unit_tests.step);
}
