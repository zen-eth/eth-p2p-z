const std = @import("std");
const build_options = @import("build_options");
const libxev = @import("xev");

fn selectBackend() libxev.Backend {
    if (build_options.libxev_backend) |name| {
        if (std.meta.stringToEnum(libxev.Backend, name)) |tag| {
            return tag;
        }
        const msg = std.fmt.comptimePrint(
            "Unsupported libxev backend: '{s}'. Expected one of: io_uring, epoll, kqueue, wasi_poll, iocp.",
            .{name},
        );
        @compileError(msg);
    }

    return libxev.Backend.default();
}

pub const backend = selectBackend();
pub const xev = backend.Api();
