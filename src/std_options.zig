const std = @import("std");
const build_options = @import("build_options");

fn resolveLogLevel() std.log.Level {
    return comptime blk: {
        if (build_options.log_level) |lvl| {
            if (std.meta.stringToEnum(std.log.Level, lvl)) |parsed| {
                break :blk parsed;
            }
            @compileError(std.fmt.comptimePrint("unsupported log level '{s}'", .{lvl}));
        }
        break :blk .info;
    };
}

pub const options: std.Options = .{
    .log_level = resolveLogLevel(),
};
