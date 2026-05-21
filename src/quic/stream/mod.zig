const handle_mod = @import("handle.zig");

pub const Stream = handle_mod.Stream;
pub const create = handle_mod.create;

pub const ReadError = Stream.ReadError;
pub const WriteError = Stream.WriteError;
pub const ShutdownWriteError = Stream.ShutdownWriteError;
pub const CloseError = Stream.CloseError;
pub const CloseReadError = Stream.CloseReadError;
pub const ResetError = Stream.ResetError;
pub const ReadOptions = Stream.ReadOptions;
pub const WriteOptions = Stream.WriteOptions;

test {
    _ = @import("byte_queue.zig");
    _ = @import("shared_state.zig");
    _ = @import("record.zig");
    _ = @import("handle.zig");
}
