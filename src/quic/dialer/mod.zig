//! Dialer module re-exports. Implementation lives in `dial.zig`; this file is
//! the public surface used by `endpoint/handle.zig`.

const dial_mod = @import("dial.zig");

pub const Context = dial_mod.Context;
pub const DialOptions = dial_mod.DialOptions;
pub const DialError = dial_mod.DialError;

pub const dial = dial_mod.dial;

test {
    _ = @import("dial_tests.zig");
}
