//! Router module re-exports. Implementation lives in `loop.zig`; this file is
//! the public surface used by `endpoint/handle.zig` and the cross-module
//! `dialer/dial.zig`.

const loop = @import("loop.zig");

pub const Context = loop.Context;
pub const AcceptChannel = loop.AcceptChannel;
pub const AcceptError = loop.AcceptError;
pub const CidMap = loop.CidMap;
pub const ListenError = loop.ListenError;
pub const RouteRegistrar = loop.RouteRegistrar;
pub const RouterLoopError = loop.RouterLoopError;

pub const bind = loop.bind;
pub const accept = loop.accept;
pub const stopAccepting = loop.stopAccepting;
pub const closeListener = loop.closeListener;
pub const localAddr = loop.localAddr;
pub const mapRoute = loop.mapRoute;

test {
    _ = @import("route_table.zig");
    _ = @import("loop_tests.zig");
}
