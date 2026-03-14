const std = @import("std");
const secp = @import("secp256k1");

var global_ctx: ?secp.Secp256k1 = null;
var initialized: bool = false;

pub fn get() *secp.Secp256k1 {
    if (!initialized) {
        global_ctx = secp.Secp256k1.genNew();
        initialized = true;
    }
    return &global_ctx.?;
}

pub fn deinit() void {
    if (global_ctx) |*ctx| {
        ctx.deinit();
        global_ctx = null;
        initialized = false;
    }
}
