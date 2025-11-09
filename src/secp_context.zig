const std = @import("std");
const secp = @import("secp256k1");

var global_ctx: ?secp.Secp256k1 = null;
var once = std.once(init);

fn init() void {
    global_ctx = secp.Secp256k1.genNew();
}

pub fn get() *secp.Secp256k1 {
    once.call();
    return &global_ctx.?;
}

pub fn deinit() void {
    if (global_ctx) |*ctx| {
        ctx.deinit();
        global_ctx = null;
    }
}
