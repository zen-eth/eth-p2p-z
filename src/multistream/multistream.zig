const std = @import("std");
const proto_binding = @import("protocol_binding.zig");
const ArrayList = std.ArrayList;
const p2p_conn = @import("../conn.zig");


pub const Multistream = struct {
    bindings: ArrayList(proto_binding.AnyProtocolBinding),
    negotiation_time_limit: u64,


    const Self = @This();

    pub fn initConn(_: *Self, _: p2p_conn.AnyRxConn, _: ?*anyopaque, _: *const fn (ud: ?*anyopaque, r: anyerror!*anyopaque) void) void {
        // const handler: p2p_conn.AnyHandler = if (conn.direction() == p2p_conn.Direction.OUTBOUND) {

        // } else {

        // };

        // conn.getPipeline().addLast(name: []const u8, handler: AnyHandler)
    }
};
 


// package io.libp2p.multistream

// import io.libp2p.core.P2PChannel
// import io.libp2p.core.P2PChannelHandler
// import io.libp2p.core.multistream.Multistream
// import io.libp2p.core.multistream.ProtocolBinding
// import java.time.Duration
// import java.util.concurrent.CompletableFuture

// class MultistreamImpl<TController>(
//     override val bindings: List<ProtocolBinding<TController>>,
//     val preHandler: P2PChannelHandler<*>? = null,
//     val postHandler: P2PChannelHandler<*>? = null,
//     val negotiationTimeLimit: Duration = DEFAULT_NEGOTIATION_TIME_LIMIT
// ) : Multistream<TController> {

//     override fun initChannel(ch: P2PChannel): CompletableFuture<TController> {
//         return with(ch) {
//             preHandler?.also {
//                 it.initChannel(ch)
//             }
//             pushHandler(
//                 if (ch.isInitiator) {
//                     Negotiator.createRequesterInitializer(
//                         negotiationTimeLimit,
//                         *bindings.flatMap { it.protocolDescriptor.announceProtocols }.toTypedArray()
//                     )
//                 } else {
//                     Negotiator.createResponderInitializer(
//                         negotiationTimeLimit,
//                         bindings.map { it.protocolDescriptor.protocolMatcher }
//                     )
//                 }
//             )
//             postHandler?.also {
//                 it.initChannel(ch)
//             }
//             val protocolSelect = ProtocolSelect(bindings)
//             pushHandler(protocolSelect)
//             protocolSelect.selectedFuture
//         }
//     }
// }
