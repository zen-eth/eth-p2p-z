const proto_binding = @import("../multistream/protocol_binding.zig");
const ProtocolDescriptor = @import("../multistream/protocol_descriptor.zig").ProtocolDescriptor;
const ProtocolMatcher = @import("../multistream/protocol_matcher.zig").ProtocolMatcher;
const ProtocolId = @import("../protocol_id.zig").ProtocolId;
const Allocactor = @import("std").mem.Allocator;
const p2p_conn = @import("../conn.zig");

pub const PlainSecureChannel = struct {
    local_key: []const u8,

    protocol_descriptor: ProtocolDescriptor,

    const Self = @This();

    pub fn init(self: *Self, local_key: []const u8, allocator: Allocactor) !void {
        var proto_desc: ProtocolDescriptor = undefined;
        const protocol_id: ProtocolId = "/plaintext/2.0.0";
        const announcements: []const ProtocolId = &[_]ProtocolId{
            protocol_id,
        };
        var proto_matcher: ProtocolMatcher = undefined;
        try proto_matcher.initAsStrict(allocator, protocol_id);
        errdefer proto_matcher.deinit();
        try proto_desc.init(allocator, announcements, proto_matcher);
        errdefer proto_desc.deinit();

        const duped_local_key = try allocator.dupe(u8, local_key);
        errdefer allocator.free(duped_local_key);

        self.* = PlainSecureChannel{
            .local_key = duped_local_key,
            .protocol_descriptor = proto_desc,
        };
    }

    pub fn deinit(self: *Self, allocator: Allocactor) void {
        allocator.free(self.local_key);
        self.protocol_descriptor.deinit();
    }

    // --- Actual Implementations ---
    pub fn getProtocolDescriptor(self: *Self) *ProtocolDescriptor {
        return &self.protocol_descriptor;
    }

    pub fn initConn(
        _: *Self,
        _: p2p_conn.AnyRxConn,
        _: ProtocolId,
        _: ?*anyopaque,
        _: *const fn (ud: ?*anyopaque, r: anyerror!*anyopaque) void,
    ) void {}

    // --- Static Wrapper Functions ---
    pub fn vtableGetProtocolDescriptionFn(instance: *anyopaque) *ProtocolDescriptor {
        const self: *Self = @ptrCast(@alignCast(instance));
        return self.getProtocolDescriptor();
    }

    pub fn vtableInitConnFn(
        instance: *anyopaque,
        conn: p2p_conn.AnyRxConn,
        protocol_id: ProtocolId,
        user_data: ?*anyopaque,
        callback: *const fn (ud: ?*anyopaque, r: anyerror!*anyopaque) void,
    ) void {
        const self: *Self = @ptrCast(@alignCast(instance));
        self.initConn(conn, protocol_id, user_data, callback);
    }

    // --- Static VTable Instance ---
    const vtable_instance = proto_binding.ProtocolBindingVTable{
        .initConnFn = vtableInitConnFn,
        .protoDescFn = vtableGetProtocolDescriptionFn,
    };

    pub fn any(self: *Self) proto_binding.AnyProtocolBinding {
        return .{ .instance = self, .vtable = &vtable_instance };
    }
};

// class PlaintextInsecureChannel(private val localKey: PrivKey) : SecureChannel {

//     @Suppress("UNUSED_PARAMETER")
//     constructor(localKey: PrivKey, muxerProtocols: List<StreamMuxer>) : this(localKey)

//     override val protocolDescriptor = ProtocolDescriptor("/plaintext/2.0.0")

//     override fun initChannel(ch: P2PChannel, selectedProtocol: String): CompletableFuture<out SecureChannel.Session> {
//         val handshakeCompleted = CompletableFuture<SecureChannel.Session>()

//         val handshaker = PlaintextHandshakeHandler(handshakeCompleted, localKey)
//         listOf(
//             LengthFieldPrepender(4),
//             LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4),
//             handshaker
//         ).forEach { ch.pushHandler(it) }

//         return handshakeCompleted
//     }
// } // PlaintextInsecureChannel

// class PlaintextHandshakeHandler(
//     private val handshakeCompleted: CompletableFuture<SecureChannel.Session>,
//     localKey: PrivKey
// ) : SimpleChannelInboundHandler<ByteBuf>() {
//     private val localPubKey = localKey.publicKey()
//     private val localPeerId = PeerId.fromPubKey(localPubKey)
//     private lateinit var remotePubKey: PubKey
//     private lateinit var remotePeerId: PeerId

//     private var active = false
//     private var read = false

//     override fun channelActive(ctx: ChannelHandlerContext) {
//         if (active) return

//         active = true

//         val pubKeyMsg = Crypto.PublicKey.newBuilder()
//             .setType(localPubKey.keyType)
//             .setData(ByteString.copyFrom(localPubKey.raw()))
//             .build()

//         val exchangeMsg = Plaintext.Exchange.newBuilder()
//             .setId(localPeerId.bytes.toProtobuf())
//             .setPubkey(pubKeyMsg)
//             .build()

//         val byteBuf = Unpooled.buffer().writeBytes(exchangeMsg.toByteArray())
//         ctx.writeAndFlush(byteBuf)

//         handshakeCompleted(ctx)
//     } // channelActive

//     override fun channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) {
//         if (read) return

//         read = true
//         val exchangeRecv = Plaintext.Exchange.parser().parseFrom(msg.nioBuffer())
//             ?: throw InvalidInitialPacket()

//         if (!exchangeRecv.hasPubkey()) {
//             throw InvalidRemotePubKey()
//         }

//         remotePeerId = PeerId(exchangeRecv.id.toByteArray())
//         remotePubKey = unmarshalPublicKey(exchangeRecv.pubkey.toByteArray())
//         val calculatedPeerId = PeerId.fromPubKey(remotePubKey)
//         if (remotePeerId != calculatedPeerId) {
//             throw InvalidRemotePubKey()
//         }

//         handshakeCompleted(ctx)
//     } // channelRead0

//     override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
//         handshakeFailed(cause)
//         ctx.channel().close()
//     } // exceptionCaught

//     override fun channelUnregistered(ctx: ChannelHandlerContext) {
//         handshakeFailed(ConnectionClosedException("Connection was closed ${ctx.channel()}"))
//         super.channelUnregistered(ctx)
//     } // channelUnregistered

//     private fun handshakeCompleted(ctx: ChannelHandlerContext) {
//         if (!active || !read) return

//         val session = SecureChannel.Session(
//             localPeerId,
//             remotePeerId,
//             remotePubKey,
//             null
//         )

//         handshakeCompleted.complete(session)
//         ctx.pipeline().remove(this)
//         ctx.fireChannelActive()
//     } // handshakeComplete

//     private fun handshakeFailed(cause: Throwable) {
//         handshakeCompleted.completeExceptionally(cause)
//     } // handshakeFailed
// } // PlaintextHandshakeHandler
