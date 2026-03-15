pub const multiaddr = @import("multiaddr.zig");
pub const Multiaddr = multiaddr.Multiaddr;
pub const Protocol = multiaddr.Protocol;
pub const Ip4Addr = multiaddr.Ip4Addr;
pub const Ip6Addr = multiaddr.Ip6Addr;
pub const ProtocolIterator = multiaddr.ProtocolIterator;
pub const ProtocolStackIterator = multiaddr.ProtocolStackIterator;
pub const Onion3Addr = multiaddr.Onion3Addr;
pub const FromUrlError = multiaddr.FromUrlError;

test {
    @import("std").testing.refAllDeclsRecursive(@This());
}
