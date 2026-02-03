const std = @import("std");
const gremlin = @import("gremlin");

// structs
const IdentifyWire = struct {
    const PROTOCOL_VERSION_WIRE: gremlin.ProtoWireNumber = 5;
    const AGENT_VERSION_WIRE: gremlin.ProtoWireNumber = 6;
    const PUBLIC_KEY_WIRE: gremlin.ProtoWireNumber = 1;
    const LISTEN_ADDRS_WIRE: gremlin.ProtoWireNumber = 2;
    const OBSERVED_ADDR_WIRE: gremlin.ProtoWireNumber = 4;
    const PROTOCOLS_WIRE: gremlin.ProtoWireNumber = 3;
};

pub const Identify = struct {
    // fields
    protocol_version: ?[]const u8 = null,
    agent_version: ?[]const u8 = null,
    public_key: ?[]const u8 = null,
    listen_addrs: ?[]const ?[]const u8 = null,
    observed_addr: ?[]const u8 = null,
    protocols: ?[]const ?[]const u8 = null,

    pub fn calcProtobufSize(self: *const Identify) usize {
        var res: usize = 0;
        if (self.protocol_version) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(IdentifyWire.PROTOCOL_VERSION_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.agent_version) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(IdentifyWire.AGENT_VERSION_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.public_key) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(IdentifyWire.PUBLIC_KEY_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.listen_addrs) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(IdentifyWire.LISTEN_ADDRS_WIRE);
                if (maybe_v) |v| {
                    res += gremlin.sizes.sizeUsize(v.len) + v.len;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        if (self.observed_addr) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(IdentifyWire.OBSERVED_ADDR_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.protocols) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(IdentifyWire.PROTOCOLS_WIRE);
                if (maybe_v) |v| {
                    res += gremlin.sizes.sizeUsize(v.len) + v.len;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        return res;
    }

    pub fn encode(self: *const Identify, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const Identify, target: *gremlin.Writer) void {
        if (self.protocol_version) |v| {
            if (v.len > 0) {
                target.appendBytes(IdentifyWire.PROTOCOL_VERSION_WIRE, v);
            }
        }
        if (self.agent_version) |v| {
            if (v.len > 0) {
                target.appendBytes(IdentifyWire.AGENT_VERSION_WIRE, v);
            }
        }
        if (self.public_key) |v| {
            if (v.len > 0) {
                target.appendBytes(IdentifyWire.PUBLIC_KEY_WIRE, v);
            }
        }
        if (self.listen_addrs) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    target.appendBytes(IdentifyWire.LISTEN_ADDRS_WIRE, v);
                } else {
                    target.appendBytesTag(IdentifyWire.LISTEN_ADDRS_WIRE, 0);
                }
            }
        }
        if (self.observed_addr) |v| {
            if (v.len > 0) {
                target.appendBytes(IdentifyWire.OBSERVED_ADDR_WIRE, v);
            }
        }
        if (self.protocols) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    target.appendBytes(IdentifyWire.PROTOCOLS_WIRE, v);
                } else {
                    target.appendBytesTag(IdentifyWire.PROTOCOLS_WIRE, 0);
                }
            }
        }
    }
};

pub const IdentifyReader = struct {
    allocator: std.mem.Allocator,
    buf: gremlin.Reader,
    _protocol_version: ?[]const u8 = null,
    _agent_version: ?[]const u8 = null,
    _public_key: ?[]const u8 = null,
    _listen_addrs: ?std.ArrayList([]const u8) = null,
    _observed_addr: ?[]const u8 = null,
    _protocols: ?std.ArrayList([]const u8) = null,

    pub fn init(allocator: std.mem.Allocator, src: []const u8) gremlin.Error!IdentifyReader {
        var buf = gremlin.Reader.init(src);
        var res = IdentifyReader{.allocator = allocator, .buf = buf};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                IdentifyWire.PROTOCOL_VERSION_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._protocol_version = result.value;
                },
                IdentifyWire.AGENT_VERSION_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._agent_version = result.value;
                },
                IdentifyWire.PUBLIC_KEY_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._public_key = result.value;
                },
                IdentifyWire.LISTEN_ADDRS_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._listen_addrs == null) {
                        res._listen_addrs = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._listen_addrs.?.append(result.value);
                },
                IdentifyWire.OBSERVED_ADDR_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._observed_addr = result.value;
                },
                IdentifyWire.PROTOCOLS_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._protocols == null) {
                        res._protocols = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._protocols.?.append(result.value);
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(self: *const IdentifyReader) void {
        if (self._listen_addrs) |arr| {
            arr.deinit();
        }
        if (self._protocols) |arr| {
            arr.deinit();
        }
    }
    pub inline fn getProtocolVersion(self: *const IdentifyReader) []const u8 { return self._protocol_version orelse &[_]u8{}; }
    pub inline fn getAgentVersion(self: *const IdentifyReader) []const u8 { return self._agent_version orelse &[_]u8{}; }
    pub inline fn getPublicKey(self: *const IdentifyReader) []const u8 { return self._public_key orelse &[_]u8{}; }
    pub fn getListenAddrs(self: *const IdentifyReader) []const []const u8 {
        if (self._listen_addrs) |arr| {
            return arr.items;
        }
        return &[_][]u8{};
    }
    pub inline fn getObservedAddr(self: *const IdentifyReader) []const u8 { return self._observed_addr orelse &[_]u8{}; }
    pub fn getProtocols(self: *const IdentifyReader) []const []const u8 {
        if (self._protocols) |arr| {
            return arr.items;
        }
        return &[_][]u8{};
    }
};

