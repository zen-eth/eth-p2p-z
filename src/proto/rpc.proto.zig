const std = @import("std");
const gremlin = @import("gremlin");

// structs
const RPCWire = struct {
    const SUBSCRIPTIONS_WIRE: gremlin.ProtoWireNumber = 1;
    const PUBLISH_WIRE: gremlin.ProtoWireNumber = 2;
    const CONTROL_WIRE: gremlin.ProtoWireNumber = 3;
};

pub const RPC = struct {
    // nested structs
    const SubOptsWire = struct {
        const SUBSCRIBE_WIRE: gremlin.ProtoWireNumber = 1;
        const TOPICID_WIRE: gremlin.ProtoWireNumber = 2;
    };
    
    pub const SubOpts = struct {
        // fields
        subscribe: bool = false,
        topicid: ?[]const u8 = null,

        pub fn calcProtobufSize(self: *const SubOpts) usize {
            var res: usize = 0;
            if (self.subscribe != false) { res += gremlin.sizes.sizeWireNumber(RPC.SubOptsWire.SUBSCRIBE_WIRE) + gremlin.sizes.sizeBool(self.subscribe); }
            if (self.topicid) |v| {
                if (v.len > 0) {
                    res += gremlin.sizes.sizeWireNumber(RPC.SubOptsWire.TOPICID_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
                }
            }
            return res;
        }

        pub fn encode(self: *const SubOpts, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
            const size = self.calcProtobufSize();
            if (size == 0) {
                return &[_]u8{};
            }
            const buf = try allocator.alloc(u8, self.calcProtobufSize());
            var writer = gremlin.Writer.init(buf);
            self.encodeTo(&writer);
            return buf;
        }


        pub fn encodeTo(self: *const SubOpts, target: *gremlin.Writer) void {
            if (self.subscribe != false) { target.appendBool(RPC.SubOptsWire.SUBSCRIBE_WIRE, self.subscribe); }
            if (self.topicid) |v| {
                if (v.len > 0) {
                    target.appendBytes(RPC.SubOptsWire.TOPICID_WIRE, v);
                }
            }
        }
    };
    
    pub const SubOptsReader = struct {
        _subscribe: bool = false,
        _topicid: ?[]const u8 = null,

        pub fn init(_: std.mem.Allocator, src: []const u8) gremlin.Error!SubOptsReader {
            var buf = gremlin.Reader.init(src);
            var res = SubOptsReader{};
            if (buf.buf.len == 0) {
                return res;
            }
            var offset: usize = 0;
            while (buf.hasNext(offset, 0)) {
                const tag = try buf.readTagAt(offset);
                offset += tag.size;
                switch (tag.number) {
                    RPC.SubOptsWire.SUBSCRIBE_WIRE => {
                      const result = try buf.readBool(offset);
                      offset += result.size;
                      res._subscribe = result.value;
                    },
                    RPC.SubOptsWire.TOPICID_WIRE => {
                      const result = try buf.readBytes(offset);
                      offset += result.size;
                      res._topicid = result.value;
                    },
                    else => {
                        offset = try buf.skipData(offset, tag.wire);
                    }
                }
            }
            return res;
        }
        pub fn deinit(_: *const SubOptsReader) void { }
        
        pub inline fn getSubscribe(self: *const SubOptsReader) bool { return self._subscribe; }
        pub inline fn getTopicid(self: *const SubOptsReader) []const u8 { return self._topicid orelse &[_]u8{}; }
    };
    
    // fields
    subscriptions: ?[]const ?RPC.SubOpts = null,
    publish: ?[]const ?Message = null,
    control: ?ControlMessage = null,

    pub fn calcProtobufSize(self: *const RPC) usize {
        var res: usize = 0;
        if (self.subscriptions) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(RPCWire.SUBSCRIPTIONS_WIRE);
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    res += gremlin.sizes.sizeUsize(size) + size;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        if (self.publish) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(RPCWire.PUBLISH_WIRE);
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    res += gremlin.sizes.sizeUsize(size) + size;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        if (self.control) |v| {
            const size = v.calcProtobufSize();
            if (size > 0) {
                res += gremlin.sizes.sizeWireNumber(RPCWire.CONTROL_WIRE) + gremlin.sizes.sizeUsize(size) + size;
            }
        }
        return res;
    }

    pub fn encode(self: *const RPC, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const RPC, target: *gremlin.Writer) void {
        if (self.subscriptions) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    target.appendBytesTag(RPCWire.SUBSCRIPTIONS_WIRE, size);
                    v.encodeTo(target);
                } else {
                    target.appendBytesTag(RPCWire.SUBSCRIPTIONS_WIRE, 0);
                }
            }
        }
        if (self.publish) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    target.appendBytesTag(RPCWire.PUBLISH_WIRE, size);
                    v.encodeTo(target);
                } else {
                    target.appendBytesTag(RPCWire.PUBLISH_WIRE, 0);
                }
            }
        }
        if (self.control) |v| {
            const size = v.calcProtobufSize();
            if (size > 0) {
                target.appendBytesTag(RPCWire.CONTROL_WIRE, size);
                v.encodeTo(target);
            }
        }
    }
};

pub const RPCReader = struct {
    allocator: std.mem.Allocator,
    buf: gremlin.Reader,
    _subscriptions_bufs: ?std.ArrayList([]const u8) = null,
    _publish_bufs: ?std.ArrayList([]const u8) = null,
    _control_buf: ?[]const u8 = null,

    pub fn init(allocator: std.mem.Allocator, src: []const u8) gremlin.Error!RPCReader {
        var buf = gremlin.Reader.init(src);
        var res = RPCReader{.allocator = allocator, .buf = buf};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                RPCWire.SUBSCRIPTIONS_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._subscriptions_bufs == null) {
                        res._subscriptions_bufs = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._subscriptions_bufs.?.append(result.value);
                },
                RPCWire.PUBLISH_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._publish_bufs == null) {
                        res._publish_bufs = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._publish_bufs.?.append(result.value);
                },
                RPCWire.CONTROL_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._control_buf = result.value;
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(self: *const RPCReader) void {
        if (self._subscriptions_bufs) |arr| {
            arr.deinit();
        }
        if (self._publish_bufs) |arr| {
            arr.deinit();
        }
    }
    pub fn getSubscriptions(self: *const RPCReader, allocator: std.mem.Allocator) gremlin.Error![]RPC.SubOptsReader {
        if (self._subscriptions_bufs) |bufs| {
            var result = try std.ArrayList(RPC.SubOptsReader).initCapacity(allocator, bufs.items.len);
            for (bufs.items) |buf| {
                try result.append(try RPC.SubOptsReader.init(allocator, buf));
            }
            return result.toOwnedSlice();
        }
        return &[_]RPC.SubOptsReader{};
    }
    pub fn getPublish(self: *const RPCReader, allocator: std.mem.Allocator) gremlin.Error![]MessageReader {
        if (self._publish_bufs) |bufs| {
            var result = try std.ArrayList(MessageReader).initCapacity(allocator, bufs.items.len);
            for (bufs.items) |buf| {
                try result.append(try MessageReader.init(allocator, buf));
            }
            return result.toOwnedSlice();
        }
        return &[_]MessageReader{};
    }
    pub fn getControl(self: *const RPCReader, allocator: std.mem.Allocator) gremlin.Error!ControlMessageReader {
        if (self._control_buf) |buf| {
            return try ControlMessageReader.init(allocator, buf);
        }
        return try ControlMessageReader.init(allocator, &[_]u8{});
    }
};

const MessageWire = struct {
    const FROM_WIRE: gremlin.ProtoWireNumber = 1;
    const DATA_WIRE: gremlin.ProtoWireNumber = 2;
    const SEQNO_WIRE: gremlin.ProtoWireNumber = 3;
    const TOPIC_WIRE: gremlin.ProtoWireNumber = 4;
    const SIGNATURE_WIRE: gremlin.ProtoWireNumber = 5;
    const KEY_WIRE: gremlin.ProtoWireNumber = 6;
};

pub const Message = struct {
    // fields
    from: ?[]const u8 = null,
    data: ?[]const u8 = null,
    seqno: ?[]const u8 = null,
    topic: ?[]const u8 = null,
    signature: ?[]const u8 = null,
    key: ?[]const u8 = null,

    pub fn calcProtobufSize(self: *const Message) usize {
        var res: usize = 0;
        if (self.from) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(MessageWire.FROM_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.data) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(MessageWire.DATA_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.seqno) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(MessageWire.SEQNO_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.topic) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(MessageWire.TOPIC_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.signature) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(MessageWire.SIGNATURE_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.key) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(MessageWire.KEY_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        return res;
    }

    pub fn encode(self: *const Message, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const Message, target: *gremlin.Writer) void {
        if (self.from) |v| {
            if (v.len > 0) {
                target.appendBytes(MessageWire.FROM_WIRE, v);
            }
        }
        if (self.data) |v| {
            if (v.len > 0) {
                target.appendBytes(MessageWire.DATA_WIRE, v);
            }
        }
        if (self.seqno) |v| {
            if (v.len > 0) {
                target.appendBytes(MessageWire.SEQNO_WIRE, v);
            }
        }
        if (self.topic) |v| {
            if (v.len > 0) {
                target.appendBytes(MessageWire.TOPIC_WIRE, v);
            }
        }
        if (self.signature) |v| {
            if (v.len > 0) {
                target.appendBytes(MessageWire.SIGNATURE_WIRE, v);
            }
        }
        if (self.key) |v| {
            if (v.len > 0) {
                target.appendBytes(MessageWire.KEY_WIRE, v);
            }
        }
    }
};

pub const MessageReader = struct {
    _from: ?[]const u8 = null,
    _data: ?[]const u8 = null,
    _seqno: ?[]const u8 = null,
    _topic: ?[]const u8 = null,
    _signature: ?[]const u8 = null,
    _key: ?[]const u8 = null,

    pub fn init(_: std.mem.Allocator, src: []const u8) gremlin.Error!MessageReader {
        var buf = gremlin.Reader.init(src);
        var res = MessageReader{};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                MessageWire.FROM_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._from = result.value;
                },
                MessageWire.DATA_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._data = result.value;
                },
                MessageWire.SEQNO_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._seqno = result.value;
                },
                MessageWire.TOPIC_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._topic = result.value;
                },
                MessageWire.SIGNATURE_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._signature = result.value;
                },
                MessageWire.KEY_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._key = result.value;
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(_: *const MessageReader) void { }
    
    pub inline fn getFrom(self: *const MessageReader) []const u8 { return self._from orelse &[_]u8{}; }
    pub inline fn getData(self: *const MessageReader) []const u8 { return self._data orelse &[_]u8{}; }
    pub inline fn getSeqno(self: *const MessageReader) []const u8 { return self._seqno orelse &[_]u8{}; }
    pub inline fn getTopic(self: *const MessageReader) []const u8 { return self._topic orelse &[_]u8{}; }
    pub inline fn getSignature(self: *const MessageReader) []const u8 { return self._signature orelse &[_]u8{}; }
    pub inline fn getKey(self: *const MessageReader) []const u8 { return self._key orelse &[_]u8{}; }
};

const ControlMessageWire = struct {
    const IHAVE_WIRE: gremlin.ProtoWireNumber = 1;
    const IWANT_WIRE: gremlin.ProtoWireNumber = 2;
    const GRAFT_WIRE: gremlin.ProtoWireNumber = 3;
    const PRUNE_WIRE: gremlin.ProtoWireNumber = 4;
    const IDONTWANT_WIRE: gremlin.ProtoWireNumber = 5;
};

pub const ControlMessage = struct {
    // fields
    ihave: ?[]const ?ControlIHave = null,
    iwant: ?[]const ?ControlIWant = null,
    graft: ?[]const ?ControlGraft = null,
    prune: ?[]const ?ControlPrune = null,
    idontwant: ?[]const ?ControlIDontWant = null,

    pub fn calcProtobufSize(self: *const ControlMessage) usize {
        var res: usize = 0;
        if (self.ihave) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(ControlMessageWire.IHAVE_WIRE);
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    res += gremlin.sizes.sizeUsize(size) + size;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        if (self.iwant) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(ControlMessageWire.IWANT_WIRE);
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    res += gremlin.sizes.sizeUsize(size) + size;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        if (self.graft) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(ControlMessageWire.GRAFT_WIRE);
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    res += gremlin.sizes.sizeUsize(size) + size;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        if (self.prune) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(ControlMessageWire.PRUNE_WIRE);
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    res += gremlin.sizes.sizeUsize(size) + size;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        if (self.idontwant) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(ControlMessageWire.IDONTWANT_WIRE);
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    res += gremlin.sizes.sizeUsize(size) + size;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        return res;
    }

    pub fn encode(self: *const ControlMessage, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const ControlMessage, target: *gremlin.Writer) void {
        if (self.ihave) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    target.appendBytesTag(ControlMessageWire.IHAVE_WIRE, size);
                    v.encodeTo(target);
                } else {
                    target.appendBytesTag(ControlMessageWire.IHAVE_WIRE, 0);
                }
            }
        }
        if (self.iwant) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    target.appendBytesTag(ControlMessageWire.IWANT_WIRE, size);
                    v.encodeTo(target);
                } else {
                    target.appendBytesTag(ControlMessageWire.IWANT_WIRE, 0);
                }
            }
        }
        if (self.graft) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    target.appendBytesTag(ControlMessageWire.GRAFT_WIRE, size);
                    v.encodeTo(target);
                } else {
                    target.appendBytesTag(ControlMessageWire.GRAFT_WIRE, 0);
                }
            }
        }
        if (self.prune) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    target.appendBytesTag(ControlMessageWire.PRUNE_WIRE, size);
                    v.encodeTo(target);
                } else {
                    target.appendBytesTag(ControlMessageWire.PRUNE_WIRE, 0);
                }
            }
        }
        if (self.idontwant) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    target.appendBytesTag(ControlMessageWire.IDONTWANT_WIRE, size);
                    v.encodeTo(target);
                } else {
                    target.appendBytesTag(ControlMessageWire.IDONTWANT_WIRE, 0);
                }
            }
        }
    }
};

pub const ControlMessageReader = struct {
    allocator: std.mem.Allocator,
    buf: gremlin.Reader,
    _ihave_bufs: ?std.ArrayList([]const u8) = null,
    _iwant_bufs: ?std.ArrayList([]const u8) = null,
    _graft_bufs: ?std.ArrayList([]const u8) = null,
    _prune_bufs: ?std.ArrayList([]const u8) = null,
    _idontwant_bufs: ?std.ArrayList([]const u8) = null,

    pub fn init(allocator: std.mem.Allocator, src: []const u8) gremlin.Error!ControlMessageReader {
        var buf = gremlin.Reader.init(src);
        var res = ControlMessageReader{.allocator = allocator, .buf = buf};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                ControlMessageWire.IHAVE_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._ihave_bufs == null) {
                        res._ihave_bufs = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._ihave_bufs.?.append(result.value);
                },
                ControlMessageWire.IWANT_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._iwant_bufs == null) {
                        res._iwant_bufs = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._iwant_bufs.?.append(result.value);
                },
                ControlMessageWire.GRAFT_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._graft_bufs == null) {
                        res._graft_bufs = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._graft_bufs.?.append(result.value);
                },
                ControlMessageWire.PRUNE_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._prune_bufs == null) {
                        res._prune_bufs = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._prune_bufs.?.append(result.value);
                },
                ControlMessageWire.IDONTWANT_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._idontwant_bufs == null) {
                        res._idontwant_bufs = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._idontwant_bufs.?.append(result.value);
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(self: *const ControlMessageReader) void {
        if (self._ihave_bufs) |arr| {
            arr.deinit();
        }
        if (self._iwant_bufs) |arr| {
            arr.deinit();
        }
        if (self._graft_bufs) |arr| {
            arr.deinit();
        }
        if (self._prune_bufs) |arr| {
            arr.deinit();
        }
        if (self._idontwant_bufs) |arr| {
            arr.deinit();
        }
    }
    pub fn getIhave(self: *const ControlMessageReader, allocator: std.mem.Allocator) gremlin.Error![]ControlIHaveReader {
        if (self._ihave_bufs) |bufs| {
            var result = try std.ArrayList(ControlIHaveReader).initCapacity(allocator, bufs.items.len);
            for (bufs.items) |buf| {
                try result.append(try ControlIHaveReader.init(allocator, buf));
            }
            return result.toOwnedSlice();
        }
        return &[_]ControlIHaveReader{};
    }
    pub fn getIwant(self: *const ControlMessageReader, allocator: std.mem.Allocator) gremlin.Error![]ControlIWantReader {
        if (self._iwant_bufs) |bufs| {
            var result = try std.ArrayList(ControlIWantReader).initCapacity(allocator, bufs.items.len);
            for (bufs.items) |buf| {
                try result.append(try ControlIWantReader.init(allocator, buf));
            }
            return result.toOwnedSlice();
        }
        return &[_]ControlIWantReader{};
    }
    pub fn getGraft(self: *const ControlMessageReader, allocator: std.mem.Allocator) gremlin.Error![]ControlGraftReader {
        if (self._graft_bufs) |bufs| {
            var result = try std.ArrayList(ControlGraftReader).initCapacity(allocator, bufs.items.len);
            for (bufs.items) |buf| {
                try result.append(try ControlGraftReader.init(allocator, buf));
            }
            return result.toOwnedSlice();
        }
        return &[_]ControlGraftReader{};
    }
    pub fn getPrune(self: *const ControlMessageReader, allocator: std.mem.Allocator) gremlin.Error![]ControlPruneReader {
        if (self._prune_bufs) |bufs| {
            var result = try std.ArrayList(ControlPruneReader).initCapacity(allocator, bufs.items.len);
            for (bufs.items) |buf| {
                try result.append(try ControlPruneReader.init(allocator, buf));
            }
            return result.toOwnedSlice();
        }
        return &[_]ControlPruneReader{};
    }
    pub fn getIdontwant(self: *const ControlMessageReader, allocator: std.mem.Allocator) gremlin.Error![]ControlIDontWantReader {
        if (self._idontwant_bufs) |bufs| {
            var result = try std.ArrayList(ControlIDontWantReader).initCapacity(allocator, bufs.items.len);
            for (bufs.items) |buf| {
                try result.append(try ControlIDontWantReader.init(allocator, buf));
            }
            return result.toOwnedSlice();
        }
        return &[_]ControlIDontWantReader{};
    }
};

const ControlIHaveWire = struct {
    const TOPIC_IDWIRE: gremlin.ProtoWireNumber = 1;
    const MESSAGE_IDS_WIRE: gremlin.ProtoWireNumber = 2;
};

pub const ControlIHave = struct {
    // fields
    topic_i_d: ?[]const u8 = null,
    message_i_ds: ?[]const ?[]const u8 = null,

    pub fn calcProtobufSize(self: *const ControlIHave) usize {
        var res: usize = 0;
        if (self.topic_i_d) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(ControlIHaveWire.TOPIC_IDWIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.message_i_ds) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(ControlIHaveWire.MESSAGE_IDS_WIRE);
                if (maybe_v) |v| {
                    res += gremlin.sizes.sizeUsize(v.len) + v.len;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        return res;
    }

    pub fn encode(self: *const ControlIHave, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const ControlIHave, target: *gremlin.Writer) void {
        if (self.topic_i_d) |v| {
            if (v.len > 0) {
                target.appendBytes(ControlIHaveWire.TOPIC_IDWIRE, v);
            }
        }
        if (self.message_i_ds) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    target.appendBytes(ControlIHaveWire.MESSAGE_IDS_WIRE, v);
                } else {
                    target.appendBytesTag(ControlIHaveWire.MESSAGE_IDS_WIRE, 0);
                }
            }
        }
    }
};

pub const ControlIHaveReader = struct {
    allocator: std.mem.Allocator,
    buf: gremlin.Reader,
    _topic_i_d: ?[]const u8 = null,
    _message_i_ds: ?std.ArrayList([]const u8) = null,

    pub fn init(allocator: std.mem.Allocator, src: []const u8) gremlin.Error!ControlIHaveReader {
        var buf = gremlin.Reader.init(src);
        var res = ControlIHaveReader{.allocator = allocator, .buf = buf};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                ControlIHaveWire.TOPIC_IDWIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._topic_i_d = result.value;
                },
                ControlIHaveWire.MESSAGE_IDS_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._message_i_ds == null) {
                        res._message_i_ds = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._message_i_ds.?.append(result.value);
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(self: *const ControlIHaveReader) void {
        if (self._message_i_ds) |arr| {
            arr.deinit();
        }
    }
    pub inline fn getTopicID(self: *const ControlIHaveReader) []const u8 { return self._topic_i_d orelse &[_]u8{}; }
    pub fn getMessageIDs(self: *const ControlIHaveReader) []const []const u8 {
        if (self._message_i_ds) |arr| {
            return arr.items;
        }
        return &[_][]u8{};
    }
};

const ControlIWantWire = struct {
    const MESSAGE_IDS_WIRE: gremlin.ProtoWireNumber = 1;
};

pub const ControlIWant = struct {
    // fields
    message_i_ds: ?[]const ?[]const u8 = null,

    pub fn calcProtobufSize(self: *const ControlIWant) usize {
        var res: usize = 0;
        if (self.message_i_ds) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(ControlIWantWire.MESSAGE_IDS_WIRE);
                if (maybe_v) |v| {
                    res += gremlin.sizes.sizeUsize(v.len) + v.len;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        return res;
    }

    pub fn encode(self: *const ControlIWant, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const ControlIWant, target: *gremlin.Writer) void {
        if (self.message_i_ds) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    target.appendBytes(ControlIWantWire.MESSAGE_IDS_WIRE, v);
                } else {
                    target.appendBytesTag(ControlIWantWire.MESSAGE_IDS_WIRE, 0);
                }
            }
        }
    }
};

pub const ControlIWantReader = struct {
    allocator: std.mem.Allocator,
    buf: gremlin.Reader,
    _message_i_ds: ?std.ArrayList([]const u8) = null,

    pub fn init(allocator: std.mem.Allocator, src: []const u8) gremlin.Error!ControlIWantReader {
        var buf = gremlin.Reader.init(src);
        var res = ControlIWantReader{.allocator = allocator, .buf = buf};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                ControlIWantWire.MESSAGE_IDS_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._message_i_ds == null) {
                        res._message_i_ds = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._message_i_ds.?.append(result.value);
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(self: *const ControlIWantReader) void {
        if (self._message_i_ds) |arr| {
            arr.deinit();
        }
    }
    pub fn getMessageIDs(self: *const ControlIWantReader) []const []const u8 {
        if (self._message_i_ds) |arr| {
            return arr.items;
        }
        return &[_][]u8{};
    }
};

const ControlGraftWire = struct {
    const TOPIC_IDWIRE: gremlin.ProtoWireNumber = 1;
};

pub const ControlGraft = struct {
    // fields
    topic_i_d: ?[]const u8 = null,

    pub fn calcProtobufSize(self: *const ControlGraft) usize {
        var res: usize = 0;
        if (self.topic_i_d) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(ControlGraftWire.TOPIC_IDWIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        return res;
    }

    pub fn encode(self: *const ControlGraft, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const ControlGraft, target: *gremlin.Writer) void {
        if (self.topic_i_d) |v| {
            if (v.len > 0) {
                target.appendBytes(ControlGraftWire.TOPIC_IDWIRE, v);
            }
        }
    }
};

pub const ControlGraftReader = struct {
    _topic_i_d: ?[]const u8 = null,

    pub fn init(_: std.mem.Allocator, src: []const u8) gremlin.Error!ControlGraftReader {
        var buf = gremlin.Reader.init(src);
        var res = ControlGraftReader{};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                ControlGraftWire.TOPIC_IDWIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._topic_i_d = result.value;
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(_: *const ControlGraftReader) void { }
    
    pub inline fn getTopicID(self: *const ControlGraftReader) []const u8 { return self._topic_i_d orelse &[_]u8{}; }
};

const ControlPruneWire = struct {
    const TOPIC_IDWIRE: gremlin.ProtoWireNumber = 1;
    const PEERS_WIRE: gremlin.ProtoWireNumber = 2;
    const BACKOFF_WIRE: gremlin.ProtoWireNumber = 3;
};

pub const ControlPrune = struct {
    // fields
    topic_i_d: ?[]const u8 = null,
    peers: ?[]const ?PeerInfo = null,
    backoff: u64 = 0,

    pub fn calcProtobufSize(self: *const ControlPrune) usize {
        var res: usize = 0;
        if (self.topic_i_d) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(ControlPruneWire.TOPIC_IDWIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.peers) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(ControlPruneWire.PEERS_WIRE);
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    res += gremlin.sizes.sizeUsize(size) + size;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        if (self.backoff != 0) { res += gremlin.sizes.sizeWireNumber(ControlPruneWire.BACKOFF_WIRE) + gremlin.sizes.sizeU64(self.backoff); }
        return res;
    }

    pub fn encode(self: *const ControlPrune, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const ControlPrune, target: *gremlin.Writer) void {
        if (self.topic_i_d) |v| {
            if (v.len > 0) {
                target.appendBytes(ControlPruneWire.TOPIC_IDWIRE, v);
            }
        }
        if (self.peers) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    const size = v.calcProtobufSize();
                    target.appendBytesTag(ControlPruneWire.PEERS_WIRE, size);
                    v.encodeTo(target);
                } else {
                    target.appendBytesTag(ControlPruneWire.PEERS_WIRE, 0);
                }
            }
        }
        if (self.backoff != 0) { target.appendUint64(ControlPruneWire.BACKOFF_WIRE, self.backoff); }
    }
};

pub const ControlPruneReader = struct {
    allocator: std.mem.Allocator,
    buf: gremlin.Reader,
    _topic_i_d: ?[]const u8 = null,
    _peers_bufs: ?std.ArrayList([]const u8) = null,
    _backoff: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, src: []const u8) gremlin.Error!ControlPruneReader {
        var buf = gremlin.Reader.init(src);
        var res = ControlPruneReader{.allocator = allocator, .buf = buf};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                ControlPruneWire.TOPIC_IDWIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._topic_i_d = result.value;
                },
                ControlPruneWire.PEERS_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._peers_bufs == null) {
                        res._peers_bufs = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._peers_bufs.?.append(result.value);
                },
                ControlPruneWire.BACKOFF_WIRE => {
                  const result = try buf.readUInt64(offset);
                  offset += result.size;
                  res._backoff = result.value;
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(self: *const ControlPruneReader) void {
        if (self._peers_bufs) |arr| {
            arr.deinit();
        }
    }
    pub inline fn getTopicID(self: *const ControlPruneReader) []const u8 { return self._topic_i_d orelse &[_]u8{}; }
    pub fn getPeers(self: *const ControlPruneReader, allocator: std.mem.Allocator) gremlin.Error![]PeerInfoReader {
        if (self._peers_bufs) |bufs| {
            var result = try std.ArrayList(PeerInfoReader).initCapacity(allocator, bufs.items.len);
            for (bufs.items) |buf| {
                try result.append(try PeerInfoReader.init(allocator, buf));
            }
            return result.toOwnedSlice();
        }
        return &[_]PeerInfoReader{};
    }
    pub inline fn getBackoff(self: *const ControlPruneReader) u64 { return self._backoff; }
};

const ControlIDontWantWire = struct {
    const MESSAGE_IDS_WIRE: gremlin.ProtoWireNumber = 1;
};

pub const ControlIDontWant = struct {
    // fields
    message_i_ds: ?[]const ?[]const u8 = null,

    pub fn calcProtobufSize(self: *const ControlIDontWant) usize {
        var res: usize = 0;
        if (self.message_i_ds) |arr| {
            for (arr) |maybe_v| {
                res += gremlin.sizes.sizeWireNumber(ControlIDontWantWire.MESSAGE_IDS_WIRE);
                if (maybe_v) |v| {
                    res += gremlin.sizes.sizeUsize(v.len) + v.len;
                } else {
                    res += gremlin.sizes.sizeUsize(0);
                }
            }
        }
        return res;
    }

    pub fn encode(self: *const ControlIDontWant, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const ControlIDontWant, target: *gremlin.Writer) void {
        if (self.message_i_ds) |arr| {
            for (arr) |maybe_v| {
                if (maybe_v) |v| {
                    target.appendBytes(ControlIDontWantWire.MESSAGE_IDS_WIRE, v);
                } else {
                    target.appendBytesTag(ControlIDontWantWire.MESSAGE_IDS_WIRE, 0);
                }
            }
        }
    }
};

pub const ControlIDontWantReader = struct {
    allocator: std.mem.Allocator,
    buf: gremlin.Reader,
    _message_i_ds: ?std.ArrayList([]const u8) = null,

    pub fn init(allocator: std.mem.Allocator, src: []const u8) gremlin.Error!ControlIDontWantReader {
        var buf = gremlin.Reader.init(src);
        var res = ControlIDontWantReader{.allocator = allocator, .buf = buf};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                ControlIDontWantWire.MESSAGE_IDS_WIRE => {
                    const result = try buf.readBytes(offset);
                    offset += result.size;
                    if (res._message_i_ds == null) {
                        res._message_i_ds = std.ArrayList([]const u8).init(allocator);
                    }
                    try res._message_i_ds.?.append(result.value);
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(self: *const ControlIDontWantReader) void {
        if (self._message_i_ds) |arr| {
            arr.deinit();
        }
    }
    pub fn getMessageIDs(self: *const ControlIDontWantReader) []const []const u8 {
        if (self._message_i_ds) |arr| {
            return arr.items;
        }
        return &[_][]u8{};
    }
};

const PeerInfoWire = struct {
    const PEER_IDWIRE: gremlin.ProtoWireNumber = 1;
    const SIGNED_PEER_RECORD_WIRE: gremlin.ProtoWireNumber = 2;
};

pub const PeerInfo = struct {
    // fields
    peer_i_d: ?[]const u8 = null,
    signed_peer_record: ?[]const u8 = null,

    pub fn calcProtobufSize(self: *const PeerInfo) usize {
        var res: usize = 0;
        if (self.peer_i_d) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(PeerInfoWire.PEER_IDWIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.signed_peer_record) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(PeerInfoWire.SIGNED_PEER_RECORD_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        return res;
    }

    pub fn encode(self: *const PeerInfo, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const PeerInfo, target: *gremlin.Writer) void {
        if (self.peer_i_d) |v| {
            if (v.len > 0) {
                target.appendBytes(PeerInfoWire.PEER_IDWIRE, v);
            }
        }
        if (self.signed_peer_record) |v| {
            if (v.len > 0) {
                target.appendBytes(PeerInfoWire.SIGNED_PEER_RECORD_WIRE, v);
            }
        }
    }
};

pub const PeerInfoReader = struct {
    _peer_i_d: ?[]const u8 = null,
    _signed_peer_record: ?[]const u8 = null,

    pub fn init(_: std.mem.Allocator, src: []const u8) gremlin.Error!PeerInfoReader {
        var buf = gremlin.Reader.init(src);
        var res = PeerInfoReader{};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                PeerInfoWire.PEER_IDWIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._peer_i_d = result.value;
                },
                PeerInfoWire.SIGNED_PEER_RECORD_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._signed_peer_record = result.value;
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(_: *const PeerInfoReader) void { }
    
    pub inline fn getPeerID(self: *const PeerInfoReader) []const u8 { return self._peer_i_d orelse &[_]u8{}; }
    pub inline fn getSignedPeerRecord(self: *const PeerInfoReader) []const u8 { return self._signed_peer_record orelse &[_]u8{}; }
};

const TopicDescriptorWire = struct {
    const NAME_WIRE: gremlin.ProtoWireNumber = 1;
    const AUTH_WIRE: gremlin.ProtoWireNumber = 2;
    const ENC_WIRE: gremlin.ProtoWireNumber = 3;
};

pub const TopicDescriptor = struct {
    // nested structs
    const AuthOptsWire = struct {
        const MODE_WIRE: gremlin.ProtoWireNumber = 1;
        const KEYS_WIRE: gremlin.ProtoWireNumber = 2;
    };
    
    pub const AuthOpts = struct {
        // nested enums
        pub const AuthMode = enum(i32) {
            NONE = 0,
            KEY = 1,
            WOT = 2,
        };
        
        // fields
        mode: TopicDescriptor.AuthOpts.AuthMode = @enumFromInt(0),
        keys: ?[]const ?[]const u8 = null,

        pub fn calcProtobufSize(self: *const AuthOpts) usize {
            var res: usize = 0;
            if (@intFromEnum(self.mode) != 0) { res += gremlin.sizes.sizeWireNumber(TopicDescriptor.AuthOptsWire.MODE_WIRE) + gremlin.sizes.sizeI32(@intFromEnum(self.mode)); }
            if (self.keys) |arr| {
                for (arr) |maybe_v| {
                    res += gremlin.sizes.sizeWireNumber(TopicDescriptor.AuthOptsWire.KEYS_WIRE);
                    if (maybe_v) |v| {
                        res += gremlin.sizes.sizeUsize(v.len) + v.len;
                    } else {
                        res += gremlin.sizes.sizeUsize(0);
                    }
                }
            }
            return res;
        }

        pub fn encode(self: *const AuthOpts, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
            const size = self.calcProtobufSize();
            if (size == 0) {
                return &[_]u8{};
            }
            const buf = try allocator.alloc(u8, self.calcProtobufSize());
            var writer = gremlin.Writer.init(buf);
            self.encodeTo(&writer);
            return buf;
        }


        pub fn encodeTo(self: *const AuthOpts, target: *gremlin.Writer) void {
            if (@intFromEnum(self.mode) != 0) { target.appendInt32(TopicDescriptor.AuthOptsWire.MODE_WIRE, @intFromEnum(self.mode)); }
            if (self.keys) |arr| {
                for (arr) |maybe_v| {
                    if (maybe_v) |v| {
                        target.appendBytes(TopicDescriptor.AuthOptsWire.KEYS_WIRE, v);
                    } else {
                        target.appendBytesTag(TopicDescriptor.AuthOptsWire.KEYS_WIRE, 0);
                    }
                }
            }
        }
    };
    
    pub const AuthOptsReader = struct {
        allocator: std.mem.Allocator,
        buf: gremlin.Reader,
        _mode: TopicDescriptor.AuthOpts.AuthMode = @enumFromInt(0),
        _keys: ?std.ArrayList([]const u8) = null,

        pub fn init(allocator: std.mem.Allocator, src: []const u8) gremlin.Error!AuthOptsReader {
            var buf = gremlin.Reader.init(src);
            var res = AuthOptsReader{.allocator = allocator, .buf = buf};
            if (buf.buf.len == 0) {
                return res;
            }
            var offset: usize = 0;
            while (buf.hasNext(offset, 0)) {
                const tag = try buf.readTagAt(offset);
                offset += tag.size;
                switch (tag.number) {
                    TopicDescriptor.AuthOptsWire.MODE_WIRE => {
                      const result = try buf.readInt32(offset);
                      offset += result.size;
                      res._mode = @enumFromInt(result.value);
                    },
                    TopicDescriptor.AuthOptsWire.KEYS_WIRE => {
                        const result = try buf.readBytes(offset);
                        offset += result.size;
                        if (res._keys == null) {
                            res._keys = std.ArrayList([]const u8).init(allocator);
                        }
                        try res._keys.?.append(result.value);
                    },
                    else => {
                        offset = try buf.skipData(offset, tag.wire);
                    }
                }
            }
            return res;
        }
        pub fn deinit(self: *const AuthOptsReader) void {
            if (self._keys) |arr| {
                arr.deinit();
            }
        }
        pub inline fn getMode(self: *const AuthOptsReader) TopicDescriptor.AuthOpts.AuthMode { return self._mode; }
        pub fn getKeys(self: *const AuthOptsReader) []const []const u8 {
            if (self._keys) |arr| {
                return arr.items;
            }
            return &[_][]u8{};
        }
    };
    
    const EncOptsWire = struct {
        const MODE_WIRE: gremlin.ProtoWireNumber = 1;
        const KEY_HASHES_WIRE: gremlin.ProtoWireNumber = 2;
    };
    
    pub const EncOpts = struct {
        // nested enums
        pub const EncMode = enum(i32) {
            NONE = 0,
            SHAREDKEY = 1,
            WOT = 2,
        };
        
        // fields
        mode: TopicDescriptor.EncOpts.EncMode = @enumFromInt(0),
        key_hashes: ?[]const ?[]const u8 = null,

        pub fn calcProtobufSize(self: *const EncOpts) usize {
            var res: usize = 0;
            if (@intFromEnum(self.mode) != 0) { res += gremlin.sizes.sizeWireNumber(TopicDescriptor.EncOptsWire.MODE_WIRE) + gremlin.sizes.sizeI32(@intFromEnum(self.mode)); }
            if (self.key_hashes) |arr| {
                for (arr) |maybe_v| {
                    res += gremlin.sizes.sizeWireNumber(TopicDescriptor.EncOptsWire.KEY_HASHES_WIRE);
                    if (maybe_v) |v| {
                        res += gremlin.sizes.sizeUsize(v.len) + v.len;
                    } else {
                        res += gremlin.sizes.sizeUsize(0);
                    }
                }
            }
            return res;
        }

        pub fn encode(self: *const EncOpts, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
            const size = self.calcProtobufSize();
            if (size == 0) {
                return &[_]u8{};
            }
            const buf = try allocator.alloc(u8, self.calcProtobufSize());
            var writer = gremlin.Writer.init(buf);
            self.encodeTo(&writer);
            return buf;
        }


        pub fn encodeTo(self: *const EncOpts, target: *gremlin.Writer) void {
            if (@intFromEnum(self.mode) != 0) { target.appendInt32(TopicDescriptor.EncOptsWire.MODE_WIRE, @intFromEnum(self.mode)); }
            if (self.key_hashes) |arr| {
                for (arr) |maybe_v| {
                    if (maybe_v) |v| {
                        target.appendBytes(TopicDescriptor.EncOptsWire.KEY_HASHES_WIRE, v);
                    } else {
                        target.appendBytesTag(TopicDescriptor.EncOptsWire.KEY_HASHES_WIRE, 0);
                    }
                }
            }
        }
    };
    
    pub const EncOptsReader = struct {
        allocator: std.mem.Allocator,
        buf: gremlin.Reader,
        _mode: TopicDescriptor.EncOpts.EncMode = @enumFromInt(0),
        _key_hashes: ?std.ArrayList([]const u8) = null,

        pub fn init(allocator: std.mem.Allocator, src: []const u8) gremlin.Error!EncOptsReader {
            var buf = gremlin.Reader.init(src);
            var res = EncOptsReader{.allocator = allocator, .buf = buf};
            if (buf.buf.len == 0) {
                return res;
            }
            var offset: usize = 0;
            while (buf.hasNext(offset, 0)) {
                const tag = try buf.readTagAt(offset);
                offset += tag.size;
                switch (tag.number) {
                    TopicDescriptor.EncOptsWire.MODE_WIRE => {
                      const result = try buf.readInt32(offset);
                      offset += result.size;
                      res._mode = @enumFromInt(result.value);
                    },
                    TopicDescriptor.EncOptsWire.KEY_HASHES_WIRE => {
                        const result = try buf.readBytes(offset);
                        offset += result.size;
                        if (res._key_hashes == null) {
                            res._key_hashes = std.ArrayList([]const u8).init(allocator);
                        }
                        try res._key_hashes.?.append(result.value);
                    },
                    else => {
                        offset = try buf.skipData(offset, tag.wire);
                    }
                }
            }
            return res;
        }
        pub fn deinit(self: *const EncOptsReader) void {
            if (self._key_hashes) |arr| {
                arr.deinit();
            }
        }
        pub inline fn getMode(self: *const EncOptsReader) TopicDescriptor.EncOpts.EncMode { return self._mode; }
        pub fn getKeyHashes(self: *const EncOptsReader) []const []const u8 {
            if (self._key_hashes) |arr| {
                return arr.items;
            }
            return &[_][]u8{};
        }
    };
    
    // fields
    name: ?[]const u8 = null,
    auth: ?TopicDescriptor.AuthOpts = null,
    enc: ?TopicDescriptor.EncOpts = null,

    pub fn calcProtobufSize(self: *const TopicDescriptor) usize {
        var res: usize = 0;
        if (self.name) |v| {
            if (v.len > 0) {
                res += gremlin.sizes.sizeWireNumber(TopicDescriptorWire.NAME_WIRE) + gremlin.sizes.sizeUsize(v.len) + v.len;
            }
        }
        if (self.auth) |v| {
            const size = v.calcProtobufSize();
            if (size > 0) {
                res += gremlin.sizes.sizeWireNumber(TopicDescriptorWire.AUTH_WIRE) + gremlin.sizes.sizeUsize(size) + size;
            }
        }
        if (self.enc) |v| {
            const size = v.calcProtobufSize();
            if (size > 0) {
                res += gremlin.sizes.sizeWireNumber(TopicDescriptorWire.ENC_WIRE) + gremlin.sizes.sizeUsize(size) + size;
            }
        }
        return res;
    }

    pub fn encode(self: *const TopicDescriptor, allocator: std.mem.Allocator) gremlin.Error![]const u8 {
        const size = self.calcProtobufSize();
        if (size == 0) {
            return &[_]u8{};
        }
        const buf = try allocator.alloc(u8, self.calcProtobufSize());
        var writer = gremlin.Writer.init(buf);
        self.encodeTo(&writer);
        return buf;
    }


    pub fn encodeTo(self: *const TopicDescriptor, target: *gremlin.Writer) void {
        if (self.name) |v| {
            if (v.len > 0) {
                target.appendBytes(TopicDescriptorWire.NAME_WIRE, v);
            }
        }
        if (self.auth) |v| {
            const size = v.calcProtobufSize();
            if (size > 0) {
                target.appendBytesTag(TopicDescriptorWire.AUTH_WIRE, size);
                v.encodeTo(target);
            }
        }
        if (self.enc) |v| {
            const size = v.calcProtobufSize();
            if (size > 0) {
                target.appendBytesTag(TopicDescriptorWire.ENC_WIRE, size);
                v.encodeTo(target);
            }
        }
    }
};

pub const TopicDescriptorReader = struct {
    _name: ?[]const u8 = null,
    _auth_buf: ?[]const u8 = null,
    _enc_buf: ?[]const u8 = null,

    pub fn init(_: std.mem.Allocator, src: []const u8) gremlin.Error!TopicDescriptorReader {
        var buf = gremlin.Reader.init(src);
        var res = TopicDescriptorReader{};
        if (buf.buf.len == 0) {
            return res;
        }
        var offset: usize = 0;
        while (buf.hasNext(offset, 0)) {
            const tag = try buf.readTagAt(offset);
            offset += tag.size;
            switch (tag.number) {
                TopicDescriptorWire.NAME_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._name = result.value;
                },
                TopicDescriptorWire.AUTH_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._auth_buf = result.value;
                },
                TopicDescriptorWire.ENC_WIRE => {
                  const result = try buf.readBytes(offset);
                  offset += result.size;
                  res._enc_buf = result.value;
                },
                else => {
                    offset = try buf.skipData(offset, tag.wire);
                }
            }
        }
        return res;
    }
    pub fn deinit(_: *const TopicDescriptorReader) void { }
    
    pub inline fn getName(self: *const TopicDescriptorReader) []const u8 { return self._name orelse &[_]u8{}; }
    pub fn getAuth(self: *const TopicDescriptorReader, allocator: std.mem.Allocator) gremlin.Error!TopicDescriptor.AuthOptsReader {
        if (self._auth_buf) |buf| {
            return try TopicDescriptor.AuthOptsReader.init(allocator, buf);
        }
        return try TopicDescriptor.AuthOptsReader.init(allocator, &[_]u8{});
    }
    pub fn getEnc(self: *const TopicDescriptorReader, allocator: std.mem.Allocator) gremlin.Error!TopicDescriptor.EncOptsReader {
        if (self._enc_buf) |buf| {
            return try TopicDescriptor.EncOptsReader.init(allocator, buf);
        }
        return try TopicDescriptor.EncOptsReader.init(allocator, &[_]u8{});
    }
};

