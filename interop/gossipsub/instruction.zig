/// Phase 2.2 — Params parser and instruction types.
///
/// Matches the JSON schema defined in test-plans/gossipsub-interop/script_instruction.py.
/// All memory lives in an ArenaAllocator owned by ExperimentParams; call
/// ExperimentParams.deinit() to free everything at once.

const std = @import("std");

// ------------------------------------------------------------------ //
// GossipSub tuning parameters (all optional — only set fields override
// the implementation's defaults)
// ------------------------------------------------------------------ //
pub const GossipSubParams = struct {
    D: ?i64 = null,
    Dlo: ?i64 = null,
    Dhi: ?i64 = null,
    Dscore: ?i64 = null,
    Dout: ?i64 = null,
    HistoryLength: ?i64 = null,
    HistoryGossip: ?i64 = null,
    Dlazy: ?i64 = null,
    GossipFactor: ?f64 = null,
    GossipRetransmission: ?i64 = null,
    HeartbeatInitialDelay: ?f64 = null,
    HeartbeatInterval: ?f64 = null,
    SlowHeartbeatWarning: ?f64 = null,
    FanoutTTL: ?f64 = null,
    PrunePeers: ?i64 = null,
    PruneBackoff: ?f64 = null,
    UnsubscribeBackoff: ?f64 = null,
    Connectors: ?i64 = null,
    MaxPendingConnections: ?i64 = null,
    ConnectionTimeout: ?f64 = null,
    DirectConnectTicks: ?i64 = null,
    DirectConnectInitialDelay: ?f64 = null,
    OpportunisticGraftTicks: ?i64 = null,
    OpportunisticGraftPeers: ?i64 = null,
    GraftFloodThreshold: ?f64 = null,
    MaxIHaveLength: ?i64 = null,
    MaxIHaveMessages: ?i64 = null,
    MaxIDontWantLength: ?i64 = null,
    MaxIDontWantMessages: ?i64 = null,
    IWantFollowupTime: ?f64 = null,
    IDontWantMessageThreshold: ?i64 = null,
    IDontWantMessageTTL: ?i64 = null,
};

// ------------------------------------------------------------------ //
// Instruction payload types
// ------------------------------------------------------------------ //
pub const Connect = struct {
    connect_to: []i64,
};

pub const IfNodeIdEquals = struct {
    node_id: i64,
    instruction: *Instruction,
};

pub const WaitUntil = struct {
    elapsed_seconds: i64,
};

pub const Publish = struct {
    message_id: i64,
    message_size_bytes: i64,
    topic_id: []const u8,
};

pub const SubscribeToTopic = struct {
    topic_id: []const u8,
    partial: bool,
};

pub const SetTopicValidationDelay = struct {
    topic_id: []const u8,
    delay_seconds: f64,
};

pub const InitGossipSub = struct {
    params: GossipSubParams,
};

pub const AddPartialMessage = struct {
    topic_id: []const u8,
    group_id: i64,
    parts: i64,
};

pub const PublishPartial = struct {
    topic_id: []const u8,
    group_id: i64,
    publish_to_node_ids: []i64,
};

// ------------------------------------------------------------------ //
// Tagged union over all instruction kinds
// ------------------------------------------------------------------ //
pub const Instruction = union(enum) {
    connect: Connect,
    if_node_id_equals: IfNodeIdEquals,
    wait_until: WaitUntil,
    publish: Publish,
    subscribe_to_topic: SubscribeToTopic,
    set_topic_validation_delay: SetTopicValidationDelay,
    init_gossipsub: InitGossipSub,
    add_partial_message: AddPartialMessage,
    publish_partial: PublishPartial,
};

// ------------------------------------------------------------------ //
// Top-level params container
// ------------------------------------------------------------------ //
pub const ExperimentParams = struct {
    arena: std.heap.ArenaAllocator,
    instructions: []Instruction,

    pub fn deinit(self: *ExperimentParams) void {
        self.arena.deinit();
    }
};

// ------------------------------------------------------------------ //
// Public entry point
// ------------------------------------------------------------------ //
pub fn readParams(allocator: std.mem.Allocator, path: []const u8) !ExperimentParams {
    if (!std.mem.endsWith(u8, path, ".json")) {
        std.log.err("params file must end in .json, got: {s}", .{path});
        return error.ParamsFileNotJson;
    }

    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const aa = arena.allocator();

    const content = try file.readToEndAlloc(aa, 4 * 1024 * 1024);

    const root = try std.json.parseFromSliceLeaky(std.json.Value, aa, content, .{});

    const root_obj = switch (root) {
        .object => |o| o,
        else => return error.InvalidParamsRoot,
    };

    const script_val = root_obj.get("script") orelse {
        std.log.err("params file missing 'script' field", .{});
        return error.MissingScriptField;
    };
    const script_arr = switch (script_val) {
        .array => |a| a,
        else => return error.ScriptNotArray,
    };

    const instructions = try aa.alloc(Instruction, script_arr.items.len);
    for (script_arr.items, 0..) |item, i| {
        instructions[i] = try parseInstruction(aa, item);
    }

    return ExperimentParams{
        .arena = arena,
        .instructions = instructions,
    };
}

// ------------------------------------------------------------------ //
// Internal helpers
// ------------------------------------------------------------------ //
const Obj = std.json.ObjectMap;

fn parseInstruction(aa: std.mem.Allocator, val: std.json.Value) anyerror!Instruction {
    const obj = switch (val) {
        .object => |o| o,
        else => return error.InstructionNotObject,
    };

    const type_str = switch (obj.get("type") orelse return error.MissingTypeField) {
        .string => |s| s,
        else => return error.TypeNotString,
    };

    if (std.mem.eql(u8, type_str, "connect")) {
        return .{ .connect = try parseConnect(aa, obj) };
    } else if (std.mem.eql(u8, type_str, "ifNodeIDEquals")) {
        return .{ .if_node_id_equals = try parseIfNodeIdEquals(aa, obj) };
    } else if (std.mem.eql(u8, type_str, "waitUntil")) {
        return .{ .wait_until = .{ .elapsed_seconds = try reqInt(obj, "elapsedSeconds") } };
    } else if (std.mem.eql(u8, type_str, "publish")) {
        return .{ .publish = try parsePublish(aa, obj) };
    } else if (std.mem.eql(u8, type_str, "subscribeToTopic")) {
        return .{ .subscribe_to_topic = try parseSubscribeToTopic(aa, obj) };
    } else if (std.mem.eql(u8, type_str, "setTopicValidationDelay")) {
        return .{ .set_topic_validation_delay = try parseSetTopicValidationDelay(aa, obj) };
    } else if (std.mem.eql(u8, type_str, "initGossipSub")) {
        return .{ .init_gossipsub = try parseInitGossipSub(obj) };
    } else if (std.mem.eql(u8, type_str, "addPartialMessage")) {
        return .{ .add_partial_message = try parseAddPartialMessage(aa, obj) };
    } else if (std.mem.eql(u8, type_str, "publishPartial")) {
        return .{ .publish_partial = try parsePublishPartial(aa, obj) };
    } else {
        std.log.warn("unknown instruction type: {s}", .{type_str});
        return error.UnknownInstructionType;
    }
}

fn parseConnect(aa: std.mem.Allocator, obj: Obj) !Connect {
    const arr = switch (obj.get("connectTo") orelse return error.MissingField) {
        .array => |a| a,
        else => return error.FieldNotArray,
    };
    const connect_to = try aa.alloc(i64, arr.items.len);
    for (arr.items, 0..) |item, i| {
        connect_to[i] = switch (item) {
            .integer => |n| n,
            else => return error.ExpectedInteger,
        };
    }
    return .{ .connect_to = connect_to };
}

fn parseIfNodeIdEquals(aa: std.mem.Allocator, obj: Obj) !IfNodeIdEquals {
    const node_id = try reqInt(obj, "nodeID");
    const nested_val = obj.get("instruction") orelse return error.MissingField;
    const nested = try aa.create(Instruction);
    nested.* = try parseInstruction(aa, nested_val);
    return .{ .node_id = node_id, .instruction = nested };
}

fn parsePublish(aa: std.mem.Allocator, obj: Obj) !Publish {
    return .{
        .message_id = try reqInt(obj, "messageID"),
        .message_size_bytes = try reqInt(obj, "messageSizeBytes"),
        .topic_id = try reqStr(aa, obj, "topicID"),
    };
}

fn parseSubscribeToTopic(aa: std.mem.Allocator, obj: Obj) !SubscribeToTopic {
    return .{
        .topic_id = try reqStr(aa, obj, "topicID"),
        .partial = optBool(obj, "partial") orelse false,
    };
}

fn parseSetTopicValidationDelay(aa: std.mem.Allocator, obj: Obj) !SetTopicValidationDelay {
    return .{
        .topic_id = try reqStr(aa, obj, "topicID"),
        .delay_seconds = try reqFloat(obj, "delaySeconds"),
    };
}

fn parseInitGossipSub(obj: Obj) !InitGossipSub {
    const params_val = obj.get("gossipSubParams") orelse return error.MissingField;
    const params_obj = switch (params_val) {
        .object => |o| o,
        else => return error.FieldNotObject,
    };
    var p = GossipSubParams{};
    if (params_obj.get("D")) |v| p.D = toInt(v);
    if (params_obj.get("Dlo")) |v| p.Dlo = toInt(v);
    if (params_obj.get("Dhi")) |v| p.Dhi = toInt(v);
    if (params_obj.get("Dscore")) |v| p.Dscore = toInt(v);
    if (params_obj.get("Dout")) |v| p.Dout = toInt(v);
    if (params_obj.get("HistoryLength")) |v| p.HistoryLength = toInt(v);
    if (params_obj.get("HistoryGossip")) |v| p.HistoryGossip = toInt(v);
    if (params_obj.get("Dlazy")) |v| p.Dlazy = toInt(v);
    if (params_obj.get("GossipFactor")) |v| p.GossipFactor = toFloat(v);
    if (params_obj.get("GossipRetransmission")) |v| p.GossipRetransmission = toInt(v);
    if (params_obj.get("HeartbeatInitialDelay")) |v| p.HeartbeatInitialDelay = toFloat(v);
    if (params_obj.get("HeartbeatInterval")) |v| p.HeartbeatInterval = toFloat(v);
    if (params_obj.get("SlowHeartbeatWarning")) |v| p.SlowHeartbeatWarning = toFloat(v);
    if (params_obj.get("FanoutTTL")) |v| p.FanoutTTL = toFloat(v);
    if (params_obj.get("PrunePeers")) |v| p.PrunePeers = toInt(v);
    if (params_obj.get("PruneBackoff")) |v| p.PruneBackoff = toFloat(v);
    if (params_obj.get("UnsubscribeBackoff")) |v| p.UnsubscribeBackoff = toFloat(v);
    if (params_obj.get("Connectors")) |v| p.Connectors = toInt(v);
    if (params_obj.get("MaxPendingConnections")) |v| p.MaxPendingConnections = toInt(v);
    if (params_obj.get("ConnectionTimeout")) |v| p.ConnectionTimeout = toFloat(v);
    if (params_obj.get("DirectConnectTicks")) |v| p.DirectConnectTicks = toInt(v);
    if (params_obj.get("DirectConnectInitialDelay")) |v| p.DirectConnectInitialDelay = toFloat(v);
    if (params_obj.get("OpportunisticGraftTicks")) |v| p.OpportunisticGraftTicks = toInt(v);
    if (params_obj.get("OpportunisticGraftPeers")) |v| p.OpportunisticGraftPeers = toInt(v);
    if (params_obj.get("GraftFloodThreshold")) |v| p.GraftFloodThreshold = toFloat(v);
    if (params_obj.get("MaxIHaveLength")) |v| p.MaxIHaveLength = toInt(v);
    if (params_obj.get("MaxIHaveMessages")) |v| p.MaxIHaveMessages = toInt(v);
    if (params_obj.get("MaxIDontWantLength")) |v| p.MaxIDontWantLength = toInt(v);
    if (params_obj.get("MaxIDontWantMessages")) |v| p.MaxIDontWantMessages = toInt(v);
    if (params_obj.get("IWantFollowupTime")) |v| p.IWantFollowupTime = toFloat(v);
    if (params_obj.get("IDontWantMessageThreshold")) |v| p.IDontWantMessageThreshold = toInt(v);
    if (params_obj.get("IDontWantMessageTTL")) |v| p.IDontWantMessageTTL = toInt(v);
    return .{ .params = p };
}

fn parseAddPartialMessage(aa: std.mem.Allocator, obj: Obj) !AddPartialMessage {
    return .{
        .topic_id = try reqStr(aa, obj, "topicID"),
        .group_id = try reqInt(obj, "groupID"),
        .parts = try reqInt(obj, "parts"),
    };
}

fn parsePublishPartial(aa: std.mem.Allocator, obj: Obj) !PublishPartial {
    var publish_to: []i64 = &.{};
    if (obj.get("publishToNodeIDs")) |v| {
        switch (v) {
            .null => {},
            .array => |arr| {
                publish_to = try aa.alloc(i64, arr.items.len);
                for (arr.items, 0..) |item, i| {
                    publish_to[i] = switch (item) {
                        .integer => |n| n,
                        else => return error.ExpectedInteger,
                    };
                }
            },
            else => return error.FieldNotArray,
        }
    }
    return .{
        .topic_id = try reqStr(aa, obj, "topicID"),
        .group_id = try reqInt(obj, "groupID"),
        .publish_to_node_ids = publish_to,
    };
}

// ---- tiny field accessors ----

fn reqInt(obj: Obj, key: []const u8) !i64 {
    return toInt(obj.get(key) orelse return error.MissingField) orelse error.ExpectedInteger;
}

fn reqFloat(obj: Obj, key: []const u8) !f64 {
    return toFloat(obj.get(key) orelse return error.MissingField) orelse error.ExpectedFloat;
}

fn reqStr(aa: std.mem.Allocator, obj: Obj, key: []const u8) ![]const u8 {
    const v = obj.get(key) orelse return error.MissingField;
    return switch (v) {
        .string => |s| try aa.dupe(u8, s),
        else => error.ExpectedString,
    };
}

fn optBool(obj: Obj, key: []const u8) ?bool {
    return switch (obj.get(key) orelse return null) {
        .bool => |b| b,
        else => null,
    };
}

fn toInt(v: std.json.Value) ?i64 {
    return switch (v) {
        .integer => |n| n,
        .float => |f| @intFromFloat(f),
        else => null,
    };
}

fn toFloat(v: std.json.Value) ?f64 {
    return switch (v) {
        .float => |f| f,
        .integer => |n| @floatFromInt(n),
        else => null,
    };
}
