const std = @import("std");

/// A generic event emitter/listener system.
/// This allows registering listeners for specific event types and emitting events to those listeners.
/// The event type must be a tagged union, and listeners are registered by the union's tag enum.
/// Listeners are type-erased using a vtable pattern to allow different listener implementations.
pub fn EventListenerVTable(comptime T: type) type {
    return struct {
        handleFn: *const fn (instance: *anyopaque, event: T) void,
    };
}

pub fn AnyEventListener(comptime T: type) type {
    return struct {
        instance: *anyopaque,
        vtable: *const EventListenerVTable(T),

        const Self = @This();

        pub fn handle(self: Self, event: T) void {
            self.vtable.handleFn(self.instance, event);
        }
    };
}

/// A generic event emitter that allows registering listeners for specific event types and emitting events to those listeners.
/// The event type T must be a tagged union.
/// Listeners are registered by the union's tag enum, not the full union type.
/// Listeners are type-erased using a vtable pattern to allow different listener implementations.
pub fn EventEmitter(comptime T: type) type {
    // Compile-time checks to ensure T is a tagged union
    comptime {
        const type_info = @typeInfo(T);
        if (type_info != .@"union") {
            @compileError("EventEmitter requires a union type, got " ++ @typeName(T));
        }
        if (type_info.@"union".tag_type == null) {
            @compileError("EventEmitter requires a tagged union, got untagged union " ++ @typeName(T));
        }
    }

    return struct {
        allocator: std.mem.Allocator,
        // Use the union's tag enum as the key type
        listeners: std.HashMapUnmanaged(std.meta.Tag(T), std.ArrayListUnmanaged(AnyEventListener(T)), std.hash_map.AutoContext(std.meta.Tag(T)), std.hash_map.default_max_load_percentage),

        const Self = @This();
        const TagType = std.meta.Tag(T);

        pub fn init(allocator: std.mem.Allocator) Self {
            return Self{
                .allocator = allocator,
                .listeners = std.HashMapUnmanaged(TagType, std.ArrayListUnmanaged(AnyEventListener(T)), std.hash_map.AutoContext(TagType), std.hash_map.default_max_load_percentage).empty,
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.listeners.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit(self.allocator);
            }
            self.listeners.deinit(self.allocator);
        }

        /// Add a listener for a specific event tag
        pub fn addListener(self: *Self, event_tag: TagType, listener: AnyEventListener(T)) !void {
            const gop = try self.listeners.getOrPut(self.allocator, event_tag);
            if (!gop.found_existing) {
                gop.value_ptr.* = std.ArrayListUnmanaged(AnyEventListener(T)).empty;
            }
            try gop.value_ptr.append(self.allocator, listener);
        }

        /// Remove a specific listener for an event tag
        pub fn removeListener(self: *Self, event_tag: TagType, listener: AnyEventListener(T)) bool {
            if (self.listeners.getPtr(event_tag)) |list| {
                for (list.items, 0..) |item, index| {
                    if (item.instance == listener.instance and item.vtable == listener.vtable) {
                        _ = list.swapRemove(index);
                        return true;
                    }
                }
            }
            return false;
        }

        /// Emit an event to all listeners registered for its tag
        /// `event` ownership is not transferred; it need to copy if needed asynchronously.
        pub fn emit(self: *Self, event: T) void {
            const event_tag = std.meta.activeTag(event);
            if (self.listeners.getPtr(event_tag)) |list| {
                for (list.items) |listener| {
                    listener.handle(event);
                }
            }
        }

        /// Get the count of listeners for a specific event tag
        pub fn getListenerCount(self: *Self, event_tag: TagType) usize {
            if (self.listeners.getPtr(event_tag)) |list| {
                return list.items.len;
            }
            return 0;
        }

        /// Remove all listeners for a specific event tag
        pub fn removeAllListeners(self: *Self, event_tag: TagType) void {
            if (self.listeners.getPtr(event_tag)) |list| {
                list.clearAndFree(self.allocator);
                _ = self.listeners.remove(event_tag);
            }
        }
    };
}

test "EventEmitter works with tagged union" {
    const allocator = std.testing.allocator;

    // Define a tagged union for events
    const MyEvent = union(enum) {
        event_a: struct { value: u32 },
        event_b: struct { message: []const u8 },
        event_c: void,

        const Tag = std.meta.Tag(@This());
    };

    var emitter = EventEmitter(MyEvent).init(allocator);
    defer emitter.deinit();

    var state: u32 = 0;

    const ListenerA = struct {
        state_ptr: *u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            switch (event) {
                .event_a => |data| {
                    self.state_ptr.* += data.value;
                },
                else => {},
            }
        }

        pub fn vtableHandleFn(
            instance: *anyopaque,
            event: MyEvent,
        ) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            return self.handle(event);
        }

        pub const vtable = EventListenerVTable(MyEvent){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) AnyEventListener(MyEvent) {
            return AnyEventListener(MyEvent){
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    var listener_a_instance = ListenerA{ .state_ptr = &state };

    try emitter.addListener(.event_a, listener_a_instance.any());

    emitter.emit(MyEvent{ .event_a = .{ .value = 5 } });
    try std.testing.expect(state == 5);

    emitter.emit(MyEvent{ .event_b = .{ .message = "hello" } });
    try std.testing.expect(state == 5);

    emitter.emit(MyEvent{ .event_a = .{ .value = 3 } });
    try std.testing.expect(state == 8);

    const removed = emitter.removeListener(.event_a, listener_a_instance.any());
    try std.testing.expect(removed);

    emitter.emit(MyEvent{ .event_a = .{ .value = 10 } });
    try std.testing.expect(state == 8);
}

test "EventEmitter with multiple listeners for same event tag" {
    const allocator = std.testing.allocator;

    const MyEvent = union(enum) {
        event_a: struct { value: u32 },
        event_b: struct { message: []const u8 },
    };

    var emitter = EventEmitter(MyEvent).init(allocator);
    defer emitter.deinit();

    var state1: u32 = 0;
    var state2: u32 = 0;
    var state3: u32 = 0;

    const ListenerA = struct {
        state_ptr: *u32,
        multiplier: u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            switch (event) {
                .event_a => |data| {
                    self.state_ptr.* += data.value * self.multiplier;
                },
                else => {},
            }
        }

        pub fn vtableHandleFn(
            instance: *anyopaque,
            event: MyEvent,
        ) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            return self.handle(event);
        }

        pub const vtable = EventListenerVTable(MyEvent){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) AnyEventListener(MyEvent) {
            return AnyEventListener(MyEvent){
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    var listener1 = ListenerA{ .state_ptr = &state1, .multiplier = 1 };
    var listener2 = ListenerA{ .state_ptr = &state2, .multiplier = 2 };
    var listener3 = ListenerA{ .state_ptr = &state3, .multiplier = 3 };

    try emitter.addListener(.event_a, listener1.any());
    try emitter.addListener(.event_a, listener2.any());
    try emitter.addListener(.event_a, listener3.any());

    try std.testing.expect(emitter.getListenerCount(.event_a) == 3);
    try std.testing.expect(emitter.getListenerCount(.event_b) == 0);

    emitter.emit(MyEvent{ .event_a = .{ .value = 2 } });
    try std.testing.expect(state1 == 2); // 2 * 1
    try std.testing.expect(state2 == 4); // 2 * 2
    try std.testing.expect(state3 == 6); // 2 * 3

    emitter.emit(MyEvent{ .event_b = .{ .message = "test" } });
    try std.testing.expect(state1 == 2); // unchanged
    try std.testing.expect(state2 == 4); // unchanged
    try std.testing.expect(state3 == 6); // unchanged

    const removed = emitter.removeListener(.event_a, listener2.any());
    try std.testing.expect(removed);
    try std.testing.expect(emitter.getListenerCount(.event_a) == 2);

    emitter.emit(MyEvent{ .event_a = .{ .value = 1 } });
    try std.testing.expect(state1 == 3);
    try std.testing.expect(state2 == 4);
    try std.testing.expect(state3 == 9);

    // Remove all listeners for event_a
    emitter.removeAllListeners(.event_a);
    try std.testing.expect(emitter.getListenerCount(.event_a) == 0);

    emitter.emit(MyEvent{ .event_a = .{ .value = 5 } });
    try std.testing.expect(state1 == 3);
    try std.testing.expect(state2 == 4);
    try std.testing.expect(state3 == 9);
}

test "EventEmitter with different event tags" {
    const allocator = std.testing.allocator;

    const MyEvent = union(enum) {
        connect: struct { peer_id: []const u8 },
        disconnect: struct { peer_id: []const u8 },
        message: struct { content: []const u8, from: []const u8 },
        err: struct { code: u32, description: []const u8 },
    };

    var emitter = EventEmitter(MyEvent).init(allocator);
    defer emitter.deinit();

    var connection_count: i32 = 0;
    var message_count: u32 = 0;
    var error_count: u32 = 0;

    const ConnectionListener = struct {
        counter: *i32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            switch (event) {
                .connect => self.counter.* += 1,
                .disconnect => self.counter.* -= 1,
                else => {},
            }
        }

        pub fn vtableHandleFn(instance: *anyopaque, event: MyEvent) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            return self.handle(event);
        }

        pub const vtable = EventListenerVTable(MyEvent){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) AnyEventListener(MyEvent) {
            return AnyEventListener(MyEvent){
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    const MessageListener = struct {
        counter: *u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            switch (event) {
                .message => self.counter.* += 1,
                else => {},
            }
        }

        pub fn vtableHandleFn(instance: *anyopaque, event: MyEvent) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            return self.handle(event);
        }

        pub const vtable = EventListenerVTable(MyEvent){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) AnyEventListener(MyEvent) {
            return AnyEventListener(MyEvent){
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    const ErrorListener = struct {
        counter: *u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            switch (event) {
                .err => self.counter.* += 1,
                else => {},
            }
        }

        pub fn vtableHandleFn(instance: *anyopaque, event: MyEvent) void {
            const self: *Self = @ptrCast(@alignCast(instance));
            return self.handle(event);
        }

        pub const vtable = EventListenerVTable(MyEvent){
            .handleFn = vtableHandleFn,
        };

        pub fn any(self: *Self) AnyEventListener(MyEvent) {
            return AnyEventListener(MyEvent){
                .instance = @ptrCast(self),
                .vtable = &Self.vtable,
            };
        }
    };

    var connListener = ConnectionListener{ .counter = &connection_count };
    var msgListener = MessageListener{ .counter = &message_count };
    var errListener = ErrorListener{ .counter = &error_count };

    try emitter.addListener(.connect, connListener.any());
    try emitter.addListener(.disconnect, connListener.any());
    try emitter.addListener(.message, msgListener.any());
    try emitter.addListener(.err, errListener.any());

    emitter.emit(MyEvent{ .connect = .{ .peer_id = "peer1" } });
    try std.testing.expect(connection_count == 1);
    try std.testing.expect(message_count == 0);
    try std.testing.expect(error_count == 0);

    emitter.emit(MyEvent{ .message = .{ .content = "hello", .from = "peer1" } });
    try std.testing.expect(connection_count == 1);
    try std.testing.expect(message_count == 1);
    try std.testing.expect(error_count == 0);

    emitter.emit(MyEvent{ .err = .{ .code = 404, .description = "Not found" } });
    try std.testing.expect(connection_count == 1);
    try std.testing.expect(message_count == 1);
    try std.testing.expect(error_count == 1);

    emitter.emit(MyEvent{ .disconnect = .{ .peer_id = "peer1" } });
    try std.testing.expect(connection_count == 0);
    try std.testing.expect(message_count == 1);
    try std.testing.expect(error_count == 1);
}
