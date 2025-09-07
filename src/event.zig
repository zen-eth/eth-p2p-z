const std = @import("std");

/// A generic event emitter/listener system.
/// This allows registering listeners for specific event types and emitting events to those listeners.
/// The event type is generic and can be any type that supports equality comparison.
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
/// The event type is generic and can be any type that supports equality comparison.
/// Listeners are type-erased using a vtable pattern to allow different listener implementations.
pub fn EventEmitter(comptime T: type) type {
    return struct {
        allocator: std.mem.Allocator,
        listeners: std.AutoHashMapUnmanaged(T, std.ArrayListUnmanaged(AnyEventListener(T))),

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator) !Self {
            return Self{
                .allocator = allocator,
                .listeners = std.AutoHashMapUnmanaged(T, std.ArrayListUnmanaged(AnyEventListener(T))).empty,
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.listeners.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit(self.allocator);
            }
            self.listeners.deinit(self.allocator);
        }

        pub fn addListener(self: *Self, event: T, listener: AnyEventListener(T)) !void {
            if (self.listeners.getPtr(event)) |l| {
                try l.append(self.allocator, listener);
            } else {
                var new_list = std.ArrayListUnmanaged(AnyEventListener(T)).empty;
                try new_list.append(self.allocator, listener);
                try self.listeners.put(self.allocator, event, new_list);
            }
        }

        pub fn removeListener(self: *Self, event: T, listener: AnyEventListener(T)) bool {
            if (self.listeners.getPtr(event)) |list| {
                for (list.items, 0..) |item, index| {
                    if (item.instance == listener.instance and item.vtable == listener.vtable) {
                        _ = list.swapRemove(index);
                        return true;
                    }
                }
            }
            return false;
        }

        pub fn emit(self: *Self, event: T) void {
            if (self.listeners.getPtr(event)) |list| {
                for (list.items) |listener| {
                    listener.handle(event);
                }
            }
        }
    };
}

test "EventEmitter works" {
    const allocator = std.testing.allocator;

    const MyEvent = enum {
        EventA,
        EventB,
    };

    var emitter = try EventEmitter(MyEvent).init(allocator);
    defer emitter.deinit();

    var state: u32 = 0;

    const ListenerA = struct {
        state_ptr: *u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            if (event == .EventA) {
                self.state_ptr.* += 1;
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

    try emitter.addListener(.EventA, listener_a_instance.any());

    emitter.emit(.EventA);
    try std.testing.expect(state == 1);

    emitter.emit(.EventB);
    try std.testing.expect(state == 1);

    emitter.emit(.EventA);
    try std.testing.expect(state == 2);

    const removed = emitter.removeListener(.EventA, listener_a_instance.any());
    try std.testing.expect(removed);

    emitter.emit(.EventA);
    try std.testing.expect(state == 2);
}

test "EventEmitter emits events to listeners" {
    const allocator = std.testing.allocator;

    const MyEvent = enum {
        EventA,
        EventB,
    };

    var emitter = try EventEmitter(MyEvent).init(allocator);
    defer emitter.deinit();

    var state: u32 = 0;

    const ListenerA = struct {
        state_ptr: *u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            if (event == .EventA) {
                self.state_ptr.* += 1;
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

    try emitter.addListener(.EventA, listener_a_instance.any());

    emitter.emit(.EventA);
    try std.testing.expect(state == 1);

    emitter.emit(.EventB);
    try std.testing.expect(state == 1);

    emitter.emit(.EventA);
    try std.testing.expect(state == 2);

    const removed = emitter.removeListener(.EventA, listener_a_instance.any());
    try std.testing.expect(removed);

    emitter.emit(.EventA);
    try std.testing.expect(state == 2);
}

test "EventEmitter with multiple listeners for same event" {
    const allocator = std.testing.allocator;

    const MyEvent = enum {
        EventA,
        EventB,
    };

    var emitter = try EventEmitter(MyEvent).init(allocator);
    defer emitter.deinit();

    var state1: u32 = 0;
    var state2: u32 = 0;
    var state3: u32 = 0;

    const ListenerA = struct {
        state_ptr: *u32,
        multiplier: u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            if (event == .EventA) {
                self.state_ptr.* += self.multiplier;
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

    // Create three different listeners for the same event
    var listener1 = ListenerA{ .state_ptr = &state1, .multiplier = 1 };
    var listener2 = ListenerA{ .state_ptr = &state2, .multiplier = 2 };
    var listener3 = ListenerA{ .state_ptr = &state3, .multiplier = 3 };

    // Add all three listeners to the same event
    try emitter.addListener(.EventA, listener1.any());
    try emitter.addListener(.EventA, listener2.any());
    try emitter.addListener(.EventA, listener3.any());

    // Emit EventA - all three listeners should be called
    emitter.emit(.EventA);
    try std.testing.expect(state1 == 1);
    try std.testing.expect(state2 == 2);
    try std.testing.expect(state3 == 3);

    // Emit EventB - no listeners should be called
    emitter.emit(.EventB);
    try std.testing.expect(state1 == 1);
    try std.testing.expect(state2 == 2);
    try std.testing.expect(state3 == 3);

    // Emit EventA again - all three listeners should be called again
    emitter.emit(.EventA);
    try std.testing.expect(state1 == 2);
    try std.testing.expect(state2 == 4);
    try std.testing.expect(state3 == 6);

    // Remove one listener and test again
    const removed = emitter.removeListener(.EventA, listener2.any());
    try std.testing.expect(removed);

    emitter.emit(.EventA);
    try std.testing.expect(state1 == 3);
    try std.testing.expect(state2 == 4);
    try std.testing.expect(state3 == 9);
}

test "EventEmitter with multiple listeners for different events" {
    const allocator = std.testing.allocator;

    const MyEvent = enum {
        EventA,
        EventB,
        EventC,
    };

    var emitter = try EventEmitter(MyEvent).init(allocator);
    defer emitter.deinit();

    var eventA_count: u32 = 0;
    var eventB_count: u32 = 0;
    var all_events_count: u32 = 0;

    const EventAListener = struct {
        counter: *u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            if (event == .EventA) {
                self.counter.* += 1;
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

    const EventBListener = struct {
        counter: *u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            if (event == .EventB) {
                self.counter.* += 10; // Use different increment to distinguish
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

    const AllEventsListener = struct {
        counter: *u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            // This listener responds to all events
            switch (event) {
                .EventA, .EventB, .EventC => self.counter.* += 100,
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

    // Create listeners for different events
    var listenerA = EventAListener{ .counter = &eventA_count };
    var listenerB = EventBListener{ .counter = &eventB_count };
    var allListener1 = AllEventsListener{ .counter = &all_events_count };
    var allListener2 = AllEventsListener{ .counter = &all_events_count };

    // Add listeners to different events
    try emitter.addListener(.EventA, listenerA.any());
    try emitter.addListener(.EventB, listenerB.any());

    // Add the all-events listener to multiple events
    try emitter.addListener(.EventA, allListener1.any());
    try emitter.addListener(.EventB, allListener1.any());
    try emitter.addListener(.EventC, allListener1.any());

    // Add another all-events listener to EventA only
    try emitter.addListener(.EventA, allListener2.any());

    // Test EventA - should trigger EventA listener + 2 all-events listeners
    emitter.emit(.EventA);
    try std.testing.expect(eventA_count == 1);
    try std.testing.expect(eventB_count == 0);
    try std.testing.expect(all_events_count == 200);

    // Test EventB - should trigger EventB listener + 1 all-events listener
    emitter.emit(.EventB);
    try std.testing.expect(eventA_count == 1);
    try std.testing.expect(eventB_count == 10);
    try std.testing.expect(all_events_count == 300);

    // Test EventC - should trigger only 1 all-events listener
    emitter.emit(.EventC);
    try std.testing.expect(eventA_count == 1);
    try std.testing.expect(eventB_count == 10);
    try std.testing.expect(all_events_count == 400);

    // Remove one all-events listener from EventA and test again
    const removed = emitter.removeListener(.EventA, allListener2.any());
    try std.testing.expect(removed);

    emitter.emit(.EventA);
    try std.testing.expect(eventA_count == 2);
    try std.testing.expect(eventB_count == 10);
    try std.testing.expect(all_events_count == 500);
}

test "EventEmitter complex scenario with mixed listeners" {
    const allocator = std.testing.allocator;

    const MyEvent = enum {
        Connect,
        Disconnect,
        Message,
        Error,
    };

    var emitter = try EventEmitter(MyEvent).init(allocator);
    defer emitter.deinit();

    var connection_count: i32 = 0;
    var message_count: u32 = 0;
    var error_count: u32 = 0;
    var total_events: u32 = 0;

    const ConnectionListener = struct {
        counter: *i32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            switch (event) {
                .Connect => self.counter.* += 1,
                .Disconnect => self.counter.* -= 1,
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
            if (event == .Message) {
                self.counter.* += 1;
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
            if (event == .Error) {
                self.counter.* += 1;
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

    const TotalEventsListener = struct {
        counter: *u32,

        const Self = @This();

        pub fn handle(self: *Self, event: MyEvent) void {
            _ = event; // All events increment the counter
            self.counter.* += 1;
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
    var msgListener1 = MessageListener{ .counter = &message_count };
    var msgListener2 = MessageListener{ .counter = &message_count };
    var errListener = ErrorListener{ .counter = &error_count };
    var totalListener = TotalEventsListener{ .counter = &total_events };

    // Set up listeners
    try emitter.addListener(.Connect, connListener.any());
    try emitter.addListener(.Disconnect, connListener.any());
    try emitter.addListener(.Message, msgListener1.any());
    try emitter.addListener(.Message, msgListener2.any()); // Two listeners for Message
    try emitter.addListener(.Error, errListener.any());

    // Total event listener listens to all event types
    try emitter.addListener(.Connect, totalListener.any());
    try emitter.addListener(.Disconnect, totalListener.any());
    try emitter.addListener(.Message, totalListener.any());
    try emitter.addListener(.Error, totalListener.any());

    // Simulate a session
    emitter.emit(.Connect);
    try std.testing.expect(connection_count == 1);
    try std.testing.expect(message_count == 0);
    try std.testing.expect(error_count == 0);
    try std.testing.expect(total_events == 1);

    emitter.emit(.Message);
    try std.testing.expect(connection_count == 1);
    try std.testing.expect(message_count == 2);
    try std.testing.expect(error_count == 0);
    try std.testing.expect(total_events == 2);

    emitter.emit(.Message);
    try std.testing.expect(connection_count == 1);
    try std.testing.expect(message_count == 4);
    try std.testing.expect(error_count == 0);
    try std.testing.expect(total_events == 3);

    emitter.emit(.Error);
    try std.testing.expect(connection_count == 1);
    try std.testing.expect(message_count == 4);
    try std.testing.expect(error_count == 1);
    try std.testing.expect(total_events == 4);

    emitter.emit(.Disconnect);
    try std.testing.expect(connection_count == 0);
    try std.testing.expect(message_count == 4);
    try std.testing.expect(error_count == 1);
    try std.testing.expect(total_events == 5);

    // Remove one message listener and test
    _ = emitter.removeListener(.Message, msgListener1.any());

    emitter.emit(.Message);
    try std.testing.expect(connection_count == 0);
    try std.testing.expect(message_count == 5);
    try std.testing.expect(error_count == 1);
    try std.testing.expect(total_events == 6);
}
