# eth-p2p-z

Zig implementation of the Ethereum peer-to-peer stack, built on top of the [libp2p](https://libp2p.io/) architecture.

**Note**: This project is pre-release software. Expect rapid iteration and frequent breaking API changes while we carve out the Ethereum-focused feature set.

## Project scope

- Transport: QUIC (lsquic-backed) is the only supported transport. TCP, WebRTC, WebTransport, and other stacks are intentionally out of scope.
- PubSub: Gossipsub v1.0 router is available today; additional Ethereum networking protocols will be layered on top in subsequent milestones.
- Platform: Zig 0.14.1 toolchain targeting modern desktop/server environments. Browser runtimes are not supported.

If you are looking for a general-purpose libp2p implementation with multiple transports and protocol stacks, this project is not a drop-in replacement.

## Prerequisites

- Zig 0.14.1

## Building

To build the project, run the following command in the root directory of the project:

```bash
zig build -Doptimize=ReleaseSafe
```

## Running Tests

To run the tests, run the following command in the root directory of the project:

```bash
zig build test --summary all
```

## Usage

Update `build.zig.zon`:

```sh
zig fetch --save git+https://github.com/zen-eth/zig-libp2p.git
```

In your `build.zig`:

```zig
const libp2p_dep = b.dependency("libp2p", .{
    .target = target,
    .optimize = optimize,
});
const libp2p_module = libp2p_dep.module("zig-libp2p");
root_module.addImport("libp2p", libp2p_module);
```
