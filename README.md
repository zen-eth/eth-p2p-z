# zig-libp2p
Zig implementation of [libp2p](https://libp2p.io/), a modular network stack that allows you to build your own peer-to-peer applications.

**Note**: This is a work in progress and not yet ready for production use. The API may change frequently as we iterate on the design and implementation.

## Prerequisites
- Zig 0.14.0

## Building
To build the project, run the following command in the root directory of the project:

```bash
zig build -Doptimize=ReleaseFast
```

## Running Tests
To run the tests, run the following command in the root directory of the project:

```bash
zig build test --summary all
```