# Contributing to eth-p2p-z

Thank you for your interest in contributing. This document is the ground truth for how we build, test, review, and ship code in this project. Read it once before opening your first pull request.

## Table of Contents

- [Project Philosophy](#project-philosophy)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Code Style](#code-style)
- [Testing](#testing)
- [Pull Requests](#pull-requests)
- [Dependency Policy](#dependency-policy)
- [Attributing Copied Code](#attributing-copied-code)
- [Security Disclosures](#security-disclosures)
- [Zig Version Policy](#zig-version-policy)
- [Guidelines](#guidelines)
- [Acknowledgements](#acknowledgements)

---

## Project Philosophy

eth-p2p-z is a Zig implementation of the Ethereum peer-to-peer networking stack built on the libp2p architecture. Three things are true that shape every decision:

**Scope is fixed.** QUIC (lsquic-backed) is the only transport. TCP, WebRTC, and WebTransport are intentionally out of scope. If you are looking for a general-purpose multi-transport libp2p implementation, this is not it. Contributions that widen the scope beyond Ethereum networking will not be accepted.

**Safety before everything.** This is networking code that will run in Ethereum nodes. A bug here can partition or degrade the network. We treat correctness as a hard constraint, not a tradeoff.

**Design goals in order: Safety > Performance > Developer Experience.** When these goals conflict, the higher priority wins. A slow but correct implementation is always preferable to a fast but incorrect one. An unergonomic but safe API is always preferable to a convenient but dangerous one.

---

## Getting Started

### Prerequisites

- Zig **0.15.2** — no other version is supported. The CI pins to this version explicitly.
- A C toolchain (clang or gcc) for building `lsquic` and `boringssl`.
- `zlib` (system library).

On macOS:
```bash
brew install zlib
```

On Ubuntu/Debian:
```bash
apt install zlib1g-dev
```

### Clone and Build

```bash
git clone https://github.com/zen-eth/eth-p2p-z.git
cd eth-p2p-z
zig build -Doptimize=ReleaseSafe
```

### Run Tests

```bash
zig build test --summary all
```

### Run a Specific Test

```bash
zig build test -Dfilter="<test name>"
```

### Check Formatting

```bash
zig fmt --check .
```

### Reformat

```bash
zig fmt .
```

### Build Options

| Option | Values | Description |
|--------|--------|-------------|
| `-Doptimize` | `Debug`, `ReleaseSafe`, `ReleaseFast`, `ReleaseSmall` | Build mode |
| `-Dlibxev-backend` | `io_uring`, `epoll`, `kqueue`, `wasi_poll`, `iocp` | Override libxev backend |
| `-Dlog-level` | `debug`, `info`, `warn`, `err` | Set log verbosity |

### Run the Transport Interop Binary

The `interop/` directory contains a binary for cross-implementation transport testing:

```bash
zig build transport-interop
./zig-out/bin/libp2p-transport-interop
```

---

## Development Workflow

### Branching

Branch off `main`. Use a short, lowercase, hyphen-separated name that describes what the branch does:

```
feat/gossipsub-v1.1
fix/quic-stream-close-race
refactor/identify-handler
```

Do not use ticket numbers as the entire branch name. The name should be readable on its own.

### Commit Messages

Commit messages are the permanent record of why code changed. Pull request descriptions are not — they are invisible in `git blame` and `git log`. Write your rationale in the commit message.

**Format:**

```
<short summary in imperative mood, 72 chars max>

<body: explain why this change is necessary and what alternatives
were considered. Reference relevant issues, specs, or prior art.
The summary line states what changed; the body explains why.>
```

**Good:**
```
Use fixed-size ring buffer for pending stream queue

Growing the queue dynamically during operation requires allocation
that can fail at an unpredictable time. A fixed upper bound makes
the worst-case memory usage explicit and eliminates OOM during
stream handling. The bound is derived from the QUIC stream concurrency
limit, so no valid workload can exceed it.
```

**Bad:**
```
fix bug in stream queue
```

The summary line must be in the imperative mood ("add", "fix", "remove", not "added", "fixed", "removed").

### Keeping Your Branch Current

Rebase onto `main` rather than merging. This keeps the commit history linear and makes `git blame` useful:

```bash
git fetch origin
git rebase origin/main
```

---

## Code Style

The codebase follows [TigerStyle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md) with the project-specific adjustments documented here. When in doubt, the TigerStyle document is the authoritative reference.

### The Short Version

Run `zig fmt .` before every commit. Write at least two assertions per function. Explain why in comments. Do not store allocators in structs. Keep functions under 70 lines.

### Formatting

- **Run `zig fmt .` before every commit.** The CI will reject unformatted code.
- **100-column line limit.** No exceptions. Use a column ruler in your editor. To wrap a long function signature or struct literal, add a trailing comma and let `zig fmt` handle the rest.
- **4-space indentation**, not 2.
- **Always add braces to `if` statements**, even single-statement bodies. The one exception is a single-line assertion implication: `if (condition) assert(invariant);`

### Naming

| Subject | Convention | Example |
|---------|-----------|---------|
| Functions | `snake_case` | `read_message` |
| Variables | `snake_case` | `message_count` |
| Types and structs | `PascalCase` | `QuicStream` |
| Files | `snake_case` | `quic_stream.zig` |
| Acronyms in types | All-caps | `VSRState`, `QUICTransport` |

**Do not abbreviate.** Write `message`, not `msg`. Write `connection`, not `conn`. Write `allocator`, not `alloc`. The single-letter loop index `i` is the only accepted abbreviation.

**Put units and qualifiers last, ordered by descending significance.**
`timeout_ms_max` not `max_timeout_ms`. This keeps related variables visually aligned when you add `timeout_ms_min` later.

**Use names that carry semantic weight.** `gpa: Allocator` and `arena: Allocator` are better than `allocator: Allocator` because they communicate lifetime and whether `deinit` needs to be called explicitly.

**Name helpers after their caller.** When a function delegates to a helper, prefix the helper with the caller's name: `connect` calls `connect_callback` and `connect_validate`. This makes the call graph legible from names alone.

**Callbacks go last** in parameter lists. They are invoked last; placing them last mirrors the control flow.

### Assertions

Assertions are not optional. They are the primary mechanism for detecting programmer errors before they become production failures. The assertion density must average **at least two assertions per function**.

```zig
pub fn send(self: *Stream, data: []const u8) !void {
    assert(data.len > 0);                          // precondition
    assert(self.state == .open);                   // precondition
    // ... send logic ...
    assert(self.bytes_sent_total >= data.len);     // postcondition
}
```

Rules for assertions:

- **Split compound assertions.** Write `assert(a); assert(b);` not `assert(a and b);`. Split assertions identify which condition failed.
- **Pair assertions.** For every invariant, find two code paths to assert it — for example, once before writing to the wire and once after reading back.
- **Assert compile-time constants.** Use `comptime { assert(@sizeOf(Header) == 64); }` to document and enforce layout invariants before the program runs.
- **Assert the positive and the negative space.** Assert what you expect to be true AND what you expect to be false. Bugs live at the boundary.
- **Use single-line `if` for implications.** `if (is_leader) assert(has_quorum);`

### Control Flow

- **No recursion.** Recursion makes bounding execution depth impossible. Use explicit stacks or iterative traversal.
- **Put a limit on everything.** Every loop must have an explicit upper bound. Every queue must have a fixed maximum capacity.
- **Split compound conditions.** Replace `if (a and b)` with nested `if/else` branches. Every branch combination must be handled or asserted.
- **State invariants positively.** `if (index < length)` is easier to reason about than `if (index >= length)`.
- **Handle both sides.** Ask yourself whether a lone `if` should also have an `else`. Missing else branches are where "goto fail"-style bugs are born.
- **Maximum 70 lines per function.** If a function exceeds 70 lines, split it. Keep control flow (`if`, `switch`) in the parent function. Move self-contained logic into pure helper functions.

### Error Handling

All errors must be handled. Silently discarding errors is not permitted.

```zig
// Good: propagate
try stream.write(data);

// Good: handle explicitly
stream.write(data) catch |err| switch (err) {
    error.BrokenPipe => return self.close(),
    error.WouldBlock => return error.StreamStalled,
};

// Bad: silently discard
_ = stream.write(data) catch {};
```

**Declare the narrowest possible error set.** Return `error{OutOfMemory}!Self` rather than `anyerror`. Narrow error sets are self-documenting and force exhaustive handling at call sites.

**Use exhaustive `switch` on error values.** Avoid bare `else =>` in error switches — it silently swallows new error cases added in the future.

**Use `catch unreachable` only when prior validation has made the error genuinely impossible, and always add a comment explaining why:**

```zig
// Already validated by parse_address() above.
const addr = Multiaddr.fromBytes(raw) catch unreachable;
```

### Memory Management

- **Pass allocators as function parameters. Never store them in structs.**

  ```zig
  // Good
  pub fn init(allocator: Allocator, options: Options) !Self { ... }
  pub fn deinit(self: *Self, allocator: Allocator) void { ... }

  // Bad
  const Self = struct {
      allocator: Allocator, // do not do this
  };
  ```

- **Use `errdefer` for every `try` that acquires a resource.** If a later step in `init` fails, earlier resources must be released.

  ```zig
  pub fn init(allocator: Allocator) !Self {
      var index = try Index.init(allocator);
      errdefer index.deinit(allocator);

      var buffer = try allocator.alloc(u8, BUFFER_SIZE);
      errdefer allocator.free(buffer);

      return Self{ .index = index, .buffer = buffer };
  }
  ```

- **Reverse initialization order in `deinit`.** Poison the struct after freeing: `self.* = undefined;`. This causes any use-after-free to crash immediately rather than corrupt silently.

- **Visually pair allocation with its `defer` or `errdefer`.** Put a blank line before the allocation and the cleanup immediately after. This makes leaks visible during code review.

- **Pre-allocate capacity at initialization; operate infallibly at runtime.** Allocations that can fail belong in `init`. Operations on hot paths must not fail with `OutOfMemory` mid-stream.

### Types

- **Use explicitly-sized integer types.** Use `u32`, `u64`, `i32`, `i64`. Do not use `usize` unless interfacing with the Zig standard library or a C API that requires it.
- **Include units in variable names.** `timeout_ms`, `buffer_size_bytes`, `offset_slots`. A bare `timeout` is ambiguous.
- **Treat `index`, `count`, and `size` as distinct concepts.** An `index` is 0-based. A `count` is `index + 1`. A `size` is `count * @sizeOf(T)`. Name them accordingly and never mix them without an explicit conversion.

### Comments

- **Explain why, not what.** The code already says what it does. Comments add the reasoning that is invisible from the code.
- **Comments are full sentences.** Capital letter, full stop (or colon if followed by something). `// We buffer events here to amortize syscall overhead.` not `// buffer events`.
- **Short end-of-line comments may be phrases**, with no punctuation. Save full prose for block comments above the code.
- **Do not comment self-evident code.** `// increment counter` above `count += 1;` is noise.

### Struct Layout

Order struct members as: **fields, then type declarations, then methods**.

```zig
const Connection = struct {
    // Fields first.
    state: State,
    peer_id: PeerId,
    bytes_sent_total: u64,

    // Type declarations second.
    const State = enum { handshaking, open, closing, closed };
    const Self = @This();

    // Methods last.
    pub fn init(allocator: Allocator, peer_id: PeerId) !Self { ... }
    pub fn deinit(self: *Self, allocator: Allocator) void { ... }
    pub fn send(self: *Self, data: []const u8) !void { ... }
};
```

### Import Order

```zig
const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const Allocator = std.mem.Allocator;

// Third-party dependencies.
const xev = @import("xev");
const multiaddr = @import("multiaddr");

// Project modules.
const libp2p = @import("../root.zig");
const conn = libp2p.conn;

// Scoped logger for this module.
const log = std.log.scoped(.quic_transport);
```

Every module that emits log messages must declare a scoped logger. This makes log output filterable and traceable back to its origin.

---

## Testing

### Requirements

Every non-trivial change must include tests. The bar is:

- New public functions must have at least one test exercising the happy path and at least one test exercising a failure or boundary condition.
- Bug fixes must include a regression test that would have caught the bug.
- Refactors that do not change behavior do not require new tests, but must not break existing ones.

### Running Tests

```bash
# All tests
zig build test --summary all

# A specific test by name filter
zig build test -Dfilter="<test name>"
```

### Writing Tests

Tests live in the same file as the code they test, in `test` blocks at the bottom of the file. Use descriptive test names:

```zig
test "QuicStream: send returns BrokenPipe after remote close" {
    // ...
}
```

Use `std.testing.expectEqual`, `std.testing.expectError`, and `std.testing.expectEqualSlices` rather than plain `assert` inside tests. The testing functions produce actionable failure messages; `assert` does not.

Test both valid and invalid inputs. The assertion golden rule applies here too: test the positive space (valid data is accepted) and the negative space (invalid data is rejected).

### Interop Testing

The `interop/transport/` binary is used for cross-implementation transport tests against other libp2p implementations. To build and run it:

```bash
zig build transport-interop
./zig-out/bin/libp2p-transport-interop
```

Changes that affect the QUIC transport or protocol negotiation layer should be verified against the interop suite before opening a pull request.

---

## Pull Requests

### Before Opening a PR

Run the full local check sequence:

```bash
zig fmt .                           # format
zig build test --summary all        # unit tests
zig build -Doptimize=ReleaseSafe    # release build
```

All three must pass. The CI enforces the same three steps and will block the merge if any fail.

### PR Description

Write a description that explains:

1. **What** the change does (one sentence or short list).
2. **Why** the change is necessary (the motivation — what problem it solves or what invariant it establishes).
3. **How** you tested it.

The description does not replace commit messages. Design rationale belongs in the commit history where `git blame` can find it.

### Reviewer Assignment

**Assign a single reviewer.** Do not use GitHub's "request review" — use "assign". The difference matters: "assign" is permanent until the PR is merged or closed, making the reviewer co-responsible for the PR's progress. "Request review" clears after each round, which diffuses responsibility.

**The author chooses the reviewer.** Pick the person best positioned to evaluate correctness, share knowledge, or balance review load.

### Merging

Once a PR has an approving review, **the author clicks "merge when ready"**. The "merge when ready" button can be engaged before the review is complete — the PR will merge automatically once it is approved. This reduces round-trips.

**Squash merges are not used.** Each commit in a PR is preserved. Write commits that make sense individually in the history.

### Pre-Submit Checklist

Before requesting review, check each item:

**Safety**
- [ ] Every function has at least 2 assertions (preconditions, postconditions, or invariants)
- [ ] No recursion; all traversals use explicit stacks or iteration
- [ ] All loops have an explicit upper bound
- [ ] No errors are silently discarded
- [ ] Explicit integer types used (`u32`/`u64`, not `usize`)

**Memory**
- [ ] `errdefer` paired with every `try` that acquires a resource in `init`
- [ ] `deinit` releases resources in reverse initialization order
- [ ] No allocator stored in a struct field
- [ ] No dynamic allocation in hot paths (pre-allocated at init)

**Control Flow**
- [ ] Functions are under 70 lines
- [ ] Compound `if` conditions are split into nested `if/else`
- [ ] Every `if` branch that can fail has a corresponding `else` or `assert`
- [ ] Braces on all `if` statements

**Naming and Style**
- [ ] `snake_case` for functions, variables, and files
- [ ] No abbreviations (except loop index `i`)
- [ ] Units included in variable names where relevant
- [ ] Related variables use same-length names for visual alignment

**Formatting**
- [ ] `zig fmt .` has been run
- [ ] All lines are under 100 columns
- [ ] Comments are full sentences explaining why

**Tests**
- [ ] New public functions have tests for the happy path and at least one failure case
- [ ] Bug fixes have a regression test

---

## Dependency Policy

This project has C dependencies (`lsquic`, `boringssl`, `zlib`). This is a deliberate exception to the zero-dependency ideal — QUIC and TLS implementations of production quality are not available as pure Zig libraries today, and writing them from scratch is out of scope.

**Adding a new dependency requires discussion before implementation.** Open an issue first. A new dependency must clear a high bar:

- No pure-Zig alternative exists or is practical.
- The dependency has a track record in production systems.
- The dependency has a permissive license compatible with the project (MIT, BSD, Apache 2.0).
- The security surface introduced by the dependency is understood and acceptable.

**New pure-Zig dependencies** are evaluated on a case-by-case basis. The same license and quality bar applies. Prefer pulling in a small amount of well-understood code over taking a large dependency.

**Vendored C code** is not accepted. All C dependencies are fetched via `build.zig.zon` with pinned commit hashes.

---

## Attributing Copied Code

When code is copied or adapted from another project, attribute it at the top of the file or the relevant function. Include the source project, the license, and a URL:

```zig
/// Copied from https://github.com/mitchellh/libxev under the MIT license.
```

If an entire file originates from another project, put the attribution in the file's opening doc comment. If only a function or block is borrowed, put the attribution directly above that code.

Do not copy code from projects with licenses incompatible with this project's license without explicit maintainer approval.

---

## Security Disclosures

eth-p2p-z is networking code for Ethereum nodes. Vulnerabilities here can affect the liveness, partitioning resistance, or peer discovery of the network.

**Do not open a public GitHub issue for security vulnerabilities.**

Report security vulnerabilities by emailing the maintainers at the address listed in the GitHub repository's security policy. If no security policy is listed yet, contact the repository owners directly via GitHub.

Include in your report:

- A clear description of the vulnerability and the conditions required to trigger it.
- The potential impact on Ethereum node operators using this library.
- Steps to reproduce or a proof-of-concept if possible.
- Whether you are aware of any active exploitation.

We will acknowledge receipt within 48 hours and aim to issue a fix within 14 days for critical issues. We will credit reporters in the release notes unless you request otherwise.

---

## Zig Version Policy

The project targets **Zig 0.15.2**. CI pins to this version. Do not submit code that requires a newer version of Zig without first opening a discussion about upgrading the project-wide toolchain. Toolchain upgrades affect every contributor and dependent project and are done as a coordinated change.

---

## Guidelines

- **Read the [libp2p spec](https://github.com/libp2p/specs)** before working on protocol-level code. Deviations from spec must be justified in the PR description with a reference to the relevant section.
- **For general questions and design discussions**, use [GitHub Discussions](https://github.com/zen-eth/eth-p2p-z/discussions/landing).
- **For bug reports**, open a [GitHub Issue](https://github.com/zen-eth/eth-p2p-z/issues). Include a minimal reproducer.
- **For development questions**, join the [Zig Discord](https://discord.gg/YCYHpkTq) — the `#networking` or relevant channel is a good place to ask before investing time in an implementation.
- **Ensure you can legally contribute.** This project uses the [Developer Certificate of Origin (DCO)](https://developercertificate.org/). By submitting a pull request you certify that you have the right to contribute the code under this project's license. Sign your commits with `git commit -s`.
- **Get in touch with the maintainers early** for anything larger than a bug fix. Opening a discussion or a draft PR before writing code saves everyone time.
- **No drive-by contributions seeking to collect airdrops.** Many projects reward contributors to common goods — that is a good thing. However, it creates an incentive for low-effort PRs submitted solely to claim rewards rather than to improve the software. These PRs consume maintainer time with little benefit to users. If we believe a PR falls into this category we may close it without comment. If you think that was done in error, contact us via GitHub and reference this section explaining why your contribution is not a drive-by.
- **Have fun.** eth-p2p-z is an ambitious project at the intersection of Ethereum and Zig. We are glad you are here.

---

## Acknowledgements

This project and its contribution practices draw on the work of:

- [TigerBeetle](https://github.com/tigerbeetle/tigerbeetle) — TigerStyle engineering methodology: the Safety > Performance > DX priority order, assertion discipline, and zero-technical-debt policy that shape how we write and review code.
- [rust-libp2p](https://github.com/libp2p/rust-libp2p) — contributor workflow and protocol design process for a production P2P networking library.
- [Lighthouse](https://github.com/sigp/lighthouse) — Ethereum client contributor practices for adversarial-network software.
