#!/usr/bin/env bash
# Shared helpers for the cross-implementation gossipsub interop scenario scripts.
# Sourced by run-anonymous-mixed.sh, run-version-fallback.sh, run-churn.sh and
# run-gossip-large.sh. It builds the three peer binaries (zig + go-libp2p +
# rust-libp2p), resolves an impl name to its binary, manages process/temp-file
# bookkeeping with a kill-everything-on-exit trap, and waits for a listener to
# publish its multiaddr.
#
# Every scenario launches real OS processes on loopback QUIC (no Shadow), bounds
# each run by a duration, and kills all leftovers on exit. The node binaries all
# share the key=value arg convention: role=listen|dial mode=pub|sub port= peer=
# peers= addr_file= topic= message= duration_ms= sign=strict|anonymous
# protocols=  (protocols=meshsub/1.1.0 restricts a go/rust peer to 1.1.0).

# Resolve paths relative to this file (interop/gossipsub/) up to the repo root.
GS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$GS_DIR/../.." && pwd)"
GO_DIR="$GS_DIR/go-peer"
RUST_DIR="$GS_DIR/rust-peer"
ZIG_BIN="$ROOT_DIR/zig-out/bin/libp2p-gossipsub-interop"
GO_BIN="$GO_DIR/gspeer"
RUST_BIN="$RUST_DIR/target/release/gspeer"

# Offline build environment (no network for go/cargo downloads here).
export GOPROXY=off
export GOSUMDB=off
export GOFLAGS=-mod=mod

# build_all builds zig + go + rust and verifies each binary exists.
build_all() {
    echo "==> Building zig gossipsub interop binary"
    ( cd "$ROOT_DIR" && zig build install )
    if [ ! -x "$ZIG_BIN" ]; then
        echo "FAIL: zig binary not found at $ZIG_BIN" >&2
        exit 1
    fi

    echo "==> Building go-libp2p peer (offline)"
    ( cd "$GO_DIR" && go build -o gspeer . )
    if [ ! -x "$GO_BIN" ]; then
        echo "FAIL: go peer not found at $GO_BIN" >&2
        exit 1
    fi

    echo "==> Building rust-libp2p peer (release, offline)"
    ( cd "$RUST_DIR" && cargo build --release --offline )
    if [ ! -x "$RUST_BIN" ]; then
        echo "FAIL: rust peer not found at $RUST_BIN" >&2
        exit 1
    fi
}

# bin_for <impl> -> path to that implementation's binary.
bin_for() {
    case "$1" in
        zig)  echo "$ZIG_BIN" ;;
        go)   echo "$GO_BIN" ;;
        rust) echo "$RUST_BIN" ;;
        *) echo "FAIL: unknown impl '$1'" >&2; exit 1 ;;
    esac
}

# Process + temp-file bookkeeping. Each scenario registers its PIDs/temp files
# here (or via track_pid / mktmp) and install_cleanup wires the EXIT trap so a
# crash or early exit still kills every child and removes the temp files.
PIDS=()
TMPFILES=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null
    for f in "${TMPFILES[@]:-}"; do
        [ -n "$f" ] && rm -f "$f"
    done
}

install_cleanup() {
    trap cleanup EXIT
}

# track_pid <pid> records a PID for teardown.
track_pid() { PIDS+=("$1"); }

# mktmp creates a tracked temp file and echoes its path.
mktmp() {
    local f
    f="$(mktemp -t "gs_scenario.XXXXXX")"
    TMPFILES+=("$f")
    echo "$f"
}

# wait_for_addr <addr_file> <pid> -> echoes the listener's multiaddr, or empty
# on timeout / process death. Polls up to ~10s.
wait_for_addr() {
    local addr_file="$1" pid="$2" waited=0 addr=""
    while [ "$waited" -lt 100 ]; do
        if [ -s "$addr_file" ]; then
            addr="$(head -n1 "$addr_file")"
            [ -n "$addr" ] && { echo "$addr"; return 0; }
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            return 1
        fi
        sleep 0.1
        waited=$((waited + 1))
    done
    return 1
}
