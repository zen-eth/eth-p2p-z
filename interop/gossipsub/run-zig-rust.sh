#!/usr/bin/env bash
# Cross-implementation gossipsub interop: a real rust-libp2p node talking to our
# zig gossipsub node over loopback QUIC. Two scenarios run back to back:
#
#   PRIMARY (rust -> zig): the ZIG node listens + subscribes; the RUST node dials
#     + publishes (Signed / StrictSign). Asserts the zig subscriber logs
#     RECV ... data=<message> and received:true. This exercises negotiation, RPC
#     framing, subscription propagation, GRAFT/PRUNE, and message receipt with
#     rust-libp2p's signed Message on the wire.
#
#   REVERSE (zig -> rust): the RUST node listens + subscribes; the ZIG node dials
#     + publishes SIGNED (StrictSign). rust-libp2p in its default Signed mode
#     ACCEPTS the zig message because zig signs per the libp2p pubsub scheme.
#     Asserts rust logs received:true. This proves the zig-side signing matches
#     libp2p byte-for-byte (rust verifies the signature + the from/key binding).
#
# Everything is bounded by duration and leftover processes are killed on exit.

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GS_DIR="$ROOT_DIR/interop/gossipsub"
RUST_DIR="$GS_DIR/rust-peer"
ZIG_BIN="$ROOT_DIR/zig-out/bin/libp2p-gossipsub-interop"
RUST_BIN="$RUST_DIR/target/release/gspeer"

TOPIC="interop-test"
MESSAGE="hello-from-rust"

# This environment has no network for crates.io downloads; the rust peer's deps
# are pinned to versions present in the local cargo registry cache, so build
# offline.
CARGO_OFFLINE_FLAG="--offline"

# --- build both peers ------------------------------------------------------
echo "==> Building zig gossipsub interop binary"
( cd "$ROOT_DIR" && zig build install )
if [ ! -x "$ZIG_BIN" ]; then
    echo "FAIL: zig binary not found at $ZIG_BIN" >&2
    exit 1
fi

echo "==> Building rust-libp2p peer (release, offline)"
( cd "$RUST_DIR" && cargo build --release $CARGO_OFFLINE_FLAG )
if [ ! -x "$RUST_BIN" ]; then
    echo "FAIL: rust peer not found at $RUST_BIN" >&2
    exit 1
fi

# --- process bookkeeping ---------------------------------------------------
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
trap cleanup EXIT

mktmp() {
    local f
    f="$(mktemp -t "gs_interop.XXXXXX")"
    TMPFILES+=("$f")
    echo "$f"
}

# wait_for_addr <addr_file> <pid> -> echoes the address, or empty on timeout
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

OVERALL=0

# ===========================================================================
# PRIMARY: rust publishes -> zig subscribes
# ===========================================================================
echo
echo "############################################################"
echo "# PRIMARY scenario: rust-libp2p publishes -> zig receives"
echo "############################################################"

ZIG_ADDR_FILE="$(mktmp)"
ZIG_LOG="$(mktmp)"
RUST_LOG="$(mktmp)"
: > "$ZIG_ADDR_FILE"

echo "==> Starting ZIG node (role=listen mode=sub) on port 4121"
GOSSIPSUB_INTEROP_DEBUG=1 "$ZIG_BIN" \
    role=listen mode=sub \
    port=4121 \
    "topic=$TOPIC" \
    "addr_file=$ZIG_ADDR_FILE" \
    duration_ms=14000 \
    > "$ZIG_LOG" 2>&1 &
ZIG_PID=$!
PIDS+=("$ZIG_PID")

echo "==> Waiting for zig listener address"
ZIG_ADDR="$(wait_for_addr "$ZIG_ADDR_FILE" "$ZIG_PID")"
if [ -z "$ZIG_ADDR" ]; then
    echo "FAIL: zig listener never published its address" >&2
    echo "--- zig log ---" >&2; cat "$ZIG_LOG" >&2
    exit 1
fi
echo "    zig address: $ZIG_ADDR"

echo "==> Starting RUST node (role=dial mode=pub, Signed) -> zig"
"$RUST_BIN" \
    role=dial mode=pub \
    "peer=$ZIG_ADDR" \
    "topic=$TOPIC" \
    "message=$MESSAGE" \
    duration_ms=12000 \
    > "$RUST_LOG" 2>&1 &
RUST_PID=$!
PIDS+=("$RUST_PID")

echo "==> Waiting for both nodes to finish"
wait "$RUST_PID" 2>/dev/null
wait "$ZIG_PID" 2>/dev/null

echo
echo "--- zig (subscriber) output ---"
cat "$ZIG_LOG"
echo "--- rust (publisher) output ---"
cat "$RUST_LOG"
echo

if grep -q "RECV topic=$TOPIC.*data=$MESSAGE" "$ZIG_LOG" \
   && grep -q '"role":"listen".*"received":true' "$ZIG_LOG"; then
    echo "PASS (PRIMARY): zig node received a message published by rust-libp2p over QUIC"
else
    echo "FAIL (PRIMARY): zig node did not receive the rust-libp2p message" >&2
    OVERALL=1
fi

# ===========================================================================
# REVERSE: zig publishes (StrictSign) -> rust subscribes. With zig-side signing
# rust's default Signed mode now ACCEPTS the message. This is the acceptance
# test proving zig's signing matches libp2p.
# ===========================================================================
echo
echo "############################################################"
echo "# REVERSE scenario: zig publishes (signed) -> rust receives"
echo "############################################################"

RUST_ADDR_FILE="$(mktmp)"
RUST_LOG2="$(mktmp)"
ZIG_LOG2="$(mktmp)"
: > "$RUST_ADDR_FILE"

echo "==> Starting RUST node (role=listen mode=sub, Signed) on port 4122"
"$RUST_BIN" \
    role=listen mode=sub \
    port=4122 \
    "topic=$TOPIC" \
    "addr_file=$RUST_ADDR_FILE" \
    duration_ms=12000 \
    > "$RUST_LOG2" 2>&1 &
RUST_PID2=$!
PIDS+=("$RUST_PID2")

echo "==> Waiting for rust listener address"
RUST_ADDR="$(wait_for_addr "$RUST_ADDR_FILE" "$RUST_PID2")"
if [ -z "$RUST_ADDR" ]; then
    echo "FAIL (REVERSE): rust listener never published its address" >&2
    cat "$RUST_LOG2" >&2
    OVERALL=1
else
    echo "    rust address: $RUST_ADDR"
    echo "==> Starting ZIG node (role=dial mode=pub) -> rust"
    "$ZIG_BIN" \
        role=dial mode=pub \
        "peer=$RUST_ADDR" \
        "topic=$TOPIC" \
        "message=hello-from-zig" \
        duration_ms=10000 \
        > "$ZIG_LOG2" 2>&1 &
    ZIG_PID2=$!
    PIDS+=("$ZIG_PID2")

    wait "$ZIG_PID2" 2>/dev/null
    wait "$RUST_PID2" 2>/dev/null

    echo
    echo "--- rust (subscriber) output ---"
    cat "$RUST_LOG2"
    echo "--- zig (publisher) output ---"
    cat "$ZIG_LOG2"
    echo

    if grep -q "RECV topic=$TOPIC.*data=hello-from-zig" "$RUST_LOG2" \
       && grep -q '"role":"listen".*"received":true' "$RUST_LOG2"; then
        echo "PASS (REVERSE): rust-libp2p (Signed) accepted the zig-published signed message"
    else
        echo "FAIL (REVERSE): rust did NOT receive the zig signed message" >&2
        OVERALL=1
    fi
fi

echo
if [ "$OVERALL" -eq 0 ]; then
    echo "OVERALL: PASS (both directions: zig<->rust signed message receipt verified)"
else
    echo "OVERALL: FAIL (a direction did not succeed)"
fi
exit "$OVERALL"
