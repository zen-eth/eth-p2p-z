#!/usr/bin/env bash
# Multi-node MIXED-implementation gossipsub interop over real QUIC on localhost.
#
# Topology: a STAR. Node 0 is the bootstrap — it listens and writes its
# /p2p/<peer-id> multiaddr to a file. Every other node both listens (so it can
# be reached) and dials the bootstrap. gossipsub then forms a mesh over those
# connections; with N small (<= D, the target mesh degree of 6) the star meshes
# fully without pruning, so every subscriber ends up in one mesh.
#
# All nodes subscribe the same topic. After a settle period (a couple of
# heartbeats, ~1s each, for GRAFT + subscription propagation) ONE leaf node
# publishes its payload, republishing every 500ms so at least one publish lands
# after the mesh forms. The harness then asserts that EVERY OTHER node (across
# all three implementations) logged RECV of that payload.
#
# The implementation-per-node, the node count, and which node publishes are all
# editable below so the next scenario phase can vary the network shape. Each
# node is a real OS process; ports are distinct; everything is bounded by a
# duration and all leftover processes are killed on exit.

set -u

# ===========================================================================
# Network definition — EDIT HERE to vary impls / count / publisher.
#
# NODES: one entry per node, "impl:mode".
#   impl = zig | go | rust   mode = pub | sub
#   Node 0 is the bootstrap (listens, writes its addr). Every other node dials
#   the bootstrap. Keep the count <= the gossipsub mesh degree D (6) so the star
#   meshes fully without pruning.
#
# The default mix crosses every implementation boundary: a zig bootstrap with
# go, rust, zig and go leaves, so a published message must traverse
# zig <-> go <-> rust links to reach everyone.
# ===========================================================================
NODES=(
    "zig:sub"   # node 0 — bootstrap
    "go:sub"    # node 1
    "rust:sub"  # node 2
    "zig:sub"   # node 3
    "go:sub"    # node 4
)
# Index (into NODES) of the single node that publishes. Its mode is forced to
# pub regardless of the entry above. Pick a leaf (not the bootstrap) so the
# message has to propagate across the star to reach everyone, but bootstrap
# also works.
PUBLISHER_INDEX=2          # node 2 (rust) publishes

TOPIC="interop-mixed-mesh"
MESSAGE="hello-mixed-mesh"
BASE_PORT=4150             # node i listens on BASE_PORT + i
DURATION_MS=16000          # whole-run budget per node (settle + propagate)
PUBLISH_DELAY_MS=2500      # how long the publisher waits before its first publish

# ===========================================================================
# Paths + offline build environment
# ===========================================================================
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GS_DIR="$ROOT_DIR/interop/gossipsub"
GO_DIR="$GS_DIR/go-peer"
RUST_DIR="$GS_DIR/rust-peer"
ZIG_BIN="$ROOT_DIR/zig-out/bin/libp2p-gossipsub-interop"
GO_BIN="$GO_DIR/gspeer"
RUST_BIN="$RUST_DIR/target/release/gspeer"

# No network for go/cargo downloads here; deps are pinned to the local caches.
export GOPROXY=off
export GOSUMDB=off
export GOFLAGS=-mod=mod

# ===========================================================================
# Build all three peers
# ===========================================================================
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

# bin_for <impl> -> path to that implementation's binary
bin_for() {
    case "$1" in
        zig)  echo "$ZIG_BIN" ;;
        go)   echo "$GO_BIN" ;;
        rust) echo "$RUST_BIN" ;;
        *) echo "FAIL: unknown impl '$1'" >&2; exit 1 ;;
    esac
}

# ===========================================================================
# Process bookkeeping — kill ALL leftovers on exit.
# ===========================================================================
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
    f="$(mktemp -t "gs_mesh.XXXXXX")"
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

N=${#NODES[@]}
echo
echo "############################################################"
echo "# Mixed-mesh: $N nodes, star topology over loopback QUIC"
echo "#   topic=$TOPIC message=$MESSAGE"
echo "#   publisher = node $PUBLISHER_INDEX"
echo "############################################################"

# Per-node state, indexed by node number.
NODE_IMPL=()
NODE_MODE=()
NODE_PORT=()
NODE_LOG=()
NODE_PID=()

# ===========================================================================
# Launch the bootstrap (node 0): listen + write its addr.
# ===========================================================================
BOOT_IMPL="${NODES[0]%%:*}"
BOOT_PORT=$((BASE_PORT))
BOOT_ADDR_FILE="$(mktmp)"
BOOT_LOG="$(mktmp)"
: > "$BOOT_ADDR_FILE"

boot_mode="sub"
boot_msg_arg=()
if [ "$PUBLISHER_INDEX" -eq 0 ]; then
    boot_mode="pub"
    boot_msg_arg=("message=$MESSAGE")
fi

echo "==> [node 0] bootstrap impl=$BOOT_IMPL role=listen mode=$boot_mode port=$BOOT_PORT"
"$(bin_for "$BOOT_IMPL")" \
    role=listen "mode=$boot_mode" \
    "port=$BOOT_PORT" \
    "topic=$TOPIC" \
    "addr_file=$BOOT_ADDR_FILE" \
    "duration_ms=$DURATION_MS" \
    "${boot_msg_arg[@]+"${boot_msg_arg[@]}"}" \
    > "$BOOT_LOG" 2>&1 &
BOOT_PID=$!
PIDS+=("$BOOT_PID")

NODE_IMPL[0]="$BOOT_IMPL"
NODE_MODE[0]="$boot_mode"
NODE_PORT[0]="$BOOT_PORT"
NODE_LOG[0]="$BOOT_LOG"
NODE_PID[0]="$BOOT_PID"

echo "==> Waiting for bootstrap address"
BOOT_ADDR="$(wait_for_addr "$BOOT_ADDR_FILE" "$BOOT_PID")"
if [ -z "$BOOT_ADDR" ]; then
    echo "FAIL: bootstrap never published its address" >&2
    echo "--- bootstrap log ---" >&2; cat "$BOOT_LOG" >&2
    exit 1
fi
echo "    bootstrap address: $BOOT_ADDR"

# ===========================================================================
# Launch every leaf (nodes 1..N-1): listen on its own port AND dial bootstrap.
# A leaf both accepts and dials, so the mesh can span all of them.
# ===========================================================================
for ((i = 1; i < N; i++)); do
    impl="${NODES[$i]%%:*}"
    port=$((BASE_PORT + i))
    log="$(mktmp)"

    mode="sub"
    extra_args=()
    if [ "$PUBLISHER_INDEX" -eq "$i" ]; then
        mode="pub"
        extra_args=("message=$MESSAGE")
    fi

    echo "==> [node $i] impl=$impl role=dial mode=$mode port=$port -> bootstrap"
    "$(bin_for "$impl")" \
        role=dial "mode=$mode" \
        "port=$port" \
        "peers=$BOOT_ADDR" \
        "topic=$TOPIC" \
        "addr_file=/dev/null" \
        "duration_ms=$DURATION_MS" \
        "${extra_args[@]+"${extra_args[@]}"}" \
        > "$log" 2>&1 &
    pid=$!
    PIDS+=("$pid")

    NODE_IMPL[$i]="$impl"
    NODE_MODE[$i]="$mode"
    NODE_PORT[$i]="$port"
    NODE_LOG[$i]="$log"
    NODE_PID[$i]="$pid"
done

# Note on settle timing: the publisher does not gate on a separate signal; it
# republishes every 500ms for the whole run (see the node binaries). The run
# duration is generously longer than the mesh-formation time, so at least one
# publish lands after every subscriber has grafted. PUBLISH_DELAY_MS is reserved
# for a future phase that wants an explicit pre-publish settle; today the
# republish loop covers it.
echo "==> All $N nodes launched; waiting up to ${DURATION_MS}ms for mesh + propagation"

# ===========================================================================
# Wait for every node to finish (each self-bounds by duration_ms).
# ===========================================================================
for ((i = 0; i < N; i++)); do
    wait "${NODE_PID[$i]}" 2>/dev/null
done

# ===========================================================================
# Collect + assert. Every node EXCEPT the publisher must have received the
# payload. The publisher is reported as the source (no receipt expected).
# ===========================================================================
echo
echo "==> Per-node output"
for ((i = 0; i < N; i++)); do
    echo "------------------------------------------------------------"
    echo "node $i  impl=${NODE_IMPL[$i]}  mode=${NODE_MODE[$i]}  port=${NODE_PORT[$i]}"
    echo "------------------------------------------------------------"
    cat "${NODE_LOG[$i]}"
done

echo
echo "============================================================"
echo "  Mixed-mesh receipt table (topic=$TOPIC)"
echo "============================================================"
printf "  %-6s %-6s %-6s %-10s %s\n" "node" "impl" "mode" "role" "result"

OVERALL=0
for ((i = 0; i < N; i++)); do
    impl="${NODE_IMPL[$i]}"
    mode="${NODE_MODE[$i]}"
    role="leaf"
    [ "$i" -eq 0 ] && role="bootstrap"

    if [ "$i" -eq "$PUBLISHER_INDEX" ]; then
        printf "  %-6s %-6s %-6s %-10s %s\n" "$i" "$impl" "$mode" "$role" "SOURCE (publisher)"
        continue
    fi

    # A subscriber passes if it logged RECV of the payload and its final JSON
    # reports received:true.
    if grep -q "RECV topic=$TOPIC.*data=$MESSAGE" "${NODE_LOG[$i]}" \
       && grep -q '"received":true' "${NODE_LOG[$i]}"; then
        printf "  %-6s %-6s %-6s %-10s %s\n" "$i" "$impl" "$mode" "$role" "PASS (received)"
    else
        printf "  %-6s %-6s %-6s %-10s %s\n" "$i" "$impl" "$mode" "$role" "FAIL (no receipt)"
        OVERALL=1
    fi
done

echo "============================================================"
if [ "$OVERALL" -eq 0 ]; then
    echo "OVERALL: PASS (message from node $PUBLISHER_INDEX propagated across the mixed zig+go+rust QUIC mesh to ALL nodes)"
else
    echo "OVERALL: FAIL (at least one node did not receive the published message)"
fi
exit "$OVERALL"
