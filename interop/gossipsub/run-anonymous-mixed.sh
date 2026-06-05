#!/usr/bin/env bash
# Scenario A — ANONYMOUS (StrictNoSign) cross-implementation gossipsub.
#
# Every node runs the anonymous signature policy: published messages carry NO
# author peer-id and NO signature, and dedup keys on a CONTENT-derived id rather
# than the from++seqno used under signing. All three impls are configured to use
# the SAME content id, sha256(topic || data):
#   zig : the anonymous policy's built-in id (rpc.contentMessageId).
#   go  : pubsub.WithMessageSignaturePolicy(StrictNoSign) + a WithMessageIdFn
#         that hashes topic then data.
#   rust: MessageAuthenticity::Anonymous + a message_id_fn that hashes topic
#         then data.
# If the ids did not agree, a relayed message would hash differently on each hop
# and dedup/propagation would break — so matching propagation across the mesh IS
# the test of the shared id scheme.
#
# Topology: a STAR (same as the base mixed mesh) crossing every impl boundary —
# a zig bootstrap with go, rust and zig leaves. One leaf publishes; the harness
# asserts (1) EVERY other node logged RECV of the payload, and (2) the received
# message has NO author (the RECV line's author= field is blank), confirming the
# anonymous wire format.

set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# Mixed anonymous mesh. impl per node; all run sign=anonymous (forced below).
NODES=(
    "zig:sub"   # node 0 — bootstrap
    "go:sub"    # node 1
    "rust:sub"  # node 2
    "go:sub"    # node 3
)
PUBLISHER_INDEX=1          # go leaf publishes (crosses go->zig->rust)

TOPIC="interop-anon-mesh"
MESSAGE="hello-anonymous"
BASE_PORT=4170
DURATION_MS=16000

build_all
install_cleanup

N=${#NODES[@]}
echo
echo "############################################################"
echo "# Anonymous mixed-mesh: $N nodes, StrictNoSign, content id"
echo "#   topic=$TOPIC message=$MESSAGE publisher=node $PUBLISHER_INDEX"
echo "############################################################"

NODE_IMPL=(); NODE_PORT=(); NODE_LOG=(); NODE_PID=()

# Bootstrap (node 0): listen + write addr; anonymous.
BOOT_IMPL="${NODES[0]%%:*}"
BOOT_PORT=$BASE_PORT
BOOT_ADDR_FILE="$(mktmp)"; BOOT_LOG="$(mktmp)"; : > "$BOOT_ADDR_FILE"
boot_mode="sub"; boot_msg=()
if [ "$PUBLISHER_INDEX" -eq 0 ]; then boot_mode="pub"; boot_msg=("message=$MESSAGE"); fi

echo "==> [node 0] bootstrap impl=$BOOT_IMPL sign=anonymous port=$BOOT_PORT"
"$(bin_for "$BOOT_IMPL")" \
    role=listen "mode=$boot_mode" "port=$BOOT_PORT" "topic=$TOPIC" \
    "addr_file=$BOOT_ADDR_FILE" "duration_ms=$DURATION_MS" sign=anonymous \
    "${boot_msg[@]+"${boot_msg[@]}"}" > "$BOOT_LOG" 2>&1 &
BOOT_PID=$!; track_pid "$BOOT_PID"
NODE_IMPL[0]="$BOOT_IMPL"; NODE_PORT[0]="$BOOT_PORT"; NODE_LOG[0]="$BOOT_LOG"; NODE_PID[0]="$BOOT_PID"

echo "==> Waiting for bootstrap address"
BOOT_ADDR="$(wait_for_addr "$BOOT_ADDR_FILE" "$BOOT_PID")"
if [ -z "$BOOT_ADDR" ]; then
    echo "FAIL: bootstrap never published its address" >&2
    cat "$BOOT_LOG" >&2
    exit 1
fi
echo "    bootstrap address: $BOOT_ADDR"

# Leaves dial the bootstrap; all anonymous.
for ((i = 1; i < N; i++)); do
    impl="${NODES[$i]%%:*}"; port=$((BASE_PORT + i)); log="$(mktmp)"
    mode="sub"; extra=()
    if [ "$PUBLISHER_INDEX" -eq "$i" ]; then mode="pub"; extra=("message=$MESSAGE"); fi
    echo "==> [node $i] impl=$impl sign=anonymous mode=$mode port=$port -> bootstrap"
    "$(bin_for "$impl")" \
        role=dial "mode=$mode" "port=$port" "peers=$BOOT_ADDR" "topic=$TOPIC" \
        "addr_file=/dev/null" "duration_ms=$DURATION_MS" sign=anonymous \
        "${extra[@]+"${extra[@]}"}" > "$log" 2>&1 &
    pid=$!; track_pid "$pid"
    NODE_IMPL[$i]="$impl"; NODE_PORT[$i]="$port"; NODE_LOG[$i]="$log"; NODE_PID[$i]="$pid"
done

echo "==> All $N nodes launched; waiting up to ${DURATION_MS}ms"
for ((i = 0; i < N; i++)); do wait "${NODE_PID[$i]}" 2>/dev/null; done

echo
echo "==> Per-node output"
for ((i = 0; i < N; i++)); do
    echo "------------------------------------------------------------"
    echo "node $i  impl=${NODE_IMPL[$i]}  port=${NODE_PORT[$i]}"
    echo "------------------------------------------------------------"
    cat "${NODE_LOG[$i]}"
done

echo
echo "============================================================"
echo "  Anonymous receipt table (topic=$TOPIC)"
echo "============================================================"
printf "  %-6s %-6s %-10s %s\n" "node" "impl" "role" "result"

OVERALL=0
for ((i = 0; i < N; i++)); do
    impl="${NODE_IMPL[$i]}"; role="leaf"; [ "$i" -eq 0 ] && role="bootstrap"
    if [ "$i" -eq "$PUBLISHER_INDEX" ]; then
        printf "  %-6s %-6s %-10s %s\n" "$i" "$impl" "$role" "SOURCE (publisher)"
        continue
    fi
    # PASS requires: a RECV of the payload, received:true, AND the RECV line's
    # author field empty (anonymous omits the author). The author= token is
    # immediately followed by a space when blank.
    if grep -q "RECV topic=$TOPIC.*data=$MESSAGE" "${NODE_LOG[$i]}" \
       && grep -q '"received":true' "${NODE_LOG[$i]}"; then
        if grep -q "RECV topic=$TOPIC author= " "${NODE_LOG[$i]}"; then
            printf "  %-6s %-6s %-10s %s\n" "$i" "$impl" "$role" "PASS (received, no author)"
        else
            printf "  %-6s %-6s %-10s %s\n" "$i" "$impl" "$role" "FAIL (received but author present)"
            OVERALL=1
        fi
    else
        printf "  %-6s %-6s %-10s %s\n" "$i" "$impl" "$role" "FAIL (no receipt)"
        OVERALL=1
    fi
done

echo "============================================================"
if [ "$OVERALL" -eq 0 ]; then
    echo "OVERALL: PASS (anonymous message propagated across the zig+go+rust StrictNoSign mesh to ALL nodes, no author on the wire, content-id dedup agreed)"
else
    echo "OVERALL: FAIL (anonymous propagation or author-omission assertion failed)"
fi
exit "$OVERALL"
