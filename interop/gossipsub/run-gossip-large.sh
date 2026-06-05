#!/usr/bin/env bash
# Scenario D (best-effort) — GOSSIP REACH + LARGE MESSAGE (IDONTWANT path).
#
# Two parts, each with its own PASS/FAIL; OVERALL passes only if both pass.
#
# D1 — Gossip reach beyond the publisher's direct mesh:
#   A larger STAR with N (= 9) > the mesh degree D (6): a zig bootstrap relay and
#   eight leaves across go/rust/zig. A LEAF publishes. A leaf is connected ONLY to
#   the bootstrap, so the other leaves are NOT in the publisher leaf's direct mesh
#   — the publisher cannot reach them directly. They receive only because the
#   bootstrap relay forwards the message onward, i.e. they are reachable via the
#   gossip/relay layer rather than a direct publisher↔subscriber mesh link. We
#   assert every non-publisher node receives.
#   HONESTY NOTE: this demonstrates relay-based reach beyond the publisher's own
#   mesh. It exercises the gossip/forwarding path, but it does NOT prove the final
#   hop used IHAVE→IWANT specifically rather than mesh forwarding — isolating the
#   IWANT path would need gossip trace logging the nodes do not emit by default.
#
# D2 — Large message (gossipsub v1.2 IDONTWANT threshold):
#   All nodes speak v1.2 (the default). The publisher sends a payload LARGER than
#   the 1 KiB IDONTWANT threshold, so on first receipt a v1.2 node may broadcast
#   an IDONTWANT to its mesh peers ("I already have this large message"). We
#   assert the large message still propagates correctly to every node — i.e. the
#   IDONTWANT path does not break delivery. Whether an IDONTWANT was actually
#   emitted is not separately asserted (no control-message trace logging); this is
#   a delivery-correctness check for the large-message path.

set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

build_all
install_cleanup

# launch_star <topic> <message> <pub_index> <duration_ms> <impl...> -> sets the
# globals STAR_PIDS / STAR_LOGS / STAR_IMPLS and returns once every node exited.
# Node 0 is a zig bootstrap (listen); the rest dial it. The node at <pub_index>
# publishes <message>; everyone subscribes.
run_star() {
    local topic="$1" message="$2" pub_index="$3" duration="$4" base_port="$5"; shift 5
    local impls=("$@")
    local n=${#impls[@]}
    STAR_PIDS=(); STAR_LOGS=(); STAR_IMPLS=("${impls[@]}")

    local boot_log boot_addr boot_pid boot_mode
    boot_log="$(mktmp)"; boot_addr="$(mktmp)"; : > "$boot_addr"
    boot_mode="sub"; local boot_msg=()
    if [ "$pub_index" -eq 0 ]; then boot_mode="pub"; boot_msg=("message=$message"); fi
    "$ZIG_BIN" role=listen "mode=$boot_mode" "port=$base_port" "topic=$topic" \
        "addr_file=$boot_addr" "duration_ms=$duration" \
        "${boot_msg[@]+"${boot_msg[@]}"}" > "$boot_log" 2>&1 &
    boot_pid=$!; track_pid "$boot_pid"
    STAR_PIDS[0]="$boot_pid"; STAR_LOGS[0]="$boot_log"

    boot_addr="$(wait_for_addr "$boot_addr" "$boot_pid")"
    if [ -z "$boot_addr" ]; then
        echo "FAIL: bootstrap never published its address" >&2; cat "$boot_log" >&2
        return 1
    fi
    echo "    bootstrap address: $boot_addr"

    local i
    for ((i = 1; i < n; i++)); do
        local impl="${impls[$i]}" port=$((base_port + i)) log mode extra=()
        log="$(mktmp)"; mode="sub"
        if [ "$pub_index" -eq "$i" ]; then mode="pub"; extra=("message=$message"); fi
        # Stagger dials so near-simultaneous QUIC connects to the bootstrap do not
        # race a transient dial timeout on loopback.
        sleep 0.3
        "$(bin_for "$impl")" role=dial "mode=$mode" "port=$port" "peers=$boot_addr" \
            "topic=$topic" "duration_ms=$duration" "addr_file=/dev/null" \
            "${extra[@]+"${extra[@]}"}" > "$log" 2>&1 &
        STAR_PIDS[$i]=$!; track_pid "${STAR_PIDS[$i]}"
        STAR_LOGS[$i]="$log"
    done

    for ((i = 0; i < n; i++)); do wait "${STAR_PIDS[$i]}" 2>/dev/null; done
    return 0
}

OVERALL=0

# --- D1: gossip reach ------------------------------------------------------
echo
echo "############################################################"
echo "# D1 — Gossip reach: 9-node star (N>D), a LEAF publishes;"
echo "#      other leaves receive only via the bootstrap relay"
echo "############################################################"
D1_TOPIC="interop-gossip-reach"
D1_MSG="gossip-reach-payload"
D1_PUB=1   # leaf node 1 publishes
# 1 zig bootstrap + 8 leaves spanning all impls.
run_star "$D1_TOPIC" "$D1_MSG" "$D1_PUB" 22000 4260 \
    zig go rust zig go rust go zig zig

D1_N=${#STAR_IMPLS[@]}
echo
printf "  %-6s %-6s %-10s %s\n" "node" "impl" "role" "result"
d1_fail=0
for ((i = 0; i < D1_N; i++)); do
    impl="${STAR_IMPLS[$i]}"; role="leaf"; [ "$i" -eq 0 ] && role="bootstrap"
    if [ "$i" -eq "$D1_PUB" ]; then
        printf "  %-6s %-6s %-10s %s\n" "$i" "$impl" "$role" "SOURCE (publisher leaf)"
        continue
    fi
    if grep -q "RECV topic=$D1_TOPIC.*data=$D1_MSG" "${STAR_LOGS[$i]}"; then
        printf "  %-6s %-6s %-10s %s\n" "$i" "$impl" "$role" "PASS (received via relay)"
    else
        printf "  %-6s %-6s %-10s %s\n" "$i" "$impl" "$role" "FAIL (no receipt)"
        d1_fail=1
    fi
done
if [ "$d1_fail" -eq 0 ]; then
    echo "  [PASS] D1: every non-publisher node reached (incl. leaves outside the publisher's direct mesh)"
else
    echo "  [FAIL] D1: a node beyond the publisher's direct mesh was not reached"
    OVERALL=1
fi

# --- D2: large message (IDONTWANT threshold) -------------------------------
echo
echo "############################################################"
echo "# D2 — Large message (>1KiB) in an all-v1.2 mesh"
echo "############################################################"
D2_TOPIC="interop-large-msg"
# A payload comfortably above the 1 KiB IDONTWANT threshold (a 2000-char token
# with no spaces/newlines so it survives the RECV line grep intact).
D2_MSG="$(printf 'L%.0s' $(seq 1 2000))"
echo "    payload size: ${#D2_MSG} bytes (threshold is 1024)"
D2_PUB=2   # rust leaf publishes the large message
run_star "$D2_TOPIC" "$D2_MSG" "$D2_PUB" 16000 4280 \
    zig go rust zig go

D2_N=${#STAR_IMPLS[@]}
echo
printf "  %-6s %-6s %-10s %s\n" "node" "impl" "role" "result"
d2_fail=0
for ((i = 0; i < D2_N; i++)); do
    impl="${STAR_IMPLS[$i]}"; role="leaf"; [ "$i" -eq 0 ] && role="bootstrap"
    if [ "$i" -eq "$D2_PUB" ]; then
        printf "  %-6s %-6s %-10s %s\n" "$i" "$impl" "$role" "SOURCE (publisher)"
        continue
    fi
    # The large payload is the same token on every RECV; match its data= prefix.
    if grep -q "RECV topic=$D2_TOPIC.*data=$D2_MSG" "${STAR_LOGS[$i]}"; then
        printf "  %-6s %-6s %-10s %s\n" "$i" "$impl" "$role" "PASS (large msg received intact)"
    else
        printf "  %-6s %-6s %-10s %s\n" "$i" "$impl" "$role" "FAIL (large msg not received)"
        d2_fail=1
    fi
done
if [ "$d2_fail" -eq 0 ]; then
    echo "  [PASS] D2: the >1KiB message propagated to every node (the IDONTWANT path did not break delivery)"
else
    echo "  [FAIL] D2: the large message did not reach every node"
    OVERALL=1
fi

echo
echo "============================================================"
if [ "$OVERALL" -eq 0 ]; then
    echo "OVERALL: PASS (D1 gossip/relay reach beyond the publisher's mesh + D2 large-message propagation across the v1.2 mesh)"
else
    echo "OVERALL: FAIL (see D1/D2 above)"
fi
exit "$OVERALL"
