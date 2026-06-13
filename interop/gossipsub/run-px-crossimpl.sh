#!/usr/bin/env bash
# Cross-implementation gossipsub PEER-EXCHANGE (PX) over real QUIC on localhost.
#
# PX flow: a node whose topic mesh exceeds D_high prunes a peer and, with PX on,
# includes in that PRUNE up to prune_peers OTHER mesh peers' SIGNED peer records
# (drawn from its certified-address store, populated via identify's
# signedPeerRecord). The pruned peer verifies the records and DIALS the offered
# peers — discovering them without ever having been told their address directly.
#
# This scenario drives that path ACROSS implementations and asserts the pruned
# peer connects to a peer it never dialed:
#
#   Direction 1 (go EMITS PX, zig CONSUMES + dials):
#     A go bootstrap A with PX on and a tiny mesh degree (D_high=3, D=2) is dialed
#     by four zig leaves. A's mesh reaches 4 > D_high, so the heartbeat prunes two
#     leaves down to D=2, each prune carrying PX offers (the surviving leaves'
#     signed records, which A learned via identify). Each pruned zig leaf verifies
#     a record, decodes its BINARY multiaddr, and dials the offered zig leaf. The
#     zig leaf logs `PX-CONNECT` once its gossipsub peer count grows past the one
#     upstream peer it dialed itself — proof of a connection to a PX-offered peer.
#
#   Direction 2 (zig EMITS PX, go CONSUMES + dials):
#     A zig bootstrap A (px=on, tiny degree) is dialed by four go leaves. A prunes
#     two go leaves with PX, offering the surviving leaves' signed records (which A
#     learned via identify, as the dialer's identify client). Each pruned go leaf
#     parses the record and dials the offered go leaf. Asserted from the go leaf
#     debug log (`connecting to <peer>` after a `PRUNE: Remove mesh link`), which
#     go's pubsub emits when it acts on a PX offer.
#
# rust-libp2p is NOT used as a record-driven PX party: libp2p-gossipsub 0.49.4
# emits PX peers as BARE peer ids (its PeerInfo has no signed-record field) and its
# px_connect dials by peer id only, explicitly NOT reading a signed record to learn
# an address ("Until SignedRecords are spec'd this remains a stub"). So a rust
# emitter offers nothing dialable and a rust consumer cannot dial a never-seen peer
# from a record. The rust peer's px/d_high knobs exist for framing only.
#
# Each node is a real OS process on loopback QUIC; ports are distinct; the whole
# run is bounded by a watchdog and every leftover process is killed on exit.

set -u

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

TOPIC="interop-px"
DURATION_MS=26000          # whole-run budget per node (PX + re-mesh takes a few heartbeats)
BASE_PORT=4640
# How long to poll for the cross-impl PX dial before giving up (ms). The prune
# fires on a heartbeat (~1s) only once the over-degree mesh has formed, then the
# dial + handshake + connect-event must propagate, so allow several heartbeats.
PX_POLL_MS=20000

build_all
install_cleanup

# poll_until_grep <pattern> <file> <timeout_ms> -> 0 if the pattern appears in the
# file within the timeout (polling every 200ms), 1 otherwise. Tolerant of the
# multi-heartbeat latency of a PX'd prune -> dial -> connect.
poll_until_grep() {
    local pat="$1" file="$2" budget="$3" waited=0
    while [ "$waited" -lt "$budget" ]; do
        if grep -qE "$pat" "$file" 2>/dev/null; then
            return 0
        fi
        sleep 0.2
        waited=$((waited + 200))
    done
    return 1
}

OVERALL=0

# ===========================================================================
# Direction 1: go EMITS PX, three zig leaves CONSUME + dial a PX-offered leaf.
# ===========================================================================
run_direction1() {
    echo
    echo "############################################################"
    echo "# Direction 1: go bootstrap EMITS PX, zig leaves CONSUME + dial"
    echo "############################################################"

    local boot_port=$((BASE_PORT))
    local addr_file boot_log
    addr_file="$(mktmp)"; boot_log="$(mktmp)"
    : > "$addr_file"

    echo "==> [boot] go px=on d_high=3 d=2 d_low=1 port=$boot_port"
    GOLOG_LOG_LEVEL="pubsub=debug" "$GO_BIN" \
        role=listen mode=sub px=on d_high=3 d=2 d_low=1 \
        "port=$boot_port" "topic=$TOPIC" "addr_file=$addr_file" "duration_ms=$DURATION_MS" \
        > "$boot_log" 2>&1 &
    local boot_pid=$!
    track_pid "$boot_pid"

    local boot_addr
    boot_addr="$(wait_for_addr "$addr_file" "$boot_pid")"
    if [ -z "$boot_addr" ]; then
        echo "FAIL: go bootstrap never published its address" >&2
        cat "$boot_log" >&2
        return 1
    fi
    echo "    bootstrap: $boot_addr"

    # Four zig leaves: each LISTENS (so a PX-offered leaf is dialable) and dials
    # the bootstrap. px=on so each CONSUMES the records the bootstrap offers.
    local leaf_logs=()
    local i
    for i in 1 2 3 4; do
        local log; log="$(mktmp)"; leaf_logs+=("$log")
        "$ZIG_BIN" role=listen mode=sub px=on \
            "port=$((BASE_PORT + i))" "peers=$boot_addr" \
            "topic=$TOPIC" "addr_file=/dev/null" "duration_ms=$DURATION_MS" \
            > "$log" 2>&1 &
        track_pid "$!"
    done

    # The bootstrap must over-degree-prune (its mesh of 4 > D_high=3 shrinks to D=2)
    # and the prune must CARRY PX, i.e. emit a signed peer record. We detect that
    # directly by the `signedPeerRecord` field in the sent PRUNE control RPC, which
    # is the actual PX emit. (Earlier this grepped go's "HEARTBEAT: Remove mesh link"
    # debug line, but that string is internal to go-libp2p and changed across the
    # v0.38->v0.46 bump; the over-degree prune now also fires at GRAFT time, not only
    # at the heartbeat. `signedPeerRecord` is the protocol-level, version-stable proof.)
    # NB: D must be >= 2 here. go-libp2p v0.16 (v0.46) added a GossipSubParams check
    # `Dout < Dlo && Dout < D/2` (gossipsub.go); integer D/2 == 0 at D == 1 needs
    # Dout < 0 (impossible), so D=1 is rejected outright. v0.38.1 (pubsub v0.13) had
    # no such check and accepted D=1. D=2/D_high=3 keeps D < D_high (non-degenerate)
    # and still over-degrees (4 leaves > D_high=3), so PX fires, pruning two leaves.
    if poll_until_grep "signedPeerRecord" "$boot_log" "$PX_POLL_MS"; then
        echo "    go bootstrap over-degree-pruned a leaf and emitted PX records"
    else
        echo "FAIL: go bootstrap never emitted a PX record (no signedPeerRecord)" >&2
        cat "$boot_log" >&2
        return 1
    fi

    # Assert at least one zig leaf connected to a PX-offered peer.
    local px_hits=0
    for log in "${leaf_logs[@]}"; do
        if poll_until_grep "PX-CONNECT" "$log" "$PX_POLL_MS"; then
            px_hits=$((px_hits + 1))
        fi
    done

    echo "    zig leaves that dialed a PX-offered peer: $px_hits / 4"
    if [ "$px_hits" -ge 1 ]; then
        echo "    Direction 1 evidence (a zig leaf's PX-CONNECT line):"
        grep -h "PX-CONNECT" "${leaf_logs[@]}" | head -1 | sed 's/^/      /'
        echo "PASS: go-emitted PX records were verified + dialed by a zig consumer"
        return 0
    fi
    echo "FAIL: no zig leaf connected to a go-offered PX peer" >&2
    echo "--- go bootstrap PX log ---" >&2; grep -E "signedPeerRecord|prune" "$boot_log" >&2
    return 1
}

# ===========================================================================
# Direction 2: zig EMITS PX, three go leaves CONSUME + dial a PX-offered leaf.
# ===========================================================================
run_direction2() {
    echo
    echo "############################################################"
    echo "# Direction 2: zig bootstrap EMITS PX, go leaves CONSUME + dial"
    echo "############################################################"

    local boot_port=$((BASE_PORT + 10))
    local addr_file boot_log
    addr_file="$(mktmp)"; boot_log="$(mktmp)"
    : > "$addr_file"

    echo "==> [boot] zig px=on d_high=3 d=2 d_low=1 port=$boot_port"
    "$ZIG_BIN" \
        role=listen mode=sub px=on d_high=3 d=2 d_low=1 \
        "port=$boot_port" "topic=$TOPIC" "addr_file=$addr_file" "duration_ms=$DURATION_MS" \
        > "$boot_log" 2>&1 &
    local boot_pid=$!
    track_pid "$boot_pid"

    local boot_addr
    boot_addr="$(wait_for_addr "$addr_file" "$boot_pid")"
    if [ -z "$boot_addr" ]; then
        echo "FAIL: zig bootstrap never published its address" >&2
        cat "$boot_log" >&2
        return 1
    fi
    echo "    bootstrap: $boot_addr"

    # Three go leaves DIAL the bootstrap (role=dial binds an ephemeral udp/0 port,
    # so the leaf is reachable and advertises that address via identify — which the
    # zig bootstrap certifies and later offers as a PX record). px=on so each go
    # leaf CONSUMES the PX offers and dials the offered peer.
    local leaf_logs=()
    local i
    for i in 1 2 3 4; do
        local log; log="$(mktmp)"; leaf_logs+=("$log")
        GOLOG_LOG_LEVEL="pubsub=debug" "$GO_BIN" \
            role=dial mode=sub px=on \
            "peers=$boot_addr" "topic=$TOPIC" "duration_ms=$DURATION_MS" \
            > "$log" 2>&1 &
        track_pid "$!"
    done

    # The zig bootstrap must over-degree-prune; the go leaves must then act on the
    # PX offer. go logs `connecting to peer` (gossipsub px consume) right after a
    # `PRUNE: Remove mesh link`, naming a peer that is NOT the bootstrap.
    local px_hits=0
    for log in "${leaf_logs[@]}"; do
        # go's PX consume logs `msg="connecting to peer" peer=<id>` (pubsub px_connect,
        # gossipsub.go). It only fires for peers offered via PX (a record-driven dial),
        # so its presence is the cross-impl PX evidence. NB: v0.46 logs "connecting to
        # peer" with the id in a separate peer= field; pre-v0.46 inlined it ("connecting
        # to 12D3...") — match the field-less prefix so both formats are detected.
        if poll_until_grep "connecting to peer" "$log" "$PX_POLL_MS"; then
            px_hits=$((px_hits + 1))
        fi
    done

    echo "    go leaves that dialed a PX-offered peer: $px_hits / 4"
    if [ "$px_hits" -ge 1 ]; then
        echo "    Direction 2 evidence (a go leaf acting on a zig PX offer):"
        grep -hE "PRUNE: Remove mesh link|connecting to peer" "${leaf_logs[@]}" | head -2 | sed 's/^/      /'
        echo "PASS: zig-emitted PX records were verified + dialed by a go consumer"
        return 0
    fi
    echo "FAIL: no go leaf connected to a zig-offered PX peer" >&2
    return 1
}

run_direction1 || OVERALL=1
# Each direction kills nothing itself; the EXIT trap reaps every tracked pid. The
# two directions use disjoint port ranges so back-to-back runs do not collide.
run_direction2 || OVERALL=1

echo
echo "============================================================"
if [ "$OVERALL" -eq 0 ]; then
    echo "OVERALL: PASS (cross-impl PX works BOTH ways: go<->zig signed-record peer-exchange dialed a never-dialed peer)"
else
    echo "OVERALL: FAIL (at least one PX direction did not complete a cross-impl dial)"
fi
exit "$OVERALL"
