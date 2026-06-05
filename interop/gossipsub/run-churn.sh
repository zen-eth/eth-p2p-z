#!/usr/bin/env bash
# Scenario C — CHURN: drop + reconnect across implementations.
#
# A mixed mesh forms over a long-lived zig bootstrap. Mid-run we KILL one node's
# process and confirm the survivors keep propagating; then we RESTART that node
# (fresh port + addr file) so it re-dials the bootstrap, re-subscribes, and
# rejoins the mesh, and confirm it receives a publish issued AFTER it rejoined.
#
# Receipts are attributed to phases by publishing a UNIQUE payload per phase from
# a short-lived, dedicated publisher process that dials the bootstrap for that
# phase only. The long-lived subscribers log every distinct payload they see, so
# grepping for a phase's payload tells us exactly who received in that phase:
#   phase1-warmup     : everyone up — sanity that the mesh propagates at all.
#   phase2-postkill   : victim dead — assert the SURVIVORS still receive.
#   phase3-postrejoin : victim restarted + rejoined — assert the VICTIM receives.
#
# The bootstrap + survivors are long-lived (duration spans all phases); the
# victim and the per-phase publishers are short-lived. All processes are killed
# on exit by the shared trap.

set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TOPIC="interop-churn"
BASE_PORT=4230
# Long-lived nodes must outlast all three phases plus settle time.
LONG_MS=42000
# Each phase publisher republishes its unique payload for this long.
PHASE_MS=7000
# Settle waits.
SETTLE_MESH=6     # seconds for initial mesh + subscription propagation
SETTLE_PHASE=7    # seconds to let a phase publisher's message propagate
SETTLE_REJOIN=7   # seconds for the restarted victim to re-dial + graft

build_all
install_cleanup

# The survivor leaves and the victim — vary impls so churn crosses impl
# boundaries (zig bootstrap, go + rust survivors, a go victim).
SURV1_IMPL="go"
SURV2_IMPL="rust"
VICTIM_IMPL="go"

BOOT_PORT=$BASE_PORT
SURV1_PORT=$((BASE_PORT + 1))
SURV2_PORT=$((BASE_PORT + 2))
VICTIM_PORT=$((BASE_PORT + 3))
VICTIM_PORT2=$((BASE_PORT + 4))   # restarted victim uses a fresh port
PUB_PORT=$((BASE_PORT + 5))

echo
echo "############################################################"
echo "# Churn: zig bootstrap + $SURV1_IMPL/$SURV2_IMPL survivors + $VICTIM_IMPL victim"
echo "#   kill the victim mid-run, then restart it and confirm rejoin"
echo "############################################################"

# --- Bootstrap (long-lived zig listener) ----------------------------------
BOOT_LOG="$(mktmp)"; BOOT_ADDR="$(mktmp)"; : > "$BOOT_ADDR"
echo "==> [bootstrap] zig listen sub port=$BOOT_PORT"
"$ZIG_BIN" role=listen mode=sub "port=$BOOT_PORT" "topic=$TOPIC" \
    "addr_file=$BOOT_ADDR" "duration_ms=$LONG_MS" > "$BOOT_LOG" 2>&1 &
BOOT_PID=$!; track_pid "$BOOT_PID"
BOOT_ADDR="$(wait_for_addr "$BOOT_ADDR" "$BOOT_PID")"
if [ -z "$BOOT_ADDR" ]; then
    echo "FAIL: bootstrap never published its address" >&2; cat "$BOOT_LOG" >&2; exit 1
fi
echo "    bootstrap address: $BOOT_ADDR"

# --- Long-lived survivors --------------------------------------------------
# Stagger the dialers slightly so they do not all hit the bootstrap's QUIC accept
# at the exact same instant (near-simultaneous dials to the same endpoint can hit
# a transient dial-timeout on loopback).
SURV1_LOG="$(mktmp)"; SURV2_LOG="$(mktmp)"
echo "==> [survivor1] $SURV1_IMPL dial sub port=$SURV1_PORT"
"$(bin_for "$SURV1_IMPL")" role=dial mode=sub "port=$SURV1_PORT" "peers=$BOOT_ADDR" \
    "topic=$TOPIC" "duration_ms=$LONG_MS" "addr_file=/dev/null" > "$SURV1_LOG" 2>&1 &
SURV1_PID=$!; track_pid "$SURV1_PID"
sleep 0.5
echo "==> [survivor2] $SURV2_IMPL dial sub port=$SURV2_PORT"
"$(bin_for "$SURV2_IMPL")" role=dial mode=sub "port=$SURV2_PORT" "peers=$BOOT_ADDR" \
    "topic=$TOPIC" "duration_ms=$LONG_MS" "addr_file=/dev/null" > "$SURV2_LOG" 2>&1 &
SURV2_PID=$!; track_pid "$SURV2_PID"
sleep 0.5

# --- Victim (short-lived; we kill it explicitly mid-run) -------------------
VICTIM_LOG="$(mktmp)"
echo "==> [victim] $VICTIM_IMPL dial sub port=$VICTIM_PORT"
"$(bin_for "$VICTIM_IMPL")" role=dial mode=sub "port=$VICTIM_PORT" "peers=$BOOT_ADDR" \
    "topic=$TOPIC" "duration_ms=$LONG_MS" "addr_file=/dev/null" > "$VICTIM_LOG" 2>&1 &
VICTIM_PID=$!; track_pid "$VICTIM_PID"

echo "==> Letting the mesh form (${SETTLE_MESH}s)"
sleep "$SETTLE_MESH"

# publish_phase <payload> -> launches a short-lived go publisher that dials the
# bootstrap, publishes <payload> for PHASE_MS, then exits.
publish_phase() {
    local payload="$1" log
    log="$(mktmp)"
    "$GO_BIN" role=dial mode=pub "port=$PUB_PORT" "peers=$BOOT_ADDR" \
        "topic=$TOPIC" "message=$payload" "duration_ms=$PHASE_MS" \
        "addr_file=/dev/null" > "$log" 2>&1 &
    track_pid "$!"
}

# --- Phase 1: warmup (everyone up) -----------------------------------------
echo "==> [phase1] warmup publish (all nodes up)"
publish_phase "phase1-warmup"
sleep "$SETTLE_PHASE"

# --- Kill the victim -------------------------------------------------------
echo "==> Killing the victim process (pid $VICTIM_PID)"
kill "$VICTIM_PID" 2>/dev/null
wait "$VICTIM_PID" 2>/dev/null

# --- Phase 2: post-kill (victim dead) --------------------------------------
echo "==> [phase2] post-kill publish (victim dead; survivors must still receive)"
publish_phase "phase2-postkill"
sleep "$SETTLE_PHASE"

# --- Restart the victim on a fresh port; it re-dials the bootstrap ---------
VICTIM2_LOG="$(mktmp)"
echo "==> Restarting the victim: $VICTIM_IMPL dial sub port=$VICTIM_PORT2 (re-dials bootstrap)"
"$(bin_for "$VICTIM_IMPL")" role=dial mode=sub "port=$VICTIM_PORT2" "peers=$BOOT_ADDR" \
    "topic=$TOPIC" "duration_ms=$LONG_MS" "addr_file=/dev/null" > "$VICTIM2_LOG" 2>&1 &
VICTIM2_PID=$!; track_pid "$VICTIM2_PID"
echo "==> Letting the restarted victim rejoin (${SETTLE_REJOIN}s)"
sleep "$SETTLE_REJOIN"

# --- Phase 3: post-rejoin (victim back) ------------------------------------
echo "==> [phase3] post-rejoin publish (restarted victim must receive)"
publish_phase "phase3-postrejoin"
sleep "$SETTLE_PHASE"

# We have everything we need; stop the long-lived nodes so their final JSON
# flushes. They will also self-exit at LONG_MS, but killing now bounds the run.
echo "==> Stopping long-lived nodes"
kill "$BOOT_PID" "$SURV1_PID" "$SURV2_PID" "$VICTIM2_PID" 2>/dev/null
wait "$BOOT_PID" "$SURV1_PID" "$SURV2_PID" "$VICTIM2_PID" 2>/dev/null

# --- Assertions ------------------------------------------------------------
echo
echo "==> Logs"
for pair in "bootstrap:$BOOT_LOG" "survivor1($SURV1_IMPL):$SURV1_LOG" \
            "survivor2($SURV2_IMPL):$SURV2_LOG" "victim-restarted($VICTIM_IMPL):$VICTIM2_LOG"; do
    name="${pair%%:*}"; log="${pair#*:}"
    echo "------------------------------------------------------------"
    echo "$name"
    echo "------------------------------------------------------------"
    grep -E "RECV topic=$TOPIC|received" "$log" | sed -E 's/(data=[^ ]+).*/\1/' | sort -u
done

# got <log> <payload> -> true if that log received that phase payload.
got() { grep -q "RECV topic=$TOPIC.*data=$2" "$1"; }

echo
echo "============================================================"
echo "  Churn assertions (topic=$TOPIC)"
echo "============================================================"
OVERALL=0

# (1) Survivors keep propagating AFTER the victim is killed.
if got "$SURV1_LOG" "phase2-postkill" && got "$SURV2_LOG" "phase2-postkill"; then
    echo "  [PASS] survivors ($SURV1_IMPL + $SURV2_IMPL) received phase2-postkill (propagation continued after the kill)"
else
    echo "  [FAIL] a survivor missed phase2-postkill"
    OVERALL=1
fi

# (2) The restarted victim receives a publish issued after it rejoined.
if got "$VICTIM2_LOG" "phase3-postrejoin"; then
    echo "  [PASS] restarted victim received phase3-postrejoin (rejoined the mesh + receiving again)"
else
    echo "  [FAIL] restarted victim did not receive phase3-postrejoin"
    OVERALL=1
fi

# (3) Sanity: the restarted victim must NOT have the phase2 payload (it was dead
#     then) — confirms phase attribution is meaningful, not a stale log.
if got "$VICTIM2_LOG" "phase2-postkill"; then
    echo "  [WARN] restarted victim shows phase2-postkill (unexpected; phase attribution may be off)"
fi

echo "============================================================"
if [ "$OVERALL" -eq 0 ]; then
    echo "OVERALL: PASS (survivors kept propagating after a cross-impl node was killed, and the restarted node rejoined and received a post-rejoin publish)"
else
    echo "OVERALL: FAIL (churn propagation or rejoin assertion failed)"
fi
exit "$OVERALL"
