#!/usr/bin/env bash
# B-41: the portable metrics, read from each arm's own client under one load, held against each other.
#
# Tolerances, as the contract states them: after a flush both arms must read EXACTLY zero bytes buffered
# and zero requests in flight; open connections may differ by the bootstrap socket the Java client keeps; the round trip UNDER
# LOAD must be positive on both and within a factor of ten - the clients average over different windows (the
# Java client's metric window, librdkafka's statistics interval), so equal is not the claim; and three
# seconds of sending must show buffered bytes on both. The counter gate runs too, with its self-test.
#
#   ci/b-41/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=kafkakn-metrics-$(date +%s)
fail=0
bad() { echo "    $*" >&2; fail=1; }

echo "=== environment ==="
date -Is
echo
echo "=== the gate that keeps a success count out, and its self-test ==="
python3 scripts/no_delivery_counters.py && python3 scripts/no_delivery_counters.py --selftest || exit 1

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$KAFKAKN_TOPIC" | head -1

echo
echo "=== both arms, under one load ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*MetricsTest*' > "build/b-41-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -30 "build/b-41-$task.out"; echo "  THE SUITE FAILED on $task"; exit 1; }
done

fact() { sed -n "s/^metrics\.$2=//p" "$OBS/$1-local.txt" | tail -1; }
echo
printf '  %-34s %-18s %-18s\n' metric jvm linuxX64
for key in rest.bufferedBytes rest.requestsInFlight rest.brokerRoundTripMillis rest.openConnections \
    load.maxBufferedBytes load.maxRequestsInFlight load.brokerRoundTripMillis \
    after.bufferedBytes after.requestsInFlight after.brokerRoundTripMillis after.openConnections; do
    printf '  %-34s %-18s %-18s\n' "$key" "$(fact jvm "$key")" "$(fact linuxX64 "$key")"
done

echo
for arm in jvm linuxX64; do
    [ "$(fact $arm after.bufferedBytes)" = 0 ] || bad "$arm: bytes held after flush"
    [ "$(fact $arm after.requestsInFlight)" = 0 ] || bad "$arm: requests out after flush"
    [ "$(fact $arm load.maxBufferedBytes)" -gt 0 ] 2>/dev/null || bad "$arm: nothing buffered under load"
done
# Open connections are NOT equal, and that is the arms' difference, written down rather than hidden:
# with one broker the Java client reads 2 and librdkafka 1, measured twice. The reading is that the Java
# client keeps its bootstrap socket open beside the broker's and librdkafka does not - inferred from the
# counts, not traced. The tolerance is that difference and no more: native <= jvm <= native + 1.
jc=$(fact jvm after.openConnections); nc=$(fact linuxX64 after.openConnections)
[ "$nc" -ge 1 ] && [ "$jc" -ge "$nc" ] && [ "$jc" -le $((nc + 1)) ] \
    || bad "open connections outside the stated difference: jvm $jc, native $nc"
# The round trip under load: after a flush librdkafka's last interval is empty and reads null.
python3 - "$(fact jvm load.brokerRoundTripMillis)" "$(fact linuxX64 load.brokerRoundTripMillis)" <<'PY' || fail=1
import sys
a, b = (float(x) for x in sys.argv[1:3])
ratio = max(a, b) / min(a, b) if min(a, b) > 0 else float("inf")
print(f"  round trip: jvm {a:.2f} ms, native {b:.2f} ms, ratio {ratio:.1f} (tolerance 10)")
sys.exit(0 if a > 0 and b > 0 and ratio <= 10 else 1)
PY

echo
[ "$fail" -eq 0 ] || { echo "B-41: RED"; exit 1; }
echo "B-41: both arms read the same machinery within the stated tolerance, and no success count exists"
