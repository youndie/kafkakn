#!/usr/bin/env bash
# B-53: the consumer's lag on both arms, against the broker's own tool.
#
# ConsumerLagTest freezes the lag (a paused partition, its position committed) and asserts it is the end
# minus the position, before and after the commit. Here kafka-consumer-groups.sh, which measures from the
# commit, must report the same frozen number for each arm's group. The gate that keeps success counts out
# of the metrics runs too, with its self-test: lag is a distance, not a count of anything handled.
#
#   ci/b-53/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }

echo "=== environment ==="
date -Is

echo
echo "=== the gate that keeps a success count out, and its self-test ==="
python3 scripts/no_delivery_counters.py && python3 scripts/no_delivery_counters.py --selftest || exit 1

echo
echo "=== broker ==="
bash "$H" up || exit 1

echo
echo "=== both arms ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*ConsumerLagTest*' > "build/b-53-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-53-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=1 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1

echo
echo "=== the frozen lag, against kafka-consumer-groups.sh ==="
for arm in jvm linuxX64; do
    topic=$(sed -n 's/^lag.topic=//p' "$OBS/$arm-local.txt" | tail -1)
    [ -n "$topic" ] || { echo "  no topic from $arm - its test did not run" >&2; exit 1; }
    position=$(sed -n 's/^lag.position=//p' "$OBS/$arm-local.txt" | tail -1)
    frozen=$(sed -n 's/^lag.frozen=//p' "$OBS/$arm-local.txt" | tail -1)
    uncommitted=$(sed -n 's/^lag.uncommitted=//p' "$OBS/$arm-local.txt" | tail -1)
    # CURRENT-OFFSET, LOG-END-OFFSET and LAG for the one partition, as the broker's tool computes them.
    read -r committed end lag <<< "$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 \
        --describe --group "$topic" 2>/dev/null | awk -v t="$topic" '$2 == t { print $4, $5, $6 }')"
    printf '  %-9s position %s, lag %s before the commit and %s after; the broker: committed %s, end %s, lag %s\n' \
        "$arm" "$position" "$uncommitted" "$frozen" "$committed" "$end" "$lag"
    [ "$committed" = "$position" ] || bad "$arm: the broker holds commit $committed, not the position $position"
    [ "$frozen" = "$lag" ] || bad "$arm: the member read lag $frozen, the broker's tool computes $lag"
    [ "$uncommitted" = "$lag" ] || bad "$arm: before the commit the member read $uncommitted, not $lag"
done
# The broker's LAG is measured from the commit, which the member never moved after resuming, so it still
# names the frozen moment: the same number the member read.

echo
[ "$fail" -eq 0 ] || { echo "B-53: RED"; exit 1; }
echo "B-53: both arms read the lag as end minus position, frozen and drained, and the broker's figures agree"
