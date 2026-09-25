#!/usr/bin/env bash
# B-62: add partitions to an existing topic, on both arms, against the broker's own tools.
#
# Each arm's AdminPartitionsTest grows a one-partition topic to four, writing eight keyed records before and
# the same eight keys after. What the arms say is compared across them (where each key went after the growth,
# and the refusal of a count that does not grow). Then, per arm:
#   - kafka-topics.sh --describe reports PartitionCount 4;
#   - kafka-get-offsets.sh --time -1 counts, per partition, what the arm said it wrote there: all eight in
#     partition 0 before, and each key's new partition after, so the moved keys are the broker's count too.
#
#   ci/b-62/run.sh
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
echo "=== broker, and both arms ==="
bash "$H" up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminPartitionsTest*' > "build/b-62-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-62-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=3 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== each arm's grown topic, as the broker's tools report it ==="
for arm in jvm linuxX64; do
    topic=$(sed -n 's/^partitions\.topic=//p' "$OBS/$arm-local.txt" | tail -1)
    after=$(sed -n 's/^partitions\.keys\.after=//p' "$OBS/$arm.txt" | tail -1)
    [ -n "$topic" ] && [ -n "$after" ] || { bad "$arm recorded no topic or keys"; continue; }
    count=$(kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --describe --topic "$topic" 2>/dev/null \
        | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -1)
    # Eight before, all in 0; then one per key where the arm said it went.
    expected=$(tr ' ' '\n' <<< "$after" | awk '{ n[$1]++ } END { n[0] += 8; for (p = 0; p < 4; p++) printf "%d:%d ", p, n[p] + 0 }')
    counted=$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$topic" --time -1 2>/dev/null \
        | awk -F: '{ print $2 ":" $3 }' | sort -n | paste -sd' ')
    printf '  %-9s PartitionCount %s; end offsets %s; expected from the keys %s\n' "$arm" "$count" "$counted" "$expected"
    [ "$count" = 4 ] || bad "$arm: kafka-topics reports $count partitions, not 4"
    [ "$counted " = "$expected" ] || bad "$arm: the broker holds '$counted', the arm's keys say '$expected'"
done

echo
[ "$fail" -eq 0 ] || { echo "B-62: RED"; exit 1; }
echo "B-62: both arms grow a topic as the broker's tools count it, move the same keys alike, and refuse alike"
