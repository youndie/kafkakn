#!/usr/bin/env bash
# B-49: where a consumer is, and what its group committed, read back on both arms.
#
# PositionTest asserts each position against the fixture's known shape: twenty records, the earliest at
# offset 0. Here the two arms' answers are held against each other, the hard case among them being the
# position before any record was read. Each arm's committed offset is held against
# kafka-consumer-groups.sh, since the consumer that committed is not a witness to its own commit.
#
#   ci/b-49/run.sh
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
echo "=== broker, and the fixture's shape ==="
bash "$H" up || exit 1
printf '  kafkakn-consume-0: earliest %s, end %s\n' \
    "$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic kafkakn-consume --time -2 2>/dev/null | cut -d: -f3)" \
    "$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic kafkakn-consume --time -1 2>/dev/null | cut -d: -f3)"

echo
echo "=== both arms ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*PositionTest*' > "build/b-49-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-49-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=8 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== what the broker holds for each arm's group ==="
for arm in jvm linuxX64; do
    group=$(sed -n 's/^position.group=//p' "$OBS/$arm-local.txt" | tail -1)
    [ -n "$group" ] || { echo "  no group from $arm - its test did not run" >&2; exit 1; }
    said=$(sed -n 's/^position.committed=//p' "$OBS/$arm.txt" | tail -1)
    held=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$group" 2>/dev/null \
        | awk '$2 == "kafkakn-consume" && $3 == 0 { print $4 }')
    printf '  %-9s committed() said %s; kafka-consumer-groups.sh says %s\n' "$arm" "$said" "${held:-nothing}"
    [ "$said" = "$held" ] || bad "$arm: committed() and the broker disagree"
done

echo
[ "$fail" -eq 0 ] || { echo "B-49: RED"; exit 1; }
echo "B-49: both arms report the same position before and after reading, and committed() is what the broker holds"
