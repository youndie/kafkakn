#!/usr/bin/env bash
# B-67: group.remote.assignor under the KIP-848 protocol, on both arms, against the broker's own tool.
#
# The fixture broker offers uniform and range (group.consumer.assignors) and settles on uniform unless asked.
# Each arm's member asks for range; the group's ASSIGNMENT-STRATEGY is read with kafka-consumer-groups.sh
# --describe --state. A name the broker does not offer is refused, and the refusal is compared across the arms.
#
#   ci/b-67/run.sh
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
echo "  the broker offers: $(kc /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --describe --all --entity-type brokers --entity-name 1 2>/dev/null \
    | sed -n 's/^ *group\.consumer\.assignors=\([^ ]*\).*/\1/p')"
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*ConsumerProtocolTest*assignor*' > "build/b-67-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-67-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=2 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== each arm's group, as the broker's tool describes it ==="
for arm in jvm linuxX64; do
    group=$(sed -n 's/^848\.assignor\.group=//p' "$OBS/$arm-local.txt" | tail -1)
    [ -n "$group" ] || { bad "$arm recorded no group"; continue; }
    strategy=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$group" --state 2>/dev/null \
        | awk -v g="$group" '$1 == g { print $4 }')
    printf '  %-9s ASSIGNMENT-STRATEGY %s; refused: %s\n' "$arm" "$strategy" "$(sed -n 's/^848\.assignor\.unknown\.said=//p' "$OBS/$arm-local.txt" | tail -1)"
    [ "$strategy" = range ] || bad "$arm: the group runs '$strategy', not the range it asked for"
done

echo
[ "$fail" -eq 0 ] || { echo "B-67: RED"; exit 1; }
echo "B-67: both arms run the assignor they name, as the broker's tool reports it, and refuse an unknown one alike"
