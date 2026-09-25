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
strategy_of() { # <group>
    kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$1" --state 2>/dev/null \
        | awk -v g="$1" '$1 == g { print $4 " (" $5 ", " $NF " member(s))" }'
}
for task in jvmTest linuxX64Test; do
    arm=${task%Test}; [ "$arm" = jvm ] || arm=linuxX64
    group=kafkakn-848-assignor-$arm-$(date +%s)
    # Read while the member is in the group: an empty group shows the broker's default, uniform, whatever it ran.
    env KAFKAKN_ASSIGNOR_GROUP="$group" KAFKAKN_ASSIGNOR_HOLD_MS=15000 \
        ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*ConsumerProtocolTest*assignor*' > "build/b-67-$task.out" 2>&1 &
    run=$!
    live=
    for _ in $(seq 1 40); do
        live=$(strategy_of "$group")
        case "$live" in *", 1 member(s))") break ;; esac
        sleep 1
    done
    wait "$run"; code=$?
    printf '  %-14s exit=%s; while its member was in it, the tool said: %s; once empty: %s\n' "$task" "$code" "${live:-nothing}" "$(strategy_of "$group")"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-67-$task.out" | head -10; bad "$task failed"; continue; }
    case "$live" in "range (Stable, 1 member(s))") ;; *) bad "$arm: while its member was in it, the group ran '$live', not range" ;; esac
done
MIN_OBSERVATIONS=2 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"
for arm in jvm linuxX64; do
    printf '  %-9s refused: %s\n' "$arm" "$(sed -n 's/^848\.assignor\.unknown\.said=//p' "$OBS/$arm-local.txt" | tail -1)"
done

echo
[ "$fail" -eq 0 ] || { echo "B-67: RED"; exit 1; }
echo "B-67: both arms run the assignor they name, as the broker's tool reports it, and refuse an unknown one alike"
