#!/usr/bin/env bash
# B-58: list and describe consumer groups, on both arms, against the broker's own tool.
#
# Two groups. One is the test's own: a kafkakn member, then no member, then a group that never existed,
# with what each arm says compared across the arms. The other has a member that is not kafkakn at all: the
# distribution's console consumer, left running here. Each arm's admin describes it, and the description is
# held against kafka-consumer-groups.sh --describe --members --verbose.
#
#   ci/b-58/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
GROUP=kafkakn-third-$(date +%s)
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and a member that is not kafkakn ==="
bash "$H" up || exit 1
docker exec -d kafkakn-broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
    --topic kafkakn-consume --group "$GROUP" --consumer-property client.id=third-party
trap 'docker exec kafkakn-broker pkill -f "group $GROUP" >/dev/null 2>&1' EXIT
# member id, host, client id, assignment. The host verbatim: the tool prints "/127.0.0.1", as both clients
# do. The assignment is CURRENT-ASSIGNMENT, the seventh column in 4.3.1's --verbose (after #PARTITIONS and
# CURRENT-EPOCH), spelled topic:partition. Reading the sixth printed the epoch's "-", and looked like a
# group still assigning.
broker_member() {
    kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$GROUP" --members --verbose 2>/dev/null \
        | awk '$1 == "'"$GROUP"'" { print $2 "," $3 "," $4 "," $7 }'
}
BROKER=
for _ in $(seq 1 30); do
    BROKER=$(broker_member)
    case "$BROKER" in *,-|"") sleep 1 ;; *) break ;; esac
done
echo "  the broker's tool: $BROKER"
[ -n "$BROKER" ] || { echo "  the console consumer never joined $GROUP" >&2; exit 1; }

echo
echo "=== both arms ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    KAFKAKN_DESCRIBE_GROUP=$GROUP ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminGroupsTest*' \
        > "build/b-58-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-58-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=6 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== the third party's member, as each arm describes it and as the broker's tool does ==="
for arm in jvm linuxX64; do
    said=$(sed -n 's/^admin.third.members=//p' "$OBS/$arm-local.txt" | tail -1)
    printf '  %-9s %s\n' "$arm" "$said"
    [ "$said" = "$BROKER" ] || bad "$arm: described $said, the broker's tool says $BROKER"
done

echo
[ "$fail" -eq 0 ] || { echo "B-58: RED"; exit 1; }
echo "B-58: both arms list and describe groups as the broker's tool does, and agree on empty and missing groups"
