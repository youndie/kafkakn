#!/usr/bin/env bash
# B-48: a named offset is committed as named, on both arms, as the broker's own tool reads the group.
#
# CommitOffsetsTest commits offset 7 of the consumer fixture and resumes from it. Here
# kafka-consumer-groups.sh says what the group actually holds, since the consumer that committed is not
# a witness to its own commit. It also says what a commit of a partition the consumer did not hold left
# behind, which the contract states as measured.
#
#   ci/b-48/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }
# The group's committed offset for one partition, as the broker's tool reports it; "-" when it has none.
committed() { # <group> <topic> <partition>
    kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$1" 2>/dev/null \
        | awk -v t="$2" -v p="$3" '$2 == t && $3 == p { print $4 }'
}

echo "=== environment ==="
date -Is

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic kafkakn > /dev/null

echo
echo "=== both arms commit and resume ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*CommitOffsetsTest*' > "build/b-48-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|Exception" "build/b-48-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=2 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1

echo
echo "=== what the broker holds for each group ==="
for arm in jvm linuxX64; do
    group=$(sed -n 's/^commit.group=//p' "$OBS/$arm-local.txt" | tail -1)
    unheld=$(sed -n 's/^commit.unheld.group=//p' "$OBS/$arm-local.txt" | tail -1)
    [ -n "$group" ] && [ -n "$unheld" ] || { echo "  no groups from $arm - its tests did not run" >&2; exit 1; }
    at=$(committed "$group" kafkakn-consume 0)
    elsewhere=$(committed "$unheld" kafkakn 1)
    outcome=$(sed -n 's/^commit.unheld=//p' "$OBS/$arm.txt" | tail -1)
    printf '  %-9s committed offset: %-3s  a partition it did not hold: %s, and the broker holds %s for it\n' \
        "$arm" "${at:-none}" "$outcome" "${elsewhere:-nothing}"
    [ "$at" = 7 ] || bad "$arm: the group holds '${at:-nothing}' for kafkakn-consume-0, not the 7 committed"
done

echo
[ "$fail" -eq 0 ] || { echo "B-48: RED"; exit 1; }
echo "B-48: each arm commits the offset it names, and a new member resumes from exactly there"
