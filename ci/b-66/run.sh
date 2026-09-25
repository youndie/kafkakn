#!/usr/bin/env bash
# B-66: a fenced static member's commit, on both arms, against the broker's own tool.
#
# Each arm's StaticMembershipTest has a member read and commit 4, a second member take its group.instance.id and
# commit 6, and the first then commit 9, then its positions. Both commits must be refused alike on both arms,
# which is compared across them. The group's offsets are then read with kafka-consumer-groups.sh --describe:
# the second member's 6, untouched by the fenced member's attempts.
#
#   ci/b-66/run.sh
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
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*StaticMembershipTest.a_fenced_members_commit*' > "build/b-66-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-66-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=3 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== what each client said, and the group's offsets as the broker's tool reads them ==="
for arm in jvm linuxX64; do
    for what in explicit positions; do
        printf '  %-9s %-9s %s\n' "$arm" "$what" "$(sed -n "s/^static\.fenced\.commit\.$what\.said=//p" "$OBS/$arm-local.txt" | tail -1)"
    done
    group=$(sed -n 's/^static\.fenced\.commit\.group=//p' "$OBS/$arm-local.txt" | tail -1)
    [ -n "$group" ] || { bad "$arm recorded no group"; continue; }
    offsets=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$group" 2>/dev/null \
        | awk -v g="$group" '$1 == g { print $3 ":" $4 }' | sort -n | paste -sd' ')
    printf '  %-9s kafka-consumer-groups --describe: %s\n' "$arm" "$offsets"
    [ "$offsets" = "0:6" ] || bad "$arm: the group's offsets are '$offsets', not the second member's 0:6"
done

echo
[ "$fail" -eq 0 ] || { echo "B-66: RED"; exit 1; }
echo "B-66: a fenced member's commits are refused alike on both arms, and the group's offsets stay the new member's"
