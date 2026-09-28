#!/usr/bin/env bash
# B-82: a commit of a partition another member of the group holds, under a subscription, on both arms and both group
# protocols. What the committing member's `commit` answered and what a new member resumed from are compared across the
# arms; what the broker stored is read with its own tool.
#
#   ci/b-82/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
export GRADLE_OPTS=-Dorg.gradle.daemon=false
fail=0
fact() { sed -n "s/^$2=//p" "$OBS/$1-local.txt" | tail -1; }

echo "=== environment ==="
date -Is
bash "$H" up || exit 1

echo
echo "=== both arms ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*ForeignCommitTest*' > "build/b-82-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|Exception" "build/b-82-$task.out" | head -6; fail=1; }
done
MIN_OBSERVATIONS=4 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== what the broker stored, by its own tool ==="
for arm in jvm linuxX64; do
    for protocol in classic consumer; do
        g=$(fact "$arm" "foreign.$protocol.group")
        p=$(fact "$arm" "foreign.$protocol.partition")
        stored=$(docker exec kafkakn-broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 \
            --describe --group "$g" 2>/dev/null | awk -v t="$g" -v p="$p" '$2 == t && $3 == p { print $4 }')
        printf '  %-9s %-9s partition %s: committed %s  %s\n' "$arm" "$protocol" "$p" "${stored:-none}" \
            "$(fact "$arm" "foreign.$protocol.refusal")"
    done
done

echo
[ "$fail" -eq 0 ] || { echo "B-82: a pass failed, or the arms disagree"; exit 1; }
echo "B-82: measured, and the arms agree"
