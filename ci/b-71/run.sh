#!/usr/bin/env bash
# B-71: offsets handed to a transaction with the group metadata of a membership the group has moved past, on both
# arms: a group that rebalanced since the metadata was taken, and a member that left. Each arm's refusal, its
# abort, and the control (fresh metadata commits) are compared across the arms.
#
#   ci/b-71/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
export GRADLE_OPTS=-Dorg.gradle.daemon=false

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms ==="
bash "$H" up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*StaleGroupMetadataTest*' > "build/b-71-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-71-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=5 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== what each client said underneath ==="
for arm in jvm linuxX64; do
    for what in generation member; do
        printf '  %-9s %-10s %s\n' "$arm" "$what" "$(sed -n "s/^stale\.$what\.said=//p" "$OBS/$arm-local.txt" | tail -1 | cut -c1-150)"
    done
done

echo
echo "B-71: stale group metadata is refused alike on both arms, the transaction aborts, and fresh metadata commits"
