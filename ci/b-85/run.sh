#!/usr/bin/env bash
# B-85: listOffsets read committed, on both arms. A transaction committed and one left open: the end read
# uncommitted counts the open one, read committed stops where it starts. AdminOffsetsTest, compared across the arms.
#
#   ci/b-85/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
OBS=kafkakn-core/build/observations
export GRADLE_OPTS=-Dorg.gradle.daemon=false
fail=0

echo "=== environment ==="
date -Is
bash ci/harness/broker.sh up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminOffsetsTest*' > "build/b-85-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|expected" "build/b-85-$task.out" | head -5; fail=1; }
done
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1
grep '^admin.offsets.isolation=' "$OBS/jvm.txt" "$OBS/linuxX64.txt" | sed 's/^/  /'

echo
[ "$fail" -eq 0 ] || { echo "B-85: RED"; exit 1; }
echo "B-85: read committed stops where the open transaction starts, alike on both arms"
