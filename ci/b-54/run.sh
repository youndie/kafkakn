#!/usr/bin/env bash
# B-54: the Flow over poll on both arms: records in order, a prompt cancellation, and what a collector
# slower than max.poll.interval.ms sees.
#
# The eviction is printed per arm rather than compared: the arms differ there, measured, and B-64 exists to
# make them agree. Until it does, this is where the difference is visible on every run.
#
#   ci/b-54/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations

echo "=== environment ==="
date -Is

echo
echo "=== broker ==="
bash "$H" up || exit 1

echo
echo "=== both arms ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*RecordsFlowTest*' > "build/b-54-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected|invisible" "build/b-54-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=1 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1

echo
echo "=== a collector slower than max.poll.interval.ms, per arm ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    printf '  %-9s cancelled in %s ms\n' "$arm" "$(sed -n 's/^flow.cancel.ms=//p' "$f" | tail -1)"
    printf '  %-9s listener: %s\n' "$arm" "$(sed -n 's/^flow.slow.events=//p' "$f" | tail -1)"
    printf '  %-9s outcome:  %s\n' "$arm" "$(sed -n 's/^flow.slow.outcome=//p' "$f" | tail -1)"
    printf '  %-9s offsets:  %s\n' "$arm" "$(sed -n 's/^flow.slow.seen=//p' "$f" | tail -1)"
done

echo
echo "B-54: the Flow reads in order and cancels promptly on both arms; the eviction is reported to the listener"
echo "      on both, and what the next poll does is printed above (B-64)"
