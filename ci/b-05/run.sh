#!/usr/bin/env bash
# B-05: one suite, both arms, and a comparison that is shown able to fail.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
OBS=kafkakn-core/build/observations
RESULTS=kafkakn-core/build/test-results

counts() {
    for arm in jvmTest linuxX64Test; do
        t=$(grep -ho 'tests="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml 2>/dev/null | grep -oE '[0-9]+' | paste -sd+ | bc)
        f=$(grep -ho 'failures="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml 2>/dev/null | grep -oE '[0-9]+' | paste -sd+ | bc)
        printf '  %-14s tests=%-4s failures=%-4s newest result file %s\n' \
            "$arm" "${t:-0}" "${f:-0}" \
            "$(ls -t "$RESULTS/$arm"/TEST-*.xml 2>/dev/null | head -1 | xargs -r stat -c %y | cut -c12-19)"
    done
}

echo "=== environment ==="
date -Is

echo
echo "=== one suite, both arms ==="
rm -rf "$OBS"
./gradlew --no-daemon --console=plain jvmTest linuxX64Test --rerun-tasks 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "SUITE FAILED"; exit 3; }
# Read the result files and their timestamps, not BUILD SUCCESSFUL through a pipe.
counts

echo
echo "=== the arms agree ==="
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1

echo
echo "=== the comparison can fail: skew the JVM arm ==="
rm -rf "$OBS"
KAFKAKN_SKEW_ARM=jvm ./gradlew --no-daemon --console=plain jvmTest linuxX64Test \
    --rerun-tasks 2>&1 | tail -2
if bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt"; then
    echo "  THE COMPARISON PASSED WITH A SKEWED ARM - it cannot tell the arms apart" >&2
    exit 1
fi
echo "  as required: the skew was caught"

echo
echo "=== and it can fail the other way: skew the native arm ==="
rm -rf "$OBS"
KAFKAKN_SKEW_ARM=linuxX64 ./gradlew --no-daemon --console=plain jvmTest linuxX64Test \
    --rerun-tasks 2>&1 | tail -2
if bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt"; then
    echo "  THE COMPARISON PASSED WITH A SKEWED ARM - it cannot tell the arms apart" >&2
    exit 1
fi
echo "  as required: the skew was caught on the other arm too"

echo
echo "=== the vacuity guard: two files that do not exist must NOT agree ==="
rm -rf "$OBS"
if bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" 2>&1 | tail -2; then
    echo "  TWO MISSING FILES AGREED - the comparison is vacuous" >&2
    exit 1
fi
echo "  as required: absence is not agreement"

echo
echo "=== restore a clean run ==="
./gradlew --no-daemon --console=plain jvmTest linuxX64Test --rerun-tasks 2>&1 | tail -2
counts
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt"
