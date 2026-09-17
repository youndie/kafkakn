#!/usr/bin/env bash
# B-08: producing past the queue bound loses nothing, checked against the broker.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC="$TOPIC" KAFKAKN_STRICT_TOPIC=kafkakn-strict

echo "=== environment ==="
date -Is

echo
echo "=== broker ==="
bash "$H" up
bash "$H" topic "$TOPIC" | head -1
bash "$H" strict-topic kafkakn-strict | head -1

echo
echo "=== both arms, with a queue small enough to overrun ==="
rm -rf "$OBS"
BEFORE=$(bash "$H" offsets "$TOPIC")
START=$(date +%s)
./gradlew --no-daemon --console=plain jvmTest linuxX64Test --rerun-tasks 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "SUITE FAILED"; exit 1; }
SECONDS_TAKEN=$(( $(date +%s) - START ))
AFTER=$(bash "$H" offsets "$TOPIC")
for arm in jvmTest linuxX64Test; do
    t=$(grep -ho 'tests="[0-9]*"' kafkakn-core/build/test-results/$arm/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    f=$(grep -ho 'failures="[0-9]*"' kafkakn-core/build/test-results/$arm/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    printf '  %-14s tests=%-4s failures=%s\n' "$arm" "$t" "$f"
    [ "$f" -eq 0 ] || exit 1
done

echo
echo "=== the accounting: what each arm says it sent, against what the broker received ==="
for arm in jvm linuxX64; do
    local_file="$OBS/$arm-local.txt"
    [ -s "$local_file" ] || { echo "  $arm recorded nothing - nothing below is checked" >&2; exit 1; }
    bp_stamp=$(sed -n 's/^backpressure\.stamp=//p' "$local_file")
    bp_count=$(sed -n 's/^backpressure\.count=//p' "$local_file")
    fl_stamp=$(sed -n 's/^flush\.stamp=//p' "$local_file")
    fl_count=$(sed -n 's/^flush\.count=//p' "$local_file")
    found_bp=$(CONSUME_MS=45000 bash "$H" consume "$TOPIC" "^$bp_stamp:")
    found_fl=$(CONSUME_MS=45000 bash "$H" consume "$TOPIC" "^$fl_stamp:")
    printf '  %-9s past-the-bound %s/%s   after-flush %s/%s\n' \
        "$arm" "$found_bp" "$bp_count" "$found_fl" "$fl_count"
    [ "$found_bp" -eq "$bp_count" ] || { echo "  $arm LOST RECORDS under backpressure" >&2; exit 1; }
    [ "$found_fl" -eq "$fl_count" ] || { echo "  $arm: flush returned before everything landed" >&2; exit 1; }
done

echo
echo "=== the native arm actually reached the bound ==="
echo "  (zero waits would mean the test never exercised backpressure - the suite asserts this too,"
echo "   and the first, serial version of the test failed exactly here)"
grep -c . "$OBS/linuxX64-local.txt" >/dev/null

echo
echo "=== the arms still agree on what only a client knows ==="
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1

echo
echo "=== a floor for the rate, not a benchmark ==="
echo "  both suites, end to end: ${SECONDS_TAKEN}s; broker end offsets $BEFORE -> $AFTER"
echo "  This box is shared and the number moves by more than the thing it would measure."
echo "  H4 is settled as NOT MEASURED in the research document, with what it would take."

echo
echo "=== verdict ==="
echo "B-08: both arms produce past their queue bound and lose nothing"
