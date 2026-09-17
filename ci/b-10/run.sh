#!/usr/bin/env bash
# B-10: what the broker stored for a record's headers, read by a third party.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
RESULTS=kafkakn-core/build/test-results
TOPIC=kafkakn
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC="$TOPIC" KAFKAKN_STRICT_TOPIC=kafkakn-strict
export KAFKAKN_SSL_BOOTSTRAP=127.0.0.1:9094
export KAFKAKN_ACCOUNTING_TOPIC=kafkakn-acct-b10-$(date +%s)

echo "=== environment ==="
date -Is

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1
bash "$H" strict-topic kafkakn-strict | head -1
for arm in jvm linuxX64; do bash "$H" topic "$KAFKAKN_ACCOUNTING_TOPIC-$arm" >/dev/null; done

echo
echo "=== both arms ==="
rm -rf "$OBS" "$RESULTS"
./gradlew --no-daemon --console=plain jvmTest linuxX64Test --rerun-tasks 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "SUITE FAILED"; exit 1; }
for arm in jvmTest linuxX64Test; do
    t=$(grep -ho 'tests="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    f=$(grep -ho 'failures="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    printf '  %-14s tests=%-4s failures=%s\n' "$arm" "$t" "$f"
    [ "$f" -eq 0 ] || exit 1
done

echo
echo "=== what the broker stored, read with kafka-console-consumer ==="
stored_jvm=""; stored_native=""
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    stamp=$(sed -n 's/^headers\.stamp=//p' "$f")
    expected=$(sed -n 's/^headers\.expected=//p' "$f")
    [ -n "$stamp" ] || { echo "  $arm recorded no header facts" >&2; exit 1; }
    line=$(CONSUME_MS=25000 bash "$H" headers "$TOPIC" "$stamp")
    [ -n "$line" ] || { echo "  $arm: no record with $stamp came back" >&2; exit 1; }
    stored=${line%%$'\t'*}
    printf '  %-9s sent     %s\n' "$arm" "$expected"
    printf '  %-9s stored   %s\n' "$arm" "$stored"
    [ "$stored" = "$expected" ] || {
        echo "  $arm: the broker holds different headers than were sent" >&2; exit 1; }
    if [ "$arm" = jvm ]; then stored_jvm=$stored; else stored_native=$stored; fi
done

echo
echo "=== and the two arms stored the same thing ==="
[ "$stored_jvm" = "$stored_native" ] || {
    echo "  THE ARMS DISAGREE: jvm '$stored_jvm' vs native '$stored_native'" >&2; exit 1; }
echo "  both: $stored_jvm"
echo "  (a duplicate name survives, and it survives IN ORDER - a client storing headers in a map"
echo "   would have answered with two entries instead of three)"

echo
echo "=== a null header value is not an empty one ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    stamp=$(sed -n 's/^headers\.null\.stamp=//p' "$f")
    line=$(CONSUME_MS=25000 bash "$H" headers "$TOPIC" "$stamp")
    stored=${line%%$'\t'*}
    printf '  %-9s %s\n' "$arm" "$stored"
    [ -n "$stored" ] || { echo "  $arm: nothing came back for $stamp" >&2; exit 1; }
    # The claim is the DIFFERENCE, not the spelling of either side. A client that turned a null
    # value into an empty one would render both the same way here and lose a distinction the
    # protocol makes and a consumer can see.
    absent=$(echo "$stored" | sed -n 's/.*absent:\([^,]*\).*/\1/p')
    empty=$(echo "$stored" | sed -n 's/.*empty:\(.*\)/\1/p')
    [ "$absent" != "$empty" ] || {
        echo "  $arm: a null header value and an empty one render identically ('$absent')" >&2
        exit 1
    }
done

echo
echo "=== the arms agree on what only a client knows ==="
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1

echo
echo "=== verdict ==="
echo "B-10: headers round-trip on both arms, in order, duplicates and all"
