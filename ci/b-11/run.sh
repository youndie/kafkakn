#!/usr/bin/env bash
# B-11: TLS on both arms, checked from outside the TLS path.
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
export KAFKAKN_ACCOUNTING_TOPIC=kafkakn-acct-b11-$(date +%s)

echo "=== environment ==="
date -Is

echo
echo "=== broker: one fixture, both listeners ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1
bash "$H" strict-topic kafkakn-strict | head -1
for arm in jvm linuxX64; do bash "$H" topic "$KAFKAKN_ACCOUNTING_TOPIC-$arm" >/dev/null; done

echo
echo "=== the fixture must be able to say NO, asked with the broker's own tools ==="
bash "$H" tls-selftest || exit 1

echo
echo "=== both arms ==="
rm -rf "$OBS" "$RESULTS"
BEFORE=$(bash "$H" offsets "$TOPIC")
./gradlew --no-daemon --console=plain jvmTest linuxX64Test --rerun-tasks 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "SUITE FAILED"; exit 1; }
for arm in jvmTest linuxX64Test; do
    t=$(grep -ho 'tests="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    f=$(grep -ho 'failures="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    printf '  %-14s tests=%-4s failures=%s\n' "$arm" "$t" "$f"
    [ "$f" -eq 0 ] || exit 1
done

echo
echo "=== records sent over TLS, counted OVER PLAINTEXT ==="
echo "  (the path that verifies the claim is deliberately not the path the claim is about)"
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    stamp=$(sed -n 's/^tls\.stamp=//p' "$f")
    count=$(sed -n 's/^tls\.count=//p' "$f")
    [ -n "$stamp" ] && [ -n "$count" ] || { echo "  $arm recorded no TLS facts" >&2; exit 1; }
    found=$(CONSUME_MS=45000 bash "$H" consume "$TOPIC" "^$stamp:")
    printf '  %-9s %s/%s over TLS\n' "$arm" "$found" "$count"
    [ "$found" -eq "$count" ] || { echo "  $arm lost records over TLS" >&2; exit 1; }
done
AFTER=$(bash "$H" offsets "$TOPIC")
echo "  end offsets on $TOPIC: $BEFORE -> $AFTER, read over plaintext"

echo
echo "=== and what each arm SAID when it refused ==="
echo "  (the assertion is inside the suite; this prints the sentences a person should read)"
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    for case in wrong-ca tls-to-plaintext; do
        line=$(sed -n "s/^tls\.$case\.failure=//p" "$f")
        [ -n "$line" ] || { echo "  $arm said nothing for $case - the scenario did not run" >&2; exit 1; }
        printf '  %-9s %-16s %s\n' "$arm" "$case" "$(echo "$line" | cut -c1-150)"
    done
done

echo
echo "=== the arms still agree on what only a client knows ==="
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1

echo
echo "=== verdict ==="
echo "B-11: both arms produce over TLS, refuse an unverifiable peer, and say which it was"
