#!/usr/bin/env bash
# B-33: SASL/OAUTHBEARER with a token the caller supplies, on both arms.
#
# The broker's own tools connect with an unsigned token first (sasl-selftest), so a refusal later is the
# client's. Records sent with the caller's tokens are counted over PLAINTEXT. The refresh test runs for
# thirty seconds on twelve-second tokens against a broker that re-authenticates every ten.
#
#   ci/b-33/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn-oauth-$(date +%s)
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=$TOPIC

echo "=== environment ==="
date -Is
echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1
bash "$H" sasl-selftest || exit 1

echo
echo "=== both arms ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*OAuthBearerTest*' > "build/b-33-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -30 "build/b-33-$task.out"; echo "  THE SUITE FAILED on $task"; exit 1; }
done

echo
echo "=== records sent with the caller's tokens, counted over plaintext ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    stamp=$(sed -n 's/^oauth\.stamp=//p' "$f" | tail -1)
    count=$(sed -n 's/^oauth\.count=//p' "$f" | tail -1)
    [ -n "$stamp" ] || { echo "  $arm recorded no stamp" >&2; exit 1; }
    found=$(CONSUME_MS=30000 bash "$H" consume "$TOPIC" "^$stamp:")
    printf '  %-9s %s/%s\n' "$arm" "$found" "$count"
    [ "$found" -eq "$count" ] || { echo "  $arm lost records" >&2; exit 1; }
done

echo
echo "=== refresh, and a provider that throws ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    printf '  %-9s %s tokens issued over %s sends in thirty seconds\n' "$arm" \
        "$(sed -n 's/^oauth\.refresh\.tokens=//p' "$f" | tail -1)" "$(sed -n 's/^oauth\.refresh\.sent=//p' "$f" | tail -1)"
    printf '  %-9s the provider threw, and the caller read: %s\n' "$arm" \
        "$(sed -n 's/^oauth\.provider-throws=//p' "$f" | tail -1 | cut -c1-220)"
done

echo
echo "B-33: both arms authenticate with the caller's tokens, refresh them, and show the provider's own failure"
