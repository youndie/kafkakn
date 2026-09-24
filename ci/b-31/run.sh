#!/usr/bin/env bash
# B-31: a broker that asks who is connecting gets an answer - on both arms.
#
# The order is the item's acceptance: the listener is shown able to say NO, with the broker's own
# tools, before anything it says yes to is read. Then the suite, and then the records sent over the
# certificate-checking listener are counted over PLAINTEXT, by kafka-console-consumer - the path that
# verifies the claim is not the path the claim is about.
#
#   ci/b-31/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn-mtls-$(date +%s)
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=$TOPIC KAFKAKN_MTLS_BOOTSTRAP=127.0.0.1:9095

echo "=== environment ==="
date -Is
echo "  topic $TOPIC, fresh"

echo
echo "=== broker: the plaintext, TLS and certificate-checking listeners, one fixture ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1

echo
echo "=== the listener must be able to say NO, asked with the broker's own tools ==="
bash "$H" mtls-selftest || exit 1
# And B-11's listener still does not ask: the new one sits beside it, it did not replace it.
bash "$H" tls-selftest || exit 1

echo
echo "=== both arms, one test task per invocation, rerun every time ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*MutualTlsTest*' > "build/b-31-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -30 "build/b-31-$task.out"; echo "  THE SUITE FAILED on $task"; exit 1; }
done

echo
echo "=== records sent with a client certificate, counted OVER PLAINTEXT ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    stamp=$(sed -n 's/^mtls\.stamp=//p' "$f" | tail -1)
    count=$(sed -n 's/^mtls\.count=//p' "$f" | tail -1)
    [ -n "$stamp" ] && [ -n "$count" ] || { echo "  $arm recorded no mTLS facts - its test did not run" >&2; exit 1; }
    found=$(CONSUME_MS=45000 bash "$H" consume "$TOPIC" "^$stamp:")
    printf '  %-9s %s/%s with a client certificate\n' "$arm" "$found" "$count"
    [ "$found" -eq "$count" ] || { echo "  $arm lost records" >&2; exit 1; }
done

echo
echo "=== and what each arm SAID when the broker refused it ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    for case in mtls-no-certificate mtls-wrong-authority; do
        line=$(sed -n "s/^tls\.$case\.failure=//p" "$f" | tail -1)
        [ -n "$line" ] || { echo "  $arm said nothing for $case - the scenario did not run" >&2; exit 1; }
        printf '  %-9s %-21s %s\n' "$arm" "$case" "$(echo "$line" | cut -c1-220)"
    done
done

echo
echo "B-31: the right client certificate connects, none and the wrong authority's are refused - on both arms"
