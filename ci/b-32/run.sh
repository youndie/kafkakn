#!/usr/bin/env bash
# B-32: SASL PLAIN and SCRAM on both arms, checked from outside the SASL path.
#
# The listeners are asked to refuse a wrong password with the broker's own tools first. Then the
# suite, and the records it sent over the SASL listeners are counted over PLAINTEXT by
# kafka-console-consumer. The password the JAAS format has to escape is one of the counted runs: the
# native arm sends it raw, so its records arriving is what says the broker holds that password.
#
#   ci/b-32/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn-sasl-$(date +%s)
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=$TOPIC

echo "=== environment ==="
date -Is
echo "  topic $TOPIC, fresh"

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1

echo
echo "=== the listeners must be able to say NO, asked with the broker's own tools ==="
bash "$H" sasl-selftest || exit 1

echo
echo "=== both arms, one test task per invocation, rerun every time ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*SaslTest*' > "build/b-32-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -30 "build/b-32-$task.out"; echo "  THE SUITE FAILED on $task"; exit 1; }
done

echo
echo "=== records sent over SASL, counted OVER PLAINTEXT ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    for what in plain scram256 scram512 ssl-scram512 quoted; do
        stamp=$(sed -n "s/^sasl\.$what\.stamp=//p" "$f" | tail -1)
        count=$(sed -n "s/^sasl\.$what\.count=//p" "$f" | tail -1)
        [ -n "$stamp" ] && [ -n "$count" ] || { echo "  $arm recorded nothing for $what - the scenario did not run" >&2; exit 1; }
        found=$(CONSUME_MS=45000 bash "$H" consume "$TOPIC" "^$stamp:")
        printf '  %-9s %-13s %s/%s\n' "$arm" "$what" "$found" "$count"
        [ "$found" -eq "$count" ] || { echo "  $arm lost records for $what" >&2; exit 1; }
    done
done

echo
echo "=== and what each arm SAID for the wrong password ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    for case in sasl-wrong-plain sasl-wrong-scram-sha-256; do
        line=$(sed -n "s/^tls\.$case\.failure=//p" "$f" | tail -1)
        [ -n "$line" ] || { echo "  $arm said nothing for $case - the scenario did not run" >&2; exit 1; }
        printf '  %-9s %-25s %s\n' "$arm" "$case" "$(echo "$line" | cut -c1-220)"
    done
done

echo
echo "B-32: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 (and over TLS) connect, the quoted password too, the wrong one is refused - on both arms"
