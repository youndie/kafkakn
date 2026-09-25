#!/usr/bin/env bash
# B-42: a PKCS#1 client key is refused at construction on both arms, with the conversion in the message;
# the PKCS#8 key every client-certificate test uses still connects (MutualTlsTest, run alongside).
#
# What the arms did before the rule is in logs/b-42/measured-before-the-rule-*.log: native constructed
# and sent with both forms, the JVM refused both with "Invalid PEM keystore configs".
#
#   ci/b-42/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=kafkakn-b42-$(date +%s)

echo "=== environment ==="
date -Is
echo
echo "=== broker, and the keys ==="
bash "$H" up || exit 1
bash "$H" topic "$KAFKAKN_TOPIC" >/dev/null
bash "$H" mtls-selftest || exit 1
for k in client.key client-pkcs1.key client-pkcs1-encrypted.key; do
    printf '  %-28s %s\n' "$k" "$(grep -m1 -- '-----BEGIN' "$HOME/.cache/kafkakn/tls/$k")"
done

echo
echo "=== both arms ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*Pkcs1KeyTest*' --tests '*MutualTlsTest*' \
        > "build/b-42-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -30 "build/b-42-$task.out"; echo "  THE SUITE FAILED on $task"; exit 1; }
done

echo
echo "=== what each arm said ==="
for arm in jvm linuxX64; do
    for form in plain encrypted; do
        line=$(sed -n "s/^pkcs1\.$form=//p" "$OBS/$arm-local.txt" | tail -1)
        [ -n "$line" ] || { echo "  $arm said nothing for $form - the test did not run" >&2; exit 1; }
        printf '  %-9s %-9s %s\n' "$arm" "$form" "$(echo "$line" | cut -c1-200)"
    done
done

echo
echo "B-42: one key file, one answer on both arms - refused, with the conversion - and PKCS#8 still connects"
