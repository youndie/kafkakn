#!/usr/bin/env bash
# B-86: APPEND and SUBTRACT on a list-valued topic key, on both arms, and APPEND on a key that is not a list.
# AdminConfigsTest, compared across the arms, and the final value read with kafka-configs.sh.
#
#   ci/b-86/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
OBS=kafkakn-core/build/observations
export GRADLE_OPTS=-Dorg.gradle.daemon=false
fail=0
fact() { sed -n "s/^$2=//p" "$OBS/$1-local.txt" | tail -1; }

echo "=== environment ==="
date -Is
bash ci/harness/broker.sh up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminConfigsTest*' > "build/b-86-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|expected" "build/b-86-$task.out" | head -5; fail=1; }
done
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1
grep '^configs.list' "$OBS/jvm.txt" | sed 's/^/  /'

echo
echo "=== the final cleanup.policy, by kafka-configs.sh ==="
for arm in jvm linuxX64; do
    t=$(fact "$arm" configs.list.topic)
    tool=$(docker exec kafkakn-broker /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9092 \
        --entity-type topics --entity-name "$t" --describe 2>/dev/null | grep -o 'cleanup.policy=[^ ]*' | head -1)
    printf '  %-9s %s   append on a non-list key said: %s\n' "$arm" "$tool" "$(fact "$arm" configs.list.not.a.list.said | cut -c1-140)"
    [ "$tool" = "cleanup.policy=delete" ] || fail=1
done

echo
[ "$fail" -eq 0 ] || { echo "B-86: RED"; exit 1; }
echo "B-86: APPEND and SUBTRACT land as kafka-configs.sh reads them, alike on both arms"
