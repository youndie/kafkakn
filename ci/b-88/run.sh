#!/usr/bin/env bash
# B-88: deleteAllRecords on both arms. Afterwards each partition's earliest equals its end, by kafka-get-offsets.sh.
#
#   ci/b-88/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
OBS=kafkakn-core/build/observations
export GRADLE_OPTS=-Dorg.gradle.daemon=false
fail=0
fact() { sed -n "s/^$2=//p" "$OBS/$1-local.txt" | tail -1; }
offsets() { docker exec kafkakn-broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 \
    --topic "$1" --time "$2" 2>/dev/null | awk -F: '{ printf "%s:%s ", $2, $3 }'; }

echo "=== environment ==="
date -Is
bash ci/harness/broker.sh up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminDeleteRecordsTest*' > "build/b-88-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|expected" "build/b-88-$task.out" | head -5; fail=1; }
done
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1

echo
echo "=== by kafka-get-offsets.sh ==="
for arm in jvm linuxX64; do
    t=$(fact "$arm" records.all.topic)
    earliest=$(offsets "$t" -2); latest=$(offsets "$t" -1)
    printf '  %-9s earliest %s  latest %s\n' "$arm" "$earliest" "$latest"
    [ -n "$earliest" ] && [ "$earliest" = "$latest" ] || { echo "    they differ" >&2; fail=1; }
done

echo
[ "$fail" -eq 0 ] || { echo "B-88: RED"; exit 1; }
echo "B-88: every record deleted, each partition starts where it ends, alike on both arms"
