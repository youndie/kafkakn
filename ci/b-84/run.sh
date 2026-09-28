#!/usr/bin/env bash
# B-84: listOffsets with OffsetSpec.MaxTimestamp on both arms, held against kafka-get-offsets.sh --time -3 on the
# same topic: records written with timestamps out of offset order, so the answer is neither the end nor the last.
#
#   ci/b-84/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
export GRADLE_OPTS=-Dorg.gradle.daemon=false
fail=0
fact() { sed -n "s/^$2=//p" "$OBS/$1-local.txt" | tail -1; }

echo "=== environment ==="
date -Is
bash "$H" up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminOffsetsTest*' > "build/b-84-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|expected" "build/b-84-$task.out" | head -5; fail=1; }
done
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1

echo
echo "=== against the broker's own tool ==="
for arm in jvm linuxX64; do
    t=$(fact "$arm" admin.offsets.max.topic)
    tool=$(docker exec kafkakn-broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 \
        --topic "$t" --time -3 2>/dev/null | awk -F: '{ printf "%s:%s ", $2, ($3 == "" ? "none" : $3) }')
    said=$(grep '^admin.offsets.max=' "$OBS/$arm.txt" | cut -d= -f2)
    printf '  %-9s kafkakn %-12s kafka-get-offsets --time -3: %s\n' "$arm" "$said" "$tool"
    case "$tool" in "0:1 "*) ;; *) echo "    the tool disagrees" >&2; fail=1 ;; esac
done

echo
[ "$fail" -eq 0 ] || { echo "B-84: RED"; exit 1; }
echo "B-84: the max-timestamp offset is the broker's, alike on both arms"
