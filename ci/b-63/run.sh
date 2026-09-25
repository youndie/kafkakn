#!/usr/bin/env bash
# B-63: delete records before an offset, on both arms, against the broker's own tool.
#
# Each arm's AdminDeleteRecordsTest deletes on a topic of its own (ten records in partition 0, five in 1):
# partition 0 before 4, then behind that (before 2), then to its end; partition 1 before 0. What the arms say
# is compared across them. Then, per arm, the low watermarks the arm was handed back are held against
# kafka-get-offsets.sh --time -2, the earliest offset as the broker reports it.
#
#   ci/b-63/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }
fact() { sed -n "s/^records\.$2=//p" "$OBS/$1-local.txt" | tail -1; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms ==="
bash "$H" up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminDeleteRecordsTest*' > "build/b-63-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-63-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=4 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== each arm's low watermarks against kafka-get-offsets.sh --time -2 ==="
for arm in jvm linuxX64; do
    topic=$(fact "$arm" topic)
    said=$(fact "$arm" watermarks)
    [ -n "$topic" ] && [ -n "$said" ] || { bad "$arm recorded no topic or watermarks"; continue; }
    earliest=$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$topic" --time -2 2>/dev/null \
        | awk -F: '{ print $2 ":" $3 }' | sort -n | paste -sd' ')
    printf '  %-9s returned %-10s the broker reports %s\n' "$arm" "$said" "$earliest"
    [ "$said" = "$earliest" ] || bad "$arm: returned '$said', the broker reports '$earliest'"
done

echo
[ "$fail" -eq 0 ] || { echo "B-63: RED"; exit 1; }
echo "B-63: both arms delete records and report the low watermark the broker's tool reports, and refuse alike"
