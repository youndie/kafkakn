#!/usr/bin/env bash
# B-52: pause a partition without leaving the group, and resume it where it stopped, on both arms.
#
# PauseTest makes its own topic, pauses partition 0 with more of it than one batch still to come, polls
# past max.poll.interval.ms, and resumes. Here the broker says how many records partition 0 holds, and
# each arm's count of what it read, and of distinct offsets, must be that number: none skipped, none
# repeated.
#
#   ci/b-52/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }

echo "=== environment ==="
date -Is

echo
echo "=== broker ==="
bash "$H" up || exit 1

echo
echo "=== both arms ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*PauseTest*' > "build/b-52-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected|Exception:" "build/b-52-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=2 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1

echo
echo "=== partition 0, against the broker ==="
for arm in jvm linuxX64; do
    topic=$(sed -n 's/^pause.topic=//p' "$OBS/$arm-local.txt" | tail -1)
    seen=$(sed -n 's/^pause.seen=//p' "$OBS/$arm-local.txt" | tail -1)
    distinct=$(sed -n 's/^pause.distinct=//p' "$OBS/$arm-local.txt" | tail -1)
    [ -n "$topic" ] || { echo "  no topic from $arm - its test did not run" >&2; exit 1; }
    held=$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$topic" --time -1 2>/dev/null \
        | awk -F: '$2 == 0 { print $3 }')
    printf '  %-9s the broker holds %s; the member read %s, %s distinct\n' "$arm" "$held" "$seen" "$distinct"
    [ "$seen" = "$held" ] && [ "$distinct" = "$held" ] || bad "$arm: read $seen ($distinct distinct) of $held"
done

echo
[ "$fail" -eq 0 ] || { echo "B-52: RED"; exit 1; }
echo "B-52: a paused partition returns nothing on both arms while the member stays, and resume skips and repeats nothing"
