#!/usr/bin/env bash
# B-51: seek in a group, and from the listener as partitions arrive, on both arms.
#
# GroupSeekTest reads offsets against the consumer fixture's known shape. Here that shape is confirmed with
# the broker's own tool (twenty records from offset 0), so "read from exactly 7" and "from exactly 12"
# name records the broker holds, and the two arms' readings are held against each other.
#
#   ci/b-51/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
kc() { docker exec kafkakn-broker "$@"; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and the fixture's shape ==="
bash "$H" up || exit 1
earliest=$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic kafkakn-consume --time -2 2>/dev/null | cut -d: -f3)
end=$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic kafkakn-consume --time -1 2>/dev/null | cut -d: -f3)
echo "  kafkakn-consume-0: earliest $earliest, end $end"
[ "$earliest" = 0 ] && [ "$end" = 20 ] || { echo "  the fixture is not twenty records from offset 0" >&2; exit 1; }

echo
echo "=== both arms ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*GroupSeekTest*' > "build/b-51-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected|Exception:" "build/b-51-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=3 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "B-51: in a group, both arms seek a held partition and read from exactly there, refuse one not held,"
echo "      and a seek from onAssigned takes effect before the first record"
