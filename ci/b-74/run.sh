#!/usr/bin/env bash
# B-74: "never queued" and "queued, outcome unknown", told apart by which step ended the wait, on both arms, against
# the broker's own tools. EnqueueTest pauses the broker (docker pause) around both; CancelledSendTest (B-73) runs
# alongside, since send is now enqueue followed by await and must still behave as B-73 measured.
#   - the record enqueue refused with RecordNotQueuedException is not in the topic, and every queued one is;
#   - the record queued and cut while awaiting is in the topic.
#
#   ci/b-74/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
export GRADLE_OPTS=-Dorg.gradle.daemon=false
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }
fact() { sed -n "s/^$2=//p" "$OBS/$1-local.txt" | tail -1; }
# The values in a topic, decoded, up to the first NUL (the fill records carry 1 KiB of padding).
values() { bash "$H" records dump "$1" | awk -F/ '{ print $4 }' | python3 -c '
import sys
for line in sys.stdin:
    v = line.strip()
    if v.startswith("x"):
        print(bytes.fromhex(v[1:]).split(b"\0")[0].decode())'; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms (the tests pause and unpause it) ==="
bash "$H" up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    KAFKAKN_BROKER_CONTROL=1 ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*EnqueueTest*' --tests '*CancelledSendTest*' \
        > "build/b-74-$task.out" 2>&1
    code=$?
    docker unpause kafkakn-broker > /dev/null 2>&1
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-74-$task.out" | head -10; bad "$task failed"; }
done
MIN_OBSERVATIONS=7 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== each arm, as the broker's tools read it ==="
for arm in jvm linuxX64; do
    t=$(fact "$arm" enqueue.full.topic)
    refused=$(fact "$arm" enqueue.full.refused.value)
    queued=$(fact "$arm" enqueue.full.queued)
    values "$t" > "build/b-74-$arm-full.txt"
    end=$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$t" --time -1 2>/dev/null | cut -d: -f3)
    printf '  %-9s full queue: %s queued, %s refused after %s ms; end offset %s; the refused record in the topic %s time(s)\n' \
        "$arm" "$queued" "$refused" "$(fact "$arm" enqueue.full.refused.after.ms)" "$end" "$(grep -cx -e "$refused" "build/b-74-$arm-full.txt")"
    [ "$(grep -cx -e "$refused" "build/b-74-$arm-full.txt")" -eq 0 ] || bad "$arm: the record refused as not queued is in the topic"
    [ "$end" = "$((queued + 1))" ] || bad "$arm: end offset $end, not warm + $queued queued"
    t=$(fact "$arm" enqueue.unknown.topic)
    printf '  %-9s cut while awaiting: queued after %s ms; the topic holds: %s\n' \
        "$arm" "$(fact "$arm" enqueue.unknown.queued.after.ms)" "$(values "$t" | paste -sd' ')"
    values "$t" | grep -qx unknown || bad "$arm: the record cut while awaiting is not in the topic"
done

echo
[ "$fail" -eq 0 ] || { echo "B-74: RED"; exit 1; }
echo "B-74: never queued and outcome unknown, told apart alike on both arms and held against the broker"
