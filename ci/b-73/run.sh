#!/usr/bin/env bash
# B-73: what a cancelled send means for its record, on each arm, against the broker's own tools. CancelledSendTest
# pauses the broker (docker pause) around a send it cuts; this runner lets it (KAFKAKN_BROKER_CONTROL) and then reads
# each arm's topics with kafka-get-offsets.sh and the Java client:
#   - cut after it was queued: the cut record is in the topic;
#   - cut while it waited for room: whether the probe is in the topic, per arm, as measured.
#
#   ci/b-73/run.sh
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
fact() { sed -n "s/^cancel\.$2=//p" "$OBS/$1-local.txt" | tail -1; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms (the tests pause and unpause it) ==="
bash "$H" up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    KAFKAKN_BROKER_CONTROL=1 ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*CancelledSendTest*' > "build/b-73-$task.out" 2>&1
    code=$?
    docker unpause kafkakn-broker > /dev/null 2>&1
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-73-$task.out" | head -10; bad "$task failed"; }
done
MIN_OBSERVATIONS=2 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1

echo
echo "=== each arm, as the broker's tools read it ==="
for arm in jvm linuxX64; do
    t=$(fact "$arm" queued.topic)
    values=$(bash "$H" records dump "$t" | awk -F/ '{ print $4 }' | while read -r v; do printf '%s ' "$(echo "${v#x}" | xxd -r -p)"; done)
    printf '  %-9s cut after queued: returned after %s ms, topic holds: %s\n' "$arm" "$(fact "$arm" queued.returned.after.ms)" "$values"
    case "$values" in *cut*) ;; *) bad "$arm: the record cut after it was queued is not in the topic" ;; esac
    t=$(fact "$arm" room.topic)
    end=$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$t" --time -1 2>/dev/null | cut -d: -f3)
    probe=$(bash "$H" records dump "$t" | awk -F/ '{ print $4 }' | grep -c "^x$(printf probe | xxd -p)$")
    printf '  %-9s cut while waiting for room: returned after %s ms, end offset %s, the probe in the topic %s time(s), parked after %s\n' \
        "$arm" "$(fact "$arm" room.returned.after.ms)" "$end" "$probe" "$(fact "$arm" room.parked)"
    [ "$probe" = "$(fact "$arm" room.probe.landed)" ] || bad "$arm: the test counted $(fact "$arm" room.probe.landed) probe(s), the topic holds $probe"
done

echo
[ "$fail" -eq 0 ] || { echo "B-73: RED"; exit 1; }
echo "B-73: a cancelled send, measured on both arms against the broker"
