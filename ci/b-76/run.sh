#!/usr/bin/env bash
# B-76: "no metadata within max.block.ms" is "never queued" on both arms, the half of the contract's row B-74 did
# not measure. EnqueueMetadataTest on each arm, then the broker's own tools:
#   - with no broker at the address, enqueue refuses after max.block.ms and close returns promptly;
#   - a topic that does not exist is refused, and is still not there afterwards (auto-creation is off);
#   - a topic created while enqueue waits is queued once it exists, and its one record is in it.
#
#   ci/b-76/run.sh
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

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms ==="
bash "$H" up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*EnqueueMetadataTest*' > "build/b-76-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected|queued" "build/b-76-$task.out" | head -10; bad "$task failed"; }
done
MIN_OBSERVATIONS=5 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== each arm, as the broker's tools read it ==="
for arm in jvm linuxX64; do
    printf '  %-9s no broker: refused after %s ms, closed in %s ms: %s\n' "$arm" \
        "$(fact "$arm" metadata.nowhere.after.ms)" "$(fact "$arm" metadata.nowhere.close.ms)" \
        "$(fact "$arm" metadata.nowhere.said | cut -c1-160)"
    t=$(fact "$arm" metadata.missing.topic)
    printf '  %-9s missing topic: refused after %s ms: %s\n' "$arm" \
        "$(fact "$arm" metadata.missing.after.ms)" "$(fact "$arm" metadata.missing.said | cut -c1-160)"
    if [ -n "$t" ] && kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list 2>/dev/null | grep -qx -e "$t"; then
        bad "$arm: $t exists after the refusal"
    fi
    printf '  %-9s created while waiting: queued after %s ms\n' "$arm" "$(fact "$arm" metadata.created.queued.after.ms)"
done
created=$(kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list 2>/dev/null | grep '^kafkakn-created-while-waiting-')
for t in $created; do
    end=$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$t" --time -1 2>/dev/null | cut -d: -f3)
    printf '  %s: end offset %s\n' "$t" "$end"
    [ "$end" = 1 ] || bad "$t holds $end records, not the one queued while it was created"
done
[ -n "$created" ] || bad "no topic was created while enqueue waited"

echo
[ "$fail" -eq 0 ] || { echo "B-76: RED"; exit 1; }
echo "B-76: no metadata within max.block.ms is never queued on both arms, held against the broker"
