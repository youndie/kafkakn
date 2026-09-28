#!/usr/bin/env bash
# B-87: new partitions with a replica assignment, on both arms. The assigned partition's replicas are read with
# kafka-topics.sh; an assignment naming a broker the cluster does not have is refused alike, and the topic is unchanged.
#
#   ci/b-87/run.sh
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
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminPartitionsTest*' > "build/b-87-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|expected" "build/b-87-$task.out" | head -5; fail=1; }
done
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1
grep '^partitions.assigned' "$OBS/jvm.txt" | sed 's/^/  /'

echo
echo "=== by kafka-topics.sh ==="
for arm in jvm linuxX64; do
    t=$(fact "$arm" partitions.assigned.topic)
    docker exec kafkakn-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --describe --topic "$t" 2>/dev/null \
        | grep -E "PartitionCount|Partition: 1" | sed "s/^/  $arm  /" | cut -c1-140
    printf '  %-9s the refusal said: %s\n' "$arm" "$(fact "$arm" partitions.assigned.refused.said | cut -c1-140)"
done

echo
[ "$fail" -eq 0 ] || { echo "B-87: RED"; exit 1; }
echo "B-87: an assigned partition lands on the broker named, and an unknown broker is refused alike"
