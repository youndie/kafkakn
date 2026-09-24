#!/usr/bin/env bash
# B-29: partitionsFor on both arms, checked against the broker's own description of the topic.
#
# The topic has seven partitions - a count no other topic in the fixture has - so an answer describing
# the wrong topic cannot pass for the right one. The oracle is `kafka-topics.sh --describe`, reduced
# to the same `partition:leader:replicas:isr` shape the suite records.
#
#   ci/b-29/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
STAMP=$(date +%s)
TOPIC=kafkakn-b29-$STAMP
METADATA=kafkakn-metadata-$STAMP
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=$TOPIC
export KAFKAKN_METADATA_TOPIC=$METADATA KAFKAKN_METADATA_PARTITIONS=7

echo "=== environment ==="
date -Is
echo "  topics $TOPIC and $METADATA (7 partitions), fresh"

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1
PARTITIONS=7 bash "$H" topic "$METADATA" > build/b-29-describe.txt
head -1 build/b-29-describe.txt
BROKER=$(awk '/Partition:/ {
        for (i = 1; i <= NF; i++) {
            if ($i == "Partition:") p = $(i + 1)
            if ($i == "Leader:") l = $(i + 1)
            if ($i == "Replicas:") r = $(i + 1)
            if ($i == "Isr:") s = $(i + 1)
        }
        print p ":" l ":" r ":" s
    }' build/b-29-describe.txt | sort -t: -k1,1n | paste -sd';')
[ -n "$BROKER" ] || { echo "  the broker described no partitions - the oracle is empty" >&2; exit 1; }
echo "  the broker says: $BROKER"

echo
echo "=== both arms, one test task per invocation, rerun every time ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*TopicMetadataTest*' > "build/b-29-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -30 "build/b-29-$task.out"; echo "  THE SUITE FAILED on $task"; exit 1; }
done

echo
echo "=== what each arm answered, against what the broker described ==="
fail=0
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    said=$(sed -n 's/^metadata\.describe=//p' "$f" | tail -1)
    [ -n "$said" ] || { echo "  $arm recorded no answer - its test did not run" >&2; exit 1; }
    if [ "$said" = "$BROKER" ]; then
        printf '  %-9s agrees: %s\n' "$arm" "$said"
    else
        printf '  %-9s DISAGREES: %s\n' "$arm" "$said" >&2
        fail=1
    fi
done

echo
echo "=== an unknown topic, and a broker that is not there - recorded, not compared ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    printf '  %-9s unknown topic after %s ms: %s\n' "$arm" \
        "$(sed -n 's/^metadata\.unknown\.ms=//p' "$f" | tail -1)" \
        "$(sed -n 's/^metadata\.unknown\.failure=//p' "$f" | tail -1 | cut -c1-200)"
    printf '  %-9s no broker, waited %s ms off the caller'"'"'s dispatcher\n' "$arm" \
        "$(sed -n 's/^metadata\.nowhere\.ms=//p' "$f" | tail -1)"
done

echo
[ "$fail" -eq 0 ] || { echo "B-29: RED"; exit 1; }
echo "B-29: both arms describe the topic as the broker does, and neither holds the caller's dispatcher"
