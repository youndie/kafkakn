#!/usr/bin/env bash
# B-28: the time a record names is the time the broker stores - unless the topic says the broker's
# own clock wins, and then the broker's clock is what the metadata reports.
#
# RecordMetadata.timestamp is the client relaying the broker. The oracle is the broker's own consumer
# printing each stored record's timestamp AND its type, CreateTime or LogAppendTime - the type is
# something the Java client cannot tell us at all, which is why kafkakn does not claim it either.
#
#   ci/b-28/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn-time-$(date +%s)
LOGAPPEND=kafkakn-logappend
CHOSEN=1600000000000
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=$TOPIC KAFKAKN_LOGAPPEND_TOPIC=$LOGAPPEND

echo "=== environment ==="
date -Is
echo "  topic $TOPIC, fresh; the LogAppendTime topic is the fixture's own, $LOGAPPEND"

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1
docker exec kafkakn-broker /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9092 \
    --entity-type topics --entity-name "$LOGAPPEND" --describe 2>/dev/null \
    | grep -q "message.timestamp.type=LogAppendTime" \
    || { echo "  $LOGAPPEND is not a LogAppendTime topic - the fixture did not make what the test needs" >&2; exit 1; }
echo "  $LOGAPPEND: message.timestamp.type=LogAppendTime, confirmed by the broker"

echo
echo "=== the sends, one test task per invocation, rerun every time ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*TimestampTest*' > "build/b-28-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -5 "build/b-28-$task.out"; echo "  THE SENDS FAILED on $task"; exit 1; }
done

echo
echo "=== what the broker stored ==="
bash "$H" timed-values "$TOPIC" > build/b-28-create.timed
bash "$H" timed-values "$LOGAPPEND" > build/b-28-logappend.timed
fail=0
for arm in jvm linuxX64; do
    create=$(sed -n 's/^timestamp.create.stamp=//p' "$OBS/$arm-local.txt" | tail -1)
    logappend=$(sed -n 's/^timestamp.logappend.stamp=//p' "$OBS/$arm-local.txt" | tail -1)
    [ -n "$create" ] && [ -n "$logappend" ] || { echo "  no stamps from $arm - its tests did not run" >&2; exit 1; }

    stored=$(awk -F'\t' -v v="$create" '$2 == v { print $1 }' build/b-28-create.timed)
    printf '  %-9s named %s on an ordinary topic   -> stored %s\n' "$arm" "$CHOSEN" "${stored:-nothing}"
    [ "$stored" = "CreateTime:$CHOSEN" ] || { echo "    $arm: expected CreateTime:$CHOSEN" >&2; fail=1; }

    stored=$(awk -F'\t' -v v="$logappend" '$2 == v { print $1 }' build/b-28-logappend.timed)
    printf '  %-9s named %s on a LogAppendTime topic -> stored %s\n' "$arm" "$CHOSEN" "${stored:-nothing}"
    case "$stored" in
        LogAppendTime:$CHOSEN) echo "    $arm: the broker kept the record's own time" >&2; fail=1 ;;
        LogAppendTime:*) ;;
        *) echo "    $arm: expected LogAppendTime:<the broker's clock>" >&2; fail=1 ;;
    esac
done

echo
[ "$fail" -eq 0 ] || { echo "B-28: RED"; exit 1; }
echo "B-28: the time a record names is stored as CreateTime, and a LogAppendTime topic replaces it - on both arms"
