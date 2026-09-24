#!/usr/bin/env bash
# B-34: the admin client on both arms, every answer checked by the broker's own tools.
#
# Nothing kafkakn created is described by kafkakn: the topics each arm created are read back with
# kafka-topics.sh and kafka-configs.sh; the topic each arm described is the fixture's, and the cluster
# id each arm reported is compared with kafka-cluster.sh.
#
#   ci/b-34/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn-b34-$(date +%s)
BOOT=127.0.0.1:9092
export KAFKAKN_BOOTSTRAP=$BOOT KAFKAKN_TOPIC=$TOPIC
kc() { docker exec kafkakn-broker "$@"; }

echo "=== environment ==="
date -Is
echo "  topic $TOPIC, fresh - the one both arms try to create a second time"

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1
kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --describe --topic kafkakn-metadata > build/b-34-metadata.txt 2>/dev/null
BROKER_DESCRIBES=$(awk '/Partition:/ {
        for (i = 1; i <= NF; i++) {
            if ($i == "Partition:") p = $(i + 1)
            if ($i == "Leader:") l = $(i + 1)
            if ($i == "Replicas:") r = $(i + 1)
            if ($i == "Isr:") s = $(i + 1)
        }
        print p ":" l ":" r ":" s
    }' build/b-34-metadata.txt | sort -t: -k1,1n | paste -sd';')
CLUSTER_ID=$(kc /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server "$BOOT" 2>/dev/null | sed -n 's/^Cluster ID: //p')
[ -n "$BROKER_DESCRIBES" ] && [ -n "$CLUSTER_ID" ] || { echo "  the oracles are empty" >&2; exit 1; }
echo "  the broker says: kafkakn-metadata $BROKER_DESCRIBES; cluster $CLUSTER_ID"

echo
echo "=== both arms, one test task per invocation, rerun every time ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminTest*' > "build/b-34-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -30 "build/b-34-$task.out"; echo "  THE SUITE FAILED on $task"; exit 1; }
done

echo
echo "=== what each arm did, as the broker's own tools see it ==="
fail=0
bad() { echo "    $*" >&2; fail=1; }
LISTED=$(kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --list 2>/dev/null)
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    created=$(sed -n 's/^admin\.created=//p' "$f" | tail -1)
    deleted=$(sed -n 's/^admin\.deleted=//p' "$f" | tail -1)
    described=$(sed -n 's/^admin\.describe=//p' "$f" | tail -1)
    cluster=$(sed -n 's/^admin\.cluster\.id=//p' "$f" | tail -1)
    [ -n "$created" ] && [ -n "$deleted" ] && [ -n "$described" ] || { echo "  $arm recorded nothing" >&2; exit 1; }

    header=$(kc /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --describe --topic "$created" 2>/dev/null | head -1)
    configs=$(kc /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOT" --describe --entity-type topics --entity-name "$created" 2>/dev/null)
    printf '  %-9s created  %s\n' "$arm" "$(echo "$header" | cut -f3,4)"
    echo "$header" | grep -q "PartitionCount: 5" || bad "$arm: expected 5 partitions"
    echo "$header" | grep -q "ReplicationFactor: 1" || bad "$arm: expected replication 1"
    echo "$configs" | grep -q "retention.ms=123456789" || bad "$arm: retention.ms did not reach the broker"
    echo "$configs" | grep -q "cleanup.policy=compact" || bad "$arm: cleanup.policy did not reach the broker"
    printf '  %-9s          %s\n' "$arm" "$(echo "$configs" | grep -o 'retention.ms=[0-9]*\|cleanup.policy=[a-z]*' | paste -sd' ')"

    if echo "$LISTED" | grep -qx "$deleted"; then bad "$arm: $deleted is still listed"; else printf '  %-9s deleted  %s is not listed\n' "$arm" "$deleted"; fi

    if [ "$described" = "$BROKER_DESCRIBES" ]; then
        printf '  %-9s describe agrees\n' "$arm"
    else
        bad "$arm: described $described"
    fi
    if [ "$cluster" = "$CLUSTER_ID" ]; then
        printf '  %-9s cluster  %s, nodes %s, controller %s\n' "$arm" "$cluster" \
            "$(sed -n 's/^admin\.cluster\.nodes=//p' "$f" | tail -1)" "$(sed -n 's/^admin\.cluster\.controller=//p' "$f" | tail -1)"
    else
        bad "$arm: cluster id $cluster"
    fi
done

echo
echo "=== recorded, not compared ==="
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    printf '  %-9s exists:  %s\n' "$arm" "$(sed -n 's/^admin\.exists\.failure=//p' "$f" | tail -1 | cut -c1-240)"
    printf '  %-9s nowhere: %s ms, %s\n' "$arm" "$(sed -n 's/^admin\.nowhere\.ms=//p' "$f" | tail -1)" \
        "$(sed -n 's/^admin\.nowhere\.failure=//p' "$f" | tail -1 | cut -c1-200)"
done

echo
[ "$fail" -eq 0 ] || { echo "B-34: RED"; exit 1; }
echo "B-34: both arms create, delete and describe topics and describe the cluster as the broker's own tools see them"
