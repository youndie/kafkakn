#!/usr/bin/env bash
# B-27: a record that names its partition is on that partition - according to the broker.
#
# RecordMetadata.partition is the client telling us where it sent the record. The oracle here is the
# broker's own consumer reading ONE PARTITION AT A TIME, so a record found on partition 1 is on
# partition 1 whatever either client believes. Every record ExplicitPartitionTest sent carries its arm
# and the partition it named in its value.
#
#   ci/b-27/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn-part-$(date +%s)
PARTITIONS=3
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=$TOPIC

echo "=== environment ==="
date -Is
echo "  topic $TOPIC, fresh, $PARTITIONS partitions"

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1

echo
echo "=== the sends, one test task per invocation ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    # `--rerun`, and it is not optional: the topic arrives through the environment, which is not an input
    # Gradle tracks, so a second run of this script with no code change found the task UP-TO-DATE,
    # exited 0, ran nothing - and the observations it depended on had just been deleted above.
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*ExplicitPartitionTest*' > "build/b-27-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { tail -5 "build/b-27-$task.out"; echo "  THE SENDS FAILED on $task"; exit 1; }
done

echo
echo "=== each partition, read on its own by the broker's consumer ==="
for p in $(seq 0 $((PARTITIONS - 1))); do
    bash "$H" partition-values "$TOPIC" "$p" > "build/b-27-p$p.values"
done
fail=0
for arm in jvm linuxX64; do
    stamp=$(sed -n 's/^partition.stamp=//p' "$OBS/$arm-local.txt" | tail -1)
    per=$(sed -n 's/^partition.per=//p' "$OBS/$arm-local.txt" | tail -1)
    [ -n "$stamp" ] || { echo "  no stamp from $arm - its test did not run" >&2; exit 1; }
    for named in $(seq 0 $((PARTITIONS - 1))); do
        row=""
        for actual in $(seq 0 $((PARTITIONS - 1))); do
            n=$(grep -c "^$stamp:$named:" "build/b-27-p$actual.values")
            row="$row p$actual=$n"
            if [ "$actual" -eq "$named" ]; then
                [ "$n" -eq "$per" ] || { echo "    $arm named $named: $n of $per on it" >&2; fail=1; }
            else
                [ "$n" -eq 0 ] || { echo "    $arm named $named: $n landed on partition $actual" >&2; fail=1; }
            fi
        done
        printf '  %-9s named %s ->%s\n' "$arm" "$named" "$row"
    done
done

echo
echo "=== a partition the topic does not have: what each arm said, and how long it took ==="
for arm in jvm linuxX64; do
    printf '  %-9s %6s ms  %s\n' "$arm" \
        "$(sed -n 's/^partition.missing.ms=//p' "$OBS/$arm-local.txt" | tail -1)" \
        "$(sed -n 's/^partition.missing.failure=//p' "$OBS/$arm-local.txt" | tail -1 | cut -c1-110)"
done

echo
[ "$fail" -eq 0 ] || { echo "B-27: RED"; exit 1; }
echo "B-27: every record is on the partition it named, on both arms, by the broker's own reading"
