#!/usr/bin/env bash
# B-60: move an empty group's offsets, delete them, delete the group, and the broker's refusal while the
# group has a member: on both arms, against the broker's own tools.
#
# Each arm's AdminGroupOffsetsTest makes a topic of its own (ten records in partition 0, four in 1). What the
# arms say is compared across them; then, per arm:
#   - the moved group's commit is what kafka-consumer-groups.sh --describe prints (0:7), and a member that is
#     not kafkakn at all, the distribution's console consumer, joining it afterwards starts partition 0 at 7;
#   - the deleted group is absent from kafka-consumer-groups.sh --list.
#
#   ci/b-60/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }
fact() { sed -n "s/^admin\.$2=//p" "$OBS/$1-local.txt" | tail -1; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms ==="
bash "$H" up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminGroupOffsetsTest*' > "build/b-60-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-60-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=6 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== what each client said when refused ==="
for arm in jvm linuxX64; do
    for what in refused.alter refused.delete.offsets refused.delete.group deleted.missing; do
        printf '  %-9s %-22s %s\n' "$arm" "$what" "$(fact "$arm" "$what.said")"
    done
done

echo
echo "=== each arm against the broker's tools ==="
for arm in jvm linuxX64; do
    moved=$(fact "$arm" reset.group)
    deleted=$(fact "$arm" deleted.group)
    [ -n "$moved" ] && [ -n "$deleted" ] || { bad "$arm recorded no group"; continue; }

    committed=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$moved" 2>/dev/null \
        | awk -v g="$moved" '$1 == g { print $3 ":" $4 }' | sort -n | paste -sd' ')
    printf '  %-9s moved group, kafka-consumer-groups --describe: %s\n' "$arm" "$committed"
    [ "$committed" = "0:7" ] || bad "$arm: the moved group's commits are '$committed', not 0:7"

    # The console consumer joins the moved group: partition 0 from its commit, partition 1 (never committed)
    # from the beginning. It commits as it goes, so it runs after the describe above.
    first=$(kc /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic "$moved" --group "$moved" \
        --consumer-property auto.offset.reset=earliest --max-messages 7 --timeout-ms 20000 \
        --property print.partition=true --property print.offset=true 2>/dev/null \
        | sed -n 's/^Partition:\([0-9]*\)[[:space:]]*Offset:\([0-9]*\).*/\1:\2/p' | awk -F: '$1 == 0' | head -1)
    printf '  %-9s moved group, first record the console consumer read from partition 0: %s\n' "$arm" "$first"
    [ "$first" = "0:7" ] || bad "$arm: a third-party member of the moved group started partition 0 at '$first', not 0:7"

    # The moved group is the control: it exists, so a listing that does not show it proves nothing absent.
    listed=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --list 2>/dev/null)
    printf '%s\n' "$listed" | grep -qx -e "$moved" || bad "$arm: the listing does not show the moved group either; it proves nothing"
    if printf '%s\n' "$listed" | grep -qx -e "$deleted"; then
        bad "$arm: the deleted group $deleted is still listed"
    else
        printf '  %-9s deleted group, kafka-consumer-groups --list: absent (the moved group, listed: yes)\n' "$arm"
    fi
done

echo
[ "$fail" -eq 0 ] || { echo "B-60: RED"; exit 1; }
echo "B-60: both arms move, delete and refuse as the broker's tools count, and agree"
