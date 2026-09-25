#!/usr/bin/env bash
# B-59: a group's committed offsets and a partition's offsets, read by the admin client on both arms, against
# the broker's own tools.
#
# Each arm's AdminOffsetsTest makes a topic of its own (three partitions: five records, three, none, with
# timestamps a second apart) and a group that commits two of them without joining. What the two arms answer
# is compared across the arms; each arm's answers are then held against the broker's tools on that arm's
# topic and group:
#   - the committed offsets against kafka-consumer-groups.sh --describe (CURRENT-OFFSET);
#   - earliest, latest and four timestamps against kafka-get-offsets.sh --time, which prints nothing for a
#     partition with no record that late: "none" on this side.
#
#   ci/b-59/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
PARTITIONS=3
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }
fact() { sed -n "s/^admin\.offsets\.$2=//p" "$OBS/$1-local.txt" | tail -1; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms ==="
bash "$H" up || exit 1
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*AdminOffsetsTest*' > "build/b-59-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-59-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=9 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

# "0:4 1:3" from the tool's lines: one entry per partition it printed, in partition order.
committed_by_tool() { # <group>
    kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$1" 2>/dev/null \
        | awk -v g="$1" '$1 == g { print $3 ":" $4 }' | sort -n | paste -sd' '
}
# "0:3 1:none 2:none": every partition, "none" where the tool printed nothing.
offsets_by_tool() { # <topic> <time>
    local printed
    printed=$(kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$1" --time "$2" 2>/dev/null)
    for ((p = 0; p < PARTITIONS; p++)); do
        o=$(printf '%s\n' "$printed" | awk -F: -v t="$1" -v p="$p" '$1 == t && $2 == p { print $3 }')
        printf '%s:%s\n' "$p" "${o:-none}"
    done | paste -sd' '
}

echo
echo "=== each arm against the broker's tools ==="
for arm in jvm linuxX64; do
    group=$(fact "$arm" group)
    topic=$(fact "$arm" topic)
    [ -n "$group" ] && [ -n "$topic" ] || { bad "$arm recorded no group or topic"; continue; }
    said=$(fact "$arm" committed)
    tool=$(committed_by_tool "$group")
    printf '  %-9s committed   %-22s kafka-consumer-groups: %s\n' "$arm" "$said" "$tool"
    [ "$said" = "$tool" ] || bad "$arm: committed $said, the broker's tool says $tool"
    for name in earliest latest between exact before after; do
        case "$name" in
            earliest | latest) time=$name ;;
            *) time=$(fact "$arm" "$name.at") ;;
        esac
        said=$(fact "$arm" "$name")
        tool=$(offsets_by_tool "$topic" "$time")
        printf '  %-9s %-11s %-22s kafka-get-offsets --time %s: %s\n' "$arm" "$name" "$said" "$time" "$tool"
        [ "$said" = "$tool" ] || bad "$arm: $name is $said, the broker's tool says $tool"
    done
done

echo
[ "$fail" -eq 0 ] || { echo "B-59: RED"; exit 1; }
echo "B-59: both arms read a group's commits and a partition's offsets as the broker's tools do, and agree"
