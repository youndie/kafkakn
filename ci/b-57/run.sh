#!/usr/bin/env bash
# B-57: the KIP-848 group protocol (group.protocol=consumer) in a mixed group: a member on each arm, and a
# third joining mid-stream. It is B-55's run, with the members under the new protocol instead of
# cooperative-sticky, and the same checks, since the new protocol's rebalances are incremental too:
#   - no member gives up everything it holds while the stream runs, and when the third joins an earlier
#     member gives up a proper subset;
#   - the third member is given partitions at all;
#   - nothing lost and nothing processed twice, against the broker's offsets (members commit only on
#     revocation, so this is also B-50's listener promise under the new protocol);
#   - the group's commits reach every partition's end;
#   - the broker's own tool lists the group as TYPE Consumer, PROTOCOL consumer, so the members did not
#     quietly fall back to the classic protocol.
#
#   ci/b-57/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
PARTITIONS=6
RECORDS=1200
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }
KEXE=./build/bin/linuxX64/debugTest/test.kexe
FILTER=io.github.youndie.kafkakn.CooperativeTest.a_member_of_a_cooperative_group
STAMP=$(date +%s)
TOPIC=kafkakn-848-$STAMP
GROUP=$TOPIC

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms built first ==="
bash "$H" up || exit 1
./gradlew --console=plain :kafkakn-core:jvmTestClasses :kafkakn-core:linkDebugTestLinuxX64 > build/b-57-build.out 2>&1 \
    || { tail -20 build/b-57-build.out; exit 1; }

echo
echo "=== a lone member on each arm, and the keys the protocol moves to the broker ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*ConsumerProtocolTest*' > "build/b-57-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-57-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=7 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"
lone=$(sed -n 's/^848\.lone\.group=//p' "$OBS/linuxX64-local.txt" | tail -1)
printf '  the native lone group, as the broker lists it: %s\n' \
    "$(kc /opt/kafka/bin/kafka-groups.sh --bootstrap-server 127.0.0.1:9092 --list 2>/dev/null | awk -v g="$lone" '$1 == g { print $2 " " $3 }')"

PARTITIONS=$PARTITIONS bash "$H" topic "$TOPIC" > /dev/null
rm -rf "$OBS"
member_env() { # <name> [join-after-ms]
    echo "KAFKAKN_COOP_PROTOCOL=consumer KAFKAKN_COOP_GROUP=$GROUP KAFKAKN_COOP_TOPIC=$TOPIC KAFKAKN_COOP_END=$((RECORDS / PARTITIONS)) KAFKAKN_COOP_NAME=$1 ${2:+KAFKAKN_COOP_JOIN_AFTER_MS=$2}"
}

echo
echo "=== A (jvm) first, B (native) with the writer, C (native) joining 30 s later ==="
# shellcheck disable=SC2046
env $(member_env A) ./gradlew --console=plain :kafkakn-core:jvmTest --rerun --tests '*CooperativeTest.a_member*' > build/b-57-A.out 2>&1 &
a=$!
sleep 20
bash "$H" records trickle "$TOPIC" "$PARTITIONS" "$RECORDS" 40 > build/b-57-trickle.out &
trickle=$!
# shellcheck disable=SC2046
( cd kafkakn-core && env $(member_env B) $KEXE --ktest_filter=$FILTER ) > build/b-57-B.out 2>&1 &
b=$!
# shellcheck disable=SC2046
( cd kafkakn-core && env $(member_env C 30000) $KEXE --ktest_filter=$FILTER ) > build/b-57-C.out 2>&1 &
c=$!
wait "$trickle"; wait "$b"; bc=$?; wait "$c"; cc=$?; wait "$a"; ac=$?
printf '  A exit=%s, B exit=%s, C exit=%s, %s\n' "$ac" "$bc" "$cc" "$(cat build/b-57-trickle.out)"
[ "$ac" -eq 0 ] && [ "$bc" -eq 0 ] && [ "$cc" -eq 0 ] || { tail -8 build/b-57-A.out build/b-57-B.out build/b-57-C.out; echo "  A MEMBER FAILED"; exit 1; }

fact() { sed -n "s/^coop\.$2\.$3=//p" "$OBS/$1-local.txt" | tail -1; }
declare -A ARM=([A]=jvm [B]=linuxX64 [C]=linuxX64)

echo
echo "=== what each listener reported ==="
for m in A B C; do
    printf '  %s (%s): %s\n' "$m" "${ARM[$m]}" "$(fact "${ARM[$m]}" "$m" events)"
    printf '       at: %s\n' "$(fact "${ARM[$m]}" "$m" times)"
done
partial=0
for m in A B C; do
    # Replay the events: whatever a "-" takes while the stream runs must be less than all that is held.
    # The last event is the revocation on close, which gives up everything and is exempt.
    verdict=$(fact "${ARM[$m]}" "$m" events | python3 -c '
import sys
import re
# One event per match: the listener renders a list as "[0, 1]", with spaces, so splitting on whitespace
# cut every event in pieces (the first version of this script did, and called a cooperative run eager).
events = re.findall(r"[-+!]\[[^\]]*\]", sys.stdin.read())
held, full, partial = set(), 0, 0
for i, e in enumerate(events):
    ps = set(int(p) for p in e[2:-1].split(",") if p.strip())
    if e[0] == "+":
        held |= ps
    else:
        if e[0] == "-" and i < len(events) - 1:
            if ps == held and held:
                full += 1
            elif ps < held:
                partial += 1
        held -= ps
print(full, partial)')
    read -r full part <<< "$verdict"
    [ "$full" -eq 0 ] || bad "$m gave up everything it held $full time(s) mid-stream: that is eager, not cooperative"
    [ "$m" = C ] || partial=$((partial + part))
done
[ "$partial" -gt 0 ] || bad "neither A nor B gave up a proper subset when C joined: nothing moved"
[ -n "$(fact linuxX64 C events | grep -o '+')" ] || bad "C was never assigned anything"

echo
echo "=== lost and duplicated, against the broker ==="
kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$TOPIC" --time -1 2>/dev/null \
    | awk -F: '{ for (o = 0; o < $3; o++) print $2 ":" o }' | sort > build/b-57-expected.txt
{ fact jvm A seen; echo; fact linuxX64 B seen; echo; fact linuxX64 C seen; } | tr ';' '\n' | grep . | sort > build/b-57-seen.txt
expected=$(wc -l < build/b-57-expected.txt)
lost=$(comm -23 build/b-57-expected.txt <(sort -u build/b-57-seen.txt) | wc -l)
twice=$(uniq -d build/b-57-seen.txt | wc -l)
printf '  %s records on the broker; %s lost, %s processed twice\n' "$expected" "$lost" "$twice"
[ "$expected" -eq "$RECORDS" ] || bad "the broker holds $expected, not $RECORDS"
[ "$lost" -eq 0 ] || bad "LOST $lost records"
[ "$twice" -eq 0 ] || bad "$twice records processed twice"
commits=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$GROUP" 2>/dev/null \
    | awk -v t="$TOPIC" '$2 == t { print $3 ":" $4 "/" $5 }' | sort -n | paste -sd' ')
printf '  committed/end per partition: %s\n' "$commits"
echo "$commits" | tr ' ' '\n' | awk -F'[:/]' 'NF && $2 != $3 { exit 1 }' || bad "the group's commits do not reach every end"

echo
echo
echo "=== the group, as the broker's own tool lists it ==="
listed=$(kc /opt/kafka/bin/kafka-groups.sh --bootstrap-server 127.0.0.1:9092 --list 2>/dev/null | awk -v g="$GROUP" '$1 == g { print $2 " " $3 }')
state=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$GROUP" --state 2>/dev/null \
    | awk -v g="$GROUP" '$1 == g { print "assignor=" $4 " state=" $5 }')
printf '  type and protocol: %s; %s\n' "${listed:-not listed}" "$state"
[ "$listed" = "Consumer consumer" ] || bad "the group is '$listed', not a consumer-protocol group"

echo
[ "$fail" -eq 0 ] || { echo "B-57: RED"; exit 1; }
echo "B-57: a mixed group under the KIP-848 protocol moves only what changes owner, and loses and repeats nothing"
