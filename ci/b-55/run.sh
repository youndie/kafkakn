#!/usr/bin/env bash
# B-55: cooperative rebalancing in a mixed group: a member on each arm, and a third joining mid-stream.
#
# Every member uses partition.assignment.strategy=cooperative-sticky, the portable spelling, and commits
# only on revocation. A writer trickles 1200 records over six partitions. Checked:
# C joins 30 s after B, so that it joins a settled pair: at 15 s both arrivals fell into one rebalance.
#   - cooperative, as each listener reports it: no member gives up everything it holds while the stream
#     runs (that is the eager protocol), and when the third joins, an earlier member gives up a proper
#     subset;
#   - the third member is given partitions at all;
#   - nothing lost and nothing processed twice, against the broker's offsets;
#   - the group's commits reach every partition's end.
#
#   ci/b-55/run.sh
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
TOPIC=kafkakn-coop-$STAMP
GROUP=$TOPIC

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms built first ==="
bash "$H" up || exit 1
./gradlew --console=plain :kafkakn-core:jvmTestClasses :kafkakn-core:linkDebugTestLinuxX64 > build/b-55-build.out 2>&1 \
    || { tail -20 build/b-55-build.out; exit 1; }
PARTITIONS=$PARTITIONS bash "$H" topic "$TOPIC" > /dev/null
rm -rf "$OBS"
member_env() { # <name> [join-after-ms]
    echo "KAFKAKN_COOP_GROUP=$GROUP KAFKAKN_COOP_TOPIC=$TOPIC KAFKAKN_COOP_END=$((RECORDS / PARTITIONS)) KAFKAKN_COOP_NAME=$1 ${2:+KAFKAKN_COOP_JOIN_AFTER_MS=$2}"
}

echo
echo "=== A (jvm) first, B (native) with the writer, C (native) joining 30 s later ==="
# shellcheck disable=SC2046
env $(member_env A) ./gradlew --console=plain :kafkakn-core:jvmTest --rerun --tests '*CooperativeTest.a_member*' > build/b-55-A.out 2>&1 &
a=$!
sleep 20
bash "$H" records trickle "$TOPIC" "$PARTITIONS" "$RECORDS" 40 > build/b-55-trickle.out &
trickle=$!
# shellcheck disable=SC2046
( cd kafkakn-core && env $(member_env B) $KEXE --ktest_filter=$FILTER ) > build/b-55-B.out 2>&1 &
b=$!
# shellcheck disable=SC2046
( cd kafkakn-core && env $(member_env C 30000) $KEXE --ktest_filter=$FILTER ) > build/b-55-C.out 2>&1 &
c=$!
wait "$trickle"; wait "$b"; bc=$?; wait "$c"; cc=$?; wait "$a"; ac=$?
printf '  A exit=%s, B exit=%s, C exit=%s, %s\n' "$ac" "$bc" "$cc" "$(cat build/b-55-trickle.out)"
[ "$ac" -eq 0 ] && [ "$bc" -eq 0 ] && [ "$cc" -eq 0 ] || { tail -8 build/b-55-A.out build/b-55-B.out build/b-55-C.out; echo "  A MEMBER FAILED"; exit 1; }

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
    | awk -F: '{ for (o = 0; o < $3; o++) print $2 ":" o }' | sort > build/b-55-expected.txt
{ fact jvm A seen; echo; fact linuxX64 B seen; echo; fact linuxX64 C seen; } | tr ';' '\n' | grep . | sort > build/b-55-seen.txt
expected=$(wc -l < build/b-55-expected.txt)
lost=$(comm -23 build/b-55-expected.txt <(sort -u build/b-55-seen.txt) | wc -l)
twice=$(uniq -d build/b-55-seen.txt | wc -l)
printf '  %s records on the broker; %s lost, %s processed twice\n' "$expected" "$lost" "$twice"
[ "$expected" -eq "$RECORDS" ] || bad "the broker holds $expected, not $RECORDS"
[ "$lost" -eq 0 ] || bad "LOST $lost records"
[ "$twice" -eq 0 ] || bad "$twice records processed twice"
commits=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$GROUP" 2>/dev/null \
    | awk -v t="$TOPIC" '$2 == t { print $3 ":" $4 "/" $5 }' | sort -n | paste -sd' ')
printf '  committed/end per partition: %s\n' "$commits"
echo "$commits" | tr ' ' '\n' | awk -F'[:/]' 'NF && $2 != $3 { exit 1 }' || bad "the group's commits do not reach every end"

echo
[ "$fail" -eq 0 ] || { echo "B-55: RED"; exit 1; }
echo "B-55: a mixed cooperative group moves only what changes owner when a member joins, and loses and repeats nothing"
