#!/usr/bin/env bash
# B-37: consumer groups on both arms - and one group with a member on EACH arm.
#
# The promise is at-least-once, so the check is for LOSS: the union of what every member saw must
# cover every offset the broker holds, and a record seen twice is allowed. The records trickle in
# while the group forms (ci/harness/Records.java trickle), so a split meets records. The group's
# commits are read by kafka-consumer-groups.sh, never by the consumer that made them.
#
#   ci/b-37/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
STAMP=$(date +%s)
PARTITIONS=4
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=kafkakn-b37-$STAMP
kc() { docker exec kafkakn-broker "$@"; }
fail=0
bad() { echo "    $*" >&2; fail=1; }

# Every partition:offset the broker holds for a topic, one per line.
expected() {
    kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$1" --time -1 2>/dev/null \
        | awk -F: '{ for (o = 0; o < $3; o++) print $2 ":" o }' | sort
}
# The group's committed offsets against the log end, per partition, as the broker's tool says.
commits() {
    kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$1" 2>/dev/null \
        | awk -v t="$2" '$2 == t { print $3 ":" $4 "/" $5 }' | sort -n
}
check() { # <label> <topic> <group> <seen-file>
    expected "$2" > "build/b-37-$1-expected.txt"
    tr ';' '\n' < "$4" | grep . | sort -u > "build/b-37-$1-seen.txt"
    lost=$(comm -23 "build/b-37-$1-expected.txt" "build/b-37-$1-seen.txt" | wc -l)
    printf '  %-9s %s records on the broker, %s seen by the group, %s lost\n' "$1" \
        "$(wc -l < "build/b-37-$1-expected.txt")" "$(wc -l < "build/b-37-$1-seen.txt")" "$lost"
    [ "$(wc -l < "build/b-37-$1-expected.txt")" -gt 0 ] || bad "$1: the broker holds nothing - the trickle did not run"
    [ "$lost" -eq 0 ] || { bad "$1: LOST $lost records:"; comm -23 "build/b-37-$1-expected.txt" "build/b-37-$1-seen.txt" | head -5 >&2; }
    c=$(commits "$3" "$2")
    printf '  %-9s committed/end per partition: %s\n' "$1" "$(echo "$c" | paste -sd' ')"
    echo "$c" | awk -F'[:/]' '$2 != $3 { exit 1 }' || bad "$1: the group's commits do not reach the log end"
    [ "$(echo "$c" | grep -c .)" -eq "$PARTITIONS" ] || bad "$1: commits for $(echo "$c" | grep -c .) partitions, not $PARTITIONS"
}

echo "=== environment ==="
date -Is
echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$KAFKAKN_TOPIC" >/dev/null
rm -rf "$OBS"

echo
echo "=== one group per arm: two members, one leaves the way a crash would ==="
# Built first, and the native arm run as its test binary rather than through Gradle: the writer runs for
# about forty seconds from the moment it starts, and a member that joins after it has finished has no
# batch to abandon. Measured: with the native task compiling and linking inside the window, the leaving
# native member found nothing left to read, and the crash had nothing to redeliver.
./gradlew --console=plain :kafkakn-core:jvmTestClasses :kafkakn-core:linkDebugTestLinuxX64 > build/b-37-build.out 2>&1 \
    || { tail -20 build/b-37-build.out; exit 1; }
for arm in jvm linuxX64; do
    topic=kafkakn-group-$arm-$STAMP
    PARTITIONS=$PARTITIONS bash "$H" topic "$topic" >/dev/null
    export KAFKAKN_GROUP_TOPIC=$topic KAFKAKN_GROUP_PARTITIONS=$PARTITIONS KAFKAKN_GROUP_LEAVE_MS=10000 KAFKAKN_GROUP_END=200
    bash "$H" records trickle "$topic" "$PARTITIONS" 800 40 > "build/b-37-$arm-trickle.out" &
    trickle=$!
    if [ "$arm" = jvm ]; then
        ./gradlew --console=plain :kafkakn-core:jvmTest --rerun --tests '*GroupTest.two_members*' > "build/b-37-$arm.out" 2>&1
    else
        ( cd kafkakn-core && ./build/bin/linuxX64/debugTest/test.kexe \
            --ktest_filter='io.github.youndie.kafkakn.GroupTest.two_members_share_the_partitions_hand_them_over_and_lose_nothing' ) \
            > "build/b-37-$arm.out" 2>&1
    fi
    code=$?
    unset KAFKAKN_GROUP_TOPIC KAFKAKN_GROUP_PARTITIONS KAFKAKN_GROUP_LEAVE_MS KAFKAKN_GROUP_END
    wait "$trickle"
    printf '  %-9s exit=%s, %s\n' "$arm" "$code" "$(cat "build/b-37-$arm-trickle.out")"
    [ "$code" -eq 0 ] || { tail -30 "build/b-37-$arm.out"; echo "  THE MEMBERS FAILED on $arm"; exit 1; }
    f="$OBS/$arm-local.txt"
    sed -n 's/^group\.seen=//p' "$f" | tail -1 > "build/b-37-$arm-seen.raw"
    abandoned=$(sed -n 's/^group\.abandoned=//p' "$f" | tail -1)
    printf '  %-9s leaving held %s; staying held %s; %s seen twice\n' "$arm" \
        "$(sed -n 's/^group\.leaving\.held=//p' "$f" | tail -1)" "$(sed -n 's/^group\.staying\.held=//p' "$f" | tail -1)" \
        "$(sed -n 's/^group\.twice=//p' "$f" | tail -1)"
    # The crash's batch is NOT in anyone's `seen` from the member that dropped it. If the union still
    # holds it, another member was given it again - which is the at-least-once promise, observed.
    [ -n "$abandoned" ] || bad "$arm: the leaving member abandoned nothing - the crash had nothing to redeliver"
    printf '  %-9s abandoned uncommitted by the leaving member: %s\n' "$arm" "$abandoned"
    check "$arm" "$topic" "$(sed -n 's/^group\.id=//p' "$f" | tail -1)" "build/b-37-$arm-seen.raw"
    missing=0
    for record in $(echo "$abandoned" | tr ';' ' '); do
        grep -qx "$record" "build/b-37-$arm-seen.txt" || { bad "$arm: abandoned $record was never delivered again"; missing=1; }
    done
    # Said only when true: a summary line printed after a failure is the kind that gets read instead of it.
    [ "$missing" -eq 1 ] || [ -z "$abandoned" ] || printf '  %-9s every abandoned record was delivered again, to the member that stayed\n' "$arm"
done

echo
echo "=== one group, one member on each arm, at the same time ==="
MIXED_TOPIC=kafkakn-mixed-$STAMP
MIXED_GROUP=kafkakn-mixed-$STAMP
PARTITIONS=$PARTITIONS bash "$H" topic "$MIXED_TOPIC" >/dev/null
rm -rf "$OBS"
# The JVM member first; it stays until it holds all four partitions and has seen the last record of
# each. The native one joins twenty seconds in, with the trickle, and leaves fifteen seconds later
# without committing its last batch. Stopping the stayer on the data rather than a clock is measured
# necessity: with fixed durations the same run passed once and lost 465 records the next, when Gradle
# happened to start faster.
KAFKAKN_MIXED_GROUP=$MIXED_GROUP KAFKAKN_MIXED_TOPIC=$MIXED_TOPIC KAFKAKN_MIXED_PARTITIONS=$PARTITIONS KAFKAKN_MIXED_END=150 \
    ./gradlew --console=plain :kafkakn-core:jvmTest --rerun --tests '*GroupTest.a_member_of_a_group*' > build/b-37-mixed-jvm.out 2>&1 &
jvm=$!
sleep 20
bash "$H" records trickle "$MIXED_TOPIC" "$PARTITIONS" 600 40 > build/b-37-mixed-trickle.out &
trickle=$!
( cd kafkakn-core && KAFKAKN_MIXED_GROUP=$MIXED_GROUP KAFKAKN_MIXED_TOPIC=$MIXED_TOPIC KAFKAKN_MIXED_LEAVE_MS=15000 \
    ./build/bin/linuxX64/debugTest/test.kexe \
    --ktest_filter='io.github.youndie.kafkakn.GroupTest.a_member_of_a_group_whose_other_member_is_the_other_arm' ) > build/b-37-mixed-native.out 2>&1
native=$?
wait "$trickle"
wait "$jvm"
jcode=$?
printf '  jvm member exit=%s, native member exit=%s, %s\n' "$jcode" "$native" "$(cat build/b-37-mixed-trickle.out)"
[ "$jcode" -eq 0 ] && [ "$native" -eq 0 ] || { tail -20 build/b-37-mixed-jvm.out build/b-37-mixed-native.out; echo "  A MEMBER FAILED"; exit 1; }
for arm in jvm linuxX64; do
    f="$OBS/$arm-local.txt"
    [ "$(sed -n 's/^group\.mixed=//p' "$f" | tail -1)" = member ] || bad "$arm: was not a member of the mixed group"
    printf '  %-9s held %s\n' "$arm" "$(sed -n 's/^group\.mixed\.held=//p' "$f" | tail -1)"
done
jvm_last=$(sed -n 's/^group\.mixed\.held=//p' "$OBS/jvm-local.txt" | tail -1 | awk -F'|' '{ print $NF }')
[ "$jvm_last" = "[0,1,2,3]" ] || bad "mixed: the JVM member ended with $jvm_last, not all four"
native_held=$(sed -n 's/^group\.mixed\.held=//p' "$OBS/linuxX64-local.txt" | tail -1)
echo "$native_held" | tr '|' '\n' | grep -qE '^\[[0-9](,[0-9]){0,2}\]$' || bad "mixed: the native member never held a proper share: $native_held"
{ sed -n 's/^group\.mixed\.seen=//p' "$OBS/jvm-local.txt" | tail -1; echo ';'; sed -n 's/^group\.mixed\.seen=//p' "$OBS/linuxX64-local.txt" | tail -1; } | tr -d '\n' > build/b-37-mixed-seen.raw
check mixed "$MIXED_TOPIC" "$MIXED_GROUP" build/b-37-mixed-seen.raw
# The native member left the way a crash would; what it abandoned must have reached the JVM member.
mixed_abandoned=$(sed -n 's/^group\.mixed\.abandoned=//p' "$OBS/linuxX64-local.txt" | tail -1)
[ -n "$mixed_abandoned" ] || bad "mixed: the native member abandoned nothing - the crash had nothing to redeliver"
missing=0
for record in $(echo "$mixed_abandoned" | tr ';' ' '); do
    grep -qx "$record" build/b-37-mixed-seen.txt || { bad "mixed: abandoned $record was never delivered again"; missing=1; }
done
[ "$missing" -eq 1 ] || [ -z "$mixed_abandoned" ] || echo "  mixed     the native member abandoned $mixed_abandoned; the JVM member was given it again"

echo
[ "$fail" -eq 0 ] || { echo "B-37: RED"; exit 1; }
echo "B-37: groups split, hand over and lose nothing on each arm, and a group with one member per arm does the same"
