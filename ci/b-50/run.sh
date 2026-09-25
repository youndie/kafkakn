#!/usr/bin/env bash
# B-50: the rebalance listener, in a group with one member on each arm, in both directions.
#
# Each member commits ONLY in its revocation callback, never in its loop. So a record processed and not
# committed on revocation is delivered again to the next owner, and a duplicate is exactly what a listener
# that does not commit, or commits the wrong thing, produces. Counted against the broker, per round:
#   - lost: a record the broker holds that no member processed;
#   - duplicated: a record processed more than once across the group;
#   - the group's commits reach every partition's end, as kafka-consumer-groups.sh reads them;
#   - the member that left revoked last, and the member that stayed was assigned everything at the end.
#
#   ci/b-50/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
PARTITIONS=4
RECORDS=1200
# The leaver leaves once it has PROCESSED this many, not after a time: a fixed fifteen seconds once left it
# holding its share too briefly to process anything, and a revocation with nothing to commit tests nothing.
LEAVE_AFTER=50
fail=0
bad() { echo "    $*" >&2; fail=1; }
kc() { docker exec kafkakn-broker "$@"; }
fact() { sed -n "s/^rebalance\.$2=//p" "$OBS/$1-local.txt" | tail -1; }
KEXE=./build/bin/linuxX64/debugTest/test.kexe
FILTER=io.github.youndie.kafkakn.RebalanceListenerTest.a_member_that_commits_only_on_revocation_hands_over_without_loss_or_duplicates

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms built first ==="
bash "$H" up || exit 1
# Built before any round starts, and the native member run as its test binary: a member whose Gradle task
# is still linking when the writer finishes has no handover to take part in (B-37 measured that).
./gradlew --console=plain :kafkakn-core:jvmTestClasses :kafkakn-core:linkDebugTestLinuxX64 > build/b-50-build.out 2>&1 \
    || { tail -20 build/b-50-build.out; exit 1; }

jvm_member() { # <group> <topic> [leave-after-records]
    # `env`, because an assignment that comes out of an expansion is not an assignment to the shell.
    env KAFKAKN_REBALANCE_GROUP="$1" KAFKAKN_REBALANCE_TOPIC="$2" KAFKAKN_REBALANCE_PARTITIONS=$PARTITIONS \
        KAFKAKN_REBALANCE_END=$((RECORDS / PARTITIONS)) ${3:+KAFKAKN_REBALANCE_LEAVE_AFTER=$3} \
        ./gradlew --console=plain :kafkakn-core:jvmTest --rerun --tests '*RebalanceListenerTest.a_member*'
}
native_member() { # <group> <topic> [leave-after-records]
    ( cd kafkakn-core && env KAFKAKN_REBALANCE_GROUP="$1" KAFKAKN_REBALANCE_TOPIC="$2" KAFKAKN_REBALANCE_PARTITIONS=$PARTITIONS \
        KAFKAKN_REBALANCE_END=$((RECORDS / PARTITIONS)) ${3:+KAFKAKN_REBALANCE_LEAVE_AFTER=$3} \
        $KEXE --ktest_filter=$FILTER )
}

round() { # <stayer-arm> <leaver-arm>
    local stayer=$1 leaver=$2 stamp topic group
    stamp=$(date +%s)
    topic=kafkakn-rebalance-$stayer-stays-$stamp
    group=$topic
    echo
    echo "=== $stayer stays, $leaver leaves after processing $LEAVE_AFTER records ==="
    PARTITIONS=$PARTITIONS bash "$H" topic "$topic" >/dev/null
    rm -rf "$OBS"
    # The stayer first. The writer starts with the leaver and writes for about fifty seconds, so the leaver
    # joins, takes a share, processes LEAVE_AFTER records of it, and hands it back mid-stream.
    "${stayer}_member_run" "$group" "$topic" > "build/b-50-$stayer-stays.out" 2>&1 &
    local stayer_pid=$!
    [ "$stayer" = jvm ] && sleep 20 || sleep 3
    bash "$H" records trickle "$topic" "$PARTITIONS" "$RECORDS" 40 > "build/b-50-$stayer-trickle.out" &
    local trickle=$!
    "${leaver}_member_run" "$group" "$topic" "$LEAVE_AFTER" > "build/b-50-$leaver-leaves.out" 2>&1
    local leaver_code=$?
    wait "$trickle"
    wait "$stayer_pid"
    local stayer_code=$?
    printf '  %s (stayed) exit=%s, %s (left) exit=%s, %s\n' "$stayer" "$stayer_code" "$leaver" "$leaver_code" \
        "$(cat "build/b-50-$stayer-trickle.out")"
    [ "$stayer_code" -eq 0 ] && [ "$leaver_code" -eq 0 ] \
        || { tail -15 "build/b-50-$stayer-stays.out" "build/b-50-$leaver-leaves.out"; bad "a member failed"; return; }
    for arm in "$stayer" "$leaver"; do
        printf '  %-9s %-6s events: %s\n' "$arm" "$(fact "$arm" member)" "$(fact "$arm" events)"
    done
    [ "$(fact "$stayer" member)" = stayed ] || bad "$stayer did not run as the member that stays"
    [ "$(fact "$leaver" member)" = left ] || bad "$leaver did not run as the member that leaves"
    last_leaver=$(fact "$leaver" events | awk '{ print $NF }')
    [ "${last_leaver#-}" != "$last_leaver" ] || bad "$leaver: the member that left did not revoke last: $last_leaver"
    last_assigned=$(fact "$stayer" events | tr ' ' '\n' | grep '^+' | tail -1)
    [ "$last_assigned" = "+[0,1,2,3]" ] || bad "$stayer: the member that stayed was last assigned $last_assigned, not everything"

    # Lost and duplicated, against every record the broker holds.
    kc /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server 127.0.0.1:9092 --topic "$topic" --time -1 2>/dev/null \
        | awk -F: '{ for (o = 0; o < $3; o++) print $2 ":" o }' | sort > "build/b-50-$stayer-expected.txt"
    { fact "$stayer" seen; echo; fact "$leaver" seen; } | tr ';' '\n' | grep . | sort > "build/b-50-$stayer-seen.txt"
    expected=$(wc -l < "build/b-50-$stayer-expected.txt")
    lost=$(comm -23 "build/b-50-$stayer-expected.txt" <(sort -u "build/b-50-$stayer-seen.txt") | wc -l)
    twice=$(uniq -d "build/b-50-$stayer-seen.txt" | wc -l)
    printf '  %s records on the broker; %s lost, %s processed twice\n' "$expected" "$lost" "$twice"
    [ "$expected" -eq "$RECORDS" ] || bad "the broker holds $expected records, not $RECORDS: the writer did not finish"
    [ "$lost" -eq 0 ] || bad "LOST $lost records"
    [ "$twice" -eq 0 ] || bad "$twice records were processed twice: a revocation did not commit what was processed"
    [ -n "$(fact "$leaver" committed)" ] || bad "$leaver committed nothing on revocation: the handover had nothing to hand over"
    printf '  %s committed on revoking: %s\n' "$leaver" "$(fact "$leaver" committed)"

    commits=$(kc /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$group" 2>/dev/null \
        | awk -v t="$topic" '$2 == t { print $3 ":" $4 "/" $5 }' | sort -n | paste -sd' ')
    printf '  committed/end per partition: %s\n' "$commits"
    echo "$commits" | tr ' ' '\n' | awk -F'[:/]' 'NF && $2 != $3 { exit 1 }' || bad "the group's commits do not reach every partition's end"
    [ "$(echo "$commits" | wc -w)" -eq "$PARTITIONS" ] || bad "commits for $(echo "$commits" | wc -w) partitions, not $PARTITIONS"
}
jvm_member_run() { jvm_member "$@"; }
linuxX64_member_run() { native_member "$@"; }

round jvm linuxX64
round linuxX64 jvm

echo
[ "$fail" -eq 0 ] || { echo "B-50: RED"; exit 1; }
echo "B-50: in a mixed group, each arm's listener commits on revocation, and the handover loses nothing and repeats nothing"
