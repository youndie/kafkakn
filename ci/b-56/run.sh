#!/usr/bin/env bash
# B-56: static membership. A member with a group.instance.id, closed and reopened within the session timeout,
# gets its partitions back and the group does not rebalance: on each arm, and in a mixed group both ways.
#
# The member's view is the other member's rebalance listener (StaticMembershipTest). The broker's view is its
# own log: kafka-consumer-groups.sh prints no generation for a classic group (GROUP-EPOCH is "-"), so the
# broker's "Preparing to rebalance group G" and "Stabilized group G generation N" lines are read over the
# restart's window, cut by docker logs --since/--until. A dynamic member's restart is the positive control:
# the same read must find a rebalance there, or finding none elsewhere proves nothing.
#
#   ci/b-56/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
KEXE=./build/bin/linuxX64/debugTest/test.kexe
MIXED=io.github.youndie.kafkakn.StaticMembershipTest.a_static_member_of_a_mixed_group
fail=0
bad() { echo "    $*" >&2; fail=1; }
fact() { sed -n "s/^static\.$2=//p" "$OBS/$1-local.txt" | tail -1; }

# The broker's rebalance lines for group $1 between epoch milliseconds $2 and $3.
rebalances() {
    docker logs --since "$(($2 / 1000))" --until "$(($3 / 1000 + 1))" kafkakn-broker 2>&1 \
        | grep -E "(Preparing to rebalance|Stabilized) group $1 " | sed 's/.*\(Preparing to rebalance\|Stabilized\) group [^ ]* /\1 /' | cut -c1-90
}
# Every rebalance line for group $1 the broker has logged: the group formed, so there is at least one.
formed() { docker logs kafkakn-broker 2>&1 | grep -c "Stabilized group $1 "; }

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms in one process each ==="
bash "$H" up || exit 1
./gradlew --console=plain :kafkakn-core:jvmTestClasses :kafkakn-core:linkDebugTestLinuxX64 > build/b-56-build.out 2>&1 \
    || { tail -20 build/b-56-build.out; exit 1; }
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*StaticMembershipTest*' > "build/b-56-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-56-$task.out" | head -10; echo "  THE SUITE FAILED on $task"; exit 1; }
done
MIN_OBSERVATIONS=5 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
sed 's/^/  /' "$OBS/jvm.txt"

echo
echo "=== the broker's log over each restart ==="
for arm in jvm linuxX64; do
    for kind in restart dynamic; do
        group=$(fact "$arm" "$kind.group")
        read -r from to <<< "$(fact "$arm" "$kind.window")"
        [ -n "$group" ] && [ -n "${to:-}" ] || { bad "$arm recorded no $kind group or window"; continue; }
        lines=$(rebalances "$group" "$from" "$to")
        printf '  %-9s %-8s formed %s time(s); in the window: %s\n' "$arm" "$kind" "$(formed "$group")" "$(echo "${lines:-nothing}" | paste -sd';')"
        [ "$(formed "$group")" -gt 0 ] || bad "$arm $kind: the broker's log never names $group; the read proves nothing"
        case "$kind" in
            restart) [ -z "$lines" ] || bad "$arm: the broker rebalanced the static group while its member restarted" ;;
            dynamic) [ -n "$lines" ] || bad "$arm: the control found no rebalance for a dynamic restart; the read proves nothing" ;;
        esac
    done
done

echo
echo "=== a mixed group, both ways: a stayer on one arm, a restarter on the other ==="
for pair in "jvm linuxX64" "linuxX64 jvm"; do
    read -r stayer restarter <<< "$pair"
    topic=kafkakn-static-mixed-$stayer-$(date +%s)
    PARTITIONS=2 bash "$H" topic "$topic" > /dev/null
    member() { # <arm> <role>
        local vars="KAFKAKN_STATIC_GROUP=$topic KAFKAKN_STATIC_TOPIC=$topic KAFKAKN_STATIC_ROLE=$2"
        if [ "$1" = jvm ]; then
            # shellcheck disable=SC2086
            env $vars ./gradlew --console=plain :kafkakn-core:jvmTest --rerun --tests '*StaticMembershipTest.a_static_member_of_a_mixed_group*'
        else
            # shellcheck disable=SC2086
            (cd kafkakn-core && env $vars $KEXE --ktest_filter=$MIXED)
        fi
    }
    rm -rf "$OBS"
    member "$stayer" stay > "build/b-56-stay-$stayer.out" 2>&1 &
    s=$!
    sleep 10
    member "$restarter" restart > "build/b-56-restart-$restarter.out" 2>&1
    rc=$?
    wait "$s"; sc=$?
    [ "$sc" -eq 0 ] && [ "$rc" -eq 0 ] || { tail -5 "build/b-56-stay-$stayer.out" "build/b-56-restart-$restarter.out"; bad "a member failed ($stayer stay=$sc, $restarter restart=$rc)"; continue; }
    read -r from to <<< "$(fact "$restarter" mixed.restarter.window)"
    partitions=$(fact "$restarter" mixed.restarter.partitions)
    heard=$(fact "$stayer" mixed.stayer.heard)
    inside=$(tr ' ' '\n' <<< "$heard" | awk -F@ -v f="$from" -v t="$to" 'NF == 2 && $2 >= f && $2 <= t' | paste -sd' ')
    lines=$(rebalances "$topic" "$from" "$to")
    printf '  stayer %-9s heard: %s\n' "$stayer" "$heard"
    printf '  restarter %-6s partitions before and after: %s; window %s..%s\n' "$restarter" "$partitions" "$from" "$to"
    printf '  the stayer, in the window: %s; the broker, in the window: %s\n' "${inside:-nothing}" "$(echo "${lines:-nothing}" | paste -sd';')"
    [ -n "$heard" ] || bad "$stayer stayer heard nothing at all, not even its own assignment; the watch proves nothing"
    [ -z "$inside" ] || bad "$stayer stayer heard a rebalance while the $restarter member restarted: $inside"
    [ -z "$lines" ] || bad "the broker rebalanced the mixed group while the $restarter member restarted"
    read -r before after <<< "$partitions"
    [ -n "$before" ] && [ "$before" = "$after" ] || bad "$restarter restarter held $before before and $after after"
done

echo
[ "$fail" -eq 0 ] || { echo "B-56: RED"; exit 1; }
echo "B-56: a static member restarts without a rebalance on both arms and in a mixed group, and a duplicate is fenced alike"
