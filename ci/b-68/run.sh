#!/usr/bin/env bash
# B-68: a native poll must not hand the caller a record of a partition that a rebalance callback run inside that
# same poll gave up. Reproduced where B-65 first saw it: a member frozen past its session and resumed. On
# SIGCONT its fetcher and its heartbeat start at once, so the loss can reach it while a poll is part-way
# through collecting records. Once is rare (1 native stray in 5 of B-65's rounds), so one run freezes the native
# member FREEZES times in a row, with a JVM member holding the group meanwhile and records trickling.
#
# Each stray is placed against the member's own onLost: its offset, and how long after the callback it was
# handed over. Zero strays over every freeze is the fix holding. And PollAfterRebalanceTest runs on both arms:
# the same promise for an ordinary eager rebalance.
#
#   ci/b-68/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
PARTITIONS=4
FREEZES=${FREEZES:-8}
EVERY=20
FREEZE_FOR=15
STAY=$((20 + FREEZES * (EVERY + FREEZE_FOR)))
RECORDS=$(((STAY + 10) * 20))
fail=0
bad() { echo "    $*" >&2; fail=1; }
KEXE=./build/bin/linuxX64/debugTest/test.kexe
FILTER=io.github.youndie.kafkakn.SessionExpiryTest.a_member_of_a_group_whose_session_may_expire

echo "=== environment ==="
date -Is

echo
echo "=== broker, and both arms built first ==="
bash "$H" up || exit 1
./gradlew --console=plain :kafkakn-core:jvmTestClasses :kafkakn-core:linkDebugTestLinuxX64 > build/b-68-build.out 2>&1 \
    || { tail -20 build/b-68-build.out; exit 1; }

echo
echo "=== an ordinary eager rebalance, on both arms ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*PollAfterRebalanceTest*' > "build/b-68-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s, strays %s\n' "$task" "$code" "$(sed -n 's/^poll\.rebalance\.strays=//p' "$OBS"/*-local.txt 2>/dev/null | tail -1)"
    [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-68-$task.out" | head -6; bad "$task failed"; }
done

echo
echo "=== the native member frozen $FREEZES times for ${FREEZE_FOR} s, every ${EVERY} s; a JVM member holds the group ==="
topic=kafkakn-b68-$(date +%s)
PARTITIONS=$PARTITIONS bash "$H" topic "$topic" > /dev/null
rm -rf "$OBS"
vars="KAFKAKN_SESSION_GROUP=$topic KAFKAKN_SESSION_TOPIC=$topic KAFKAKN_SESSION_END=$((RECORDS / PARTITIONS)) KAFKAKN_SESSION_STAY_S=$STAY"
# shellcheck disable=SC2086
env $vars KAFKAKN_SESSION_NAME=jvm ./gradlew --console=plain :kafkakn-core:jvmTest --rerun --tests '*SessionExpiryTest.a_member*' \
    > build/b-68-jvm.out 2>&1 &
gradle=$!
# shellcheck disable=SC2086
( cd kafkakn-core && exec env $vars KAFKAKN_SESSION_NAME=linuxX64 $KEXE --ktest_filter=$FILTER ) > build/b-68-native.out 2>&1 &
native=$!
bash "$H" records trickle "$topic" "$PARTITIONS" "$RECORDS" 50 > build/b-68-trickle.out &
trickle=$!
sleep 20
for ((i = 1; i <= FREEZES; i++)); do
    kill -STOP "$native"
    sleep "$FREEZE_FOR"
    kill -CONT "$native"
    sleep "$EVERY"
done
wait "$trickle"
wait "$gradle"; jc=$?
wait "$native"; nc=$?
printf '  jvm exit=%s, native exit=%s, %s\n' "$jc" "$nc" "$(cat build/b-68-trickle.out)"
[ "$jc" -eq 0 ] && [ "$nc" -eq 0 ] || { tail -6 build/b-68-jvm.out build/b-68-native.out; bad "a member failed"; }

fact() { sed -n "s/^session\.$2\.$3=//p" "$OBS/$1-local.txt" | tail -1; }
heard=$(fact linuxX64 linuxX64 heard)
strays=$(fact linuxX64 linuxX64 stray.records)
lost=$(grep -o '!\[' <<< "$heard" | wc -l)
printf '  native onLost heard %s times; native strays: %s; jvm strays: %s\n' "$lost" "$(fact linuxX64 linuxX64 strays)" "$(fact jvm jvm strays)"
[ "$lost" -ge $((FREEZES / 2)) ] || bad "the native member heard onLost only $lost times in $FREEZES freezes: the run did not do what it is for"
python3 - "$heard" "$strays" <<'PY'
import re, sys
events = [(k, p, int(t)) for k, p, t in re.findall(r"([-+!])(\[[^\]]*\])@(\d+)", sys.argv[1])]
for s in sys.argv[2].split():
    po, t = s.split("@")
    t = int(t)
    before = [e for e in events if e[2] <= t]
    last = before[-1] if before else None
    print("    stray %s at %d: %s" % (po, t, "after %s%s by %d ms" % (last[0], last[1], t - last[2]) if last else "before any event"))
PY
[ "$(fact linuxX64 linuxX64 strays)" = 0 ] || bad "the native member was handed records of a partition it had given up"
[ "$(fact jvm jvm strays)" = 0 ] || bad "the JVM member was handed records of a partition it had given up"

echo
[ "$fail" -eq 0 ] || { echo "B-68: RED"; exit 1; }
echo "B-68: no poll hands over a record of a partition given up inside it, over $FREEZES freezes and an ordinary rebalance"
