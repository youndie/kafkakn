#!/usr/bin/env bash
# B-09: the broker holds exactly the records the caller handed in - and the guard that says so is
# shown failing against a producer that drops them.
#
# THE SECOND PASS IS THE POINT. A reconciliation that has only ever been green is a reconciliation
# whose failure mode nobody has seen, and this repository has produced four checks that passed while
# testing nothing. So the same test runs again against ci-only NaiveProducer, which counts an
# enqueue refusal and moves on exactly as the measured naive binding did, and THIS SCRIPT FAILS IF
# THAT PASS IS GREEN.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
RESULTS=kafkakn-core/build/test-results
ARMS="jvm linuxX64"
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=kafkakn KAFKAKN_STRICT_TOPIC=kafkakn-strict

# Fresh topics, because the oracle is a delta on end offsets and a delta is only attributable while
# nothing else writes to the topic. One per arm: both arms run against one broker in one pass, and a
# shared topic would add their two counts into a number no single assertion can check.
PREFIX=kafkakn-acct-$(date +%s)
NAIVE_PREFIX=$PREFIX-naive

echo "=== environment ==="
date -Is

echo
echo "=== broker ==="
bash "$H" up

make_topics() {
    for arm in $ARMS; do
        bash "$H" topic "$1-$arm" >/dev/null
        n=$(bash "$H" offsets "$1-$arm")
        [ "$n" = "0" ] || { echo "  $1-$arm is not empty ($n) - a delta on it would not be ours" >&2; exit 1; }
    done
    echo "  $1-{$(echo $ARMS | tr ' ' ',')}: created, end offsets 0"
}
make_topics "$PREFIX" || exit 1
make_topics "$NAIVE_PREFIX" || exit 1

echo
echo "=== pass 1: the real producer, both arms ==="
rm -rf "$OBS" "$RESULTS"
KAFKAKN_ACCOUNTING_TOPIC=$PREFIX \
    ./gradlew --no-daemon --console=plain jvmTest linuxX64Test --rerun-tasks 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "SUITE FAILED - accounting cannot be read off a red run"; exit 1; }
for arm in jvmTest linuxX64Test; do
    t=$(grep -ho 'tests="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    f=$(grep -ho 'failures="[0-9]*"' "$RESULTS/$arm"/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    printf '  %-14s tests=%-4s failures=%s\n' "$arm" "$t" "$f"
    [ "$f" -eq 0 ] || exit 1
done

echo
echo "=== the accounting: handed in, answered, and what the broker holds ==="
for arm in $ARMS; do
    f="$OBS/$arm-local.txt"
    [ -s "$f" ] || { echo "  $arm recorded nothing - nothing below is checked" >&2; exit 1; }
    topic=$(sed -n 's/^accounting\.topic=//p' "$f")
    handed=$(sed -n 's/^accounting\.handed_in=//p' "$f")
    dropped=$(sed -n 's/^accounting\.dropped=//p' "$f")
    [ -n "$topic" ] && [ -n "$handed" ] || { echo "  $arm recorded no accounting facts" >&2; exit 1; }
    [ "$topic" = "$PREFIX-$arm" ] || { echo "  $arm accounted on $topic, not $PREFIX-$arm" >&2; exit 1; }
    landed=$(bash "$H" offsets "$topic")
    printf '  %-9s handed in %s   answered-without-sending %s   end offsets 0 -> %s\n' \
        "$arm" "$handed" "$dropped" "$landed"
    # Exactly, in both directions: fewer is the measured defect, more would be a duplicate the
    # caller never asked for and a count of successes would hide just as well.
    [ "$landed" -eq "$handed" ] || {
        echo "  $arm: the broker holds $landed of $handed handed in" >&2; exit 1; }
    [ "$dropped" -eq 0 ] || { echo "  $arm: $dropped records were answered and never sent" >&2; exit 1; }
done

echo
echo "=== pass 2: the same test against a producer that drops - THIS MUST BE RED ==="
rm -rf "$OBS" "$RESULTS"
KAFKAKN_ACCOUNTING_TOPIC=$NAIVE_PREFIX KAFKAKN_NAIVE=1 \
    ./gradlew --no-daemon --console=plain jvmTest linuxX64Test --rerun-tasks --continue \
    --tests 'io.github.youndie.kafkakn.AccountingTest' 2>&1 | tail -3
control=${PIPESTATUS[0]}
if [ "$control" -eq 0 ]; then
    echo "  THE CONTROL PASSED. The guard does not catch the defect it exists for." >&2
    exit 1
fi
echo "  the control run is red, as it must be"

echo
echo "=== and red for the right reason, on both arms ==="
for arm in jvmTest linuxX64Test; do
    x=$(ls "$RESULTS/$arm"/TEST-*AccountingTest.xml 2>/dev/null | head -1)
    [ -n "$x" ] || { echo "  $arm: AccountingTest did not run in the control pass" >&2; exit 1; }
    f=$(grep -o 'failures="[0-9]*"' "$x" | head -1 | grep -oE '[0-9]+')
    [ "$f" -gt 0 ] || { echo "  $arm: AccountingTest passed against a dropping producer" >&2; exit 1; }
    printf '  %-14s AccountingTest failures=%s\n' "$arm" "$f"
done

echo
echo "=== the control actually dropped something (an empty control is red for nothing) ==="
for arm in $ARMS; do
    f="$OBS/$arm-local.txt"
    dropped=$(sed -n 's/^accounting\.dropped=//p' "$f")
    handed=$(sed -n 's/^accounting\.handed_in=//p' "$f")
    landed=$(bash "$H" offsets "$NAIVE_PREFIX-$arm")
    printf '  %-9s dropped %s of %s   end offsets 0 -> %s\n' "$arm" "$dropped" "$handed" "$landed"
    [ "$dropped" -gt 0 ] || { echo "  $arm: the naive producer dropped nothing" >&2; exit 1; }
    [ "$landed" -lt "$handed" ] || {
        echo "  $arm: the broker holds $landed - the drop never reached the oracle" >&2; exit 1; }
    [ $(( handed - landed )) -eq "$dropped" ] || {
        echo "  $arm: NOTE shortfall $(( handed - landed )) is not the $dropped the producer admits to"; }
done

echo
echo "=== verdict ==="
echo "B-09: both arms land exactly what the caller handed in, and the guard is red when they do not"
