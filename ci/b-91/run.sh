#!/usr/bin/env bash
# B-91: close(timeout) with records queued and the broker gone (paused, then stopped), on both arms. close must return
# within the timeout and a margin, and every record not acknowledged fails with ClosedBeforeAcknowledgedException. What
# the topic holds once the broker is back is read with the broker's own reader: a record in flight may be there.
# Also CloseWithTimeoutTest, with the broker answering.
#
#   ci/b-91/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
export GRADLE_OPTS=-Dorg.gradle.daemon=false
fail=0
fact() { sed -n "s/^$2=//p" "$OBS/$1-local.txt" | tail -1; }
values() { bash "$H" records dump "$1" | awk -F/ '{ print $4 }' | python3 -c '
import sys
for line in sys.stdin:
    v = line.strip()
    if v.startswith("x"):
        print(bytes.fromhex(v[1:]).decode(errors="replace"))'; }

echo "=== environment ==="
date -Is
bash "$H" up || exit 1

for variant in paused stopped; do
    echo
    echo "=== the broker $variant, close(3 s) ==="
    rm -rf "$OBS"
    for task in jvmTest linuxX64Test; do
        KAFKAKN_BROKER_STOP=1 KAFKAKN_CLOSE_VARIANT=$variant ./gradlew --console=plain ":kafkakn-core:$task" --rerun \
            --tests 'io.github.youndie.kafkakn.CloseWithBrokerGoneTest.close_with_a_timeout_gives_up_on_what_is_not_acknowledged' \
            > "build/b-91-$variant-$task.out" 2>&1
        code=$?
        docker unpause kafkakn-broker > /dev/null 2>&1
        docker start kafkakn-broker > /dev/null 2>&1
        bash "$H" up > /dev/null || { echo "  THE BROKER DID NOT COME BACK" >&2; exit 1; }
        printf '  %-14s exit=%s\n' "$task" "$code"
        [ "$code" -eq 0 ] || { grep -E "FAILED|expected|took" "build/b-91-$variant-$task.out" | head -5; fail=1; }
    done
    MIN_OBSERVATIONS=2 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1
    for arm in jvm linuxX64; do
        f="$OBS/$arm-local.txt"
        t=$(fact "$arm" "close.timeout.$variant.topic")
        printf '  %-9s close took %s ms; answers: %s\n' "$arm" "$(fact "$arm" "close.timeout.$variant.ms")" \
            "$(fact "$arm" "close.timeout.$variant.answers")"
        [ -n "$t" ] && printf '            the topic holds, once the broker is back: %s\n' "$(values "$t" | paste -sd' ')"
    done
done

echo
echo "=== with the broker answering ==="
rm -rf "$OBS"
for task in jvmTest linuxX64Test; do
    ./gradlew --console=plain ":kafkakn-core:$task" --rerun --tests '*CloseWithTimeoutTest*' > "build/b-91-answering-$task.out" 2>&1
    code=$?
    printf '  %-14s exit=%s\n' "$task" "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|expected|took" "build/b-91-answering-$task.out" | head -5; fail=1; }
done
MIN_OBSERVATIONS=2 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1

echo
[ "$fail" -eq 0 ] || { echo "B-91: RED"; exit 1; }
echo "B-91: close(timeout) keeps its bound and names what it gave up on, alike on both arms"
