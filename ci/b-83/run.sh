#!/usr/bin/env bash
# B-83: a MEASUREMENT. Records queued, the broker gone (paused, then stopped), then close(), on each arm. How long
# close took, what each record's Delivery answered afterwards, and what the topic holds once the broker is back,
# read with the broker's own reader.
#
#   ci/b-83/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
export GRADLE_OPTS=-Dorg.gradle.daemon=false
fact() { sed -n "s/^$2=//p" "$1" | tail -1; }
values() { bash "$H" records dump "$1" | awk -F/ '{ print $4 }' | python3 -c '
import sys
for line in sys.stdin:
    v = line.strip()
    if v.startswith("x"):
        print(bytes.fromhex(v[1:]).decode(errors="replace"))'; }

echo "=== environment ==="
date -Is
bash "$H" up || exit 1

fail=0
for variant in paused stopped; do
    for task in jvmTest linuxX64Test; do
        arm=jvm; [ "$task" = linuxX64Test ] && arm=linuxX64
        echo
        echo "=== $arm, the broker $variant ==="
        rm -rf "$OBS"
        KAFKAKN_BROKER_STOP=1 KAFKAKN_CLOSE_VARIANT=$variant ./gradlew --console=plain ":kafkakn-core:$task" --rerun \
            --tests '*CloseWithBrokerGoneTest*' > "build/b-83-$variant-$arm.out" 2>&1
        code=$?
        docker unpause kafkakn-broker > /dev/null 2>&1
        docker start kafkakn-broker > /dev/null 2>&1
        bash "$H" up > /dev/null || { echo "  THE BROKER DID NOT COME BACK" >&2; exit 1; }
        printf '  exit=%s\n' "$code"
        [ "$code" -eq 0 ] || { grep -E "FAILED|Exception" "build/b-83-$variant-$arm.out" | head -5; fail=1; }
        f="$OBS/$arm-local.txt"
        printf '  close took %s ms %s\n' "$(fact "$f" "close.$variant.ms")" "$(fact "$f" "close.$variant.threw")"
        for i in 0 1 2 3 4; do printf '    r-%s: %s\n' "$i" "$(fact "$f" "close.$variant.r-$i" | cut -c1-150)"; done
        t=$(fact "$f" "close.$variant.topic")
        [ -n "$t" ] && printf '  the topic holds, once the broker is back: %s\n' "$(values "$t" | paste -sd' ')"
    done
done

echo
[ "$fail" -eq 0 ] || { echo "B-83: a pass did not complete - the measurement is incomplete"; exit 1; }
echo "B-83: measured; the answers are above"
