#!/usr/bin/env bash
# B-77, B-80: the broker is stopped (docker stop, connections refused) with a topic already known, and each arm
# enqueues at 0, 5 and 20 s after the stop, then once more when the broker is back. The answer at 0 s is printed
# and not asserted: neither client has noticed the stop yet, and the JVM answered it both ways. Two strategies, both arms each,
# compared: as they ship (metadata.recovery.strategy=rebootstrap), where both must refuse; and with
# metadata.recovery.strategy=none, where both must queue. The topic is read back with the broker's own reader.
#
#   ci/b-77/run.sh
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
bad() { echo "    $*" >&2; fail=1; }
for strategy in rebootstrap none; do
    extra=; [ "$strategy" = none ] && extra="metadata.recovery.strategy=none"
    echo
    echo "=== metadata.recovery.strategy=$strategy ==="
    rm -rf "$OBS"
    for task in jvmTest linuxX64Test; do
        KAFKAKN_BROKER_STOP=1 KAFKAKN_STOPPED_EXTRA="$extra" ./gradlew --console=plain ":kafkakn-core:$task" --rerun \
            --tests '*StoppedBrokerTest*' > "build/b-77-$strategy-$task.out" 2>&1
        code=$?
        # Started again whatever happened: the next pass, and every other run, needs the broker.
        docker start kafkakn-broker > /dev/null 2>&1
        bash "$H" up > /dev/null || { echo "  THE BROKER DID NOT COME BACK" >&2; exit 1; }
        printf '  %-14s exit=%s\n' "$task" "$code"
        [ "$code" -eq 0 ] || { grep -E "FAILED|AssertionError|expected" "build/b-77-$strategy-$task.out" | head -6; bad "$task failed"; }
    done
    MIN_OBSERVATIONS=3 bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || fail=1
    for arm in jvm linuxX64; do
        f="$OBS/$arm-local.txt"
        echo "  $arm:"
        for i in 0 1 2; do
            printf '    r-%s after %5s ms: %s\n' "$i" "$(fact "$f" "stopped.$i.after.ms")" "$(fact "$f" "stopped.$i.said" | cut -c1-150)"
        done
        printf '    back: %s\n' "$(fact "$f" stopped.back.said | cut -c1-80)"
        t=$(fact "$f" stopped.topic)
        [ -n "$t" ] && printf '    the topic holds: %s\n' "$(values "$t" | paste -sd' ')"
    done
done

echo
[ "$fail" -eq 0 ] || { echo "B-80: RED"; exit 1; }
echo "B-80: with every broker down both arms refuse as they ship and queue with metadata.recovery.strategy=none"
