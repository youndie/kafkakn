#!/usr/bin/env bash
# B-77: a MEASUREMENT. The broker is stopped (docker stop, connections refused) with a topic already known, and each
# arm enqueues at 0, 5 and 20 s after the stop. What each said, how long it took, and what became of every record it
# queued, read back with the broker's own tools once the broker is started again.
#
# Three passes: the JVM as it ships, the JVM with metadata.recovery.strategy=none (the hypothesis that Kafka 4's
# rebootstrap drops a known topic's metadata when every broker is unreachable), and native.
#
#   ci/b-77/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
export GRADLE_OPTS=-Dorg.gradle.daemon=false
kc() { docker exec kafkakn-broker "$@"; }
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
pass() {
    local name=$1 task=$2 arm=$3 extra=$4
    echo
    echo "=== $name ==="
    rm -rf "$OBS"
    KAFKAKN_BROKER_STOP=1 KAFKAKN_STOPPED_EXTRA="$extra" ./gradlew --console=plain ":kafkakn-core:$task" --rerun \
        --tests '*StoppedBrokerTest*' > "build/b-77-$name.out" 2>&1
    code=$?
    # Started again whatever happened, and waited for: the next pass, and every other run, needs the broker.
    docker start kafkakn-broker > /dev/null 2>&1
    bash "$H" up > /dev/null || { echo "  THE BROKER DID NOT COME BACK" >&2; exit 1; }
    printf '  exit=%s\n' "$code"
    [ "$code" -eq 0 ] || { grep -E "FAILED|Exception" "build/b-77-$name.out" | head -5; fail=1; }
    local f="$OBS/$arm-local.txt"
    for i in 0 1 2; do
        printf '  at %6s ms: %-6s after %5s ms  %s\n' "$(fact "$f" "stopped.$i.at.ms")" \
            "$(fact "$f" "stopped.$i.said" | cut -c1-6)" "$(fact "$f" "stopped.$i.after.ms")" \
            "$(fact "$f" "stopped.$i.said" | cut -c1-200)"
        printf '             fate of r-%s: %s\n' "$i" "$(fact "$f" "stopped.r-$i.fate" | cut -c1-160)"
    done
    t=$(fact "$f" stopped.topic)
    [ -n "$t" ] && printf '  the topic holds: %s\n' "$(values "$t" | paste -sd' ')"
    cp "$f" "build/b-77-$name.facts" 2>/dev/null
}

pass jvm-default jvmTest jvm ""
pass jvm-no-rebootstrap jvmTest jvm "metadata.recovery.strategy=none"
pass native linuxX64Test linuxX64 ""

echo
[ "$fail" -eq 0 ] || { echo "B-77: a pass did not complete - the measurement is incomplete"; exit 1; }
echo "B-77: measured; the answers are above, and the decision is the owner's"
