#!/usr/bin/env bash
# B-79: a MEASUREMENT. Rounds of "create a topic, then at once a burst of 2 x 1 000 concurrent records", on each arm,
# without and with one record per partition first. Each round's counts, and the broker's OutOfOrderSequence
# complaints about that round's topic, read from its own log.
#
#   ROUNDS=10 ci/b-79/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
ROUNDS=${ROUNDS:-10}
export GRADLE_OPTS=-Dorg.gradle.daemon=false

echo "=== environment ==="
date -Is
uptime
bash "$H" up || exit 1

for warm in 0 1; do
    for task in jvmTest linuxX64Test; do
        arm=jvm; [ "$task" = linuxX64Test ] && arm=linuxX64
        echo
        echo "=== $arm, warm=$warm, $ROUNDS rounds ==="
        rm -rf "$OBS"
        since=$(date -u +%Y-%m-%dT%H:%M:%S)
        KAFKAKN_FRESH_ROUNDS=$ROUNDS KAFKAKN_FRESH_WARM=$warm ./gradlew --console=plain ":kafkakn-core:$task" --rerun \
            --tests '*FreshTopicBurstTest*' > "build/b-79-$arm-$warm.out" 2>&1
        printf '  exit=%s\n' "$?"
        sed -n 's/^fresh\.[0-9]*=//p' "$OBS/$arm-local.txt" | while read -r line; do
            topic=$(printf '%s\n' "$line" | sed -n 's/.*topic=\([^ ]*\).*/\1/p')
            oos=$(docker logs --since "$since" kafkakn-broker 2>&1 | grep -c "OutOfOrderSequenceException.*$topic")
            printf '    %s  oos=%s\n' "$(printf '%s' "$line" | sed 's/ topic=[^ ]*//' | cut -c1-150)" "$oos"
        done
    done
done
