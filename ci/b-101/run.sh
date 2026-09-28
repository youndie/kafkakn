#!/usr/bin/env bash
# B-101: a MEASUREMENT. Rounds of "create a topic, describe it at once; grow it, describe it at once", on each arm:
# what the first description said, and how long until the broker's description agreed.
#
#   ROUNDS=200 ci/b-101/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
OBS=kafkakn-core/build/observations
ROUNDS=${ROUNDS:-200}
export GRADLE_OPTS=-Dorg.gradle.daemon=false

echo "=== environment ==="
date -Is
uptime
bash ci/harness/broker.sh up || exit 1

for task in ${TASKS:-jvmTest linuxX64Test}; do
    arm=jvm; [ "$task" = linuxX64Test ] && arm=linuxX64
    echo
    echo "=== $arm, $ROUNDS rounds ==="
    rm -rf "$OBS"
    KAFKAKN_B101_ROUNDS=$ROUNDS ./gradlew --console=plain ":kafkakn-core:$task" --rerun \
        --tests '*CreateThenDescribeTest*' > "build/b-101-$arm.out" 2>&1
    printf '  exit=%s\n' "$?"
    sed -n 's/^b101=//p' "$OBS/$arm-local.txt"
    # The rounds whose first description disagreed, and the slowest to agree.
    sed -n 's/^b101\.[0-9]*=//p' "$OBS/$arm-local.txt" | grep -v '^created=1 .*grown=2 ' | head -10 | sed 's/^/    /'
    printf '  slowest to show the topic: %s ms; the growth: %s ms\n' \
        "$(sed -n 's/^b101\.[0-9]*=created=[^ ]* visible.ms=\([-0-9]*\).*/\1/p' "$OBS/$arm-local.txt" | sort -n | tail -1)" \
        "$(sed -n 's/^b101\.[0-9]*=.*grown=[^ ]* visible.ms=\([-0-9]*\)$/\1/p' "$OBS/$arm-local.txt" | sort -n | tail -1)"
done
