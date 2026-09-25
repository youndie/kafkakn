#!/usr/bin/env bash
# B-38: exactly-once read-process-write on both arms, with the processor stopped three times at random.
#
# The input trickles in from a third party; the processor writes one output record per input record,
# named `stamp:partition:offset`, and commits the input's progress inside the output's transaction.
# The oracle reads the OUTPUT twice with the broker's own consumer: under read_committed every input
# record must appear exactly once; under read_uncommitted the aborted attempts must appear too - or the
# stops never produced the failures the green is about.
#
#   ci/b-38/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
STAMP=$(date +%s)
COUNT=300
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=kafkakn-b38-$STAMP
fail=0
bad() { echo "    $*" >&2; fail=1; }

echo "=== environment ==="
date -Is
echo
echo "=== broker, and both arms built first ==="
bash "$H" up || exit 1
bash "$H" topic "$KAFKAKN_TOPIC" >/dev/null
./gradlew --console=plain :kafkakn-core:jvmTestClasses :kafkakn-core:linkDebugTestLinuxX64 > build/b-38-build.out 2>&1 \
    || { tail -20 build/b-38-build.out; exit 1; }
rm -rf "$OBS"

for arm in jvm linuxX64; do
    echo
    echo "=== $arm: $COUNT input records, three stops ==="
    input=kafkakn-eos-in-$arm-$STAMP
    output=kafkakn-eos-out-$arm-$STAMP
    PARTITIONS=2 bash "$H" topic "$input" >/dev/null
    PARTITIONS=2 bash "$H" topic "$output" >/dev/null
    export KAFKAKN_EOS_INPUT=$input KAFKAKN_EOS_OUTPUT=$output KAFKAKN_EOS_COUNT=$COUNT
    bash "$H" records trickle "$input" 2 "$COUNT" 25 > "build/b-38-$arm-trickle.out" &
    trickle=$!
    if [ "$arm" = jvm ]; then
        ./gradlew --console=plain :kafkakn-core:jvmTest --rerun --tests '*ExactlyOnceTest*' > "build/b-38-$arm.out" 2>&1
    else
        ( cd kafkakn-core && ./build/bin/linuxX64/debugTest/test.kexe \
            --ktest_filter='io.github.youndie.kafkakn.ExactlyOnceTest.*' ) > "build/b-38-$arm.out" 2>&1
    fi
    code=$?
    unset KAFKAKN_EOS_INPUT KAFKAKN_EOS_OUTPUT KAFKAKN_EOS_COUNT
    wait "$trickle"
    printf '  processor exit=%s, %s\n' "$code" "$(cat "build/b-38-$arm-trickle.out")"
    [ "$code" -eq 0 ] || { tail -30 "build/b-38-$arm.out"; echo "  THE PROCESSOR FAILED on $arm"; exit 1; }
    f="$OBS/$arm-local.txt"
    stamp=$(sed -n 's/^eos\.stamp=//p' "$f" | tail -1)
    printf '  seed %s; stops: %s\n' "$(sed -n 's/^eos\.seed=//p' "$f" | tail -1)" "$(sed -n 's/^eos\.stops=//p' "$f" | tail -1)"

    CONSUME_MS=20000 ISOLATION=read_committed bash "$H" values "$output" | grep -a "^$stamp:" | sort > "build/b-38-$arm-committed.txt"
    CONSUME_MS=20000 ISOLATION=read_uncommitted bash "$H" values "$output" | grep -a "^$stamp:" | sort > "build/b-38-$arm-uncommitted.txt"
    rc=$(wc -l < "build/b-38-$arm-committed.txt")
    rc_distinct=$(sort -u "build/b-38-$arm-committed.txt" | wc -l)
    ru=$(wc -l < "build/b-38-$arm-uncommitted.txt")
    printf '  read_committed:   %s output records, %s distinct inputs (expected %s, each once)\n' "$rc" "$rc_distinct" "$COUNT"
    printf '  read_uncommitted: %s output records - the aborted attempts are the %s above that\n' "$ru" "$((ru - rc))"
    [ "$rc" -eq "$COUNT" ] && [ "$rc_distinct" -eq "$COUNT" ] || bad "$arm: not exactly once under read_committed"
    [ "$ru" -gt "$rc" ] || bad "$arm: no aborted attempt is visible under read_uncommitted - the stops produced nothing to abort"
    dupes=$(uniq -d "build/b-38-$arm-committed.txt" | head -3)
    [ -z "$dupes" ] || bad "$arm: committed twice: $dupes"
done

echo
[ "$fail" -eq 0 ] || { echo "B-38: RED"; exit 1; }
echo "B-38: every input record reached the committed output exactly once across three stops, on both arms"
