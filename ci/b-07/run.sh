#!/usr/bin/env bash
# B-07: the native arm, and the first real comparison of two independent implementations.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations
TOPIC=kafkakn
STRICT=kafkakn-strict
export KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC="$TOPIC" KAFKAKN_STRICT_TOPIC="$STRICT"

echo "=== environment ==="
date -Is

echo
echo "=== broker and topics ==="
bash "$H" up
bash "$H" topic "$TOPIC" | head -1
bash "$H" strict-topic "$STRICT" | head -1

echo
echo "=== both arms, the same suite ==="
rm -rf "$OBS"
BEFORE=$(bash "$H" offsets "$TOPIC")
./gradlew --no-daemon --console=plain jvmTest linuxX64Test --rerun-tasks 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "SUITE FAILED"; exit 1; }
AFTER=$(bash "$H" offsets "$TOPIC")
for arm in jvmTest linuxX64Test; do
    t=$(grep -ho 'tests="[0-9]*"' kafkakn-core/build/test-results/$arm/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    f=$(grep -ho 'failures="[0-9]*"' kafkakn-core/build/test-results/$arm/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
    printf '  %-14s tests=%-4s failures=%s\n' "$arm" "$t" "$f"
    [ "$f" -eq 0 ] || exit 1
done
echo "  end offsets moved $BEFORE -> $AFTER while both arms produced"

echo
echo "=== the point of the whole design: do the two implementations agree? ==="
bash ci/harness/compare-arms.sh "$OBS/jvm.txt" "$OBS/linuxX64.txt" || exit 1
echo "  the partitioner answers compared above are the ones that disagreed before"
echo "  librdkafka's default partitioner was set to murmur2_random; see the item"

echo
echo "=== the native arm used rd_kafka_produce, never the variadic producev ==="
PRODUCEV=$(python3 ci/lib/token_in_code.py \
    kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt \
    rd_kafka_producev)
echo "  rd_kafka_producev outside comments: $PRODUCEV"
[ "$PRODUCEV" -eq 0 ] || { echo "  the variadic call has no usable shape through cinterop" >&2; exit 1; }

echo
echo "=== flush waits on the queue length, not on a return code ==="
OUTQ=$(python3 ci/lib/token_in_code.py \
    kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt \
    rd_kafka_outq_len)
echo "  rd_kafka_outq_len used $OUTQ time(s) outside comments"
[ "$OUTQ" -ge 1 ] || { echo "  completion is not measured by the queue length" >&2; exit 1; }

echo
echo "=== positive control: a failure on librdkafka's thread must reach the caller ==="
echo "(the first version of this control expected the process to die. Measuring it found something"
echo " worse: an exception in the callback does not terminate the process and surfaces nowhere - it"
echo " leaves the caller suspended for ever. The producer now unparks before anything else and"
echo " wraps the rest in a catch; DeliveryCallbackCrashControlTest asserts the consequence.)"
grep -l DeliveryCallbackCrashControlTest kafkakn-core/build/test-results/linuxX64Test/TEST-*.xml >/dev/null 2>&1 \
    || { echo "  THE CONTROL DID NOT RUN" >&2; exit 1; }
CONTROL_FAILURES=$(grep -ho 'failures="[0-9]*"' \
    kafkakn-core/build/test-results/linuxX64Test/TEST-*DeliveryCallbackCrashControlTest.xml \
    | grep -oE '[0-9]+')
echo "  the control ran, failures=$CONTROL_FAILURES"
[ "$CONTROL_FAILURES" -eq 0 ] || exit 1

echo
echo "=== verdict ==="
echo "B-07: the native arm produces, the two implementations agree, and a crash in the callback is visible"
