#!/usr/bin/env bash
# B-25: a retried record whose acknowledgement was lost - written once, or twice?
#
# enable.idempotence defaulted to true in kafka-clients and to false in librdkafka, so the two arms
# could disagree on exactly this. The native default now follows the reference; this is the run that
# shows the disagreement was real, and that it is gone.
#
# THE FAULT: the broker is frozen with `docker pause` while the driver is producing, for longer than
# the client's request timeout. A batch the broker appended just before the freeze gets no
# acknowledgement in time, the client retries it after the thaw, and without idempotence the retry is
# appended a second time. Values are unique by construction, so a value on the topic twice is one
# record written twice.
#
# THE CONTROL RUNS FIRST and the run stops if it is clean: the native arm with enable.idempotence=false
# must show a duplicate. A fixture that cannot produce one has not been shown able to see one, and
# every zero after it would mean nothing.
#
#   ci/b-25/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
CONTAINER=kafkakn-broker
. ci/lib/coordinate.sh
kafkakn_coordinate || exit 2
REPO=$ROOT/build/local-repo
DRIVER=ci/b-25/driver
DURATION=${DURATION:-45}
PAUSES=${PAUSES:-5}
PAUSE_SECONDS=${PAUSE_SECONDS:-6}
RUN=$(date +%s)

JVM_BIN=$DRIVER/build/install/kafkakn-idempotence-driver-jvm/bin/kafkakn-idempotence-driver
NATIVE_BIN=$DRIVER/build/bin/linuxX64/releaseExecutable/kafkakn-idempotence-driver.kexe

echo "=== environment ==="
date -Is
echo "  $GROUP:kafkakn-core:$VERSION, candidate from this checkout"
echo "  $DURATION s of producing per run, $PAUSES pauses of ${PAUSE_SECONDS} s against a 2 s request timeout"

for tool in docker python3; do
    command -v "$tool" >/dev/null || { echo "  $tool is not on PATH" >&2; exit 1; }
done

echo
echo "=== broker ==="
bash "$H" up || exit 1

echo
echo "=== the candidate, and a driver built against it the way a stranger would build one ==="
rm -rf "$REPO"
./gradlew --no-daemon --console=plain :kafkakn-core:publishAllPublicationsToLocalRepository 2>&1 | tail -2
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "  PUBLISH FAILED"; exit 1; }
./gradlew --no-daemon --console=plain -p "$DRIVER" -Pkafkakn.repo="file://$REPO" -Pkafkakn.version="$VERSION" \
    --refresh-dependencies installJvmDist linkReleaseExecutableLinuxX64 2>&1 | tail -2
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "  THE DRIVER COULD NOT BUILD"; exit 1; }
[ -x "$JVM_BIN" ] && [ -x "$NATIVE_BIN" ] || { echo "  a driver binary is missing" >&2; exit 1; }

wait_for_broker() {
    for _ in $(seq 1 30); do
        docker exec "$CONTAINER" /opt/kafka/bin/kafka-broker-api-versions.sh \
            --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1 && return 0
        sleep 2
    done
    return 1
}

# One run: a fresh topic, the driver in the background, the broker frozen under it several times.
run() {
    local label=$1 arm=$2 idempotence=$3
    local topic="kafkakn-idem-$RUN-$label"
    local out="build/b-25-$label.out"
    local bin=$JVM_BIN
    [ "$arm" = native ] && bin=$NATIVE_BIN

    bash "$H" topic "$topic" >/dev/null
    "$bin" 127.0.0.1:9092 "$topic" "$label" "$DURATION" "$idempotence" > "$out" 2>&1 &
    local pid=$!

    # The freezes begin once records are actually landing, not when the process started: a pause
    # before the first request tests connection setup, which is a different question.
    for _ in $(seq 1 60); do
        [ "$(bash "$H" offsets "$topic")" -gt 0 ] && break
        sleep 0.5
    done

    for _ in $(seq 1 "$PAUSES"); do
        sleep "$(awk -v s="$RANDOM" 'BEGIN { srand(s); printf "%.2f", 2 + rand() * 3 }')"
        docker pause "$CONTAINER" >/dev/null
        sleep "$PAUSE_SECONDS"
        docker unpause "$CONTAINER" >/dev/null
    done

    wait "$pid"
    local code=$?
    wait_for_broker || { echo "  the broker did not come back after the pauses" >&2; exit 1; }

    bash "$H" values "$topic" > "build/b-25-$label.values"
    local total distinct dups
    total=$(wc -l < "build/b-25-$label.values")
    distinct=$(sort -u "build/b-25-$label.values" | wc -l)
    dups=$(sort "build/b-25-$label.values" | uniq -d | wc -l)
    printf '  %-16s records=%-7s distinct=%-7s written-twice=%-5s exit=%s  %s\n' \
        "$label" "$total" "$distinct" "$dups" "$code" "$(grep -o 'handed-in=.*' "$out")"
    [ "$total" -gt 0 ] || { echo "  $label: nothing reached the topic - the run measured nothing" >&2; exit 1; }
    echo "$dups" > "build/b-25-$label.dups"
}

echo
echo "=== the control: native, idempotence off - THIS MUST WRITE SOMETHING TWICE ==="
run native-off native false
if [ "$(cat build/b-25-native-off.dups)" -eq 0 ]; then
    echo "  the fixture produced no duplicate with idempotence off, so it cannot see one." >&2
    echo "  Nothing after this line would mean anything; raise PAUSES or PAUSE_SECONDS." >&2
    exit 1
fi
echo "  it did: the fixture can see a record written twice"

echo
echo "=== the same fault, the Java client with idempotence off, for comparison ==="
run jvm-off jvm false

echo
echo "=== the defaults, which is what this item changed ==="
run native-default native default
run jvm-default jvm default

echo
echo "=== verdict ==="
fail=0
for label in native-default jvm-default; do
    d=$(cat "build/b-25-$label.dups")
    [ "$d" -eq 0 ] || { echo "  $label wrote $d record(s) twice" >&2; fail=1; }
done
[ "$fail" -eq 0 ] || exit 1
echo "B-25: with idempotence off the native arm writes a retried record twice; by default neither arm does"
