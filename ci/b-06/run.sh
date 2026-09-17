#!/usr/bin/env bash
# B-06: the JVM arm, checked against the broker rather than against itself.
#
# The suite asserts what the CLIENT returns and records the stamps it used; this script then asks
# the BROKER what actually landed. A producer verified by its own library can be wrong in both
# directions at once.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
OBS=kafkakn-core/build/observations/jvm-local.txt
TOPIC=kafkakn
STRICT=kafkakn-strict

echo "=== environment ==="
date -Is

echo
echo "=== broker and topics ==="
bash "$H" up
bash "$H" topic "$TOPIC" | head -1
bash "$H" strict-topic "$STRICT" | head -1

echo
echo "=== the suite, on the JVM arm ==="
BEFORE=$(bash "$H" offsets "$TOPIC")
rm -rf kafkakn-core/build/observations
KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC="$TOPIC" KAFKAKN_STRICT_TOPIC="$STRICT" \
    ./gradlew --no-daemon --console=plain jvmTest --rerun-tasks 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "SUITE FAILED"; exit 1; }
AFTER=$(bash "$H" offsets "$TOPIC")
t=$(grep -ho 'tests="[0-9]*"' kafkakn-core/build/test-results/jvmTest/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
f=$(grep -ho 'failures="[0-9]*"' kafkakn-core/build/test-results/jvmTest/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
echo "  jvmTest tests=$t failures=$f"

echo
echo "=== what the client SAID it did ==="
[ -s "$OBS" ] || { echo "  no observations - the suite recorded nothing, so nothing below is checked" >&2; exit 1; }
sed 's/^/    /' "$OBS"

echo
echo "=== what the BROKER says happened ==="
NOKEY_STAMP=$(sed -n 's/^produce\.no-key\.stamp=//p' "$OBS")
NOKEY_COUNT=$(sed -n 's/^produce\.no-key\.count=//p' "$OBS")
SAMEKEY_STAMP=$(sed -n 's/^produce\.same-key\.stamp=//p' "$OBS")
SAMEKEY_COUNT=$(sed -n 's/^produce\.same-key\.count=//p' "$OBS")
EXPECTED=$((NOKEY_COUNT + SAMEKEY_COUNT))
echo "  end offsets: $BEFORE -> $AFTER (grew by $((AFTER - BEFORE)), the suite claims $EXPECTED)"
FOUND_NOKEY=$(CONSUME_MS=25000 bash "$H" consume "$TOPIC" "^$NOKEY_STAMP:")
FOUND_SAMEKEY=$(CONSUME_MS=25000 bash "$H" consume "$TOPIC" "^$SAMEKEY_STAMP:")
echo "  independent consumer: $FOUND_NOKEY of $NOKEY_COUNT with no key, $FOUND_SAMEKEY of $SAMEKEY_COUNT with a key"

echo
echo "=== the oracle can still say no ==="
NEVER=$(CONSUME_MS=10000 bash "$H" consume "$TOPIC" "^never-produced-$$:")
echo "  a stamp that was never produced: $NEVER (expected 0)"

echo
echo "=== verdict ==="
echo "tests=$t failures=$f offsets=+$((AFTER - BEFORE))/$EXPECTED nokey=$FOUND_NOKEY/$NOKEY_COUNT samekey=$FOUND_SAMEKEY/$SAMEKEY_COUNT absent=$NEVER"
if [ "$f" -eq 0 ] && [ "$((AFTER - BEFORE))" -eq "$EXPECTED" ] &&
   [ "$FOUND_NOKEY" -eq "$NOKEY_COUNT" ] && [ "$FOUND_SAMEKEY" -eq "$SAMEKEY_COUNT" ] &&
   [ "$NEVER" -eq 0 ]; then
    echo "B-06: the JVM arm does what it says, and the broker agrees"
else
    echo "B-06: NOT GREEN - read the lines above"
    exit 1
fi
