#!/usr/bin/env bash
# B-04: the broker fixture, and proof that its oracles can say no.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
H="$ROOT/ci/harness/broker.sh"
TOPIC=b04

echo "=== environment ==="
date -Is
docker --version
grep -m1 "image:" "$ROOT/ci/broker/docker-compose.yml"

echo
echo "=== up, and a topic with three partitions ==="
bash "$H" up
bash "$H" topic "$TOPIC"

echo
echo "=== positive control 1: the broker check must fail against a dead port ==="
echo "(a helper that cannot say no makes every later green meaningless)"
bash "$H" selftest

echo
echo "=== positive control 2: a produce with no stdin attached writes NOTHING ==="
echo "(docker exec without -i: the console producer reads EOF, exits zero, and writes nothing."
echo " Only the end offsets reveal it - this is the failure the harness is built to make visible.)"
BEFORE=$(bash "$H" offsets "$TOPIC")
printf 'ghost\n' | docker exec kafkakn-broker /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server 127.0.0.1:9092 --topic "$TOPIC" 2>/dev/null
AFTER=$(bash "$H" offsets "$TOPIC")
echo "  exit was 0, and end offsets went $BEFORE -> $AFTER"
if [ "$BEFORE" -eq "$AFTER" ]; then
    echo "  CONFIRMED: a clean exit code and nothing written. The offsets are the only witness."
else
    echo "  UNEXPECTED: something was written without stdin; the harness note is wrong" >&2
    exit 1
fi

echo
echo "=== the oracles, working ==="
STAMP="b04-$(date +%s)"
BEFORE=$(bash "$H" offsets "$TOPIC")
printf '%s\n' "$STAMP" | bash "$H" produce "$TOPIC"
AFTER=$(bash "$H" offsets "$TOPIC")
echo "  end offsets: $BEFORE -> $AFTER (expected +1)"
FOUND=$(bash "$H" consume "$TOPIC" "^$STAMP\$")
echo "  console consumer found: $FOUND (expected 1)"

echo
echo "=== the consumer oracle must also be able to say no ==="
MISSING=$(bash "$H" consume "$TOPIC" "^never-produced-$$\$")
echo "  a pattern that was never produced: $MISSING (expected 0)"

echo
echo "=== verdict ==="
echo "offsets-grew=$((AFTER - BEFORE)) found=$FOUND absent=$MISSING"
if [ "$((AFTER - BEFORE))" -eq 1 ] && [ "$FOUND" -eq 1 ] && [ "$MISSING" -eq 0 ]; then
    echo "B-04: the fixture stands and both oracles can say yes and no"
else
    echo "B-04: NOT GREEN - read the lines above"
    exit 1
fi
