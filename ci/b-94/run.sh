#!/usr/bin/env bash
# B-94: a @Serializable type as JSON Schema, held against the registry's own serializers, both ways, on both arms.
#   1. each arm encodes the samples; Confluent's KafkaJsonSchemaDeserializer reads the bytes, validating them against
#      the schema their id names, and what it reads is compared with the JSON each value is;
#   2. Confluent's KafkaJsonSchemaSerializer writes the same values under kafkakn's generated schema; each arm decodes
#      those bytes into the same samples.
#
#   ci/b-94/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
export GRADLE_OPTS=-Dorg.gradle.daemon=false
REGISTRY=http://127.0.0.1:18081
export KAFKAKN_RUN=b94-$(date +%s)
export KAFKAKN_B94_DIR=$ROOT/build/b-94
fail=0
bad() { echo "    $*" >&2; fail=1; }
rm -rf "$KAFKAKN_B94_DIR"; mkdir -p "$KAFKAKN_B94_DIR"

echo "=== environment ==="
date -Is
bash ci/harness/broker.sh up || exit 1
./gradlew --console=plain -p ci/b-94/oracle installDist > build/b-94-oracle-build.out 2>&1 || { tail -15 build/b-94-oracle-build.out; exit 1; }
ORACLE=ci/b-94/oracle/build/install/kafkakn-registry-oracle/bin/kafkakn-registry-oracle

suite() { # <phase>
    for task in jvmTest linuxX64Test; do
        KAFKAKN_B94_PHASE=$1 ./gradlew --console=plain ":kafkakn-schema-registry:$task" --rerun \
            --tests '*JsonSchemaOracleTest*' > "build/b-94-$1-$task.out" 2>&1
        code=$?
        printf '  %-14s %-7s exit=%s\n' "$task" "$1" "$code"
        [ "$code" -eq 0 ] || { grep -E "FAILED|expected|Exception" "build/b-94-$1-$task.out" | head -6; bad "$task $1 failed"; }
    done
}

echo
echo "=== 1. kafkakn writes, the registry's deserializer reads ==="
suite encode
for arm in jvm linuxX64; do
    lines=$KAFKAKN_B94_DIR/kafkakn-$arm.lines
    [ -s "$lines" ] || { bad "$arm wrote nothing"; continue; }
    "$ORACLE" read "$REGISTRY" "kafkakn-b94-$arm-$KAFKAKN_RUN" "$lines" > "$KAFKAKN_B94_DIR/read-$arm.json" 2> "$KAFKAKN_B94_DIR/read-$arm.err" \
        || { head -5 "$KAFKAKN_B94_DIR/read-$arm.err"; bad "$arm: the deserializer refused kafkakn's bytes"; continue; }
    python3 - "$lines" "$KAFKAKN_B94_DIR/read-$arm.json" "$arm" <<'PY' || fail=1
import json, sys
wrote = [json.loads(l.split("\t")[1]) for l in open(sys.argv[1]) if l.strip()]
read = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
same = wrote == read
print(f"  {sys.argv[3]:<9} {len(read)} of {len(wrote)} read back by KafkaJsonSchemaDeserializer, validated, {'equal' if same else 'DIFFERENT'}")
for w, r in zip(wrote, read):
    if w != r: print(f"    wrote {w}\n    read  {r}")
sys.exit(0 if same and len(read) == len(wrote) else 1)
PY
done

echo
echo "=== 2. the registry's serializer writes, kafkakn reads ==="
"$ORACLE" write "$REGISTRY" "kafkakn-b94-confluent-$KAFKAKN_RUN" "$KAFKAKN_B94_DIR/schema.json" "$KAFKAKN_B94_DIR/kafkakn-jvm.lines" \
    > "$KAFKAKN_B94_DIR/confluent.lines" 2> "$KAFKAKN_B94_DIR/write.err" || { head -5 "$KAFKAKN_B94_DIR/write.err"; bad "the serializer refused"; }
echo "  KafkaJsonSchemaSerializer wrote $(grep -c . "$KAFKAKN_B94_DIR/confluent.lines") record(s)"
suite decode

echo
[ "$fail" -eq 0 ] || { echo "B-94: RED"; exit 1; }
echo "B-94: kafkakn's JSON Schema bytes and the registry's own serializers read each other, on both arms"
