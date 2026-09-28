#!/usr/bin/env bash
# B-95: a @Serializable type as Protobuf, held against the registry's own serializers, both ways, on both arms.
#   1. each arm encodes the samples; Confluent's KafkaProtobufDeserializer reads the bytes, and what it reads, printed
#      in Protobuf's JSON mapping, is compared with the JSON each value is;
#   2. Confluent's KafkaProtobufSerializer writes the same values as DynamicMessages of kafkakn's generated .proto;
#      each arm decodes those bytes into the same samples.
# The harness is B-94's, which has both formats.
#
#   ci/b-95/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
export GRADLE_OPTS=-Dorg.gradle.daemon=false
REGISTRY=http://127.0.0.1:18081
export KAFKAKN_RUN=b95-$(date +%s)
export KAFKAKN_B95_DIR=$ROOT/build/b-94
fail=0
bad() { echo "    $*" >&2; fail=1; }
rm -rf "$KAFKAKN_B95_DIR"; mkdir -p "$KAFKAKN_B95_DIR"

echo "=== environment ==="
date -Is
bash ci/harness/broker.sh up || exit 1
./gradlew --console=plain -p ci/b-94/oracle installDist > build/b-95-oracle-build.out 2>&1 || { tail -15 build/b-95-oracle-build.out; exit 1; }
ORACLE=ci/b-94/oracle/build/install/kafkakn-registry-oracle/bin/kafkakn-registry-oracle

suite() { # <phase>
    for task in jvmTest linuxX64Test; do
        KAFKAKN_B95_PHASE=$1 ./gradlew --console=plain ":kafkakn-schema-registry:$task" --rerun \
            --tests '*ProtobufOracleTest*' > "build/b-95-$1-$task.out" 2>&1
        code=$?
        printf '  %-14s %-7s exit=%s\n' "$task" "$1" "$code"
        [ "$code" -eq 0 ] || { grep -E "FAILED|expected|Exception" "build/b-95-$1-$task.out" | head -6; bad "$task $1 failed"; }
    done
}

echo
echo "=== 1. kafkakn writes, the registry's deserializer reads ==="
suite encode
for arm in jvm linuxX64; do
    lines=$KAFKAKN_B95_DIR/kafkakn-$arm.lines
    [ -s "$lines" ] || { bad "$arm wrote nothing"; continue; }
    "$ORACLE" read-proto "$REGISTRY" "kafkakn-b95-$arm-$KAFKAKN_RUN" "$lines" > "$KAFKAKN_B95_DIR/read-$arm.json" 2> "$KAFKAKN_B95_DIR/read-$arm.err" \
        || { head -5 "$KAFKAKN_B95_DIR/read-$arm.err"; bad "$arm: the deserializer refused kafkakn's bytes"; continue; }
    python3 - "$lines" "$KAFKAKN_B95_DIR/read-$arm.json" "$arm" <<'PY' || fail=1
import json, sys
# Protobuf's JSON mapping writes an int64 as a string ("1"), and kotlinx as a number: compared as numbers.
def norm(v):
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    if isinstance(v, list): return [norm(x) for x in v]
    if isinstance(v, str) and v.lstrip("-").isdigit(): return int(v)
    return v
wrote = [norm(json.loads(l.split("\t")[1])) for l in open(sys.argv[1]) if l.strip()]
read = [norm(json.loads(l)) for l in open(sys.argv[2]) if l.strip()]
same = wrote == read
print(f"  {sys.argv[3]:<9} {len(read)} of {len(wrote)} read back by KafkaProtobufDeserializer, {'equal' if same else 'DIFFERENT'}")
for w, r in zip(wrote, read):
    if w != r: print(f"    wrote {w}\n    read  {r}")
sys.exit(0 if same and len(read) == len(wrote) else 1)
PY
done

echo
echo "=== 2. the registry's serializer writes, kafkakn reads ==="
"$ORACLE" write-proto "$REGISTRY" "kafkakn-b95-confluent-$KAFKAKN_RUN" "$KAFKAKN_B95_DIR/schema.proto" "$KAFKAKN_B95_DIR/kafkakn-jvm.lines" \
    > "$KAFKAKN_B95_DIR/confluent.lines" 2> "$KAFKAKN_B95_DIR/write.err" || { head -5 "$KAFKAKN_B95_DIR/write.err"; bad "the serializer refused"; }
echo "  KafkaProtobufSerializer wrote $(grep -c . "$KAFKAKN_B95_DIR/confluent.lines") record(s)"
suite decode

echo
[ "$fail" -eq 0 ] || { echo "B-95: RED"; exit 1; }
echo "B-95: kafkakn's Protobuf bytes and the registry's own serializers read each other, on both arms"
