#!/usr/bin/env bash
# B-13: a service that is not part of this build, resolving the PUBLISHED artefact and running.
#
# Compiling is where a consumer stops noticing. The native binary is LINKED and RUN here, because a
# klib that compiles against a cinterop and cannot be linked is a library nobody can ship - and the
# suite in this repository links its own test binaries with linker options the consumer never sees.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
H=ci/harness/broker.sh
TOPIC=kafkakn
VERSION=$(sed -n 's/^version=//p' gradle.properties)
GROUP=$(sed -n 's/^group=//p' gradle.properties)
# A HOME OF ITS OWN, emptied of this group: a consumer that resolves from a warm cache proves the
# cache. Kotlin's own artefacts stay, because re-downloading the compiler measures the network.
CONSUMER_HOME=${CONSUMER_HOME:-$HOME/.cache/kafkakn/b13-gradle-home}

echo "=== environment ==="
date -Is
echo "  $GROUP:kafkakn-core:$VERSION, from the network"

echo
echo "=== broker ==="
bash "$H" up || exit 1
bash "$H" topic "$TOPIC" | head -1

echo
echo "=== the consumer knows a coordinate and a URL, and its cache holds neither ==="
rm -rf "$CONSUMER_HOME/caches/modules-2/files-2.1/$GROUP" \
       "$CONSUMER_HOME/caches/modules-2/metadata-"*/descriptors/"$GROUP"
GRADLE_USER_HOME=$CONSUMER_HOME ./gradlew --no-daemon --console=plain \
    -p ci/consumer -Pkafkakn.version="$VERSION" --refresh-dependencies \
    installJvmDist linkReleaseExecutableLinuxX64 2>&1 | tail -6
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "  THE CONSUMER COULD NOT BUILD"; exit 1; }

JVM_START=ci/consumer/build/install/kafkakn-consumer-jvm/bin/kafkakn-consumer
NATIVE_BIN=$(ls ci/consumer/build/bin/linuxX64/releaseExecutable/*.kexe 2>/dev/null | head -1)
[ -x "$JVM_START" ] || { echo "  no jvm launcher at $JVM_START" >&2; exit 1; }
[ -n "$NATIVE_BIN" ] || { echo "  NOTHING WAS LINKED - no native executable" >&2; exit 1; }
echo "  jvm launcher:    $JVM_START"
echo "  native binary:   $NATIVE_BIN ($(du -h "$NATIVE_BIN" | cut -f1))"

echo
echo "=== and it runs - both of them ==="
BEFORE=$(bash "$H" offsets "$TOPIC")
STAMP_JVM=b13-jvm-$(date +%s)
STAMP_NATIVE=b13-native-$(date +%s)
RECORDS=50
"$JVM_START" 127.0.0.1:9092 "$TOPIC" "$STAMP_JVM" "$RECORDS" || { echo "  the jvm consumer failed"; exit 1; }
"$NATIVE_BIN" 127.0.0.1:9092 "$TOPIC" "$STAMP_NATIVE" "$RECORDS" || { echo "  the native consumer failed"; exit 1; }
AFTER=$(bash "$H" offsets "$TOPIC")

echo
echo "=== what an independent reader sees ==="
for stamp in "$STAMP_JVM" "$STAMP_NATIVE"; do
    found=$(CONSUME_MS=30000 bash "$H" consume "$TOPIC" "^$stamp:")
    printf '  %-24s %s/%s\n' "$stamp" "$found" "$RECORDS"
    [ "$found" -eq "$RECORDS" ] || { echo "  $stamp: the broker holds $found of $RECORDS" >&2; exit 1; }
done
echo "  end offsets on $TOPIC: $BEFORE -> $AFTER"

echo
echo "=== the headers it sent survived too ==="
for stamp in "$STAMP_JVM" "$STAMP_NATIVE"; do
    # No leading anchor: `print.headers=true` puts the headers FIRST on the line, so the value is
    # not at the start of it. The trailing one still distinguishes `:0` from `:10`.
    line=$(CONSUME_MS=30000 bash "$H" headers "$TOPIC" "$stamp:0\$")
    stored=${line%%$'\t'*}
    printf '  %-24s %s\n' "$stamp" "$stored"
    [ "$stored" = "from:$stamp" ] || { echo "  $stamp: headers came back as '$stored'" >&2; exit 1; }
done

echo
echo "=== verdict ==="
echo "B-13: a build that is not this one resolves the published artefact, links a native binary,"
echo "      runs both, and an independent reader sees every record"
