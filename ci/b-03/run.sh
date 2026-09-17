#!/usr/bin/env bash
# B-03: the C bundle links into a Kotlin/Native binary with NO toolchain overrides, and the binary
# resolves nothing at runtime that a Kafka-free binary does not.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
BIN=kafkakn-core/build/bin/linuxX64/debugTest/test.kexe

echo "=== the C bundle ==="
bash ci/librdkafka/build.sh 2>&1 | sed -n '/what configure SELECTED/,$p'

echo
echo "=== with the C bundle: link, run, and keep the binary ==="
./gradlew --no-daemon --console=plain :kafkakn-core:linuxX64Test 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "BUILD FAILED"; exit 3; }
cp "$BIN" "$WORK/with.kexe"
n=$(grep -ho 'tests="[0-9]*"' kafkakn-core/build/test-results/linuxX64Test/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
f=$(grep -ho 'failures="[0-9]*"' kafkakn-core/build/test-results/linuxX64Test/TEST-*.xml | grep -oE '[0-9]+' | paste -sd+ | bc)
echo "  linuxX64Test tests=$n failures=$f"
grep -q CinteropLinkTest <(ls kafkakn-core/build/test-results/linuxX64Test/) \
    || { echo "  THE CINTEROP TEST DID NOT RUN - a green build that linked nothing" >&2; exit 3; }
echo "  CinteropLinkTest is among them, so librdkafka was linked AND called"

echo
echo "=== without it: the same build, Kafka-free, for the comparison ==="
./gradlew --no-daemon --console=plain -Pkafkakn.noKafkaC :kafkakn-core:linuxX64Test 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "BASELINE BUILD FAILED"; exit 3; }
cp "$BIN" "$WORK/without.kexe"
ls kafkakn-core/build/test-results/linuxX64Test/ | grep -q CinteropLinkTest \
    && { echo "  THE BASELINE STILL CONTAINS THE CINTEROP TEST - it is not Kafka-free" >&2; exit 3; }
echo "  the baseline has no CinteropLinkTest, as it must"

echo
echo "=== the two binaries ==="
printf '  %-10s %12s bytes  md5=%s\n' with "$(stat -c%s "$WORK/with.kexe")" "$(md5sum "$WORK/with.kexe" | cut -d' ' -f1)"
printf '  %-10s %12s bytes  md5=%s\n' without "$(stat -c%s "$WORK/without.kexe")" "$(md5sum "$WORK/without.kexe" | cut -d' ' -f1)"
[ "$(md5sum "$WORK/with.kexe" | cut -d' ' -f1)" != "$(md5sum "$WORK/without.kexe" | cut -d' ' -f1)" ] \
    || { echo "  THE TWO BINARIES ARE IDENTICAL - the switch did nothing" >&2; exit 3; }

echo
echo "=== RQ: does linking librdkafka add anything to what is resolved at runtime? ==="
ldd "$WORK/with.kexe"    | awk '{print $1}' | sort > "$WORK/with.ldd"
ldd "$WORK/without.kexe" | awk '{print $1}' | sort > "$WORK/without.ldd"
echo "  Kafka-free:"; sed 's/^/    /' "$WORK/without.ldd"
echo "  with librdkafka:"; sed 's/^/    /' "$WORK/with.ldd"
if diff -q "$WORK/with.ldd" "$WORK/without.ldd" >/dev/null; then
    echo "  LDD: identical - librdkafka, OpenSSL, zlib and zstd resolve nothing at runtime"
    LDD_OK=1
else
    echo "  LDD: DIFFERS"; diff "$WORK/without.ldd" "$WORK/with.ldd" | sed 's/^/    /'; LDD_OK=0
fi

echo
echo "=== and no toolchain override was used ==="
# Asked of the CODE. The first form of this check counted 1 - its own comment saying that no
# override is used. A checker that cannot tell documentation from code produces a false reading and
# teaches people to delete the documentation, so the comment-stripping lives in
# ci/lib/token_in_code.py and is reused rather than re-improvised.
OVERRIDES=$(python3 ci/lib/token_in_code.py kafkakn-core/build.gradle.kts Xoverride-konan-properties)
echo "  -Xoverride-konan-properties outside comments: $OVERRIDES"
if [ "$OVERRIDES" -ne 0 ]; then
    echo "  AN OVERRIDE IS IN USE - the route B-03 measures is not the one that was built" >&2
    exit 1
fi

echo
echo "=== verdict ==="
if [ "$LDD_OK" -eq 1 ] && [ "$f" -eq 0 ]; then
    echo "B-03: the bundle links with no overrides, and adds nothing to the runtime set"
else
    echo "B-03: NOT GREEN - read the lines above"
    exit 1
fi
