#!/usr/bin/env bash
# B-39: linuxArm64, compiled on the Linux box and RUN on real arm64 - a Mac's Docker, which is an
# aarch64 Linux (a linuxkit VM on Apple silicon), not emulation.
#
# Runs ON THE MAC, like ci/b-40/run.sh. The C bundle comes from `KAFKAKN_ARCH=aarch64
# ci/librdkafka/build.sh` on this Mac, because the one arm64 build needs an arm64 Docker. Kotlin is
# cross-compiled on the Linux box, as every native build is, and the broker, its certificates and the
# JVM arm stay there.
#
#   KAFKAKN_BROKER_SSH="-p 2222 you@127.0.0.1" KAFKAKN_BROKER_DIR=kafkakn bash ci/b-39/run.sh
#
# What it establishes, in order:
#   1. the published linuxArm64 klib carries its C, and a downstream build links against it with no
#      configuration of its own;
#   2. the native suite passes on arm64 against the broker, and agrees with the JVM arm;
#   3. the downstream binary produces on arm64, and the broker's own consumer sees every record;
#   4. the glibc floor of that binary, measured as ci/b-16/run.sh measures it for x64, and proved by
#      running it on that glibc (B-44).
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
. ci/lib/coordinate.sh
kafkakn_coordinate || exit 2
SSH_ARGS=${KAFKAKN_BROKER_SSH:?set KAFKAKN_BROKER_SSH to the ssh arguments that reach the broker box}
REMOTE_DIR=${KAFKAKN_BROKER_DIR:-kafkakn}
LIBRDKAFKA=$(sed -n 's/^librdkafka = "\(.*\)"/\1/p' gradle/libs.versions.toml)
BUNDLE_NAME=librdkafka-$LIBRDKAFKA-linuxArm64
BUNDLE=$HOME/.cache/kafkakn/$BUNDLE_NAME
# A stock image, nothing installed into it: what runs the binaries is what a stranger's arm64 host has.
IMAGE=ubuntu:24.04
OUT=$ROOT/build/b-39
PORTS="9092 9094 9095 9096 9097"
ACCOUNTING=kafkakn-acct-b39-$(date +%s)
# The build output on the far side is copied out at once, to a directory mutagen does not own.
STAGE=b39-stage
# shellcheck disable=SC2086
remote() { ssh -o BatchMode=yes $SSH_ARGS "cd $REMOTE_DIR && $*"; }
# shellcheck disable=SC2086
fetch() { ssh -o BatchMode=yes $SSH_ARGS "cat $1" > "$2"; }

echo "=== environment ==="
date "+%Y-%m-%dT%H:%M:%S%z"
echo "  $GROUP:kafkakn-core:$VERSION, librdkafka $LIBRDKAFKA"
echo "  Mac: $(sw_vers -productVersion), $(uname -m); Docker: $(docker info --format '{{.OperatingSystem}}, {{.Architecture}}, kernel {{.KernelVersion}}, {{.NCPU}} CPUs')"
[ "$(uname -s)-$(uname -m)" = "Darwin-arm64" ] || { echo "run this on an arm64 Mac" >&2; exit 1; }
[ "$(docker info --format '{{.Architecture}}')" = aarch64 ] || { echo "this Docker is not aarch64: it would be emulation" >&2; exit 1; }
[ -f "$BUNDLE/lib/librdkafka-static.a" ] || { echo "no arm64 bundle at $BUNDLE: KAFKAKN_ARCH=aarch64 bash ci/librdkafka/build.sh" >&2; exit 1; }
rm -rf "$OUT" && mkdir -p "$OUT/work"

echo
echo "=== the arm64 bundle, to the Linux box, where the target is compiled ==="
dirs=$(cd "$BUNDLE" && ls -d include lib lib64 2>/dev/null | tr '\n' ' ')
# shellcheck disable=SC2086
# --no-xattrs: the bundle came out of Docker Desktop, whose file-sharing ownership attributes a
# Mac's tar would carry and GNU tar would complain about, once per file.
tar --no-xattrs -C "$BUNDLE" -cf - $dirs | ssh -o BatchMode=yes $SSH_ARGS \
    "rm -rf .cache/kafkakn/$BUNDLE_NAME && mkdir -p .cache/kafkakn/$BUNDLE_NAME && tar -C .cache/kafkakn/$BUNDLE_NAME -xf - && echo \"  \$(du -sh .cache/kafkakn/$BUNDLE_NAME | cut -f1) in $dirs\""
if command -v mutagen >/dev/null; then mutagen sync flush kafkakn >/dev/null || { echo "  mutagen flush failed" >&2; exit 1; }; fi

echo
echo "=== 1. publish with linuxArm64, and a downstream build links against it ==="
remote "rm -rf \$HOME/$STAGE && mkdir -p \$HOME/$STAGE && . ci/lib/coordinate.sh && kafkakn_coordinate || exit 1
    rm -rf build/local-repo
    ./gradlew --no-daemon --console=plain -Pkafkakn.linuxArm64 \
        :kafkakn-core:publishAllPublicationsToLocalRepository :kafkakn-core:linkDebugTestLinuxArm64 \
        > \$HOME/$STAGE/build.out 2>&1 || { tail -30 \$HOME/$STAGE/build.out; echo '  THE LIBRARY BUILD FAILED'; exit 1; }
    cp kafkakn-core/build/bin/linuxArm64/debugTest/test.kexe \$HOME/$STAGE/test.kexe
    dir=build/local-repo/\$GROUP_PATH/kafkakn-core-linuxarm64/\$VERSION
    [ -d \"\$dir\" ] || { echo \"  no linuxarm64 coordinate in the local repository\"; exit 1; }
    echo \"  coordinate: kafkakn-core-linuxarm64 (\$(ls \$dir | grep -cvE '\\.(sha1|sha256|sha512|md5)\$') files)\"
    for klib in \$dir/*.klib; do
        archives=\$(unzip -l \"\$klib\" | grep -oE '[a-z0-9_-]+\\.a\$' | sort -u | tr '\\n' ' ')
        [ -n \"\$archives\" ] && echo \"  \$(basename \$klib) carries: \$archives\"
    done
    cp -r build/local-repo \$HOME/$STAGE/repo
    DH=\$HOME/.cache/kafkakn/downstream-gradle-home
    rm -rf \$DH/caches/modules-2/files-2.1/\$GROUP \$DH/caches/modules-2/metadata-*/descriptors/\$GROUP
    GRADLE_USER_HOME=\$DH ./gradlew --no-daemon --console=plain -p ci/downstream -Pkafkakn.linuxArm64 \
        -Pkafkakn.version=\$VERSION -Pkafkakn.repo=file://\$HOME/$STAGE/repo --refresh-dependencies \
        linkReleaseExecutableLinuxArm64 > \$HOME/$STAGE/downstream.out 2>&1 \
        || { tail -30 \$HOME/$STAGE/downstream.out; echo '  THE DOWNSTREAM BUILD COULD NOT LINK'; exit 1; }
    cp ci/downstream/build/bin/linuxArm64/releaseExecutable/*.kexe \$HOME/$STAGE/downstream.kexe
    for bin in test downstream; do
        f=\$HOME/$STAGE/\$bin.kexe
        printf '  %-11s %s, %s bytes, needs: %s\n' \$bin \"\$(readelf -h \$f | sed -n 's/^ *Machine: *//p')\" \
            \"\$(stat -c %s \$f)\" \"\$(readelf -d \$f | sed -n 's/.*Shared library: \\[\\(.*\\)\\]/\\1/p' | tr '\\n' ' ')\"
    done" || exit 1
fetch "\$HOME/$STAGE/test.kexe" "$OUT/test.kexe"
fetch "\$HOME/$STAGE/downstream.kexe" "$OUT/downstream.kexe"
chmod +x "$OUT"/*.kexe

echo
echo "=== the broker, on the Linux box ==="
remote "bash ci/harness/broker.sh up" || exit 1
remote "bash ci/harness/broker.sh topic kafkakn >/dev/null; bash ci/harness/broker.sh strict-topic kafkakn-strict >/dev/null; \
    for arm in jvm linuxArm64; do bash ci/harness/broker.sh topic $ACCOUNTING-\$arm >/dev/null; done; echo '  topics ready'"
mkdir -p "$HOME/.cache/kafkakn/tls"
# shellcheck disable=SC2086
ssh -o BatchMode=yes $SSH_ARGS "tar -C \$HOME/.cache/kafkakn/tls -cf - ." | tar -C "$HOME/.cache/kafkakn/tls" -xf -
echo "  the fixture's certificates: $(ls "$HOME/.cache/kafkakn/tls" | wc -l | tr -d ' ') files"
FORWARDS=$(for p in $PORTS; do printf -- '-L %s:127.0.0.1:%s ' "$p" "$p"; done)
# shellcheck disable=SC2086
ssh -o BatchMode=yes -o ExitOnForwardFailure=yes -f -N $FORWARDS $SSH_ARGS || { echo "  the tunnel did not open" >&2; exit 1; }
TUNNEL=$(pgrep -f -- "-L 9092:127.0.0.1:9092" | head -1)
trap '[ -n "$TUNNEL" ] && kill "$TUNNEL" 2>/dev/null' EXIT
echo "  ports $PORTS forwarded to the Mac (ssh pid $TUNNEL), and inside the container to the Mac"

echo
echo "=== 2. the native suite, on arm64 ==="
# ONE container for everything that runs on arm64, with the forwarder in it: the suite and the downstream
# binary run in the same stock image, started once.
CONTAINER=$(docker run -d --rm --platform linux/arm64 -e HOME=/root \
    -v "$OUT:/b39" -v "$HOME/.cache/kafkakn/tls:/root/.cache/kafkakn/tls:ro" \
    -v "$HERE/forward.pl:/forward.pl:ro" -w /b39/work "$IMAGE" \
    bash -c "perl /forward.pl $PORTS & exec sleep infinity") || { echo "  the container did not start" >&2; exit 1; }
trap '[ -n "$TUNNEL" ] && kill "$TUNNEL" 2>/dev/null; docker kill "$CONTAINER" >/dev/null 2>&1' EXIT
sleep 1
echo "  inside: $(docker exec "$CONTAINER" uname -m), $(docker exec "$CONTAINER" bash -c 'ldd --version | head -1')"

docker exec -e KAFKAKN_BOOTSTRAP=127.0.0.1:9092 -e KAFKAKN_TOPIC=kafkakn -e KAFKAKN_STRICT_TOPIC=kafkakn-strict \
    -e KAFKAKN_SSL_BOOTSTRAP=127.0.0.1:9094 -e KAFKAKN_ACCOUNTING_TOPIC="$ACCOUNTING" \
    "$CONTAINER" /b39/test.kexe > "$OUT/suite.out" 2>&1
code=$?
# "1 test" and "2 tests": the summary line is singular for one, and a pattern for the plural reads a
# single failure as none.
passed=$(sed -n 's/^\[  PASSED  \] \([0-9]*\) tests*\..*/\1/p' "$OUT/suite.out" | tail -1)
failed=$(sed -n 's/^\[  FAILED  \] \([0-9]*\) tests*, listed below.*/\1/p' "$OUT/suite.out" | tail -1)
printf '  test.kexe exit=%s passed=%s failed=%s\n' "$code" "${passed:-0}" "${failed:-0}"
[ "$code" -eq 0 ] && [ "${passed:-0}" -gt 0 ] && [ "${failed:-0}" -eq 0 ] \
    || { grep -E '^\[  FAILED  \]' "$OUT/suite.out" | head -20; echo "  THE NATIVE SUITE FAILED ON ARM64"; exit 1; }

echo
echo "=== the JVM arm, on the Linux box, against the same broker ==="
remote "rm -rf kafkakn-core/build/observations; KAFKAKN_BOOTSTRAP=127.0.0.1:9092 KAFKAKN_TOPIC=kafkakn KAFKAKN_STRICT_TOPIC=kafkakn-strict \
    KAFKAKN_SSL_BOOTSTRAP=127.0.0.1:9094 KAFKAKN_ACCOUNTING_TOPIC=$ACCOUNTING \
    ./gradlew --console=plain :kafkakn-core:jvmTest --rerun > \$HOME/$STAGE/jvm.out 2>&1; echo \"  jvmTest exit=\$?\"; \
    cp kafkakn-core/build/observations/jvm.txt \$HOME/$STAGE/jvm.txt"
fetch "\$HOME/$STAGE/jvm.txt" "$OUT/jvm.txt"
bash ci/harness/compare-arms.sh "$OUT/jvm.txt" "$OUT/work/build/observations/linuxArm64.txt" || exit 1

echo
echo "=== 3. the downstream binary produces on arm64, and the broker's own consumer sees it ==="
STAMP=b39-arm64-$(date +%s)
RECORDS=50
docker exec "$CONTAINER" /b39/downstream.kexe 127.0.0.1:9092 kafkakn "$STAMP" "$RECORDS" > "$OUT/downstream.out" 2>&1 \
    || { cat "$OUT/downstream.out"; echo "  the downstream binary failed on arm64"; exit 1; }
sed 's/^/  /' "$OUT/downstream.out"
found=$(remote "CONSUME_MS=30000 bash ci/harness/broker.sh consume kafkakn '^$STAMP:'")
printf '  %s: the broker holds %s of %s\n' "$STAMP" "$found" "$RECORDS"
[ "$found" = "$RECORDS" ] || { echo "  RECORDS MISSING" >&2; exit 1; }
line=$(remote "CONSUME_MS=30000 bash ci/harness/broker.sh headers kafkakn '$STAMP:0\$'")
stored=${line%%$'\t'*}
[ "$stored" = "from:$STAMP" ] || { echo "  headers came back as '$stored'" >&2; exit 1; }
echo "  and its header survived: $stored"

echo
echo "=== 4. the oldest glibc the arm64 binary would run on ==="
# readelf rather than b-16's objdump -T: the same versioned references, read by a tool that does not
# care which architecture wrote the file. The x86 binutils on the Linux box cannot disassemble aarch64.
floor() { remote "readelf -V --wide \$HOME/$STAGE/$1.kexe | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1"; }
printf '  downstream: %s\n' "$(floor downstream)"
printf '  test:       %s\n' "$(floor test)"
# The same floor as x64 (B-44). It was GLIBC_2.25: OpenSSL's weak `getentropy`, bound against
# Kotlin/Native's aarch64 sysroot (glibc 2.25). The aarch64 bundle now seeds OpenSSL from /dev/urandom
# (`--with-rand-seed=devrandom`), which compiles the reference out. inside.sh refuses any archive that
# still has it.
CLAIMED=GLIBC_2.17
[ "$(floor downstream)" = "$CLAIMED" ] || { echo "  the documents say $CLAIMED and this binary says $(floor downstream)" >&2; exit 1; }
echo "  the documents say $CLAIMED, and so does the binary"

echo
echo "=== and it runs there: the downstream binary, on glibc 2.17, produces to the broker ==="
# A number read off the binary is not the loader's opinion. This is: the oldest glibc the claim names,
# in the image the C bundle was built in, running the binary a stranger would link. Before B-44 it
# stopped here with "version 'GLIBC_2.25' not found".
OLD_IMAGE=quay.io/pypa/manylinux2014_aarch64
STAMP_OLD=b39-glibc217-$(date +%s)
docker run --rm --platform linux/arm64 -v "$OUT:/b39:ro" -v "$HERE/forward.pl:/forward.pl:ro" "$OLD_IMAGE" \
    bash -c "echo \"inside: \$(ldd --version | head -1)\"; perl /forward.pl 9092 & sleep 1; /b39/downstream.kexe 127.0.0.1:9092 kafkakn $STAMP_OLD $RECORDS" \
    > "$OUT/glibc217.out" 2>&1 || { cat "$OUT/glibc217.out"; echo "  THE BINARY DOES NOT RUN ON GLIBC 2.17" >&2; exit 1; }
sed 's/^/  /' "$OUT/glibc217.out"
found=$(remote "CONSUME_MS=30000 bash ci/harness/broker.sh consume kafkakn '^$STAMP_OLD:'")
printf '  %s: the broker holds %s of %s\n' "$STAMP_OLD" "$found" "$RECORDS"
[ "$found" = "$RECORDS" ] || { echo "  RECORDS MISSING" >&2; exit 1; }

echo
echo "B-39: linuxArm64 links from its published klib, and runs on arm64 hardware against the broker"
