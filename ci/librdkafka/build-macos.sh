#!/usr/bin/env bash
# The C bundle for macosArm64 (B-40): a contributor's native loop on a Mac, not a published target.
#
# Built ON the Mac, because D4 does not apply here: macOS has no glibc floor, so there is no old-glibc
# image to build in, and the question is only which OpenSSL the bundle links - its own, statically, as
# on Linux. Homebrew's librdkafka is the rejected alternative: a system dependency is the shape this
# project exists not to have (research §1.1).
#
# The SAME sources as the Linux bundle, copied from the Linux box's cache rather than downloaded again,
# and checked against the checksums build.sh printed there - one set of inputs for both bundles.
# The local patch is NOT applied: it guards `sys/random.h` for the glibc 2.17 image, and macOS has it.
#
#   LOCAL=1 bash ci/librdkafka/build-macos.sh
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
VERSION=$(sed -n 's/^librdkafka = "\(.*\)"/\1/p' "$ROOT/gradle/libs.versions.toml")
OUT="$HOME/.cache/kafkakn/librdkafka-$VERSION-macosArm64"
SRC="$OUT/src"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
JOBS=$(sysctl -n hw.ncpu)
export CFLAGS="-O2 -arch arm64 -mmacosx-version-min=11.0"

[ "$(uname -s)-$(uname -m)" = "Darwin-arm64" ] || { echo "this builds the macosArm64 bundle, on an arm64 Mac" >&2; exit 1; }
for name in zlib-1.3.1.tar.gz zstd-1.5.6.tar.gz openssl-3.0.13.tar.gz "v$VERSION.tar.gz"; do
    [ -s "$SRC/$name" ] || { echo "missing $SRC/$name - copy it from the Linux box's cache (see the header)" >&2; exit 1; }
    printf '  %-26s sha256 %s…\n' "$name" "$(shasum -a 256 "$SRC/$name" | cut -c1-16)"
done

if [ ! -f "$OUT/lib/libz.a" ]; then
    echo "  zlib"
    mkdir -p "$WORK/zlib" && tar xzf "$SRC"/zlib-*.tar.gz -C "$WORK/zlib" --strip-components=1
    (cd "$WORK/zlib" && ./configure --prefix="$OUT" --static >/dev/null && make -j"$JOBS" >/dev/null && make install >/dev/null)
fi
if [ ! -f "$OUT/lib/libzstd.a" ]; then
    echo "  zstd"
    mkdir -p "$WORK/zstd" && tar xzf "$SRC"/zstd-*.tar.gz -C "$WORK/zstd" --strip-components=1
    (cd "$WORK/zstd/lib" && make -j"$JOBS" libzstd.a >/dev/null && make PREFIX="$OUT" install-static install-includes >/dev/null)
fi
if [ ! -f "$OUT/lib/libcrypto.a" ]; then
    echo "  openssl"
    mkdir -p "$WORK/ssl" && tar xzf "$SRC"/openssl-*.tar.gz -C "$WORK/ssl" --strip-components=1
    # The trust store: macOS keeps no PEM bundle where Linux does, and this project's tests always pass
    # ssl.ca.location, so the directory only matters to a caller who relies on the default.
    (cd "$WORK/ssl" && ./Configure darwin64-arm64-cc no-shared no-zlib no-tests --prefix="$OUT" --openssldir=/etc/ssl >/dev/null \
        && make -j"$JOBS" >/dev/null && make install_sw >/dev/null)
fi
if [ ! -f "$OUT/lib/librdkafka-static.a" ]; then
    echo "  librdkafka"
    mkdir -p "$WORK/rdk" && tar xzf "$SRC"/v*.tar.gz -C "$WORK/rdk" --strip-components=1
    (
        cd "$WORK/rdk"
        export CPPFLAGS="-I$OUT/include" LDFLAGS="-L$OUT/lib" PKG_CONFIG_PATH="$OUT/lib/pkgconfig"
        ./configure --prefix="$OUT" --enable-static --enable-ssl --enable-zlib --enable-zstd \
            --disable-lz4-ext --disable-curl --disable-gssapi >/dev/null
        echo "  --- what configure SELECTED ---"
        grep -E "^(WITH_SSL|WITH_ZLIB|WITH_ZSTD|WITH_CURL|WITH_SASL_SCRAM)=" Makefile.config | sed 's/^/    /'
        grep -q "^WITH_SSL=[[:space:]]*y" Makefile.config || { echo "SSL IS OFF - refusing" >&2; exit 1; }
        make -j"$JOBS" >/dev/null && make install >/dev/null
    )
fi

for a in librdkafka-static.a libssl.a libcrypto.a libz.a libzstd.a; do
    [ -s "$OUT/lib/$a" ] || { echo "the bundle has no $a" >&2; exit 1; }
done
echo "  bundle in $OUT:"
for a in librdkafka-static.a libssl.a libcrypto.a libz.a libzstd.a; do
    printf '    %-22s %s\n' "$a" "$(lipo -archs "$OUT/lib/$a" 2>/dev/null || echo '?')"
done
