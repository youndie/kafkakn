#!/usr/bin/env bash
# Runs INSIDE the old-glibc image. No network: the tarballs are fetched on the host, where curl is
# modern, and mounted read-only. That also makes the build repeatable.
set -euo pipefail

SRC=/src
OUT=/out
PATCHES=/patches
JOBS=$(nproc)
FLAGS="-fPIC -O2"
# aarch64 (B-39): this image's gcc 10 compiles every atomic as a call to a helper in ITS libgcc
# (`__aarch64_ldadd4_acq_rel` and the rest, the "outline atomics"), and Kotlin/Native's aarch64
# toolchain links an older libgcc that has none of them. The first link failed on exactly those
# symbols. Inline LSE-less atomics are what every aarch64 CPU runs, so nothing is given up.
[ "$(uname -m)" = aarch64 ] && FLAGS="$FLAGS -mno-outline-atomics"
export CFLAGS="$FLAGS"
export CXXFLAGS="$FLAGS"

echo "  host: $(ldd --version | head -1), $(gcc --version | head -1)"

build_zlib() {
    echo "  zlib"
    rm -rf /tmp/zlib && mkdir -p /tmp/zlib && tar xzf "$SRC"/zlib-*.tar.gz -C /tmp/zlib --strip-components=1
    cd /tmp/zlib && ./configure --prefix="$OUT" --static >/dev/null && make -j"$JOBS" >/dev/null && make install >/dev/null
}

build_zstd() {
    echo "  zstd"
    rm -rf /tmp/zstd && mkdir -p /tmp/zstd && tar xzf "$SRC"/zstd-*.tar.gz -C /tmp/zstd --strip-components=1
    cd /tmp/zstd/lib && make -j"$JOBS" libzstd.a >/dev/null && make PREFIX="$OUT" install-static install-includes >/dev/null
}

build_openssl() {
    echo "  openssl"
    rm -rf /tmp/ssl && mkdir -p /tmp/ssl && tar xzf "$SRC"/openssl-*.tar.gz -C /tmp/ssl --strip-components=1
    cd /tmp/ssl
    # no-zlib: OpenSSL's optional zlib link would add a second opinion about which zlib is in the
    # binary, and librdkafka links zlib itself.
    ./Configure "${OPENSSL_TARGET:-linux-x86_64}" no-shared no-zlib no-tests --prefix="$OUT" --openssldir=/etc/ssl >/dev/null
    make -j"$JOBS" >/dev/null && make install_sw >/dev/null
}

build_librdkafka() {
    echo "  librdkafka"
    rm -rf /tmp/rdk && mkdir -p /tmp/rdk && tar xzf "$SRC"/v*.tar.gz -C /tmp/rdk --strip-components=1
    cd /tmp/rdk
    # Two different refusals rather than one message for two opposite actions - see the script.
    bash /apply-patches.sh /tmp/rdk "$PATCHES"
    export CPPFLAGS="-I$OUT/include"
    export LDFLAGS="-L$OUT/lib -L$OUT/lib64"
    export PKG_CONFIG_PATH="$OUT/lib/pkgconfig:$OUT/lib64/pkgconfig"
    ./configure --prefix="$OUT" --enable-static --enable-ssl --enable-zlib --enable-zstd \
        --disable-lz4-ext --disable-curl --disable-gssapi >/dev/null

    echo "  --- what configure SELECTED (the fallbacks must be chosen, not silently skipped) ---"
    grep -E "^(WITH_SSL|WITH_ZLIB|WITH_ZSTD|WITH_CURL|WITH_SASL_SCRAM)=" Makefile.config | sed 's/^/    /'
    grep -iE "c11threads|getentropy|strlcpy|pthread_setname" config.h | sed 's/^/    /' || true
    grep -q "^WITH_SSL=[[:space:]]*y" Makefile.config || { echo "SSL IS OFF - refusing" >&2; exit 1; }
    # The whole route depends on C11 threads being ABSENT here, so librdkafka uses its bundled
    # tinycthread. If this image ever grows them, mtx_lock comes straight back as a libc dependency.
    grep -q "^#define WITH_C11THREADS" config.h && { echo "C11 THREADS FOUND - the fallback will not be used" >&2; exit 1; }
    echo "    (no WITH_C11THREADS: the bundled tinycthread is what will be linked)"

    make -j"$JOBS" >/dev/null && make install >/dev/null
}

[ -f "$OUT/lib/libz.a" ] || build_zlib
[ -f "$OUT/lib/libzstd.a" ] || build_zstd
{ [ -f "$OUT/lib/libcrypto.a" ] || [ -f "$OUT/lib64/libcrypto.a" ]; } || build_openssl
[ -f "$OUT/lib/librdkafka-static.a" ] || build_librdkafka

echo
echo "=== does any archive still need a symbol newer than glibc 2.19? ==="
# THE PAIR, with anchored names. `nm -u` on a static archive lists undefined symbols PER MEMBER,
# including ones another member satisfies - by that reading librdkafka "needs" mtx_lock from libc
# when it defines it itself. The question is: defined anywhere in this archive, or genuinely
# expected from outside?
LIB=$OUT/lib; [ -f "$OUT/lib64/libcrypto.a" ] && LIB64=$OUT/lib64 || LIB64=$OUT/lib
printf '  %-22s %-14s %s\n' ARCHIVE SYMBOL VERDICT
fail=0
for a in "$OUT/lib/librdkafka-static.a" "$LIB64/libssl.a" "$LIB64/libcrypto.a" "$OUT/lib/libz.a" "$OUT/lib/libzstd.a"; do
    [ -f "$a" ] || { echo "  MISSING $a" >&2; exit 1; }
    for sym in mtx_lock thrd_create cnd_signal strlcpy getentropy __isoc23_strtol; do
        d=$(nm --defined-only "$a" 2>/dev/null | grep -cE "[TtWw] ${sym}$" || true)
        u=$(nm -u "$a" 2>/dev/null | grep -cE "U ${sym}$" || true)
        if [ "$d" -gt 0 ]; then v="defined here"
        elif [ "$u" -gt 0 ]; then v="NEEDS LIBC"; fail=1
        else continue; fi
        printf '  %-22s %-14s %s\n' "$(basename "$a")" "$sym" "$v"
    done
done
[ "$fail" -eq 0 ] || { echo "  a post-2.19 symbol is still expected from libc - the route does not hold" >&2; exit 1; }
echo "  no archive expects a post-2.19 symbol from libc"

echo
echo "=== does any archive expect a helper only this image's libgcc has? ==="
# The same question for the compiler's runtime rather than libc: whatever an archive leaves undefined
# for libgcc must be in the libgcc Kotlin/Native links, which is older. Outline atomics are the known
# case; the check is for the family, so a flag that stops working shows here, not at a stranger's link.
for a in "$OUT/lib/librdkafka-static.a" "$LIB64/libssl.a" "$LIB64/libcrypto.a" "$OUT/lib/libz.a" "$OUT/lib/libzstd.a"; do
    n=$(nm -u "$a" 2>/dev/null | grep -c "U __aarch64_" || true)
    [ "$n" -eq 0 ] || { printf '  %-22s %s references to __aarch64_* helpers\n' "$(basename "$a")" "$n"; fail=1; }
done
[ "$fail" -eq 0 ] || { echo "  an archive calls a libgcc helper Kotlin/Native does not link" >&2; exit 1; }
echo "  none: nothing is left for a libgcc newer than Kotlin/Native's"
