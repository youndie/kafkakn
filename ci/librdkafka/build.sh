#!/usr/bin/env bash
# Build the C bundle against glibc 2.17 so Kotlin/Native needs no sysroot overrides.
#
#   ci/librdkafka/build.sh                        x86_64, for linuxX64, on the Linux box
#   KAFKAKN_ARCH=aarch64 ci/librdkafka/build.sh   aarch64, for linuxArm64 (B-39), on an arm64 Docker -
#                                                 a Mac's is one; the Linux box would need emulation
#
# Downloads happen HERE, not in the container: manylinux2014 carries curl 7.29 with NSS, and a
# failure to negotiate TLS with a download host would look like a build problem.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
VERSION=$(sed -n 's/^librdkafka = "\(.*\)"/\1/p' "$ROOT/gradle/libs.versions.toml")
[ -n "$VERSION" ] || { echo "librdkafka version not found in the catalogue" >&2; exit 1; }
ARCH=${KAFKAKN_ARCH:-x86_64}
# The x86_64 path carries no architecture because it predates the second one, and every script and
# the Gradle build already read it; the arm64 bundle gets its own, so neither overwrites the other.
case "$ARCH" in
    x86_64) SUFFIX="" ;;
    aarch64) SUFFIX=-linuxArm64 ;;
    *) echo "KAFKAKN_ARCH is x86_64 or aarch64, not $ARCH" >&2; exit 1 ;;
esac
[ "$(docker info --format '{{.Architecture}}')" = "$ARCH" ] || {
    echo "this Docker runs $(docker info --format '{{.Architecture}}'), not $ARCH: building here means emulation" >&2
    exit 1
}
IMAGE=kafkakn/oldglibc-$ARCH
# Outside the source tree: it is build output, it is large, and on a machine where this checkout is
# synchronised one-way it would be erased anyway.
CACHE="$HOME/.cache/kafkakn/librdkafka-$VERSION$SUFFIX"
SRC="$CACHE/src"

# Name and URL, one per line: a list rather than an associative array, which a Mac's bash 3.2 lacks.
SOURCES="zlib-1.3.1.tar.gz https://zlib.net/fossils/zlib-1.3.1.tar.gz
zstd-1.5.6.tar.gz https://github.com/facebook/zstd/releases/download/v1.5.6/zstd-1.5.6.tar.gz
openssl-3.0.13.tar.gz https://github.com/openssl/openssl/releases/download/openssl-3.0.13/openssl-3.0.13.tar.gz
v$VERSION.tar.gz https://github.com/confluentinc/librdkafka/archive/refs/tags/v$VERSION.tar.gz"

mkdir -p "$SRC" "$CACHE"
echo "=== sources (librdkafka $VERSION, from the catalogue; $ARCH) ==="
while read -r name url; do
    [ -s "$SRC/$name" ] || curl -sfL -o "$SRC/$name" "$url"
    printf '  %-26s sha256 %s…\n' "$name" "$(sha256sum "$SRC/$name" | cut -c1-16)"
done <<< "$SOURCES"

echo
echo "=== build image ==="
docker build -q --build-arg ARCH="$ARCH" -t "$IMAGE" -f "$HERE/oldglibc.Dockerfile" "$HERE" | sed 's/^/  /'

echo
echo "=== building in $IMAGE ==="
docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -e OPENSSL_TARGET="linux-$ARCH" \
    -v "$SRC:/src:ro" -v "$CACHE:/out" \
    -v "$HERE/inside.sh:/inside.sh:ro" -v "$HERE/patches:/patches:ro" \
    -v "$HERE/apply-patches.sh:/apply-patches.sh:ro" \
    "$IMAGE" bash /inside.sh

echo
echo "cache: $CACHE"
