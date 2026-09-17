#!/usr/bin/env bash
# Build the C bundle against glibc 2.17 so Kotlin/Native needs no sysroot overrides.
#
# Downloads happen HERE, not in the container: manylinux2014 carries curl 7.29 with NSS, and a
# failure to negotiate TLS with a download host would look like a build problem.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
VERSION=$(sed -n 's/^librdkafka = "\(.*\)"/\1/p' "$ROOT/gradle/libs.versions.toml")
[ -n "$VERSION" ] || { echo "librdkafka version not found in the catalogue" >&2; exit 1; }
IMAGE=kafkakn/oldglibc
# Outside the source tree: it is build output, it is large, and on a machine where this checkout is
# synchronised one-way it would be erased anyway.
CACHE="$HOME/.cache/kafkakn/librdkafka-$VERSION"
SRC="$CACHE/src"

declare -A URLS=(
    [zlib-1.3.1.tar.gz]=https://zlib.net/fossils/zlib-1.3.1.tar.gz
    [zstd-1.5.6.tar.gz]=https://github.com/facebook/zstd/releases/download/v1.5.6/zstd-1.5.6.tar.gz
    [openssl-3.0.13.tar.gz]=https://github.com/openssl/openssl/releases/download/openssl-3.0.13/openssl-3.0.13.tar.gz
    [v$VERSION.tar.gz]=https://github.com/confluentinc/librdkafka/archive/refs/tags/v$VERSION.tar.gz
)

mkdir -p "$SRC" "$CACHE"
echo "=== sources (librdkafka $VERSION, from the catalogue) ==="
for name in "${!URLS[@]}"; do
    [ -s "$SRC/$name" ] || curl -sfL -o "$SRC/$name" "${URLS[$name]}"
    printf '  %-26s sha256 %s…\n' "$name" "$(sha256sum "$SRC/$name" | cut -c1-16)"
done

echo
echo "=== build image ==="
docker build -q -t "$IMAGE" -f "$HERE/oldglibc.Dockerfile" "$HERE" | sed 's/^/  /'

echo
echo "=== building in $IMAGE ==="
docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp \
    -v "$SRC:/src:ro" -v "$CACHE:/out" \
    -v "$HERE/inside.sh:/inside.sh:ro" -v "$HERE/patches:/patches:ro" \
    "$IMAGE" bash /inside.sh

echo
echo "cache: $CACHE"
