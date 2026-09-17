#!/usr/bin/env bash
# B-12: what a publication actually produced, and whether a stranger can resolve it.
#
# "It published" is a statement about a Gradle task. The two statements worth making are what
# COORDINATES exist afterwards - a KMP module has one per target and a route that covers one covers
# none of the others - and whether a build that knows nothing but a coordinate and a URL can compile
# against them.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3

VERSION=$(sed -n 's/^version=//p' gradle.properties)
GROUP_PATH=io/github/youndie/kafkakn
REPO=$ROOT/build/local-repo
EMPTY_REPO=$ROOT/build/empty-repo
# Kept between runs so Kotlin's own artefacts are not re-downloaded every time; OUR group is purged
# below, which is the part a warm cache would hide.
CONSUMER_HOME=${CONSUMER_HOME:-$HOME/.cache/kafkakn/consumer-gradle-home}

echo "=== environment ==="
date -Is
echo "  version $VERSION"

echo
echo "=== publish to a real Maven repository on disk ==="
rm -rf "$REPO"
./gradlew --no-daemon --console=plain :kafkakn-core:publishAllPublicationsToLocalRepository 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "PUBLISH FAILED"; exit 1; }

echo
echo "=== every coordinate, named one by one ==="
missing=0
for artefact in kafkakn-core kafkakn-core-jvm kafkakn-core-linuxx64; do
    dir=$REPO/$GROUP_PATH/$artefact/$VERSION
    if [ ! -d "$dir" ]; then
        echo "  MISSING $artefact" >&2
        missing=$((missing + 1))
        continue
    fi
    # The .module file is what a Gradle consumer reads; without it the variants are invisible and
    # resolution silently falls back to the POM.
    files=$(ls "$dir" | grep -vE '\.(sha1|sha256|sha512|md5)$' | tr '\n' ' ')
    printf '  %-24s %s\n' "$artefact" "$files"
    for required in .module .pom; do
        ls "$dir" | grep -q -- "$required\$" || { echo "  $artefact has no *$required" >&2; missing=$((missing + 1)); }
    done
done
[ "$missing" -eq 0 ] || { echo "  $missing coordinate(s) or file(s) missing" >&2; exit 1; }

echo
echo "=== the probe must FAIL against an empty repository ==="
echo "  (otherwise 'it resolved' says nothing about where it resolved FROM)"
rm -rf "$EMPTY_REPO"; mkdir -p "$EMPTY_REPO"
rm -rf "$CONSUMER_HOME/caches/modules-2/files-2.1/io.github.youndie.kafkakn" \
       "$CONSUMER_HOME/caches/modules-2/metadata-"*/descriptors/io.github.youndie.kafkakn
if GRADLE_USER_HOME=$CONSUMER_HOME ./gradlew --no-daemon --console=plain \
        -p ci/publish/consumer -Pkafkakn.repo="file://$EMPTY_REPO" -Pkafkakn.version="$VERSION" \
        --refresh-dependencies compileKotlinJvm >/dev/null 2>&1; then
    echo "  THE PROBE COMPILED AGAINST AN EMPTY REPOSITORY - it is resolving from somewhere else" >&2
    exit 1
fi
echo "  it fails, as it must"

echo
echo "=== a build that knows only a coordinate and a URL, on a cache purged of this group ==="
rm -rf "$CONSUMER_HOME/caches/modules-2/files-2.1/io.github.youndie.kafkakn" \
       "$CONSUMER_HOME/caches/modules-2/metadata-"*/descriptors/io.github.youndie.kafkakn
GRADLE_USER_HOME=$CONSUMER_HOME ./gradlew --no-daemon --console=plain \
    -p ci/publish/consumer -Pkafkakn.repo="file://$REPO" -Pkafkakn.version="$VERSION" \
    --refresh-dependencies compileKotlinJvm compileKotlinLinuxX64 \
    compileCommonMainKotlinMetadata 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "  THE PROBE COULD NOT USE THE PUBLISHED ARTEFACT"; exit 1; }
echo "  jvm, linuxX64 and the common metadata all compiled against the published module"

echo
echo "=== where each variant came from ==="
find "$CONSUMER_HOME/caches/modules-2/files-2.1/io.github.youndie.kafkakn" -name '*.jar' -o -name '*.klib' 2>/dev/null \
    | sed "s|$CONSUMER_HOME/caches/modules-2/files-2.1/||" | sed 's|/[0-9a-f]\{20,\}/|/|' | sort | sed 's/^/  /'

echo
echo "=== verdict ==="
echo "B-12: three coordinates published; a separate build resolves and compiles against them,"
echo "      and the same probe fails when the repository is empty"
