#!/usr/bin/env bash
# Resolve what was just published, from the network, as a stranger would.
#
# AN UPLOAD THAT RETURNED 2xx IS NOT A PUBLICATION. The files can be there and still be unusable: a
# metadata module whose `.module` lists a variant that was never uploaded, a POM under the wrong
# group, a coordinate whose maven-metadata.xml does not name the snapshot that was just written.
# Every one of those answers 200 to a HEAD and fails the first consumer to try.
#
# So the check is the only one that means anything: a SEPARATE build, knowing a coordinate and a
# URL, compiling against all three variants with the group purged from its cache.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3

REPO_URL=${REPO_URL:-https://reposilite.kotlin.website/snapshots}
GROUP=$(sed -n 's/^group=//p' gradle.properties)
VERSION=$(sed -n 's/^version=//p' gradle.properties)
CONSUMER_HOME=${CONSUMER_HOME:-$HOME/.cache/kafkakn/consumer-gradle-home}

echo "=== $GROUP:$VERSION, resolved from $REPO_URL ==="

# The group goes out of the cache first. Resolving what is already on disk proves the disk.
rm -rf "$CONSUMER_HOME/caches/modules-2/files-2.1/$GROUP" \
       "$CONSUMER_HOME/caches/modules-2/metadata-"*/descriptors/"$GROUP"

GRADLE_USER_HOME=$CONSUMER_HOME ./gradlew --no-daemon --console=plain \
    -p ci/publish/consumer -Pkafkakn.repo="$REPO_URL" -Pkafkakn.version="$VERSION" \
    --refresh-dependencies compileKotlinJvm compileKotlinLinuxX64 compileCommonMainKotlinMetadata \
    2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || {
    echo "  THE PUBLISHED ARTEFACT CANNOT BE USED - the upload succeeded and the publication did not" >&2
    exit 1
}

echo "  jvm, linuxX64 and the common metadata all compiled against the published module"
echo "  what it fetched:"
find "$CONSUMER_HOME/caches/modules-2/files-2.1/$GROUP" -name '*.jar' -o -name '*.klib' 2>/dev/null \
    | sed "s|$CONSUMER_HOME/caches/modules-2/files-2.1/||" | sed 's|/[0-9a-f]\{20,\}/|/|' | sort | sed 's/^/    /'
