#!/usr/bin/env bash
# Resolve what was just published, from the network, as a stranger would.
#
# AN UPLOAD THAT RETURNED 2xx IS NOT A PUBLICATION. The files can be there and still be unusable: a
# metadata module whose `.module` lists a variant that was never uploaded, a POM under the wrong
# group, a coordinate whose maven-metadata.xml does not name the snapshot that was just written.
# Every one of those answers 200 to a HEAD and fails the first build that tries to resolve it.
#
# So the check is the only one that means anything: a SEPARATE build, knowing a coordinate and a
# URL, compiling against all three variants with the group purged from its cache.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3

REPO_URL=${REPO_URL:-https://reposilite.kotlin.website/snapshots}
. ci/lib/coordinate.sh
kafkakn_coordinate || exit 2
DOWNSTREAM_HOME=${DOWNSTREAM_HOME:-$HOME/.cache/kafkakn/downstream-gradle-home}

echo "=== $GROUP:$VERSION, resolved from $REPO_URL ==="

# The group goes out of the cache first. Resolving what is already on disk proves the disk.
rm -rf "$DOWNSTREAM_HOME/caches/modules-2/files-2.1/$GROUP" \
       "$DOWNSTREAM_HOME/caches/modules-2/metadata-"*/descriptors/"$GROUP"

GRADLE_USER_HOME=$DOWNSTREAM_HOME ./gradlew --no-daemon --console=plain \
    -p ci/publish/downstream -Pkafkakn.repo="$REPO_URL" -Pkafkakn.version="$VERSION" \
    --refresh-dependencies compileKotlinJvm compileKotlinLinuxX64 compileCommonMainKotlinMetadata \
    2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || {
    echo "  THE PUBLISHED ARTEFACT CANNOT BE USED - the upload succeeded and the publication did not" >&2
    exit 1
}

echo "  jvm, linuxX64 and the common metadata all compiled against the published module"
echo "  what it fetched:"
find "$DOWNSTREAM_HOME/caches/modules-2/files-2.1/$GROUP" -name '*.jar' -o -name '*.klib' 2>/dev/null \
    | sed "s|$DOWNSTREAM_HOME/caches/modules-2/files-2.1/||" | sed 's|/[0-9a-f]\{20,\}/|/|' | sort | sed 's/^/    /'
