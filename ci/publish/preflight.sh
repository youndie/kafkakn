#!/usr/bin/env bash
# Can this token write to EVERY coordinate, before anything is uploaded to any of them?
#
# WHY THIS EXISTS. A Reposilite route is a raw string prefix with a trailing slash, so a token
# granted `…/kafkakn-core/` can write the metadata module and nothing else — and a KMP module has one
# coordinate per target. Measured 2026-09-17: the publish refused `kafkakn-core-jvm` with 403.
#
# That run happened to fail on its first PUT and left the server untouched. It is TASK ORDER that
# decided it: publish the metadata module first and the same missing route leaves a version half
# uploaded, which is worse than a refusal because the coordinate then exists and answers, carrying
# one variant of three. This asks every route first and uploads nothing until all of them answer.
#
# The artefact list is READ FROM THE PUBLICATION, never written here: a hand-kept list beside a
# growing set is wrong on the first target somebody adds.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3

HOST=${HOST:-https://reposilite.kotlin.website}
MAVEN_REPO=${MAVEN_REPO:-snapshots}
. ci/lib/coordinate.sh
kafkakn_coordinate || exit 2
REPO=$ROOT/build/local-repo

[ -n "${REPOSILITE_USER:-}" ] && [ -n "${REPOSILITE_SECRET:-}" ] || {
    echo "REPOSILITE_USER and REPOSILITE_SECRET are required" >&2; exit 2; }
[ -d "$REPO/$GROUP_PATH" ] || {
    echo "no local publication under $REPO/$GROUP_PATH - run ci/publish/run.sh first" >&2; exit 2; }

# Credentials out of argv: `curl -u` puts the secret where `ps` can read it for the life of the call.
credentials=$(mktemp)
chmod 600 "$credentials"
trap 'rm -f "$credentials"' EXIT
printf 'user = "%s:%s"\n' "$REPOSILITE_USER" "$REPOSILITE_SECRET" > "$credentials"

artefacts=$(ls "$REPO/$GROUP_PATH")
[ -n "$artefacts" ] || { echo "the local publication produced no artefacts - nothing to check" >&2; exit 2; }

echo "=== can the token write every coordinate under $GROUP? ==="
refused=0
for artefact in $artefacts; do
    probe="/$MAVEN_REPO/$GROUP_PATH/$artefact/.token-probe"
    code=$(printf 'probe\n' | curl -sS -o /dev/null -w '%{http_code}' \
        --config "$credentials" --upload-file - "$HOST$probe")
    case "$code" in
        2*) printf '  %-28s %s\n' "$artefact" "$code" ;;
        *)  printf '  %-28s %s  REFUSED\n' "$artefact" "$code"; refused=$((refused + 1)) ;;
    esac
    curl -sS -o /dev/null -X DELETE --config "$credentials" "$HOST$probe" 2>/dev/null
done

if [ "$refused" -gt 0 ]; then
    echo >&2
    echo "  $refused of the coordinates above are not writable by this token, and NOTHING was" >&2
    echo "  uploaded. A Reposilite route is a string prefix with a trailing slash, so a route at" >&2
    echo "  one artefact grants nothing at its siblings: the token needs a route per coordinate," >&2
    echo "  or one at /$MAVEN_REPO/$GROUP_PATH/ which covers the project and every target it grows." >&2
    exit 1
fi
echo "  every coordinate is writable - the upload can proceed"
