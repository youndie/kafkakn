#!/usr/bin/env bash
# What a binary that links kafkakn needs at run time, against one that does not.
#
# The README used to say "no runtime dependency beyond libc". That is false in the first word that
# matters - `libgcc_s` is not libc - and a reader can refute it with one command. It was going to be
# replaced by the spike's stronger claim, that the `ldd` set is IDENTICAL to a Kafka-free binary
# built the same way; the first run of this script refuted that too, with four libraries the
# baseline does not need. So what the README says now is the list this script prints, and what this
# script does is fail when that list changes.
#
# The subject is `ci/downstream`, not this repository's own test binary: that is what a stranger
# links, and this repository's own binaries were once linked with options no stranger had (B-15).
#
#   ci/b-16/run.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT" || exit 3
. ci/lib/coordinate.sh
kafkakn_coordinate || exit 2
REPO_URL=${REPO_URL:-https://reposilite.kotlin.website/snapshots}

echo "=== environment ==="
date -Is
echo "  $GROUP:kafkakn-core:$VERSION, from $REPO_URL"

echo
echo "=== the two binaries ==="
./gradlew --no-daemon --console=plain \
    -p ci/downstream -Pkafkakn.version="$VERSION" -Pkafkakn.repo="$REPO_URL" \
    linkReleaseExecutableLinuxX64 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "  THE DOWNSTREAM BUILD COULD NOT BUILD"; exit 1; }

./gradlew --no-daemon --console=plain \
    -p ci/b-16/baseline linkReleaseExecutableLinuxX64 2>&1 | tail -3
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "  THE BASELINE COULD NOT BUILD"; exit 1; }

WITH=$(ls ci/downstream/build/bin/linuxX64/releaseExecutable/*.kexe 2>/dev/null | head -1)
WITHOUT=$(ls ci/b-16/baseline/build/bin/linuxX64/releaseExecutable/*.kexe 2>/dev/null | head -1)
[ -n "$WITH" ] && [ -n "$WITHOUT" ] || { echo "  one of the two was not linked" >&2; exit 1; }
printf '  with kafkakn:    %s (%s bytes)\n' "$WITH" "$(stat -c %s "$WITH")"
printf '  without kafkakn: %s (%s bytes)\n' "$WITHOUT" "$(stat -c %s "$WITHOUT")"

# The name of the library, without the path and without the address it happened to be mapped at -
# both differ between machines and neither is the question.
names() { ldd "$1" 2>&1 | sed 's/^[[:space:]]*//; s/ =>.*//; s/ (0x[0-9a-f]*)//' | sort -u; }

echo
echo "=== ldd, with kafkakn ==="
names "$WITH" | sed 's/^/  /'
echo
echo "=== ldd, without ==="
names "$WITHOUT" | sed 's/^/  /'

echo
echo "=== what kafkakn adds ==="
# THE FIRST BASELINE MADE THIS COME OUT WRONG. It was a hello-world, so it differed from the downstream build
# in two things - kafkakn and kotlinx-coroutines - and the comparison reported `libcrypt`,
# `libresolv`, `librt` and `libutil` as kafkakn's. They are the coroutines runtime's. The baseline is
# the downstream build with the library removed for exactly that reason: a difference with two possible
# causes attributes nothing, and it was about to be written into the README as a fact.
comm -13 <(names "$WITHOUT") <(names "$WITH") > /tmp/kafkakn-ldd-added
comm -23 <(names "$WITHOUT") <(names "$WITH") > /tmp/kafkakn-ldd-removed
sed 's/^/  + /' /tmp/kafkakn-ldd-added
sed 's/^/  - /' /tmp/kafkakn-ldd-removed
if [ -s /tmp/kafkakn-ldd-added ] || [ -s /tmp/kafkakn-ldd-removed ]; then
    echo "  THE SETS DIFFER - the README's claim is no longer true" >&2
    exit 1
fi
echo "  nothing, in either direction: the two sets are identical"

echo
echo "=== the oldest glibc each one would run on ==="
# The other half of what somebody shipping a binary needs, and it is a number rather than an
# argument: the highest versioned glibc symbol a binary references is the floor it can run on. If
# kafkakn's C bundle were built against the host's glibc instead of manylinux2014's 2.17, the left
# column would be higher than the right, and this is where it would show.
floor() {
    objdump -T "$1" 2>/dev/null \
        | grep -o 'GLIBC_[0-9.]*' \
        | sort -uV \
        | tail -1
}
printf '  with kafkakn:    %s\n' "$(floor "$WITH")"
printf '  without:         %s\n' "$(floor "$WITHOUT")"
# THE NUMBER THE README PRINTS, checked here so the two cannot drift apart. It is manylinux2014's
# glibc, which is where the C bundle is built (D4) - if the bundle ever gets built somewhere else,
# this is the line that says so, and it says so before a user on an older distribution does.
CLAIMED=GLIBC_2.17
[ "$(floor "$WITH")" = "$CLAIMED" ] || {
    echo "  the README says $CLAIMED and this binary says $(floor "$WITH")" >&2
    exit 1
}
echo "  the README says $CLAIMED, and so does the binary"

echo
echo "=== the check can say no ==="
# A comparison that cannot come out non-empty proves nothing, and this one now expects an empty
# answer - which is exactly the shape that passes when it is broken. Held against a binary that
# plainly needs other libraries it must report them, and it is the SAME `comm`: a self-test that
# exercises a different mechanism says nothing about the one above it.
if [ -z "$(comm -13 <(names "$WITHOUT") <(names /bin/bash))" ]; then
    echo "  the comparison found nothing added to /bin/bash - it is not comparing anything" >&2
    exit 1
fi
echo "  held against /bin/bash, the same comparison reports what it adds, as it must"
