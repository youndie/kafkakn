#!/usr/bin/env bash
# Applies the local patches, and REFUSES IN THREE DIFFERENT WAYS.
#
# A bump arrives on its own - `renovate.json` is in this repository - and every one of them re-applies
# the patch in `patches/`. One of those bumps will be the one where upstream guards the include
# itself, and on that day the patch stops being a fix and becomes weight that is still carried.
#
# `patch --batch --forward` says the same thing on both roads:
#
#     Ignoring previously applied (or reversed) patch.
#
# ...and exits 1. That is one message for "upstream fixed it, delete this" and for "upstream moved
# the code, rewrite this" - two opposite actions. The likely reading of a red bump is "the patch needs
# work", and the likely outcome is a patch re-applied for years after it stopped doing anything.
#
# So the source is asked first, and the answer decides which sentence is printed. Both still fail:
# the point is not to continue, it is to be actionable.
#
# **`--forward` IS LOAD-BEARING ON THE REVERSE PROBE, and the first version of this script left it
# out.** Given `-R` against a source the patch is NOT applied to, GNU patch 2.7.6 prints
# `Unreversed patch detected!  Ignoring -R.` and applies it forward, exiting 0 - so the probe
# reported "already applied" for a pristine source, which is the one answer that gets a live patch
# deleted. Measured on librdkafka 2.13.0's own `src/rdrand.c`, patched and unpatched:
#
#                                   unpatched   already patched
#     patch -R --dry-run              exit 0        exit 0      <- tells the two apart not at all
#     patch -R --dry-run --forward    exit 1        exit 0      <- this is the question
#     patch    --dry-run              exit 0        exit 0      <- auto-reverses, silently
#     patch    --dry-run --forward    exit 0        exit 1
#
#   apply-patches.sh <source dir> <patches dir>
set -euo pipefail

SRC=${1:?source directory}
PATCHES=${2:?patch directory}
cd "$SRC"

shopt -s nullglob
found=0
for p in "$PATCHES"/*.patch; do
    found=1
    name=$(basename "$p")

    # BEFORE either probe. A file that is not there fails both of them, and "neither applies nor
    # reverses" would send a reader looking for moved code when what happened is that the file was
    # renamed or deleted.
    missing=$(sed -n 's|^+++ b/||p' "$p" | while read -r f; do [ -e "$f" ] || echo "$f"; done)
    if [ -n "$missing" ]; then
        cat >&2 <<MSG

    PATCH TARGET MISSING: $name

    The file it edits is not in this source:
$(echo "$missing" | sed 's/^/      /')
    Upstream renamed or removed it. Find where the code went before deciding whether the patch
    still has a job - this is not the same as its context having moved.
MSG
        exit 1
    fi

    # Already in the source? Then the reverse applies, and `--forward` is what stops patch from
    # quietly deciding to apply it the other way instead (see the table above).
    if patch -p1 --batch --dry-run --reverse --forward < "$p" >/dev/null 2>&1; then
        cat >&2 <<MSG

    PATCH OBSOLETE: $name

    It reverses cleanly against this source, which means upstream now does what it does. The
    action is to DELETE it - ci/librdkafka/patches/$name - and the sentences that carry it:
    docs/research/research-architecture.md §1.3 and docs/backlog/B-03-c-bundle-old-glibc.md.
    Do not rewrite it. Nothing is reported upstream from this project, so this check is the only
    thing that would ever say the patch had done its job.
MSG
        exit 1
    fi

    # Not already there, and will not go in: the code around it has moved.
    if ! patch -p1 --batch --dry-run --forward < "$p" >/dev/null 2>&1; then
        cat >&2 <<MSG

    PATCH NO LONGER APPLIES: $name

    It neither applies nor reverses, so the code around it has moved. The action is to REWRITE it
    against the new source - after checking that the problem it solves is still there, because a
    patch rewritten out of habit is the thing this check exists to prevent. What it is about:
    <sys/random.h> included under '#ifndef _WIN32' while the getentropy() call is guarded by
    HAVE_GETENTROPY, and the header arrived in glibc 2.25.

    What patch itself said:
MSG
        patch -p1 --batch --dry-run --forward < "$p" 2>&1 | sed 's/^/      /' >&2 || true
        exit 1
    fi

    echo "    applying $name"
    # The output is kept rather than silenced: "succeeded with fuzz 1" is how a patch says its
    # context has already drifted, which is a warning long before it becomes one of the failures
    # above. Today's patch applies to librdkafka 2.13.0 with fuzz 1.
    patch -p1 --batch --forward < "$p" | sed "s/^/    $name: /"
done

[ "$found" -eq 1 ] || { echo "    no patches in $PATCHES - is the directory mounted?" >&2; exit 1; }
