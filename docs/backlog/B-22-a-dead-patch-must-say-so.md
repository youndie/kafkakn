---
id: B-22
title: "A bump that makes the patch dead must say so, not merely fail"
status: wip
priority: P1
size: S
stage: stage-2-real-use
---

# B-22 — A bump that makes the patch dead must say so, not merely fail

`renovate.json` is in the repository, so **librdkafka bumps arrive on their own**, and every one of
them re-applies the local patch to `rdrand.c` ([B-03](B-03-c-bundle-old-glibc.md)). One of those
bumps will be the one where upstream guards the include itself, and on that day the patch stops
being a fix and becomes dead weight that still has to be carried.

**Half of this is already true and the item is about the other half.** The patch is applied by a
script rather than by hand — `ci/librdkafka/inside.sh` loops over `patches/*.patch` — and
`set -euo pipefail` means a patch that fails to apply stops the build. What the build does **not**
do is say which of the two things happened:

```
patching file src/rdrand.c
Ignoring previously applied (or reversed) patch.
```

…and exit 1. That is the same output for *upstream fixed it, delete the patch* and for *upstream
moved the code, rewrite the patch* — two opposite actions behind one message. The likely reading of
a red bump is "the patch needs work", and the likely outcome is a patch that is re-applied for
years after it stopped doing anything.

- **The decision and its reason.** The bundle build tells the two apart and names the action.
  `patch --dry-run --reverse` answers "is this already in the source?" before the real apply, so
  *already applied* prints that the patch is obsolete and which file to delete, while *does not
  apply* prints that it needs rewriting against the new source. Both still fail the build — the
  point is not to continue, it is to be actionable.
- The rejected alternative is `patch --forward` continuing quietly when the patch is already
  applied. That is exactly the silent path this item exists to close: the bundle would build, the
  bump would merge, and nothing would ever say the patch is dead.
- Not covered: removing the patch when that day comes, and anything about reporting it upstream —
  nothing from this project goes upstream ([B-03](B-03-c-bundle-old-glibc.md)).

- AC: the build fails with a message naming **which** case it is and what to do about it.
- AC: **both** failures are shown happening — one against a source that already has the guard, one
  against a source the patch cannot apply to. A message nobody has watched print is a message that
  is wrong as often as not.
- AC: the check lives in the bundle build, so it runs wherever the bundle is built — including the
  publish workflow's cache miss, which is where a Renovate bump would first hit it.
- Anchors: `ci/librdkafka/inside.sh`, `ci/librdkafka/patches/`, `renovate.json`.
