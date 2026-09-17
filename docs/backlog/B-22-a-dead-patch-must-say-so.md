---
id: B-22
title: "A bump that makes the patch dead must say so, not merely fail"
status: done
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
- Anchors: `ci/librdkafka/apply-patches.sh`, `ci/librdkafka/inside.sh`, `ci/librdkafka/patches/`,
  `ci/b-22/run.sh`, `renovate.json`.

## What happened

**Three refusals, not two, and each names the action.** The patch application moved out of
`inside.sh` into `ci/librdkafka/apply-patches.sh`, which asks the source a question before it changes
anything: *obsolete* (upstream guards the include itself — delete this file and the sentences that
carry it), *no longer applies* (the code moved — rewrite it, after checking the problem is still
there), and a third that was not in this item: *target missing* (`rdrand.c` is gone — find where the
code went, which is neither of the other two). All three fail the build; the point was never to
continue.

**The first version of that script was wrong in the most expensive direction, and running it is what
showed that.** Its probe was `patch --dry-run --reverse`, which reads as "is this already in the
source?" and is not. Given `-R` against a source the patch is *not* applied to, GNU patch 2.7.6
prints `Unreversed patch detected!  Ignoring -R.`, applies it forward and exits 0 — so the script
reported **PATCH OBSOLETE against a pristine librdkafka 2.13.0**, which is the one answer whose cost
is asymmetric: a live patch deleted on a bump.

Measured, and the table is in [research §2.16](../research/research-architecture.md):

| | unpatched | already patched |
|---|---|---|
| `patch -R --dry-run` | **exit 0** | exit 0 |
| `patch -R --dry-run --forward` | exit 1 | exit 0 |
| `patch --dry-run` | exit 0 | **exit 0** |
| `patch --dry-run --forward` | exit 0 | exit 1 |

Without `--forward` neither probe can say no — both rows are `0 0`. With it, each answers exactly one
question.

**Every answer watched happening.** `ci/b-22/run.sh` holds the script against four fixtures — the
include block of 2.13.0's `rdrand.c` pristine, already guarded, rearranged, and absent — and requires
four different answers plus evidence that the green case actually changed the file. It also asserts
the three refusals are three *distinct* sentences, because a script that printed one refusal for
everything would satisfy every exit-code assertion above it.

**And through the real path**, not only the fixtures: the bundle was rebuilt from scratch in the
`manylinux2014` container with the archive removed from the cache, and the patch went on through the
new script — `Hunk #1 succeeded at 32 with fuzz 1 (offset 1 line)`. That output is no longer
swallowed: **the patch applies to 2.13.0 only with fuzz**, which is how a patch says its context has
begun to drift long before it becomes one of the three refusals.
