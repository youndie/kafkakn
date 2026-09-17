---
id: B-13
title: "Acceptance from outside: a downstream project that uses the published artefact"
status: done
priority: P1
size: M
stage: stage-3-usable-by-others
blocked_by: [B-12, B-15]
---

# B-13 — Acceptance from outside: a downstream project that uses the published artefact

A small service, outside this repository's source set, that depends on the **published** coordinate
and produces to a broker. On both targets.

- **The decision and its reason.** A library with one in-tree caller accumulates API that only its
  own tests exercise, and publication defects are invisible from inside. The first build outside this one to depend on a
  sibling project found four defects in a day that its own suite could not see.
- The rejected alternative is a sample module inside this repository. It shares the build, the
  version catalogue and the source set — three of the things that break on publication.
- Not covered: anything that would make the downstream build a product. It exists to fail.

- AC: the downstream build resolves `io.github.youndie.kafkakn:kafkakn-core` from reposilite with a **cold** Gradle
  cache, builds for `jvm` and `linuxX64`, and produces records an independent reader can see.
- AC: the downstream build's native binary is **linked and run**, not merely compiled — the point is that a
  downstream link with our cinterop archives works.
- AC: whatever it finds is written into this repository as items, not fixed silently in the
  consumer.
- Anchors: `ci/downstream/`, `docs/research/research-architecture.md` (risks).

## Iteration 1 — 2026-09-17: it found what it exists to find, on its first run

`ci/downstream` is a build of its own — not a module, not a source set, no version catalogue in
common, **no `mavenLocal`**. It knows a coordinate and a repository URL, which is what a stranger
knows. `ci/b-13/run.sh` builds it with the group purged from a Gradle home of its own and runs it.

**The jvm half is proven end to end.** Resolved `io.github.youndie.kafkakn:kafkakn-core:0.1.0-SNAPSHOT`
from the network, built an executable, ran it, and an independent reader counted **50 of 50**
records — with the header it sent, `from:<stamp>`, intact.

**The native half does not link**, and that is [B-15](B-15-native-klib-carries-no-c.md): nine
undefined symbols, `rd_kafka_produceva` and `rd_kafka_poll` among them. The published klib carries
the bindings and nothing about the archives they bind to, because the archives are named in **this
project's own build file**, on **this project's own test binaries**, at absolute paths in a cache.
Every item so far was green for that reason and none of them could have noticed.

Per this item's third criterion the finding is an item rather than a fix in the downstream build — a
`linkerOpts` block added here would have made the run green and left the library exactly as
unusable.

**Also fixed here, in the harness rather than the library.** The header check first grepped
`^<stamp>:0$`, which cannot match: `print.headers=true` puts the headers at the **start** of the
line, so the value is never there. It would have failed on a correct producer.

Resumes when B-15 lands: the downstream build is written, the script is written, and the native path is one
`linkReleaseExecutableLinuxX64` away from being a measurement.

## Closed — 2026-09-17, against the network

The same build, the same script, `ci/b-13/run.sh` with its default repository — the server:

| | jvm | linuxX64 |
|---|---|---|
| resolved from reposilite, cache purged of the group | yes | yes |
| built | an executable | a **linked** 9.6 MB binary |
| ran | yes | yes |
| what an independent reader counted | 50/50 | 50/50 |
| the header it sent | `from:<stamp>` | `from:<stamp>` |

The native binary carries no linker configuration of its own, which is the criterion that was
failing: [B-15](B-15-native-klib-carries-no-c.md) put the archives inside the klib.

**What this item bought.** One defect, and it was invisible from every other vantage point in the
project: the library's own suite, the differential oracle, the publication proof and the gate were
all green while the published native artefact could not be linked by anyone. It cost a build of its
own — no shared source set, no shared catalogue, no `mavenLocal` — and that is exactly the price of
noticing.

**The second finding was in the harness**, not the library: the header check first grepped
`^<stamp>:0$`, which cannot match, because `print.headers=true` puts the headers at the start of the
line. It would have failed on a correct producer.

**Not covered, as the item says.** Anything that would make the downstream build a product. It produces 50
records and prints where the last one landed.
