---
id: B-13
title: "Acceptance from outside: a consumer project that uses the published artefact"
status: wip
priority: P1
size: M
stage: stage-3-usable-by-others
blocked_by: [B-12, B-15]
---

# B-13 — Acceptance from outside: a consumer project that uses the published artefact

A small service, outside this repository's source set, that depends on the **published** coordinate
and produces to a broker. On both targets.

- **The decision and its reason.** A library with one in-tree caller accumulates API that only its
  own tests exercise, and publication defects are invisible from inside. The first consumer of a
  sibling project found four defects in a day that its own suite could not see.
- The rejected alternative is a sample module inside this repository. It shares the build, the
  version catalogue and the source set — three of the things that break on publication.
- Not covered: anything that would make the consumer a product. It exists to fail.

- AC: the consumer resolves `io.github.youndie.kafkakn:kafkakn-core` from reposilite with a **cold** Gradle
  cache, builds for `jvm` and `linuxX64`, and produces records an independent reader can see.
- AC: the native consumer's binary is **linked and run**, not merely compiled — the point is that a
  downstream link with our cinterop archives works.
- AC: whatever it finds is written into this repository as items, not fixed silently in the
  consumer.
- Anchors: `ci/consumer/`, `docs/research/research-architecture.md` (risks).

## Iteration 1 — 2026-09-17: it found what it exists to find, on its first run

`ci/consumer` is a build of its own — not a module, not a source set, no version catalogue in
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

Per this item's third criterion the finding is an item rather than a fix in the consumer — a
`linkerOpts` block added here would have made the run green and left the library exactly as
unusable.

**Also fixed here, in the harness rather than the library.** The header check first grepped
`^<stamp>:0$`, which cannot match: `print.headers=true` puts the headers at the **start** of the
line, so the value is never there. It would have failed on a correct producer.

Resumes when B-15 lands: the consumer is written, the script is written, and the native path is one
`linkReleaseExecutableLinuxX64` away from being a measurement.
