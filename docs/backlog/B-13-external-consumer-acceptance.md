---
id: B-13
title: "Acceptance from outside: a consumer project that uses the published artefact"
status: wip
priority: P1
size: M
stage: stage-3-usable-by-others
blocked_by: [B-12]
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
