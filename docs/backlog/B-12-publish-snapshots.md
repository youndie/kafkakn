---
id: B-12
title: "Publish snapshots to reposilite"
status: open
priority: P1
size: S
stage: stage-3-usable-by-others
blocked_by: [B-09]
---

# B-12 — Publish snapshots to reposilite

`io.github.youndie:kafkakn-core` as snapshots on `reposilite.kotlin.website/snapshots`, so that
[B-13](B-13-external-consumer-acceptance.md) can consume the artefact rather than the sources.

- **The decision and its reason.** Snapshots only, no Maven Central, no release
  ([D7](../research/research-architecture.md)). A published coordinate is a promise, and nobody has
  decided to make one.
- The rejected alternative is `mavenLocal` for the consumer test. It hides exactly the defects
  publication introduces — a missing variant, a wrong `group`, a dependency that resolves only
  because it is already in the local cache.
- Not covered: signing, a release line, and any version scheme beyond the snapshot one.

- AC: both variants publish — the metadata module **and** `kafkakn-core-linuxx64` **and**
  `kafkakn-core-jvm`. A KMP module has as many coordinates as it has targets, and a publication
  route that covers one does not cover the others.
- AC: the repository is declared with a **content filter**, so an outage at that host cannot fail
  the resolution of anything else.
- AC: a resolution from a clean cache is shown working, not a resolution that happened to hit a
  warm one.
- AC: the version is the snapshot line and the log names what was actually published, coordinate by
  coordinate.
- Anchors: `build.gradle.kts`, `settings.gradle.kts`, `ci/publish/`.
