---
id: B-12
title: "Publish snapshots to reposilite"
status: question
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

## Iteration 1 — 2026-09-17, and what it waits on

Everything except the upload itself, measured by `ci/publish/run.sh`.

- **Three coordinates publish**, listed file by file: `kafkakn-core` (metadata), `kafkakn-core-jvm`,
  `kafkakn-core-linuxx64` — each with its `.module` and `.pom`, the native one carrying the cinterop
  klib beside its own.
- **A separate build resolves them.** `ci/publish/consumer` is a build of its own, not a module: it
  knows a coordinate and a repository URL and nothing else, declares **no `mavenLocal`**, and
  compiles `jvm`, `linuxX64` and the common metadata against the published module on a cache purged
  of `io.github.youndie` with `--refresh-dependencies`.
- **The probe is shown failing** against an empty repository first. Without that, "it resolved" says
  nothing about where it resolved *from* — a warm cache and a stray `~/.m2` both answer the same way.
- **The content filter is in place**, so an outage at that host cannot fail the resolution of
  Kotlin, coroutines or `kafka-clients`.

### What is not done, and why it is not something this loop can do

**The upload needs credentials this repository does not have.** The portfolio's convention is the
Gradle properties `REPOSILITE_USER` and `REPOSILITE_SECRET`, supplied in CI as
`ORG_GRADLE_PROJECT_*` from repository secrets. `youndie/kafkakn` has **no secrets set**; the values
exist only as secrets on other repositories, and a secret's value cannot be read back from one — by
design. They are not on this machine either.

`.github/workflows/publish.yaml` is written and does everything but the upload: it builds the C
bundle, runs the proof above, and then either uploads or prints a warning saying it did not. That
split is deliberate — a publish workflow only its secret-holder can exercise is a workflow whose
first real run is also its first test.

**What a person has to do:** add `REPOSILITE_USER` and `REPOSILITE_SECRET` to
`youndie/kafkakn` → Settings → Secrets and variables → Actions, then run the `publish` workflow. The
item closes when a run of it uploads and the three coordinates answer over HTTP.

[B-13](B-13-external-consumer-acceptance.md) stays blocked until then: its whole point is consuming
the artefact from the network rather than from a directory.
