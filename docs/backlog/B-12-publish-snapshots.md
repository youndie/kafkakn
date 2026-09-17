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
  of `io.github.youndie.kafkakn` with `--refresh-dependencies`.
- **The probe is shown failing** against an empty repository first. Without that, "it resolved" says
  nothing about where it resolved *from* — a warm cache and a stray `~/.m2` both answer the same way.
- **The content filter is in place**, so an outage at that host cannot fail the resolution of
  Kotlin, coroutines or `kafka-clients`.

### Iteration 2 — the group moved before the first token was issued

`io.github.youndie` → **`io.github.youndie.kafkakn`**, and re-measured: the three coordinates
publish and the separate build resolves them under the new group.

The reason came from the credential rather than from the build. Secrets here are issued by a job in
the infrastructure repository that turns coordinates into Reposilite routes, and **a route is a raw
string prefix**: `…/kafkakn-core/` grants nothing under `…/kafkakn-core-jvm/`. Under the account's
group that is three routes now and a fourth for every target ever added — a credential re-issued for
a build-matrix row. Under the project's group it is one directory and one route.
[research §2.11](../research/research-architecture.md).

### Iteration 3 — the first real upload was refused, and it was refused where it was predicted

Run [35237621184](https://github.com/youndie/kafkakn/actions/runs/35237621184), with the token
issued: `403 Forbidden` on the first PUT of **`kafkakn-core-jvm`**. The token's route reaches
`…/kafkakn/kafkakn-core/` and no sibling directory, which is what
[research §2.11](../research/research-architecture.md) says a route is.

**Nothing landed** — all three `maven-metadata.xml` answer 404 — so the version is not half
published. That was task order rather than design, and the difference matters: publish the metadata
module first and the same missing route leaves a coordinate that exists, answers, and carries one
variant of three. `ci/publish/preflight.sh` now asks **every** coordinate before anything is
uploaded, reading the list from the local publication rather than from a list kept by hand beside a
growing set of targets.

**What the token needs**, either way:

- a route per coordinate — three today, a fourth the day `linuxArm64` is added; or
- one route at `/snapshots/io/github/youndie/kafkakn/`, which covers the project and every target it
  grows. `k8s/reposilite/token.sh` supports this as `--path`; the workflow that calls it exposes
  only `coordinates` and `plugin-ids`, so it is an input away.

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
