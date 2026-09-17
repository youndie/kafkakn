---
id: B-20
title: "RQ-C: does the artefact resolve and link on a machine that has never seen this repository?"
status: open
priority: P1
size: S
stage: stage-4-a-real-user
---

# B-20 — RQ-C: does the artefact resolve and link on a machine that has never seen this repository?

`ci/downstream` proves the artefact resolves and links — **in CI, on a runner this project
configured, against a cache this project purges**. RQ-C is the same claim from a laptop that has
never seen the repository, because that is how a stranger will try it, and the difference between
those two is every step the README does not name.

- **The decision and its reason.** The measurement is **wall-clock from `git clone` to a record on
  the topic**, on a fresh Linux box, following only what the README says. A time is used rather than
  a yes/no because "it works, eventually" is how a README gets to keep a missing step: ten minutes
  is short enough that a forgotten prerequisite shows up as a failure rather than as patience.
  The clock is **split at the toolchain download** rather than started after it — a stranger waits
  for that too — so the verdict can say which side of the split failed.
- The rejected alternative is trusting the CI run. CI has a JDK, a warm toolchain and a Docker
  daemon because a workflow put them there; a stranger has whatever they have.
- Not covered: macOS and Windows — out of scope
  ([README](../../README.md)), and a Mac contributor cannot run the native arm locally by design.

- AC: fresh Linux box, Gradle, **the README's lines and nothing else**, a binary that produces one
  record, inside **10 minutes** from `git clone`.
- AC: **two numbers in the log, not one** — the wall clock up to the end of the Kotlin/Native
  toolchain download, and the wall clock after it. The ten minutes still cover everything, because
  that is what the stranger experiences; but most of a first run is `~/.konan` arriving, and that is
  **Kotlin/Native's price, not kafkakn's**. Recording both means a red caused only by the download
  is read as what it is instead of being argued about afterwards.
- AC: **red** is anything that needed a step the README does not name; the missing step becomes
  README text **before** [B-21](B-21-does-anyone-want-this.md) starts.
- AC: red twice is a kill criterion for the stage — the artefact is not shippable and the first
  impression is not spent on it.
- Anchors: `README.md`, `ci/downstream/`.
