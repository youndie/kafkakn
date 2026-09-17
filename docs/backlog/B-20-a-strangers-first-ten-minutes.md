---
id: B-20
title: "RQ-C: does the artefact resolve and link on a machine that has never seen this repository?"
status: done
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
- Anchors: `README.md`, `ci/b-20/run.sh`. (This item was written naming `ci/downstream/`, which does
  not exist: the rename is [B-17](B-17-consumer-is-the-wrong-word-here.md) and has not happened. The
  measurement does not use that directory at all — it builds the README's own build file — so the
  anchor is the harness instead.)

## What happened

**Red first, in 25 seconds, and the README was the reason.** *Getting it* showed two fragments: a
`repositories { }` block and a top-level `dependencies { implementation(...) }`. A multiplatform
project — the only kind that can link the native artefact — has no `implementation` configuration at
the top level, so the build failed at script compilation with `Unresolved reference 'implementation'`.
`mavenCentral()` was missing too, so nothing else would have resolved. The README now carries a
**whole build file**, and the harness pastes it **verbatim** instead of into a scaffold of its own:
a scaffold written here is exactly the part a stranger does not have, and it would have hidden this
for ever.

**Then green, twice**, against a ten-minute budget:

| | run 1 | run 2 |
|---|---|---|
| total, from an empty machine to a record on the topic | **106 s** | **109 s** |
| the Kotlin/Native toolchain arriving | 74 s | 78 s |
| everything after it | 32 s | 31 s |

About **70% of the wait is Kotlin/Native downloading its own LLVM, sysroot and libffi** — the split
the item asked for, and it earns its keep immediately: a first run that overran would otherwise be
argued about rather than read off the log. The end offsets moved 0 → 1 and 1 → 2, so the record is
the broker's word rather than the binary's.

**Two things the number is not.** The empty machine is a container on the build box, so the CPU and
the network are that box's — a floor, not a universal figure. And the toolchain lands inside the
container rather than in the mounted home, which is what keeps every run cold; a stranger's second
build does not pay the 74 s again.

**The red run is the positive control and nobody had to arrange it.** A check that asks "did it work"
passes equally well when it is not looking; this one was watched failing for a real reason before it
was watched passing.

**Two things the README gained besides the build file**, both of which a stranger would otherwise
have had to discover: the Kotlin version is part of the instructions rather than a detail, because a
klib carries metadata another compiler refuses; and the coroutines runtime arrives with the
dependency, so a caller using `runBlocking` compiles with nothing else added.
