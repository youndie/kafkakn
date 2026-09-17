---
id: B-01
title: "Gradle skeleton: jvm and linuxX64 targets, pinned catalogue"
status: done
priority: P0
size: S
stage: stage-0-it-builds
---

# B-01 — Gradle skeleton: jvm and linuxX64 targets, pinned catalogue

Nothing can be red for the right reason until something compiles. One module, `kafkakn-core`, two
targets, and a version catalogue that pins exact versions rather than ranges.

- **The decision and its reason.** `jvm` and `linuxX64` from the first commit, not `linuxX64` alone
  with the JVM "later". The JVM arm is the oracle ([research §1.1](../research/research-architecture.md)),
  and an oracle added after the implementation is an oracle shaped by it.
- The rejected alternative is a native-only skeleton, which is what the prior art has and why it
  cannot check itself.
- Not covered: `linuxArm64` ([D6](../research/research-architecture.md)), publication
  ([B-12](B-12-publish-snapshots.md)), and any producer code.

- AC: `./gradlew build` succeeds with both targets declared and no source beyond an empty package.
- AC: `gradle/libs.versions.toml` pins Kotlin 2.4.20, coroutines 1.11.0, `kafka-clients` 4.3.1,
  librdkafka 2.13.0 and the broker image tag as exact versions, and the run log shows what actually
  resolved rather than what was requested.
- AC: `commonTest` exists and runs on **both** targets — an empty suite that executes twice, so the
  harness is proved before it has anything to prove.
- Anchors: `settings.gradle.kts`, `build.gradle.kts`, `gradle/libs.versions.toml`,
  `kafkakn-core/build.gradle.kts`.

---

## Findings — 2026-09-17

**Done.** Both targets build, and the common suite is proved to run — and to be able to fail — on
each arm separately.

| | |
|---|---|
| `./gradlew build` | green, 12 tasks, `jvm` and `linuxX64` |
| result files | `jvmTest` 1 test 0 failures, `linuxX64Test` 1 test 0 failures — read from the XML, with timestamps, not from `BUILD SUCCESSFUL` |
| resolved | Kotlin **2.4.20** off the dependency tree, matching the catalogue |

### The one test that exists is the harness proving itself

`HarnessTest` asserts `2 + 2 == 4`, which is worth nothing as an assertion and everything as a
check on the mechanism: the project's whole argument is that `commonTest` runs on **both** arms
(research §1.1), and that claim is cheapest to verify now, while there is no implementation to
confuse it with.

It was then **mutated** — the assertion made false — and both arms reported the failure
independently (`jvmTest failures=1`, `linuxX64Test failures=1`). Without that, "both green" and
"neither ran" are the same observation. Restored, and both are green again.

### Two things settled while writing the build

- **`jvmToolchain(21)` with the foojay resolver**, rather than whatever JDK is on the machine. The
  two arms have to compile the same way on a laptop and on the build box, and the box carries JDK 25
  — a target that Kotlin 2.4.20 does not offer.
- **"No Maven Central" means publication, not resolution.** The settings file says so in a comment,
  because the two readings are easy to confuse and the confusing one breaks the build: Kotlin,
  coroutines and `kafka-clients` live nowhere else. Publication goes to reposilite and nowhere else
  ([D7](../research/research-architecture.md)), which is [B-12](B-12-publish-snapshots.md).

### Not covered

No `commonMain` source at all — the surface is [B-02](B-02-expect-surface.md). `linuxArm64` is not
declared ([D6](../research/research-architecture.md)); nothing in the build names a target outside
the two `kotlin {}` lines, so adding it stays a line plus a matrix row. No publication, no cinterop,
no broker.
