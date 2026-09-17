---
id: B-01
title: "Gradle skeleton: jvm and linuxX64 targets, pinned catalogue"
status: open
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
