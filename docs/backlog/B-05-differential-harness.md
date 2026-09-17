---
id: B-05
title: "The differential harness: one suite, both actuals, one broker"
status: wip
priority: P0
size: M
stage: stage-0-it-builds
blocked_by: [B-02, B-04]
---

# B-05 — The differential harness: one suite, both actuals, one broker

The mechanism the whole project rests on: `commonTest` runs on `jvm` and on `linuxX64`, both against
the same broker, and a disagreement between them fails the build.

- **The decision and its reason.** This is built in M0, before either producer works
  ([research §1.1](../research/research-architecture.md)). A harness written after the
  implementation is written to fit it — and the prior art's whole problem is that it had no second
  implementation to disagree with.
- The rejected alternative is per-platform test sources with a shared naming convention. It compiles
  and it drifts: the two suites diverge silently and nothing ever compares their results.
- Not covered: running the two arms in one process (they cannot be), and comparing timings — only
  observable behaviour is compared.

- AC: `./gradlew jvmTest linuxX64Test` runs the **same** `commonTest` sources against the broker on
  both, and both report to files the harness reads by timestamp rather than by scraping stdout.
- AC: **H1 is settled in writing** — either every producer assertion can be expressed in
  `commonTest`, or the ones that cannot are listed with the reason, in
  [research §3](../research/research-architecture.md).
- AC (vacuity guard): a deliberately wrong expectation is shown failing **on each arm separately**,
  so "both green" is known not to mean "neither ran". A suite that cannot find its subject scores as
  a pass otherwise.
- AC: a test that asserts the two arms agree — same input, same partition, same offsets — exists and
  is shown failing when one arm is stubbed to differ.
- Anchors: `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/`,
  `ci/harness/`, `kafkakn-core/build.gradle.kts`.
