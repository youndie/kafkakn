---
id: B-05
title: "The differential harness: one suite, both actuals, one broker"
status: done
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

---

## Findings — 2026-09-17

**Done.** The mechanism the whole project rests on exists and is shown able to fail — on each arm
separately, and on absence.

| | |
|---|---|
| one suite, both arms | `jvmTest` 6 tests, `linuxX64Test` 7, 0 failures, result files read by timestamp |
| the arms agree | 5 observations each, identical |
| skew the JVM arm | **caught**: `record.topic=t-skewed` against `record.topic=t` |
| skew the native arm | **caught**, the same way round |
| two missing files | **not** agreement — the comparison refuses |

The extra native test is `CinteropLinkTest` from [B-03](B-03-c-bundle-old-glibc.md), which is
platform-seam work and belongs where it is.

### H1 is settled, and the hypothesis asked the wrong question

It assumed one answer. There are three
([research §2.1](../research/research-architecture.md)): assertions whose truth is the **broker's**
go in `commonTest` unchanged and agreement follows from both arms being right; assertions about what
**only the client knows** are expressible in `commonTest` but cannot be compared *across* arms
inside a test, because the two are separate processes — they are recorded and diffed afterwards; and
assertions about the **platform seam** belong in one arm's source set and that is correct rather than
a leak.

So "a test that can only be written in one arm" is a warning sign for two kinds and the normal case
for the third. `CLAUDE.md` now says it that way instead of as a prohibition.

### The comparison is the oracle, so it gets the guards

Two files that do not exist agree perfectly; so do two empty ones. A suite that never ran would
otherwise produce the strongest possible "the arms agree", which is why `compare-arms.sh` fails on a
missing file and on fewer observations than expected, and why this item demonstrates that case
rather than describing it.

### The skew mechanism was wrong first, and the guard caught it

`-Dkafkakn.skewArm=jvm` sets a system property on the **Gradle** process, not on the forked test
JVM, so the skew never reached the test and the comparison cheerfully reported agreement. The script
noticed — it treats "passed while skewed" as a failure — and the mechanism moved to an environment
variable, which both arms' test processes inherit and which is one mechanism instead of two.

That is the point of demonstrating a failure rather than asserting one: the first attempt at making
the oracle fail did not work, and only a check that expected the failure could have said so.

### Not covered

Any producer assertion — there is no producer. The observations recorded today are about the common
surface, and they exist to exercise the path; when
[B-06](B-06-jvm-actual.md) and [B-07](B-07-native-actual.md) land, the partitioner's choice per key
goes through the same path unchanged.
