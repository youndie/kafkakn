---
id: B-45
title: "Three places in the tree still say nothing is built, and one of them is code"
status: wip
priority: P1
size: S
stage: stage-10-housekeeping
---

# B-45 — three places in the tree still say nothing is built, and one of them is code

Three places in the tree still describe the repository as it was before its first item. The
state line in `CLAUDE.md` was corrected for the same fault; these three were missed:

- `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt` holds
  `internal object UnimplementedProducer`: every method is `TODO("no producer yet")`, and nothing
  references it. It is internal, so no caller can reach it. It is still the first implementation a
  reader of the common source set meets.
- `kafkakn-core/build.gradle.kts`, line 1: *"Nothing is implemented; this declares the shape."*
- `docs/features/feature-produce-a-record.md`, §1: *"`status: draft`: nothing here is built. Every
  scenario below is target."* The same file's own scenarios then carry ten `**Automated:**` lines.

- **The decision and its reason.** Delete the stub and correct both sentences. A sentence that
  outlives its truth is read as a fact by the next person, and here the next person is usually an
  agent starting a session.
- A guard against the family, if one is cheap: a `make check` line that fails on `TODO(` in `commonMain`,
  or on "nothing is built" in a document whose scenarios carry `**Automated:**`. Only if it does not
  flag the backlog's own history.
- Not covered: rewriting the feature document's scenarios. That is [B-46](B-46-feature-documents-for-what-was-built-after-the-producer.md).

- AC: `grep -rn "no producer yet\|Nothing is implemented\|nothing here is built"` over `kafkakn-core`,
  `docs/features` and `README.md` finds nothing.
- AC: `ktlintCheck` and `make check` pass, and both arms' suites still compile.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt`,
  `kafkakn-core/build.gradle.kts`, `docs/features/feature-produce-a-record.md`.
