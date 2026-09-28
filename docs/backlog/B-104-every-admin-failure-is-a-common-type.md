---
id: B-104
title: "An admin failure kafkakn does not name is still a platform type: KafkaAdminException on native, the Java client's on the JVM"
status: open
priority: P2
size: S
stage: stage-18-what-the-consumer-and-the-harness-found
blocked_by: []
---

# B-104 — an admin failure kafkakn does not name is still a platform type

Listed by [B-103](B-103-an-unknown-topic-is-two-types.md). The admin client names five of its failures with common
types; the rest reach common code as a type it cannot name. On native, `KafkaAdminException`, public but declared in
`nativeMain` only, from 14 places (an answer that never arrived, an event-level error, unmapped per-topic results,
`deleteRecords` and `listOffsets` without an answer for a partition). On the JVM, the Java client's own `ApiException`
subclasses (`TimeoutException`, `TopicAuthorizationException`, `PolicyViolationException`, ...).

- **The decision and its reason.** Move `KafkaAdminException` to `commonMain`, open, with a cause, and make the named
  types its subclasses where that does not change their supertype today (it would: `TopicExistsException` is an
  `IllegalStateException`, so the item decides between a common base and leaving them as they are). The JVM wraps
  every `ApiException` it does not map in `KafkaAdminException`, the client's exception as the cause, as the producer
  does for its errors.
- AC: a timeout and an authorization refusal reach common code as `KafkaAdminException` on both arms, measured.
- AC: the contract's error table says what an unnamed admin failure is.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.native.kt`,
  `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.jvm.kt`.
