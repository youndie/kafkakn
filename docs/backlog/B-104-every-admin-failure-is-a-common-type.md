---
id: B-104
title: "An admin failure kafkakn does not name is still a platform type: KafkaAdminException on native, the Java client's on the JVM"
status: done
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

## Findings

- **Done, 2026-09-29.** `KafkaAdminException` is common now, open, with a cause. The named failures keep their types
  (`TopicExistsException`, `UnknownTopicException`, `GroupNotEmptyException` as `IllegalStateException`s, a refused
  argument as `IllegalArgumentException`): the contract promises those supertypes, and making them subclasses of a
  new base would change what a caller's existing `catch` catches.
- **On the JVM, one place.** Every admin call already awaited its future through `answer()`. The call-specific
  mappings that used to be caught around it (B-60, B-61, B-62, B-63, B-87) are passed into it instead, and any other
  `KafkaException` is wrapped there. `describeConsumerGroups`, which awaits its futures itself for B-58, wraps the same
  way. Native needed nothing but the move: every failure it did not name was already `KafkaAdminException`.
- **Measured** (`AdminUnnamedFailureTest`, a broker that is not there, each arm's short admin timeout):
  `describeCluster` and `describeTopics` throw `KafkaAdminException` on both arms. The JVM's cause is the Java client's
  `TimeoutException` (*"Timed out waiting for a node assignment. Call: listNodes"*); native's message is librdkafka's
  (*"Failed while waiting for controller: Local: Timed out (-185)"*). Red first on the JVM, with the bare
  `TimeoutException`.
- **Mutant**, `answer()` ignoring the call's own mapping and wrapping everything: five tests failed by name, one per
  named mapping (`AdminConfigsTest`, `AdminDeleteRecordsTest`, `AdminGroupOffsetsTest`, two in `AdminPartitionsTest`).
  All nine admin test classes green on both arms without it.
- **Not measured: an authorization refusal.** The fixture broker has no authorizer, and ACLs are out of scope by
  decision (`backlog.md`). It reaches the same `catch` as the timeout on the JVM, and the same generic result path on
  native; that is read in the code, not observed.
