---
id: B-103
title: "describeTopics of an unknown topic throws a different type on each arm"
status: done
priority: P2
size: S
stage: stage-18-what-the-consumer-and-the-harness-found
blocked_by: []
---

# B-103 — `describeTopics` of an unknown topic throws a different type on each arm

Found by [B-101](B-101-describe-right-after-create-is-an-unknown-topic.md): its measurement caught
`KafkaAdminException` and did not compile on the JVM, where that type does not exist. For the same broker error,
`UNKNOWN_TOPIC_OR_PARTITION` (3), the JVM arm lets the Java client's `UnknownTopicOrPartitionException` through, and
the native arm throws `KafkaAdminException`, a public class declared in `nativeMain` only. A caller in common code can
catch neither by type.

The contract's error table names one type per case for the admin errors it measured (`TopicExistsException`,
`GroupNotEmptyException`, `IllegalArgumentException`); this case has no row.

- **The decision and its reason, to be made in the item:** which common type an unknown topic is. The candidates:
  a common `UnknownTopicException`, as `TopicExistsException` is common for its case; or `NoSuchElementException`.
  `KafkaAdminException` staying native-only is part of the same question: every public type an arm throws should be
  one common code can name.
- AC: `describeTopics` of a topic that does not exist throws one common type on both arms, measured, and the
  contract's error table has its row.
- AC: which other admin calls can leak `KafkaAdminException` or a Java client exception to common code is listed.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.native.kt`,
  `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.jvm.kt`.

## Findings

- **Done, 2026-09-29. `UnknownTopicException`**, common, an `IllegalStateException` like `TopicExistsException` and
  `GroupNotEmptyException`, with the client's own error as the cause. Chosen over `NoSuchElementException` for that
  reason: the precedent, and a cause the JVM can carry (common `NoSuchElementException` takes none). The topic may
  exist a moment later (B-101), so "illegal state" fits better than "no such element" anyway.
- **Where it is thrown:** on the JVM, by the one `answer()` every admin call goes through, with the call and its
  topics in the message, since the Java client's words (*"This server does not host this topic-partition."*) do not
  name one. On native, by `describeTopics` and by the per-topic results of create, delete and grow.
- **Measured** (`AdminUnknownTopicTest`, both arms): describing and deleting a topic that was never created. Red first
  on both arms, the JVM with `UnknownTopicOrPartitionException`. Mutant, native `describeTopics` without the mapping:
  `AdminUnknownTopicTest.describing_a_topic_that_does_not_exist_is_an_unknown_topic_on_both_arms[linuxX64]` failed.
  The admin suites (`AdminTest`, `AdminPartitionsTest`) green on both arms; B-101's helper now asks the type.
- **What still leaks, listed (the second AC):**
  - **Native, `KafkaAdminException`, public and declared only in `nativeMain`,** from 14 places: an answer that never
    arrived (`no answer on the result queue`), an event-level error, a per-topic result other than the mapped ones,
    `deleteRecords` and `listOffsets` with no answer for a partition, a refused isolation level, and the group and
    config calls' unmapped results.
  - **JVM, any `ApiException` the client throws and kafkakn does not map:** `TimeoutException`,
    `TopicAuthorizationException`, `PolicyViolationException`, `UnsupportedVersionException` among them. Mapped today:
    topic exists, unknown topic, invalid partitions and replica assignment, offset out of range, invalid configuration,
    and the three ways a broker refuses to touch an active group (to `GroupNotEmptyException`). A group id not found is
    not an exception: `describeConsumerGroups` answers it as a DEAD group (B-58).
  - Common code can catch neither family by type. That is [B-104](B-104-every-admin-failure-is-a-common-type.md).
