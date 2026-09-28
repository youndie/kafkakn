---
id: B-103
title: "describeTopics of an unknown topic throws a different type on each arm"
status: open
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
