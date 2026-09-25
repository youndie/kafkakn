---
id: B-49
title: "Read back committed offsets and the current position"
status: wip
priority: P2
size: S
stage: stage-11-everyday-gaps
blocked_by: [B-48]
---

# B-49 — read back committed offsets and the current position

A consumer cannot ask where it is or what its group has committed. Both clients answer:
`Consumer.position(tp)` and `committed(Set<TopicPartition>)` on the JVM; `rd_kafka_position` and
`rd_kafka_committed` in librdkafka 2.13.0's `rdkafka.h`. Without them, a caller who wants to report
progress, or check a commit, reads it from outside with `kafka-consumer-groups`.

- **The decision and its reason.** `position(partition): Long` and
  `committed(partitions): Map<TopicPartition, Long?>`. Null means nothing is committed; −1 means the
  same thing inside both clients, and a caller could read −1 as an offset.
- The trap to test for: `rdkafka.h` says `rd_kafka_position` gives *"the offset of the last consumed
  message + 1, or RD_KAFKA_OFFSET_INVALID in case there was no previous message"*. So before the first
  record, native has no position. The Java client's `position` answers anyway, from a remote call if
  it must (its documentation, to check). A position before the first record is where the arms differ,
  and the contract says how, rather than hiding it under a tolerance.

- AC: after a commit, `committed` on each arm equals what `kafka-consumer-groups --describe` shows.
- AC: after `poll` returns a batch, `position` on each arm is the offset after its last record, and the
  two arms agree for the same records.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.kt`,
  `docs/api/consumer-contract.md`.
