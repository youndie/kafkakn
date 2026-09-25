---
id: B-64
title: "After an eviction, the native poll throws where the JVM's rejoins"
status: open
priority: P1
size: S
stage: stage-11-everyday-gaps
---

# B-64 — after an eviction, the native poll throws where the JVM's rejoins

Found by [B-54](B-54-a-flow-over-poll.md), measured. A member that stops polling for longer than
`max.poll.interval.ms` is removed from its group. Both arms report the partitions as lost to the rebalance
listener (`onLost`). They then part at the next `poll`:
- **JVM:** it rejoins silently. The listener sees `onAssigned` again, and `poll` returns records. With
  nothing committed, it starts again from `auto.offset.reset`.
- **Native:** the next `poll` throws `KafkaConsumeException: poll: Local: Maximum application poll interval
  (max.poll.interval.ms) exceeded`. librdkafka hands the event over as a message carrying
  `RD_KAFKA_RESP_ERR__MAX_POLL_EXCEEDED`, and `read()` treats every message error except end-of-partition as
  fatal.

By the project's rule, the native arm is correct when it agrees with the reference implementation, so this
is the native arm's defect. The event is informational: the listener's `onLost` has already told the caller,
and librdkafka rejoins when the application polls again.

- **The decision and its reason.** Treat `__MAX_POLL_EXCEEDED` as an event, not a failure: skip it, as
  `__PARTITION_EOF` is skipped, and let the next poll rejoin. The caller learns of the eviction the way the
  Java client tells it, through `onLost`.
- The rejected alternative is making the JVM throw too. That would move the reference to match the arm
  under test, and would break a caller who already relies on the Java client's behaviour.

- AC: `RecordsFlowTest.a_collector_slower_than_max_poll_interval_is_evicted_and_the_caller_is_told` records
  the same outcome on both arms, and asserts it: completed, with `onLost` then `onAssigned`, and the records
  after the eviction delivered again from the committed offset.
- AC: the consumer contract's §2 paragraph on eviction says one behaviour, not two.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.native.kt`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/RecordsFlowTest.kt`.
