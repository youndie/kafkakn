---
id: B-91
title: "close(timeout) on both arms: what is not acknowledged in time fails, and may have been written"
status: wip
priority: P1
size: M
stage: stage-20-what-waited-for-a-caller
epic: feature-backpressure-and-accounting
blocked_by: [B-83]
---

# B-91 — `close(timeout)` on both arms

[B-83](B-83-close-with-records-queued-and-the-broker-gone.md) measured `close()` with the broker gone: 2 minutes on
the JVM and 5 on native, bounded by nothing but each client's delivery timeout. It also measured that a record
timed out while in flight may have been written. The owner chose on 2026-09-28 to bound it with `close(timeout)`.

- **The decision and its reason.** `close(timeout: Duration)`, beside `close()`, which is unchanged. On the JVM it is
  the client's own `close(Duration)`: once the timeout passes, the sender is force-closed and every unacknowledged
  batch fails with *"Producer is closed forcefully."*. On native it is `rd_kafka_flush` for the timeout, then
  `rd_kafka_purge` of the queue and of what is in flight, then destroy. Each purged record's report says
  `_PURGE_QUEUE` or `_PURGE_INFLIGHT`. Every record not acknowledged in time fails its `Delivery.await()` with one
  kafkakn type, `ClosedBeforeAcknowledgedException`. Its contract is that the record was never sent, or was in
  flight and may have been written. The JVM cannot say which, so the type does not either.
- AC: records queued, the broker paused and then stopped, `close(3.seconds)`. `close` returns within the timeout and
  a bounded margin on both arms, and every record's `await()` throws `ClosedBeforeAcknowledgedException`. What the
  topic holds once the broker is back is recorded.
- AC: `close(timeout)` with nothing outstanding returns at once, and records that can be delivered in time land.
  This runs in the suite.
- AC: the contract's `close` section and its errors table name the type and what it means.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt`,
  `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`,
  `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt`.
