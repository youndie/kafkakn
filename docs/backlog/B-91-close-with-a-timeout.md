---
id: B-91
title: "close(timeout) on both arms: what is not acknowledged in time fails, and may have been written"
status: done
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

## Findings (2026-09-28)

*Measured*, `ci/b-91/run.sh` on the Linux box. Five records queued, then `close(3 s)`:

| the broker | JVM | native | answers | the topic afterwards |
|---|---|---|---|---|
| paused | 3 010 ms | 3 002 ms | 5 × `ClosedBeforeAcknowledgedException` | `warm t-0 … t-4`: in flight, written |
| stopped | 3 008 ms | 3 002 ms | 5 × `ClosedBeforeAcknowledgedException` | `warm` |

Before: 158 and 301 s paused, 118 and 307 s stopped (B-83). With the broker answering, `close(10 s)` with nothing
outstanding returned at once, and 20 queued records all landed, on both arms. The arms agree on every observation.

- The JVM recognises its force close by exact class and text, `KafkaException` *"Producer is closed forcefully."*
  (`RecordAccumulator.abortIncompleteBatches`, kafka-clients 4.3.1, line 1146), so that another failure arriving
  during the close keeps its own type.
- Mutants, both killed by `CloseWithBrokerGoneTest.close_with_a_timeout_gives_up_on_what_is_not_acknowledged`: native
  reporting a purged record as `KafkaProduceException`, and the JVM not recognising its force close.
