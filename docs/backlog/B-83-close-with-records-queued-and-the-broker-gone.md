---
id: B-83
title: "How long close takes with records queued and the broker gone, on both arms"
status: done
priority: P2
size: M
stage: stage-20-what-waited-for-a-caller
epic: feature-backpressure-and-accounting
blocked_by: []
---

# B-83 — how long close takes with records queued and the broker gone, on both arms

[B-76](B-76-enqueue-without-metadata.md) left it open: *"Whether `close` needs a bound is a separate question, for
whoever needs one first."* mostik measured 300 200 ms on native for one record queued against an unreachable
broker (`message.timeout.ms`). Its shutdown did not need a bound, because kore gives the release stage 3 s. A caller
without kore gets whatever `close` does, and the contract says only that `close` flushes.

- **The decision and its reason.** Measure both arms first. The Java client offers `close(Duration)`, and librdkafka
  a flush with a timeout followed by a purge. If the arms can be held to one bound, the item proposes a
  `close(timeout)` that both honour, with what happens to the records still queued, and ends in a `question` for
  the owner before any API is added.
- AC: records queued, the broker stopped, then `close()`. How long each arm takes, and what each queued record's
  `Delivery.await()` answers.
- AC: the contract's `close` section states the measurement.
- Anchors: `docs/api/producer-contract.md`, `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`,
  `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt`.

## Findings (2026-09-28)

*Measured*, `ci/b-83/run.sh` (`CloseWithBrokerGoneTest`) on the Linux box. Five records were queued (with
`metadata.recovery.strategy=none`, so a stopped broker leaves them queued, B-80), the broker was made unreachable,
and `close()` was called. The topic was read with the broker's own reader once the broker was back:

| the broker | JVM `close` | native `close` | each `Delivery.await()` | the topic afterwards |
|---|---|---|---|---|
| paused | 158 330 ms | 300 884 ms | threw, all five (`TimeoutException` / `Local: Message timed out`) | all five written |
| stopped | 117 743 ms | 307 412 ms | threw, all five | none |

- **`close` lasts the client's delivery timeout**: `delivery.timeout.ms` on the JVM (120 s), `message.timeout.ms` on
  native (300 s). Those are two platform keys with two defaults, so one caller's `close` takes 2 minutes or 5
  depending on the arm.
- **Found on the way, and corrected in the contract in this change: a record whose `await()` threw may have been
  written.** With the broker paused, the requests were in flight. Both clients timed them out, and the broker wrote
  all five when it answered again. The contract's row *"`await()` threw: queued, and failed"* said otherwise. It now
  says: not written if never sent, possibly written if in flight. mostik answers from this row, so this is worth
  telling it.
- **What each client can do about the length, read in its source.** `KafkaProducer.close(Duration)`
  (`kafka-clients-4.3.1-sources.jar!/org/apache/kafka/clients/producer/KafkaProducer.java`, line 1386): once the
  timeout passes, the sender is force-closed and every incomplete batch fails with *"Producer is closed
  forcefully."* (`RecordAccumulator.java`, `abortIncompleteBatches`, line 1127). librdkafka: `rd_kafka_flush(rk,
  timeout)`, then `rd_kafka_purge(rk, RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT)` (`rdkafka.h`, line 4992).
  Each purged record's report says `_PURGE_QUEUE` (never sent) or `_PURGE_INFLIGHT` (sent, outcome unknown).

## Question for the owner

`close` with the broker gone takes 2 minutes on the JVM and 5 on native, and nothing bounds it but those timeouts.
Which of these?

1. **`close(timeout: Duration)` on both arms** (recommended). On the JVM it is the client's own `close(Duration)`. On
   native it is a flush for the timeout, then a purge of what is left, then destroy. Every record not acknowledged
   in time fails its `Delivery.await()` with one kafkakn type, never silently, and the contract says it may have
   been written when it was in flight. `close()` without an argument stays as it is.
2. **One default instead of an API.** Native defaults `message.timeout.ms` to the JVM's 120 s, as kafkakn already
   defaults its partitioner to the Java client's. `close` then takes the same time on both arms. A caller who wants
   it shorter still sets a platform key per arm.
3. **Leave it.** The contract now states the times, and a service bounds its own shutdown, as kore does for mostik.

## Decision (the owner, 2026-09-28)

**Option 1: `close(timeout)` on both arms.** Done in [B-91](B-91-close-with-a-timeout.md).
