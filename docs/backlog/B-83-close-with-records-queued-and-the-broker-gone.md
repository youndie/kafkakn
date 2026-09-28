---
id: B-83
title: "How long close takes with records queued and the broker gone, on both arms"
status: open
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
