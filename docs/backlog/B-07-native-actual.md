---
id: B-07
title: "The native actual: produce and delivery reports across the callback seam"
status: wip
priority: P0
size: L
stage: stage-1-produce
blocked_by: [B-03, B-06]
---

# B-07 — The native actual: produce and delivery reports across the callback seam

librdkafka behind the same surface. The load-bearing part is not the produce call; it is the seam
where a C callback arriving on librdkafka's own thread resumes a Kotlin coroutine
([research §1.6](../research/research-architecture.md)).

- **The decision and its reason.** `staticCFunction` for the delivery-report callback, atomics for
  what it touches, and a registry keyed by the message opaque so a report finds its continuation.
  Measured over 1.8M messages with no crash and no deadlock — the mechanism is known to hold; what
  is unknown is our use of it.
- The rejected alternative is polling for completion without a callback. It costs a thread and
  cannot report per-record outcomes, which the contract requires.
- Not covered: backpressure ([B-08](B-08-suspend-on-backpressure.md)) and accounting
  ([B-09](B-09-accounting.md)) — deliberately separate items, because folding them in here is how
  they end up half-done.

- AC: `rd_kafka_produce`, not `rd_kafka_producev` — the latter is variadic and unusable through
  cinterop ([research §1.5](../research/research-architecture.md)).
- AC: the scenarios of [feature-produce-a-record](../features/feature-produce-a-record.md) pass on
  `linuxX64`, **and** the cross-arm scenario passes.
- AC: a deliberately crashing callback is shown crashing the run, so "no crash" is a statement the
  harness could contradict.
- AC: `flush` is implemented as `rd_kafka_outq_len` reaching zero, never as the return of
  `rd_kafka_flush` — which is an error code.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/NativeProducer.kt`,
  `kafkakn-core/src/nativeInterop/cinterop/rdkafka.def`.
