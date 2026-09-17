---
id: B-08
title: "Suspend on backpressure instead of failing"
status: open
priority: P0
size: M
stage: stage-1-produce
blocked_by: [B-07]
---

# B-08 — Suspend on backpressure instead of failing

`rd_kafka_produce` refuses with `QUEUE_FULL` when the queue is at its bound. That refusal is
backpressure, and the contract says `send` suspends
([research §1.4](../research/research-architecture.md), [D3](../research/research-architecture.md)).

- **The decision and its reason.** The call does not return until the record is accepted. Returning a
  failure the caller may ignore reproduces, in a new place, the defect that lost 264 826 of
  1 000 000 records in the measurement this project starts from.
- The rejected alternative is a blocking retry loop — which is what the spike used and is wrong on a
  coroutine runtime: it holds a thread while the remedy is to let others run.
- Not covered: a configurable timeout on the suspension. Deliberately: a timeout turns backpressure
  back into an ignorable failure, and nobody has asked for one.

- AC: the scenarios of
  [feature-backpressure-and-accounting](../features/feature-backpressure-and-accounting.md) pass on
  both arms.
- AC: the queue bound is lowered in the suite so the case is reachable in a test that finishes; with
  the default of 100 000 a short test never reaches it and the assertion is vacuous.
- AC: the suspension is shown resuming — a test asserts the call has not returned while the queue is
  full, and returns once it drains.
- AC: cancelling a suspended `send` does not leave the record queued.
- AC: **H4 settled in writing** — whether the suspending shape costs throughput against the blocking
  one, measured as a ratio on one host with the spread beside it, or recorded as not measured.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/NativeProducer.kt`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/BackpressureTest.kt`.
