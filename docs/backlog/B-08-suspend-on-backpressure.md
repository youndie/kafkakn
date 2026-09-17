---
id: B-08
title: "Suspend on backpressure instead of failing"
status: done
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

---

## Findings — 2026-09-17

**Done.** Both arms produce past their queue bound and lose nothing, checked against the broker.

| | |
|---|---|
| suite | `jvmTest` 14, `linuxX64Test` 16, 0 failures |
| past the bound | jvm **3000/3000**, native **3000/3000**, counted by an independent consumer |
| after `flush` | 500/500 on both |
| the arms | still agree on all 14 observations |

### `send` now parks before it enqueues, and suspends while the queue is full

The registry holds a `CompletableDeferred` rather than a raw continuation, so the caller can be
**parked before the record is offered** — a registry filled afterwards races with the very callback
it is for. Then `enqueue` suspends on `QUEUE_FULL` with `rd_kafka_poll` plus `delay`: the poll is
what drains the queue, the delay is what makes it a suspension rather than a spin, and neither holds
a thread.

Cancelling while suspended there means the record was **never queued**, which is the one moment at
which cancellation is clean. Cancelling after it is queued does not recall it, and the contract does
not pretend otherwise.

### A serial `send` cannot experience backpressure

The first version of this test sent 3 000 records one at a time and the vacuity guard failed it:
`backpressureWaitCount()` was **zero**, so the queue bound had never been reached and the test proved
nothing. `send` awaits the acknowledgement, so a caller awaiting each record has exactly one record
in flight and cannot fill a queue of any size.

The sends are concurrent now. Two consequences, both recorded in
[research §2.5](../research/research-architecture.md): the test is only meaningful that way, and a
caller who wants throughput has to supply the concurrency — a real property of a `send` that waits,
and the argument somebody will make for a batching entry point later.

Without the guard this test would have passed while exercising nothing, which is precisely the
defect the project exists to prevent, in the test for that defect.

### The two clients name the bound differently, and `runTest` would have hidden the wait

librdkafka counts records (`queue.buffering.max.messages`); the Java client counts bytes
(`buffer.memory`) and waits (`max.block.ms`). There is no common spelling, so the suite lowers the
queue through a per-arm helper — a common test with a platform-specific fixture, rather than a common
key that quietly means something different on each side
([research §2.6](../research/research-architecture.md)).

Separately, the tests run on `Dispatchers.Default` rather than in `runTest`'s scheduler: virtual time
skips `delay`, which is exactly the wait under test.

### H4 is settled as **not measured**, deliberately

The blocking shape was replaced rather than kept, so there is no second arm to compare against, and
restoring one would mean maintaining two implementations of the central path to produce a number
this stand cannot support — both suites take about two minutes on a shared machine whose timings
move by more than the difference being asked about. [Research §2.4](../research/research-architecture.md)
records what it would take if the question becomes live.

### Not covered

The scenario *"a full queue suspends rather than failing"* is covered only in its consequence — no
record is lost — rather than by observing a call that has not returned; that assertion is timing-
dependent and was not worth the flakiness. *"Cancelling a suspended send leaves nothing queued"* is
implemented and reasoned about above but **not** asserted by a test, for the same reason. Both are
named here rather than ticked.
