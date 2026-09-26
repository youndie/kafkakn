---
id: B-74
title: "A caller whose wait was cut can tell 'never queued' from 'queued, outcome unknown'"
status: done
priority: P0
size: M
stage: stage-16-a-deadline-on-send
epic: feature-backpressure-and-accounting
blocked_by: [B-73]
---

# B-74 — a caller whose wait was cut can tell "never queued" from "queued, outcome unknown"

A caller that bounds `send` with a deadline gets the same `CancellationException` in two very different
situations:

- the record was never queued. Nothing was written, and trying again is safe;
- the record was queued and its fate is unknown. It may land, and trying again may write it twice.

[B-73](B-73-a-cancelled-send.md) writes down and measures that both exist. This item lets the caller see
which one happened. The HTTP bridge that raised the question answers `429` for the first and
`504 outcome-unknown` for the second. It cannot choose between them today, so every expired deadline has to
be `504`, including the ones where the answer "not written" was available and true.

The same split also matters without a deadline. A native wait for room ends at a fixed
`BACKPRESSURE_LIMIT_MS = 120_000` (`KafkaProducer.native.kt:802`) and throws `KafkaProduceException`. The
JVM wait ends at `max.block.ms` and throws `TimeoutException`. Both mean "never queued", but they are
different types after different times, and neither says it in a way a caller can match on.

- **The decision and its reason.** The caller learns *whether the record was queued* from the type of what
  the call returns or throws, the same on both arms. It does not learn it from a message, and not from
  cancellation itself. This is the contract's own rule: error **type** is contract, text is not.
- The shape is decided here, first, as a contract change, before any code. Candidates:
  - a `send` that takes a deadline and throws a distinct type for "not queued within it";
  - a two-step call: queue, then await the acknowledgement, where the caller bounds each step
    separately. This is `kafka-clients`' own shape, `send` returning a `Future`.
  - Rejected in advance: a subclass of `CancellationException` that carries the answer. Coroutine
    machinery may replace a cancellation cause on its way up, and a caller cannot rely on a type it did
    not throw.
- **The JVM arm is the hard half.** `kafka-clients` blocks inside `send` while it waits for room or for
  metadata, so "never queued within the deadline" may only be reachable by bounding `max.block.ms` from the
  caller's deadline. If B-73 shows it cannot be made to hold on one arm, the contract says so in its
  table of differences, and the item records what a caller gets there. It does not promise the difference
  away.
- Not covered: whether a record whose `send` **threw** after being queued (a local message timeout) was
  persisted. librdkafka has `rd_kafka_message_status` for that, and the Java client has nothing equivalent.
  It is a separate question, to be filed if a caller needs it.

- AC: the chosen shape is in `producer-contract.md`, with the rejected candidates and the reason.
- AC: on both arms, a caller whose deadline passes while the queue is full gets the "not queued" answer,
  and the record is not in the topic after the queue drains. A caller whose deadline passes after the
  record is queued gets the "unknown" answer, and B-73's measurement says what that means.
- AC: the fixed native 120 s limit and the JVM `max.block.ms` expiry give the same "not queued" type, or the
  contract names the difference.
- Anchors: `docs/api/producer-contract.md`,
  `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt`, both actuals,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/`.

## Findings (2026-09-27)

- **The shape: the two-step one, `kafka-clients`' own.** `enqueue(record): Delivery` returns once the
  record is queued. `Delivery.await()` returns once the broker acknowledged it. `send` is exactly the two
  in a row. The contract states the rejected candidates and why:
  - a `send` with a per-call deadline cannot be honoured on the JVM, where `max.block.ms` is fixed per
    producer;
  - a `CancellationException` subclass is a type the caller did not throw and cannot rely on.
- **AC: "not queued" when the deadline passes with the queue full, and not in the topic; "unknown" once
  queued, and B-73 says what that means.** `EnqueueTest`, with the broker paused (`ci/b-74/run.sh`):
  - with `max.block.ms` 2 000, `enqueue` queued until the queue was full: 100 records on native, 30 × 1 KiB
    on the JVM's 32 KiB buffer. The next one threw `RecordNotQueuedException` after 2 398 ms on native and
    2 010 ms on the JVM. Once the broker answered, every queued record was in the topic, and the refused
    one was not, read by the Java client;
  - an `enqueue` with the broker paused was queued at once (0 ms). Its `await()` was cut at 1 s, and it
    landed at offset 1 once the broker answered.

  7 observations agree across the arms, with B-73's `CancelledSendTest` run alongside and unchanged.
- **AC: the native 120 s limit and the JVM `max.block.ms` give the same "not queued" type.** Both throw
  `RecordNotQueuedException` now. On native the limit **is** `max.block.ms`: kafkakn's own wait for room
  reads the key (librdkafka has none) with the Java client's default of 60 s, and the fixed 120 s is gone.
  On the JVM, the Java client's `TimeoutException` becomes the cause, and it is recognised as "not queued"
  because the client calls back on the calling thread, before `send` returns, for a record it never
  queued. A flag set when `send` returns would race with a queued record that fails fast.
- **Nothing else moved.** The full suites pass on both arms (158 JVM, 147 native), apart from
  `AccountingTest`, which needs its runner. `ci/b-09/run.sh`, the accounting, is green: 3 000 handed in
  and 3 000 on the broker on both arms. Its control dropped 2 900 and was red for it.
- **Mutants:** all four killed by `a_record_the_queue_has_no_room_for_within_max_block_ms_is_not_queued`:
  - native ignoring `max.block.ms`: refused after 60 s, not 2;
  - native throwing its old type;
  - the JVM not reading the thread: the refused record became a delivery, and the test ran out of time;
  - the JVM not mapping `TimeoutException`.
- **For the HTTP bridge that raised this:** `enqueue` under a `max.block.ms` shorter than the request's
  deadline. `RecordNotQueuedException` is a true `429`. A deadline that passes in `await()` is a true
  `504 outcome-unknown`.
