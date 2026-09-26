---
id: B-74
title: "A caller whose wait was cut can tell 'never queued' from 'queued, outcome unknown'"
status: open
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
