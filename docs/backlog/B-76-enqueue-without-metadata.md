---
id: B-76
title: "Native enqueue refuses a record whose topic has no metadata within max.block.ms, as the contract says"
status: wip
priority: P0
size: M
stage: stage-16-a-deadline-on-send
epic: feature-backpressure-and-accounting
blocked_by: [B-74]
---

# B-76 — native `enqueue` refuses a record whose topic has no metadata within `max.block.ms`, as the contract says

The contract's error table, since [B-74](B-74-a-cut-wait-says-whether-the-record-was-queued.md), says that
when there is *no room in the queue, or no metadata*, within `max.block.ms`, `enqueue` (and so `send`) throws
`RecordNotQueuedException` on both arms. B-74 measured the queue-full half only. The metadata half was measured
on 2026-09-27 by a consumer, an HTTP bridge planned on keel. It used a bootstrap address nobody listens on,
`max.block.ms` 1 000, and one `enqueue` to a topic:

| | JVM | native |
|---|---|---|
| `enqueue` | threw `RecordNotQueuedException` at 1 070 ms: *"Topic orders not present in metadata after 1000 ms"* | **returned a `Delivery` at 0 ms**: the record was queued, with no metadata |
| `close()` afterwards | 13 ms: nothing was queued | **300 200 ms**: the flush waited out `message.timeout.ms` for the queued record |

librdkafka queues a record for a topic it has no metadata for, and waits for the metadata in the background.
The Java client waits for it inside `send`, up to `max.block.ms`. So the contract row is true on the JVM and
false on native. A caller holding a deadline gets two different true answers: "never queued, retry is safe" on
one arm, and "queued, outcome unknown" on the other. That is exactly the distinction B-74 exists to make the
same on both.

- **The decision and its reason.** Make the native arm honour the row, rather than correct the row to the
  measurement. The owner chose this on 2026-09-27, for the consumer that found it. A caller must not need to
  know which arm it runs on to know whether a retry can write twice. The shape is decided here. Two candidates,
  both of which wait **off the caller's dispatcher**, as `partitionsFor` does:
  - before the first `enqueue` to a topic, wait for its metadata (`rd_kafka_metadata` for that topic) for the
    time left of `max.block.ms`, and cache topics already known;
  - or check the topic's metadata state after `rd_kafka_produceva` and purge the record if it is still in the
    unassigned queue when `max.block.ms` runs out. This is the more intrusive candidate, and it is listed so
    it can be rejected with its reason.
- **A topic that does not exist is the other half of the same row.** With auto-creation off, the native
  `partitionsFor` learns that from the broker in about 57 ms (`KafkaMetadataException`), while the JVM waits
  `max.block.ms` (the contract's table for `partitionsFor`). For `enqueue`, both have to answer
  `RecordNotQueuedException`: the type is the contract. The time may differ, and the contract names the
  difference, as it already does for `partitionsFor`.
- Not covered: how long `close` takes when records are queued and the broker is gone. The 300 s above is
  `close` doing what the contract says, flushing. Whether `close` needs a bound is a separate question, for
  whoever needs one first.

- AC: on both arms, with no broker at the bootstrap address and `max.block.ms` 1 000, `enqueue` throws
  `RecordNotQueuedException` after at least 1 000 ms. `close` then returns promptly, because nothing was
  queued.
- AC: on both arms, against the test broker, `enqueue` to a topic that does not exist throws
  `RecordNotQueuedException`. The contract records the time each arm took, and the record is not in any topic.
- AC: `EnqueueTest` (B-74, queue full) and `CancelledSendTest` (B-73) pass unchanged, and a topic whose
  metadata arrives in time is queued as before: the accounting run (`ci/b-09/run.sh`) is green.
- AC: the contract's error table carries a *measured* date for the metadata half of the row, per arm.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`,
  `docs/api/producer-contract.md`, `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/`.
