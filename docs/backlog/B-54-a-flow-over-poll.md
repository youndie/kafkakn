---
id: B-54
title: "A Flow of records, built on poll"
status: done
priority: P2
size: S
stage: stage-11-everyday-gaps
---

# B-54 — a Flow of records, built on poll

The consumer contract chose an explicit `poll` first and promised: *"The `Flow` comes later, as an
extension over `poll`, not instead of it."* (§2) A Kotlin caller expects a `Flow`, and writing one
correctly is where they would get it wrong: cancellation, the empty poll, and the member evicted
because the collector was slower than `max.poll.interval.ms`.

- **The decision and its reason.** An extension, `KafkaConsumer.records(pollTimeout): Flow<ConsumerRecord>`,
  in common code, so both arms get one implementation. Cold, one consumer per collector, and it never
  closes the consumer it did not open.
- The eviction problem goes in the KDoc and in a test: a collector slower than `max.poll.interval.ms`
  is removed from its group, and the test shows what the caller sees when it happens.
- Not covered: a `Flow` of batches, or anything that commits for the caller.

- AC: collecting N records from a fixture topic returns exactly those N, in order per partition, on
  both arms.
- AC: cancelling the collector returns promptly, and the consumer is usable afterwards (the
  cancellation promise of [B-36](B-36-assign-and-poll.md)).
- AC: a deliberately slow collector's eviction is observed and documented, not hidden.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/`, `docs/api/consumer-contract.md`.

## Findings (2026-09-25)

- **AC: N records in order.** The fixture's twenty come back in order, on both arms.
- **AC: cancellation.** A cancelled collector stops in under 20 ms on both arms, and the consumer reads
  again afterwards. The test bounds the wait and launches the collector in a scope of its own, so a
  collector that does not stop fails by name instead of hanging the run.
- **AC: the slow collector's eviction, observed and documented.** Stalled 10 s against a 6 s
  `max.poll.interval.ms`, the collector:
  - keeps receiving the rest of the batch it holds, from a partition that is already lost;
  - sees `onLost` in the listener, on both arms;
  - **then the arms part.** The JVM rejoins at the next `poll` and re-reads from the start (nothing was
    committed). Native throws "Maximum application poll interval exceeded".

  Both are in the contract. The native arm disagrees with the reference, so this is filed as
  [B-64](B-64-native-poll-throws-where-the-jvm-rejoins.md), P1, rather than widening this item.
- **Mutants:**
  - emitting only the first record of each batch is killed by `collecting_returns_the_records_in_order`;
  - swallowing cancellation inside the flow is caught, but **not by name**. The cancelled collector then
    spins on `poll` until the test JVM runs out of memory, and the run dies with an `OutOfMemoryError`
    rather than reporting a failure. It counts as detected, not as a named kill.
