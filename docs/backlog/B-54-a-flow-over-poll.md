---
id: B-54
title: "A Flow of records, built on poll"
status: wip
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
