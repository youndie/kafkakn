---
id: B-48
title: "Commit named offsets, not only everything poll returned"
status: open
priority: P1
size: S
stage: stage-11-everyday-gaps
---

# B-48 — commit named offsets, not only everything poll returned

`commit()` commits the position after everything `poll` has returned, for every partition held
(consumer contract §2). A caller who processes records one by one, or hands them to workers, cannot
commit what was actually processed: the only choice is all or nothing. Both clients take explicit
offsets: `commitSync(Map<TopicPartition, OffsetAndMetadata>)` and `rd_kafka_commit(rk, offsets, 0)`.

- **The decision and its reason.** `commit(offsets: Map<TopicPartition, Long>)`, where each value is
  the next offset to read, as the producer's `sendOffsetsToTransaction` already takes it. One meaning
  across the library.
- Offset metadata strings are left out: one more thing to hold equal across the arms, and nothing
  here needs them yet.
- Not covered: asynchronous commit. `commitAsync` exists on both clients, and so do the callbacks that
  report its failure after the fact; a suspending `commit` already frees the caller's thread.

- AC: each arm commits an offset in the middle of a batch, and `kafka-consumer-groups --describe`
  shows exactly that offset.
- AC: a new member of the group resumes from it, and reads the records after it and none before.
- AC: what each arm does with an offset for a partition this consumer does not hold is measured, and
  the contract says it. If the arms differ, the portable behaviour is the refusal.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.kt`,
  `docs/api/consumer-contract.md`.
