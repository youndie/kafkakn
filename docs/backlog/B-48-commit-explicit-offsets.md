---
id: B-48
title: "Commit named offsets, not only everything poll returned"
status: done
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

## Findings (2026-09-25)

- **AC: the offset committed mid-batch is what the broker shows.** Each arm reads the twenty-record
  fixture, commits offset 7, and `kafka-consumer-groups.sh --describe` shows 7 for its group
  (`ci/b-48/run.sh`).
- **AC: a new member resumes from it.** The second consumer in the group reads offsets 7 to 19, exactly,
  on both arms (`CommitOffsetsTest.a_named_offset_is_what_the_group_resumes_from`).
- **AC: a partition this consumer does not hold.** Both arms accept the commit, and the broker stores it
  (it holds 0 for `kafkakn-1`, as committed). The arms agree, so kafkakn passes it through. Measured with
  `assign`, where the group has no generation to check against. Whether the broker judges such a commit
  differently under a subscription is left to [B-50](B-50-a-rebalance-listener.md), and the contract
  says so.
- **Also:** an empty map commits nothing, on both arms, and a negative offset is refused in common code.
  Native reads librdkafka's per-partition errors as well as the call's.
- **Mutants, each caught by `a_named_offset_is_what_the_group_resumes_from` on the arm mutated:**
  - native committing the stored offsets instead of the list;
  - JVM `commitSync()` without the map;
  - native committing one past the offset named.
- **ktlint's catch-all rule flagged the test** for catching `Exception` without looking at it. The
  refusal's class and message are now kept as an arm fact, so a refusal would say what it was.
