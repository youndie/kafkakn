---
id: B-71
title: "sendOffsetsToTransaction after the group moved on: one exception on both arms"
status: done
priority: P2
size: S
stage: stage-15-a-consumer-of-our-own
---

# B-71 — `sendOffsetsToTransaction` after the group moved on: one exception on both arms

Found by [B-70](B-70-kafkakn-soak.md)'s soak under chaos (37 minutes of it, not the hour first written; see B-72). An exactly-once service frozen past its session wakes
in the middle of a transaction. The group has rebalanced without it, and it hands `sendOffsetsToTransaction`
the group metadata it had before:
- the JVM throws `CommitFailedException`, the Java client's own type: *"Transaction offset Commit failed due
  to consumer group metadata mismatch: The coordinator is not aware of this member."*;
- native throws `KafkaProduceException`: *"sendOffsetsToTransaction: ILLEGAL_GENERATION (22)"* or
  *"UNKNOWN_MEMBER_ID (25)"*, marked *abortable*.

A portable caller cannot catch both with one `catch`. The Java type is one kafkakn promises nobody. And the
contract does not say what the caller does next: abort, and read again from the group's commit.

- **The decision and its reason.** One kafkakn exception for "the group moved on, this transaction's
  offsets are refused", on both arms, with each client's own error kept as the cause. The consumer contract
  says what follows: abort the transaction, and carry on from the group's commit. Nothing is lost, which
  B-70 showed at scale. This is B-56's fencing treatment, applied to the next refusal down.
- Measure it deterministically, not by an hour of chaos. Freeze a member in the middle of a transaction, on
  each arm, and let another member take its partitions.
- Also measure why each JVM freeze in B-70 ended in an exit and only 3 of 12 native freezes did. Either the
  freezes landed in different places, or one arm lets a stale commit through. B-70's zero duplicates says
  the second did not cost correctness there, and this item checks that it cannot.

- AC: on both arms, `sendOffsetsToTransaction` with the group metadata of a membership the group has since
  dropped throws the same kafkakn exception, and the transaction can be aborted afterwards.
- AC: the contract says what a caller does after it.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt`,
  `docs/api/producer-contract.md`.

## Findings (2026-09-26)

- **Reproduced on purpose, not by chaos** (`StaleGroupMetadataTest`, both arms), two ways:
  - metadata taken before a second member joined and the group rebalanced: refused as `ILLEGAL_GENERATION`;
  - metadata taken before the member left: refused as `UNKNOWN_MEMBER_ID`.

  Measured raw first, the refusals were the same on both arms except in type: the JVM threw the Java
  client's `CommitFailedException` (*"consumer group metadata mismatch"*), and native threw
  `KafkaProduceException` with librdkafka's code, marked *abortable*.
- **AC: the same kafkakn exception on both arms, and the transaction aborts afterwards.** Both now throw
  `StaleGroupMetadataException`, with each client's words kept. `abortTransaction` succeeds after it on both
  arms. The control holds as well: the same member with fresh metadata, the same partition, begins, sends
  offsets and commits. So the refusal is about the staleness and nothing else. `ci/b-71/run.sh`: 5
  observations agree across the arms.
- **AC: the contract says what a caller does next.** Abort, and read again from the group's commit. The
  producer stays usable. That is in the producer contract's `sendOffsetsToTransaction` section and its
  errors table.
- **B-70's other question, whether native lets a stale commit through: it does not.** Both refusals are
  refused on native as on the JVM. So each JVM freeze in B-70 ending in an exit, against 3 of 12 native
  freezes, is not a difference in correctness. How often a freeze lands inside a transaction rather than in
  `poll` differs between the arms. That was not measured.
- **Mutants:** all three killed, each by name:
  - the native mapping removed: both tests;
  - the native mapping without `UNKNOWN_MEMBER_ID`: only the member-that-left test, as it should;
  - the JVM mapping removed: both tests.
