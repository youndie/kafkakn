---
id: B-71
title: "sendOffsetsToTransaction after the group moved on: one exception on both arms"
status: wip
priority: P2
size: S
stage: stage-15-a-consumer-of-our-own
---

# B-71 — `sendOffsetsToTransaction` after the group moved on: one exception on both arms

Found by [B-70](B-70-kafkakn-soak.md)'s hour of chaos. An exactly-once service frozen past its session wakes
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
