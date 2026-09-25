---
id: B-50
title: "A rebalance listener: say which partitions arrive and which leave"
status: wip
priority: P1
size: L
stage: stage-11-everyday-gaps
blocked_by: [B-48]
---

# B-50 — a rebalance listener: say which partitions arrive and which leave

A consumer in a group is not told when partitions move. The first consumer refused rebalance
callbacks on purpose (consumer contract §2, §5): with auto-commit off and nothing to flush, each
client's own handling was the whole promise. That stops being enough once a caller keeps state per
partition, commits explicit offsets ([B-48](B-48-commit-explicit-offsets.md)) or buffers work. Losing
a partition without being told is then a duplicate, or a commit made after another member took over.

Both clients have the hook, and they are shaped differently:
- JVM: `subscribe(topics, ConsumerRebalanceListener)`, called on the polling thread inside `poll`.
- librdkafka: `rd_kafka_conf_set_rebalance_cb`. The callback must itself call `rd_kafka_assign` or
  `rd_kafka_incremental_assign`, and `rd_kafka_assignment_lost` distinguishes a lost assignment from a
  revoked one.

- **The decision and its reason.** Design it in the contract first, as the consumer was (B-35):
  - what the listener is: a suspending callback, or events the caller reads from `poll`;
  - on which thread it runs;
  - what it may call;
  - how "revoked" and "lost" differ, on both arms.

  Implement it only after that. The obvious shape, a callback run on librdkafka's thread, is the
  shape the threading section refused for everything else.
- The acceptance is the at-least-once harness of [B-37](B-37-consumer-groups.md), extended: a member
  commits on revocation, and no record is lost or processed twice beyond what the contract allows.
- Not covered: cooperative rebalancing ([B-55](B-55-cooperative-rebalancing.md)), which changes what
  "revoked" means.

- AC: the contract section exists and is reviewed before the code.
- AC: in a group with one member on each arm, one member leaves mid-stream. The listener on the other
  reports the partitions it gained, and the one that left reports the partitions it gave up before it
  stops.
- AC: a member committing in its revocation callback loses no record and duplicates none, counted
  against the broker's consumer.
- Anchors: `docs/api/consumer-contract.md`, `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.kt`,
  `ci/b-37/run.sh`.

## Design review (2026-09-25, the owner)

The contract section (§2a) was written first and put to the owner interactively, with three shapes:
- plain callbacks with a commit scope;
- suspending callbacks;
- rebalances as events returned from `poll`.

**Chosen: plain callbacks with `RebalanceScope`**, as §2a describes. The code follows the section, not
the other way round.
