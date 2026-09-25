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

## Iteration 1 (2026-09-25): built and green; one mutant left unfinished when the build box went down

- **Built as §2a designs it:**
  - `subscribe(topics, listener)`, with `RebalanceListener` (`onRevoked` with a `RebalanceScope`,
    `onAssigned`, `onLost`) as plain functions;
  - a per-thread re-entry guard that refuses a call to the consumer from inside a callback;
  - no callback with an empty list.

  Native always installs a `rebalance_cb`, which applies the assignment itself, eager or incremental.
  A listener's exception is kept and rethrown by the `poll` or `close` the callback ran inside.
- **Green, measured on the Linux box (`ci/b-50/run.sh`):** in a group with one member on each arm, in
  both directions, where each member commits only in `onRevoked`, 1200 records gave 0 lost and 0
  processed twice. The group's commits reach every partition's end, the member that left revoked last,
  and the one that stayed was finally assigned `[0,1,2,3]`. Also green on both arms: re-entry is refused,
  with the contract's message, and a lone member sees exactly `+[0] -[0]`.
- **Mutants:**
  - a native scope that commits nothing: 137 and 188 duplicates, red;
  - a JVM scope that commits nothing: 136 and 142 duplicates, red.
- **Stopped at:** the third mutant, native's re-entry guard disabled, which should hang and fail
  `calling_the_consumer_from_inside_a_callback_throws_instead_of_deadlocking`. The build box went down
  during that run ("Host is down"). The mutant was reverted on the Mac.
- **Next:** rerun that mutant, and the full `ci/b-50/run.sh` once more on the committed code. Then
  update the contract's §2a from *target* to measured, and close.
