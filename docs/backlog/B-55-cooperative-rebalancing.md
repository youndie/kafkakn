---
id: B-55
title: "Cooperative rebalancing: partitions move without stopping the whole group"
status: done
priority: P2
size: L
stage: stage-12-group-protocols
blocked_by: [B-50]
---

# B-55 — cooperative rebalancing: partitions move without stopping the whole group

With the default eager protocol every member gives up every partition at each rebalance. Cooperative
rebalancing moves only the partitions that change owner. Both clients support it:
`CooperativeStickyAssignor` on the JVM (already in its default list, behind `RangeAssignor`), and
`cooperative-sticky` in librdkafka with `rd_kafka_incremental_assign`/`_unassign` in the rebalance
callback. Today `partition.assignment.strategy` is a platform key (consumer contract §3): spelled as
class names on one arm and as words on the other. A mixed group settles on `range`.

- **The decision and its reason.** One portable spelling that each arm translates, as TLS and SASL
  keys are translated. The native rebalance callback switches to incremental assignment when
  `rd_kafka_rebalance_protocol` says `COOPERATIVE`. The listener's meaning of "revoked" changes (only
  what moves) and the contract says so.
- The hard case is the mixed group: a JVM member and a native member must agree on the strategy, or
  the group will not form. That case is the test.

- AC: in a group with one member on each arm, both on cooperative-sticky, a third member joins. Only
  the partitions that move are revoked, as each listener reports, and the others keep being read.
- AC: no record is lost across the rebalance, counted against the broker.
- AC: the configuration key is portable, and a value only one arm understands is refused at
  construction.
- Anchors: `docs/api/consumer-contract.md`, `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.native.kt`.

## Findings (2026-09-25)

- **AC: a third member joins a mixed cooperative group.** Measured, with wall-clock times on every
  listener event, since the members are three processes:
  - the native member joins: the JVM member gives up `[3,4,5]` and keeps reading `[0,1,2]`;
  - the third member joins 30 s later: the JVM member gives up `[2]`, the native member gives up `[5]`,
    and the third receives `[2,5]`;
  - at the end, as they leave, the third picks up the rest incrementally.

  No member gave up everything mid-stream.
- **AC: nothing lost:** 1200 records, 0 lost, 0 processed twice, and commits at every partition's end.
- **AC: one portable key.** librdkafka's words, which the JVM arm translates into class names. A Java class
  name, `sticky`, or cooperative next to an eager assignor is refused at construction on both arms.
- **Native needed nothing new for the protocol itself.** B-50's callback already switches to incremental
  assignment under `COOPERATIVE`. The mutant that makes it call a plain `assign` is refused by librdkafka
  ("must be made using incremental_assign()"). The member never receives a partition, and
  `a_member_of_a_cooperative_group` fails by name at its limit.
- **Three faults in my own harness, each found by reading the result rather than the verdict:**
  - the event parser split `[0, 1]` on its space, and called a cooperative run eager;
  - the third member's 30 s wait was a `delay` on `runTest`'s virtual clock, skipped instantly, so it
    joined in the same millisecond as the second;
  - so the first "green" had two members arrive in one rebalance, which is not the AC.

  Timestamps made the last two visible.
- **Mutants, each caught by name:**
  - native using a plain `assign` under cooperative;
  - the JVM passing the portable word untranslated (`the_portable_spellings_are_accepted`);
  - the JVM skipping the portability check (`a_strategy_only_one_arm_understands_is_refused_at_construction`).
- **The gate caught a platform name in common code.** The first version kept the Java class names in
  `commonMain`, and `scripts/common_is_platform_free.py` refused them. They now live in the JVM arm, and
  common code keeps only the set of portable words. The strategy tests and ktlint pass again on both
  arms after the move.
