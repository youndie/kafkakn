---
id: B-55
title: "Cooperative rebalancing: partitions move without stopping the whole group"
status: open
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
