---
id: B-37
title: "Consumer groups: subscribe, rebalance, commit — and a group with one consumer from each arm"
status: open
priority: P2
size: XL
stage: stage-8-consume
blocked_by: [B-36]
---

# B-37 — consumer groups

The part D2 warned about. Subscribing to topics, taking part in a rebalance, and committing offsets —
`subscribe` and `commitSync` on the JVM, `rd_kafka_subscribe`, `rd_kafka_incremental_assign` and
`rd_kafka_commit` on native ([research §1.8](../research/research-architecture.md)).

- **The decision and its reason.** At-least-once is the promise: a record may be delivered twice
  across a rebalance, and it is never skipped. The test for that is loss, not duplication — the
  inverse of the producer's accounting, and just as easy to get backwards.
- **The differential test here is unusual and it is the point.** One group with a JVM consumer and a
  native consumer in it. Both speak the same group protocol to the same coordinator, and their
  default assignment strategies differ (§1.8) — so the group either converges on one both support or
  fails to form, and either answer is worth more than any single-arm test.
- Committed offsets are read by `kafka-consumer-groups.sh --describe`, never by the consumer that
  committed them.
- The rejected alternative is auto-commit only. It makes "processed" and "committed" different events
  the caller cannot order, which is the at-least-once promise with a hole in it.
- Not covered: exactly-once ([B-38](B-38-exactly-once-read-process-write.md)), the consumer group
  protocol `consumer` (KIP-848) — `classic` is the default on both arms.

- AC: two consumers in one group split the partitions; stopping one hands its partitions to the other
  and **no record is skipped**, counted against what `kafka-console-producer` wrote.
- AC: a mixed group — one consumer per arm — forms, splits, rebalances and loses nothing.
- AC: manual commit, read back by `kafka-consumer-groups.sh`, matches what the caller committed on
  both arms.
- Anchors: `ci/harness/broker.sh`, `docs/research/research-architecture.md`.
