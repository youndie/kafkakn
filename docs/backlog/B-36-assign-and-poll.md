---
id: B-36
title: "A consumer without a group: assign partitions, seek, and read as a Flow"
status: open
priority: P2
size: L
stage: stage-8-consume
blocked_by: [B-35]
---

# B-36 — assign and poll

The first consumer code, and deliberately the half without group coordination: assign partitions
explicitly, seek to the beginning, the end, an offset or a time, and read. Both clients underneath
have every piece — `assign`, `poll`, `seek`, `offsetsForTimes` on the JVM; `rd_kafka_assign`,
`rd_kafka_consumer_poll`, `rd_kafka_seek_partitions`, `rd_kafka_offsets_for_times` on native
([research §1.8](../research/research-architecture.md)).

- **The decision and its reason.** Built to the contract [B-35](B-35-the-consumer-designed-first.md)
  produces, with its threading decision; nothing here re-decides it. Groups are left out so that the
  first thing measured is whether both arms read the same records in the same order — without a
  rebalance that could explain a difference away.
- **The oracle is a third party on both ends.** Records are written by `kafka-console-producer`, not
  by kafkakn, and include what a text-shaped path would damage: non-UTF-8 values, null keys, null
  values, duplicate header names.
- The rejected alternative is reading back what this library's own producer wrote. It would pass for
  a consumer and a producer that are wrong in the same way.

- AC: both arms read every record of an assigned partition, in order, byte for byte against what
  `kafka-console-consumer` reads.
- AC: seeking to beginning, end, an offset and a timestamp lands where the broker's own
  `kafka-get-offsets.sh` says it should, on both arms.
- AC: the two arms' readings are compared with each other, and a difference is a failure rather than a
  warning.
- Anchors: `ci/harness/broker.sh`, `docs/research/research-architecture.md`.
