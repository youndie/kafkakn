---
id: B-62
title: "Add partitions to an existing topic"
status: done
priority: P3
size: S
stage: stage-13-admin
---

# B-62 — add partitions to an existing topic

A topic's partition count can only grow, and growing it is how a topic is scaled. Both clients:
`Admin.createPartitions` and `rd_kafka_CreatePartitions`.

- **The decision and its reason.** `createPartitions(topic, totalCount)`, with the broker's refusal of
  a smaller or equal count kept as an error. The contract says what adding partitions does to keyed
  records: the key-to-partition mapping changes for new records, and the library does not hide that.

- AC: each arm grows a topic, and `kafka-topics --describe` shows the new count.
- AC: a count that does not grow the topic is refused on both arms with the same exception.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.

## Findings (2026-09-25)

- **AC: each arm grows a topic, and `kafka-topics --describe` shows the new count.** It reported
  `PartitionCount: 4` for each arm's topic (`ci/b-62/run.sh`). The broker's end offsets, `0:11 1:2 2:1 3:2`,
  are exactly where each arm said its records went. The grown count was visible to a describe within 7 to
  31 ms.
- **AC: a count that does not grow the topic is refused on both arms with the same exception.** Equal and
  smaller counts were measured raw first: the Java client's `InvalidPartitionsException` and librdkafka's
  per-topic `INVALID_PARTITIONS` (37). Both arms now throw `IllegalArgumentException`, and the topic keeps
  its three partitions. On native, the mapping is scoped to this call, not to every topic result.
- **The contract says what growth does to keys.** Eight keys that all sat in partition 0 went to
  `1 0 2 3 1 0 0 3` of four, identically on both arms (murmur2). The records are counted by the broker, so
  the move is not only the client's word.
- **Mutants:** all four killed, each by name:
  - the count asked for off by one, on each arm: killed by the growth test;
  - the refusal mapping removed, on each arm: killed by the refusal test.
