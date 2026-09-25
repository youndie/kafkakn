---
id: B-62
title: "Add partitions to an existing topic"
status: wip
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
