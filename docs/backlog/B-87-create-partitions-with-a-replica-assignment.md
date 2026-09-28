---
id: B-87
title: "createPartitions takes a replica assignment for the new partitions"
status: wip
priority: P3
size: S
stage: stage-20-what-waited-for-a-caller
epic: feature-administer-topics
blocked_by: []
---

# B-87 — createPartitions takes a replica assignment for the new partitions

Left out of [B-62](B-62-create-partitions.md). The fixture has one broker, so the only assignment it can check is
`[[1]]`, and a wrong broker id is the refusal worth measuring. The owner lifted "wait for a caller" on 2026-09-28.

- AC: new partitions with an explicit assignment land on the broker named, as `kafka-topics --describe` reports.
  An assignment naming a broker that does not exist is refused on both arms, with the same type, and the topic is
  unchanged.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.
