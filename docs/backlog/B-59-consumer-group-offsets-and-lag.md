---
id: B-59
title: "A consumer group's committed offsets and its lag, read by the admin client"
status: wip
priority: P2
size: M
stage: stage-13-admin
blocked_by: [B-58]
---

# B-59 — a consumer group's committed offsets and its lag, read by the admin client

An operator's question is *how far behind is this group*, asked from outside the group. That needs the
group's committed offsets and each partition's end offset. Both clients have both:
`Admin.listConsumerGroupOffsets` and `listOffsets`, and `rd_kafka_ListConsumerGroupOffsets` and
`rd_kafka_ListOffsets`.

- **The decision and its reason.** `listConsumerGroupOffsets(group)` returning committed offsets, and
  `listOffsets(partitions, spec)` for earliest, latest or a timestamp. Lag is left to the caller as
  end minus committed: a derived number the library could get subtly wrong, and one line of code for
  the caller.
- `listOffsets` also answers "where does this partition start and end" for everyone else. On the
  consumer side that has been a seek away until now.

- AC: for a group that has committed, both arms' offsets agree with each other and with
  `kafka-consumer-groups --describe`.
- AC: `listOffsets` for earliest, latest and a timestamp agrees with `kafka-get-offsets` on both arms.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.
