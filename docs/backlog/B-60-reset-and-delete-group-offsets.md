---
id: B-60
title: "Reset a group's offsets, delete them, and delete a group"
status: wip
priority: P2
size: M
stage: stage-13-admin
blocked_by: [B-59]
---

# B-60 — reset a group's offsets, delete them, and delete a group

Replaying a topic for a consumer service means moving its group's offsets while the group is empty,
which today takes `kafka-consumer-groups --reset-offsets`. Both clients do it:
`Admin.alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets` and `deleteConsumerGroups`, and
librdkafka's `rd_kafka_AlterConsumerGroupOffsets`, `rd_kafka_DeleteConsumerGroupOffsets` and
`rd_kafka_DeleteGroups`.

- **The decision and its reason.** The three calls, with the broker's own refusals kept as errors:
  altering the offsets of a group that has members is refused by the broker, and that refusal is the
  safety.
- Not covered: a `--to-datetime`-style convenience. [B-59](B-59-consumer-group-offsets-and-lag.md)'s
  `listOffsets` plus this call is that convenience, in the caller's code.

- AC: each arm moves an empty group's offsets, and a member that joins afterwards reads from exactly
  there, as the broker counts.
- AC: altering the offsets of a group with an active member is refused on both arms with the same
  exception.
- AC: a deleted group no longer appears in `kafka-consumer-groups --list`.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.
