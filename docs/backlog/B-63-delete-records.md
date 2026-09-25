---
id: B-63
title: "Delete records before an offset"
status: wip
priority: P3
size: S
stage: stage-13-admin
---

# B-63 — delete records before an offset

Truncating a partition's head, for example after a bad batch or to reclaim space before retention
would, is `DeleteRecords`. Both clients: `Admin.deleteRecords` and `rd_kafka_DeleteRecords`.

- **The decision and its reason.** `deleteRecords(Map<TopicPartition, Long>)`, where each value is the
  new earliest offset. It returns the low watermark the broker reports, so the caller sees what
  actually happened rather than what it asked for.

- AC: after each arm deletes before an offset, `kafka-get-offsets --time -2` reports that offset as the
  earliest, and the returned low watermark agrees.
- AC: an offset past the end is refused on both arms.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.
