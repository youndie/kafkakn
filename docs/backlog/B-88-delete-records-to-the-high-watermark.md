---
id: B-88
title: "deleteRecords to the high watermark, without reading it first"
status: wip
priority: P3
size: S
stage: stage-20-what-waited-for-a-caller
epic: feature-administer-topics
blocked_by: []
---

# B-88 — deleteRecords to the high watermark, without reading it first

Left out of [B-63](B-63-delete-records.md). Both clients accept `-1` as "before the high watermark", which deletes
everything written so far without a `listOffsets` first (`RecordsToDelete.beforeOffset(-1)`,
`RD_KAFKA_OFFSET_END`). The owner lifted "wait for a caller" on 2026-09-28.

- AC: a named way to say it, not a bare `-1`, on both arms. Afterwards the partition's earliest offset equals its
  end, by `kafka-get-offsets`.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.
