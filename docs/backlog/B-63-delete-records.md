---
id: B-63
title: "Delete records before an offset"
status: done
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

## Findings (2026-09-25)

- **AC: after each arm deletes before an offset, `kafka-get-offsets --time -2` reports that offset as the
  earliest, and the returned low watermark agrees.** `ci/b-63/run.sh`: each arm returned `0:10 1:0`, and the
  tool reports `0:10 1:0` for each arm's topic. The steps before that were:
  - partition 0 before 4 returned 4, and `listOffsets(Earliest)` agreed;
  - before 2, behind that, returned 4, not 2: the case the decision to return the broker's answer exists for;
  - up to the end returned 10, and the end stayed 10.
- **AC: an offset past the end is refused on both arms.** Measured raw first, the refusals were the Java
  client's `OffsetOutOfRangeException` and librdkafka's per-partition `OFFSET_OUT_OF_RANGE` (1). Both arms
  now throw `IllegalArgumentException`, and the partition keeps its records.
- **Mutants:** all four killed, each by name:
  - the requested offset returned instead of the broker's watermark, on each arm: killed by the main test,
    at the "behind" step;
  - the refusal mapping removed, on each arm: killed by the refusal test.
