---
id: B-84
title: "listOffsets answers OffsetSpec.MaxTimestamp on both arms"
status: done
priority: P3
size: S
stage: stage-20-what-waited-for-a-caller
epic: feature-administer-topics
blocked_by: []
---

# B-84 — listOffsets answers OffsetSpec.MaxTimestamp on both arms

Left out of [B-59](B-59-consumer-group-offsets-and-lag.md), where the contract says *"The max-timestamp spec is left out: it answers a
different question"*. The owner lifted "wait for a caller" on 2026-09-28. `OffsetSpec.maxTimestamp()` in the Java
client and `RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP` in librdkafka return the offset of the record with the highest
timestamp, and that timestamp.

- AC: `OffsetSpec.MaxTimestamp` on both arms. It is checked against the broker's `kafka-get-offsets --time -3`, on a
  partition whose highest timestamp is not its last record.
- AC: what each arm returns for an empty partition is recorded and compared.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`, `docs/api/producer-contract.md`.

## Findings (2026-09-28)

- `OffsetSpec.MaxTimestamp`: `ApacheOffsetSpec.maxTimestamp()` on the JVM, `RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP` (-3) on
  native. *Measured*, `ci/b-84/run.sh`: a partition written at t, t+5 s, t+2 s answers 1, and an empty partition
  answers null, on both arms. `kafka-get-offsets.sh --time -3` prints `0:1` and nothing for the empty partition. The
  arms agree on every observation of `AdminOffsetsTest`.
- Mutant: native asking `RD_KAFKA_OFFSET_SPEC_LATEST` for it was killed by
  `AdminOffsetsTest.the_offset_of_the_highest_timestamp_is_the_brokers`.
