---
id: B-85
title: "listOffsets takes an isolation level"
status: open
priority: P3
size: S
stage: stage-20-what-waited-for-a-caller
epic: feature-administer-topics
blocked_by: []
---

# B-85 — listOffsets takes an isolation level

Left out of [B-59](B-59-consumer-group-offsets-and-lag.md): `listOffsets` reads uncommitted, the default of both clients. With
`read_committed`, `Latest` is the last stable offset, which stops before an open transaction. That is what a
reader of a transactional topic actually sees. The owner lifted "wait for a caller" on 2026-09-28.

- AC: `listOffsets(partitions, spec, isolation)`, defaulting to uncommitted. With a transaction left open, `Latest`
  read committed is below `Latest` read uncommitted on both arms. The broker's own reader (`Records.java`, read
  committed) agrees.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.
