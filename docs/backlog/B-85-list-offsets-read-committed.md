---
id: B-85
title: "listOffsets takes an isolation level"
status: done
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

## Findings (2026-09-28)

- `listOffsets(partitions, spec, isolation = IsolationLevel.ReadUncommitted)`: `ListOffsetsOptions(IsolationLevel)`
  on the JVM, `rd_kafka_AdminOptions_set_isolation_level` on native. *Measured*, `ci/b-85/run.sh`: two records
  committed in a transaction and three in one left open answer `uncommitted=6 committed=3 default=6` on both arms.
  The arms agree on every observation of `AdminOffsetsTest`.
- **One AC was met another way.** "The broker's own reader agrees" was not run: `Records.java`, reading read
  committed, waits out its 600 s deadline while a transaction is open, and `kafka-get-offsets.sh` has no isolation
  switch. What holds the answer instead is the JVM arm, the oracle, together with the transaction's arithmetic: two
  records and a commit marker make the last stable offset 3.
- Mutant: native asking read uncommitted for `ReadCommitted` was killed by
  `AdminOffsetsTest.latest_read_committed_stops_where_an_open_transaction_starts`.
