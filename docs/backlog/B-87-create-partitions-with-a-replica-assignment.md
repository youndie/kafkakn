---
id: B-87
title: "createPartitions takes a replica assignment for the new partitions"
status: done
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

## Findings (2026-09-28)

- `createPartitions(topic, totalCount, assignment = null)`: `NewPartitions.increaseTo(total, assignment)` on the JVM,
  `rd_kafka_NewPartitions_set_replica_assignment` per new partition on native. *Measured*, `ci/b-87/run.sh`: `[[1]]`
  gives replicas `[1]` and leader 1, by `kafka-topics.sh`. `[[99]]` is refused with `IllegalArgumentException` on both
  arms (`INVALID_REPLICA_ASSIGNMENT`), and the topic keeps two partitions. The arms agree on every observation.
- Mutant: native not passing the assignment was killed by
  `AdminPartitionsTest.new_partitions_land_on_the_broker_named_and_an_unknown_broker_is_refused`. Without the
  assignment the broker placed the partition itself, and the growth that should have been refused went through.
