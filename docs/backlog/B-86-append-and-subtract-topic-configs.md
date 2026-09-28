---
id: B-86
title: "alterTopicConfigs appends to and subtracts from list-valued keys"
status: wip
priority: P3
size: S
stage: stage-20-what-waited-for-a-caller
epic: feature-administer-topics
blocked_by: []
---

# B-86 — alterTopicConfigs appends to and subtracts from list-valued keys

Left out of [B-61](B-61-topic-configs.md), which does `SET` and `DELETE`. `APPEND` and `SUBTRACT` edit a
list-valued key, such as `cleanup.policy=compact,delete`, without the caller reading it first. Both clients have
them (`AlterConfigOp.OpType`, `RD_KAFKA_ALTER_CONFIG_OP_TYPE_APPEND`/`SUBTRACT`). The owner lifted "wait for a
caller" on 2026-09-28.

- AC: append `delete` to `cleanup.policy=compact`, then subtract `compact`. After each step, `kafka-configs
  --describe` shows the list, on both arms.
- AC: what each arm does with `APPEND` on a key that is not a list is recorded, and the contract states it.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.
