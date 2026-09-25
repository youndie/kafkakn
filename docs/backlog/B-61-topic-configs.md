---
id: B-61
title: "Describe a topic's configuration and change it incrementally"
status: wip
priority: P3
size: M
stage: stage-13-admin
---

# B-61 — describe a topic's configuration and change it incrementally

`createTopics` takes a configuration map. After that the admin client cannot read or change it,
though retention and compaction are routinely changed on existing topics. Both clients:
`Admin.describeConfigs`/`incrementalAlterConfigs`, and librdkafka's
`rd_kafka_DescribeConfigs`/`rd_kafka_IncrementalAlterConfigs`.

- **The decision and its reason.** `describeTopicConfigs(names)` returning each key's value and its
  source (default, set on the topic, set on the broker). `alterTopicConfigs(name, set, delete)` is
  incremental only: the non-incremental `alterConfigs` resets every key not named, which is the
  mistake to leave out.
- Topics only. Broker configuration is an operator's tool, not a service's.

- AC: a value set by each arm is what `kafka-configs --describe` reports, with the right source.
- AC: deleting a key returns it to its default on both arms, and a key the broker does not know is
  refused on both.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.
