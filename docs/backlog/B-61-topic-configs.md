---
id: B-61
title: "Describe a topic's configuration and change it incrementally"
status: done
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

## Findings (2026-09-25)

- **The shape.** `describeTopicConfigs(names): Map<String, Map<String, TopicConfigEntry>>`, each entry a
  value and a `ConfigSource`, and `alterTopicConfigs(name, set, delete)`, incremental only. The broker's
  three sources (dynamic, dynamic default, static) are one `BROKER`, since a service can change none of them.
- **AC: a value set by each arm is what `kafka-configs --describe` reports, with the right source.**
  `ci/b-61/run.sh` checks each arm's full description after a set and a delete against
  `kafka-configs.sh --describe --all`: 33 keys, 0 differ on either arm. Along the way:
  - `retention.ms` is `86400000/TOPIC` as created;
  - `max.message.bytes` is `2000000/TOPIC` once set;
  - the key set at creation is untouched by that change.
- **AC: deleting a key returns it to its default on both arms, and a key the broker does not know is
  refused on both.** `retention.ms` went back to `604800000/DEFAULT`. Measured raw first, an unknown key
  and an unreadable value were refused as the Java client's `InvalidConfigurationException` and as
  librdkafka's per-resource `INVALID_CONFIG` (40). They are now `IllegalArgumentException` on both arms. A
  call pairing a bad value with a good one applies neither. That is checked after a sentinel change is seen,
  so an applied half could not still be on its way. A topic that does not exist is each client's own failure,
  compared as refused.
- **Found: a change is visible a moment after the call returns, not at once.** The first full run failed on
  the native arm: a describe right after the set still read `1048588/DEFAULT`. The controller accepts the
  change, and the broker's view follows. The test now waits for it, with a bound, and records how long it
  took: 7 to 110 ms. The contract says so.
- **Found in the runner, not the product.** The tool prints no source, only synonyms. A key nobody set has
  `synonyms={}`, and the first reading of that as `UNKNOWN` made the run red on 9 keys where both clients
  said `DEFAULT`, correctly.
- **Mutants:** all six killed, each by name on its arm:
  - the deletes dropped (JVM, native);
  - the topic source mapped to `BROKER` (native, and the common mapping the JVM uses);
  - the `INVALID_CONFIG` mapping removed (native, JVM).
