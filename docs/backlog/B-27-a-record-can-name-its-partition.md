---
id: B-27
title: "A record can name its partition, as it can in every other client"
status: open
priority: P1
size: S
stage: stage-5-producer-parity
---

# B-27 — a record can name its partition

`ProducerRecord` carries topic, key, value and headers. Both clients underneath take an explicit
partition — `ProducerRecord(topic, partition, …)` on the JVM, a field of `rd_kafka_produceva` on
native ([research §1.8](../research/research-architecture.md)) — and a caller who manages its own
ordering or co-partitions two topics cannot use kafkakn without it.

- **The decision and its reason.** `partition: Int? = null` on `ProducerRecord`. When it is set the
  partitioner is not consulted; when it is null nothing changes, including the `murmur2_random`
  default of §2.2.
- **Where the arms may differ is the failure, and it is observed rather than promised.** A partition
  that does not exist fails on both — but how, how fast and with what exception is a question for
  `recordObservation`, the same way §2.9 treated a peer that cannot be verified.
- The rejected alternative is a custom partitioner interface. It would run Kotlin code on librdkafka's
  thread for every record, which is §2.3's territory, and a caller who wants a partition can compute
  it and name it.
- Not covered: timestamps ([B-28](B-28-a-record-carries-its-timestamp.md)), topic metadata
  ([B-29](B-29-topic-metadata.md)).

- AC: records sent with explicit partitions land on exactly those partitions on both arms, read by
  `kafka-console-consumer --partition`.
- AC: a keyed record with an explicit partition goes where the partition says, not where the key
  hashes — the test that shows the partitioner was bypassed.
- AC: a partition that does not exist fails on both arms, and what each says is recorded.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerRecord.kt`,
  `docs/api/producer-contract.md`.
