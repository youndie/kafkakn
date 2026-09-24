---
id: B-29
title: "partitionsFor: what a topic looks like, from the producer that writes to it"
status: open
priority: P2
size: S
stage: stage-5-producer-parity
---

# B-29 — topic metadata

A caller who names partitions ([B-27](B-27-a-record-can-name-its-partition.md)) needs to know how
many there are. Both clients answer that from the producer itself — `Producer.partitionsFor(topic)` on
the JVM, `rd_kafka_metadata` on native ([research §1.8](../research/research-architecture.md)).

- **The decision and its reason.** `suspend fun partitionsFor(topic: String): List<PartitionInfo>` —
  partition id, leader, replicas, in-sync replicas. Suspending, and **off the caller's dispatcher on
  both arms**: the Java call blocks for up to `max.block.ms` and `rd_kafka_metadata` blocks for its
  timeout, which is exactly the thread-holding §2.13 found and fixed for `send`.
- An unknown topic fails on both arms, and how is recorded rather than promised.
- The rejected alternative is a separate metadata client. The producer already holds the connection
  and the cache; a second client would be a second set of sockets for one question.
- Not covered: cluster-wide description, which is administration ([B-34](B-34-a-minimal-admin.md)).

- AC: the answer agrees with `kafka-topics.sh --describe` on partition count, leaders and replicas, on
  both arms.
- AC: the call does not hold the caller's dispatcher — held against a single-threaded dispatcher the
  way `JvmDispatcherSeamTest` holds `send`.
- AC: an unknown topic's failure is recorded per arm.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt`,
  `docs/api/producer-contract.md`.
