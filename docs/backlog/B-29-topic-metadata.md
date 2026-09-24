---
id: B-29
title: "partitionsFor: what a topic looks like, from the producer that writes to it"
status: done
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

## Findings (2026-09-24)

**Measured, `ci/b-29/run.sh`.** A fresh topic of seven partitions — a count nothing else in the
fixture has — described by both arms exactly as `kafka-topics.sh --describe` describes it,
`partition:leader:replicas:isr` for all seven. An unknown topic fails on both: native in 57 ms with
the broker's *"Unknown topic or partition"*, read from the described topic's own error; the JVM with
`TimeoutException` after `max.block.ms` (20 021 ms; 17 081 ms in another run). Recorded in
[producer-contract](../api/producer-contract.md), not promised.

**The dispatcher test was wrong first, and the order of the work is how that was found.** Written
against a deliberately blocking implementation, it passed on both arms: the ticker started its clock
after the call had finished. Corrected, the blocking version held a single-lane dispatcher for 20 s
(JVM) and 5 s (native); on `Dispatchers.IO` it does not. [Research §2.22](../research/research-architecture.md).

**§2.18 again, caught by the full suite.** The test's default topic, `kafkakn-metadata`, was created
only by B-29's own script, so the full run through `ci/b-11/run.sh` failed on it after 60 s. It is
now the fixture's topic, created on every `broker.sh up` beside `kafkakn-logappend`.

**Left out, as new work.** The same reading showed the native `flush` calling a blocking
`rd_kafka_flush` on the caller's thread, against §2.13's "never blocking at all" —
[B-43](B-43-native-flush-may-hold-the-callers-thread.md), not measured here.
