---
id: B-27
title: "A record can name its partition, as it can in every other client"
status: done
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
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/ExplicitPartitionTest.kt`,
  `ci/b-27/run.sh`, `docs/api/producer-contract.md`.

## What happened

`partition: Int? = null` on `ProducerRecord`, the fifth field so every existing call is unchanged; the
JVM arm passes it to `ProducerRecord(topic, partition, …)` and the native arm adds an
`RD_KAFKA_VTYPE_PARTITION` entry to the `rd_kafka_produceva` array — `int32_t`, the union's `i32`, as
rdkafka.h declares for that tag — only when it is set. A negative partition is refused where the
record is made: librdkafka's own "unassigned" is -1, the value a caller could pass by mistake and have
silently mean "let the partitioner choose".

**Red first, for the intended reasons**: the field was added before either arm read it, so the test
compiled and failed because records went where the partitioner put them, a key won over the named
partition, and partition 99 "succeeded" because nobody read it.

**By the broker's own reading** (`ci/b-27/run.sh`, each partition read on its own by
`kafka-console-consumer --partition`): 50 of 50 on the named partition and 0 on the other two, for
each of three partitions, on both arms. And a keyed record with a named partition lands on the named
one, not the one its key hashes to.

**A partition the topic does not have fails on both arms — four thousand times apart.**

| | what it says | time |
|---|---|---|
| JVM | *"Partition 99 of topic … with partition count 3 is not present in metadata after 20000 ms"* | 20 004 ms — `max.block.ms`, 60 s at the default |
| native | *"…: Local: Unknown partition"* | 4 ms |

Recorded rather than equalised, and in the contract: the Java client waits for metadata that might
still grow the topic, librdkafka answers from what it has.

**The runner was wrong once and said so.** Its test tasks answered `exit=0` without running a test:
the topic arrives through the environment, which Gradle does not track as an input, so an unchanged
tree found the tasks up to date — and the observations they would have written had just been
deleted. The runner's "no stamp from jvm — its test did not run" guard is what caught it. Both this
runner and `ci/b-26/run.sh`, which had the same flaw latent, now pass `--rerun`.
