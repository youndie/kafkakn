---
id: B-38
title: "Exactly-once read-process-write: offsets committed inside the producer's transaction"
status: done
priority: P3
size: L
stage: stage-8-consume
blocked_by: [B-30, B-37]
---

# B-38 — exactly-once read-process-write

The last piece of what other clients call exactly-once: consume, transform, produce, and commit the
consumed offsets **inside** the producer's transaction, so that the output and the progress become
visible together or not at all. `sendOffsetsToTransaction` exists on both arms
([research §1.8](../research/research-architecture.md)); it needs the consumer's group metadata, which
is why it waits for [B-30](B-30-transactions.md) and [B-37](B-37-consumer-groups.md).

- **The decision and its reason.** One call, `sendOffsetsToTransaction`, on the transactional
  producer, taking what the consumer contract of [B-35](B-35-the-consumer-designed-first.md) calls its
  group metadata. Nothing more: the loop around it is the caller's.
- **`isolation.level` matters here more than anywhere.** The arms disagree on its default (§1.8), and a
  read-process-write loop that reads uncommitted input is not exactly-once whatever it does with its
  output. The consumer contract's decision on that default is a precondition of this item's green.
- The oracle is an input topic written by `kafka-console-producer`, an output topic read under
  `read_committed` by `kafka-console-consumer`, and a process stopped part way through.
- Not covered: Kafka Streams-style state.

- AC: with the processor stopped at random points and restarted, every input record appears in the
  output **exactly once** under `read_committed`, on both arms.
- AC: under `read_uncommitted` the same run shows the aborted attempts — proof that the fixture
  produced the failures the green is about.
- Anchors: `docs/research/research-architecture.md`, `ci/harness/broker.sh`.

## Findings (2026-09-25)

**Measured, `ci/b-38/run.sh`, both arms.** 300 input records were trickled in by a third party. The
processor was stopped three times at random points (seeded, and the seed is recorded) and restarted
with the same `transactional.id`. Each stop came after one to three committed batches, either after
the output or after the offsets, and left the transaction open.
- Under `read_committed`: 300 of 300, each input exactly once.
- Under `read_uncommitted`: 8 aborted attempts on the JVM, 16 on native. That is exactly the sum of the
  batches the stops abandoned (6+1+1 and 1+14+1).

**The first version of the test could not fail.** Every stop fell in an instance's first batch, before
anything had been committed. The last instance then processed everything from the start, and the run
passed with the native `sendOffsetsToTransaction` adding no offsets at all. Stops now follow committed
work, and that mutant leaves 395 output records for 300 inputs under `read_committed` (`logs/b-38/`).
The run script also refuses a run whose stops do not all follow committed work.

**Native group metadata travels as librdkafka's serialised bytes**
(`rd_kafka_consumer_group_metadata_write`/`_read`, "for client binding use"). No C object has to
outlive the call that needs it.
