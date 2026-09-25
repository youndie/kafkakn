---
id: B-38
title: "Exactly-once read-process-write: offsets committed inside the producer's transaction"
status: wip
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
