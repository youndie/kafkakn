---
id: B-47
title: "A producer can write a tombstone: a record whose value is null"
status: open
priority: P1
size: S
stage: stage-11-everyday-gaps
---

# B-47 — a producer can write a tombstone: a record whose value is null

`ProducerRecord.value` is `ByteArray`, not nullable (`ProducerRecord.kt`). So kafkakn cannot write a
tombstone. On a compacted topic that means it cannot delete a key, which every other client can. The
consumer side already reads one (`ConsumerRecord.value: ByteArray?`), and the consumer contract says
it plainly: *"this library cannot write a tombstone yet."*

- **The decision and its reason.** `value: ByteArray?`. A null value and an empty one are two
  different records, and the header work already settled the same question the same way (a header
  with no value is not one with an empty value).
- Native: `RD_KAFKA_VTYPE_VALUE` with a null pointer and length 0. JVM: `null` to `ProducerRecord`.
  Whether each arm keeps null and empty apart is the test, not an assumption.
- It changes a public type. The artefact is a snapshot with no version promise, and callers that
  construct records are unaffected; callers that read `ProducerRecord.value` get a nullable value.
- Not covered: deserialisers, and deciding compaction for the caller.

- AC: a null value and an empty value, sent by each arm, reach the broker as two different records.
  `kafka-console-consumer` prints `null` for one and an empty value for the other.
- AC: on a topic with `cleanup.policy=compact`, a key's tombstone sent by each arm removes the key
  after compaction, as the broker's own consumer reads the topic.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerRecord.kt`,
  `docs/api/producer-contract.md`, `docs/api/consumer-contract.md`.
