---
id: B-47
title: "A producer can write a tombstone: a record whose value is null"
status: done
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

## Findings (2026-09-25)

- **AC: null and empty reach the broker as two different records.** Measured by `ci/b-47/run.sh` on
  both arms. The distribution's own client (`records dump`) reads each key's last record as `~` for the
  null value and `x`, zero bytes, for the empty one. The item had named `kafka-console-consumer` as the
  reader; the dump is the stricter one. The console consumer prints a null as the text `null`, which a
  four-byte value `null` would also print.
- **AC: compaction.** After the cleaner has run, the key that got a tombstone holds only the tombstone,
  and the key that got an empty value keeps it. The earlier value of both is gone. The fixture
  `kafkakn-compact` is one partition, `cleanup.policy=compact`, with one-second segments.
- **The first compaction wait found nothing to wait for.** The records have to leave the active
  segment, and a segment rolls only when a record arrives after `segment.ms`. The script's rolling
  record had no key. A compacted topic refuses that ("Compacted topic cannot accept message without
  key"), and `broker.sh produce` discards the refusal with its stderr. So the cleaner ran on the older
  segments and never saw this run's records. `broker.sh produce-keyed` exists for that, and the script
  now checks that the rolling record landed.
- **Mutants, each caught by name** in step 1 of the run:
  - native null sent as empty: *"linuxX64: the null value was stored as 'x'"*;
  - JVM null sent as empty: *"jvm: the null value was stored as 'x'"*;
  - native empty sent as null: *"linuxX64: the empty value was stored as '~'"*.
- Documents: the producer contract has a section on tombstones, with how each arm passes them. The
  consumer contract's *"cannot write a tombstone yet"* is corrected. The produce feature document has
  the scenario, with its `**Automated:**` line.
