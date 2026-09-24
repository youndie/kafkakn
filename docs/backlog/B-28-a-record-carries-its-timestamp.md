---
id: B-28
title: "A record carries its timestamp, and the metadata says which time the broker kept"
status: wip
priority: P1
size: S
stage: stage-5-producer-parity
---

# B-28 — a record carries its timestamp

Every Kafka record has a timestamp, and both clients let the caller set it
([research §1.8](../research/research-architecture.md): the Java `ProducerRecord` constructor takes a
`Long` timestamp; `rd_kafka_produceva` has the field). kafkakn does not, so every record carries the
moment the client happened to enqueue it — which is not the event time a caller replaying history
means.

- **The decision and its reason.** `timestamp: Long? = null` on `ProducerRecord`, epoch milliseconds,
  and `timestamp: Long` on `RecordMetadata`. Milliseconds because that is the wire; a
  `kotlin.time.Instant` would be a conversion the protocol does not have, and a caller who holds one
  converts it where they can see the decision.
- **The metadata's timestamp is the interesting half.** On a topic configured with
  `message.timestamp.type=LogAppendTime` the broker replaces the caller's time with its own, and the
  metadata should say what the broker kept. Whether both arms report it the same way is measured, not
  assumed.
- The rejected alternative is leaving `RecordMetadata` as it is. A caller who set a timestamp on a
  LogAppendTime topic would then have no way to learn it was discarded.
- Not covered: timestamp type as a separate field — decided inside the item once the two arms have
  been read.

- AC: a record with a timestamp arrives with exactly it, read by `kafka-console-consumer --property
  print.timestamp=true` on both arms.
- AC: on a LogAppendTime topic both arms return the broker's time in `RecordMetadata`, and the
  record's own timestamp is shown to have been replaced.
- AC: a record without one still gets a timestamp, and the contract says whose clock it is.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerRecord.kt`,
  `docs/api/producer-contract.md`.
