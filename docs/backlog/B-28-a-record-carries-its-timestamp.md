---
id: B-28
title: "A record carries its timestamp, and the metadata says which time the broker kept"
status: done
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
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/TimestampTest.kt`,
  `ci/b-28/run.sh`, `ci/harness/broker.sh`, `docs/api/producer-contract.md`.

## What happened

`timestamp: Long? = null` on `ProducerRecord` and `timestamp: Long` on `RecordMetadata`. The JVM arm
passes it to `ProducerRecord(topic, partition, timestamp, …)` and reads `RecordMetadata.timestamp()`;
the native arm adds `RD_KAFKA_VTYPE_TIMESTAMP` (`int64_t`, the union's `i64`) only when set, and reads
`rd_kafka_message_timestamp` in the delivery report. A negative timestamp is refused where the record
is made — `-1` is what both clients use internally for "no timestamp".

**The type field the item left open is decided by reading, not by preference.** librdkafka's
`rd_kafka_message_timestamp` returns the value and its type; the Java client's `RecordMetadata` has
`timestamp()` and nothing that says which kind. A type field would be one arm's fact and the other's
guess, so there is none, and the contract says why.

**Red first on both arms**, for the intended reason: both reported `-1` until wired, failing the three
timestamp assertions; the negative-timestamp refusal passed from the start because it lives in the
record.

**By the broker's own reading** (`ci/b-28/run.sh`, `kafka-console-consumer` printing each record's
stored timestamp and its type):

| | named 1600000000000 on an ordinary topic | named 1600000000000 on a LogAppendTime topic |
|---|---|---|
| jvm | `CreateTime:1600000000000` | `LogAppendTime:1790283630243` |
| linuxX64 | `CreateTime:1600000000000` | `LogAppendTime:1790283632676` |

`RecordMetadata.timestamp` equalled the broker's kept time on both topics on both arms, and a record
that names none carries the client's clock at `send`.

**The LogAppendTime topic is the fixture's, not the item's.** Six scripts run the whole suite, and
each creates its own topics by hand; a seventh added to six lists is the list that goes stale. So
`broker.sh up` creates it once, the default name in the suite names something the fixture always
makes — the condition a default has to meet since B-24 — and `ci/b-28/run.sh` asks the broker to
confirm the topic's configuration before reading anything from it.

**`RecordMetadata` gained a required field, which breaks anything constructing one.** It is a
snapshot and nothing outside this repository is known to build one; inside, only the test double
`NaiveProducer` did.
