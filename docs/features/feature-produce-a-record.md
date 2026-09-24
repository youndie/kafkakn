---
id: feature-produce-a-record
title: Produce a record
type: feature
status: active
owner: unassigned
involved_services:
  - kafkakn-core
  - test-broker
client_entries: []
api:
  - producer-contract
tags: [producer]
---

# Produce a record

## 1. Overview

A Kotlin service — on the JVM or compiled to a single native binary — hands a record to kafkakn and
gets back where it landed. That is the whole feature. Everything else in this repository exists to
make this one call honest on both platforms.

**`status: draft`: nothing here is built.** Every scenario below is *target*.

## 2. Business rules

- A record carries a topic, an optional key, and a value as bytes. Values are bytes, not `String`:
  Kafka's value space has no encoding, and the first non-UTF-8 payload is what finds out.
- With a key, partition assignment is by key and is stable. Without one, the client distributes.
- `send` returns only after the broker has acknowledged at the configured `acks`.
- Configuration keys are Kafka's own names ([producer-contract](../api/producer-contract.md)).

## 3. Scenarios (BDD / test cases)

Every one is **target**: nothing is built.

### Scenario: A record with no key reaches the topic
* **Given:** a producer against the test broker with `acks=all`, and a topic with 3 partitions.
* **When:** 100 records with no key are sent.
* **Then:** every `send` returns `RecordMetadata` naming the topic, the topic's summed end offsets
  have grown by exactly 100, and `kafka-console-consumer` reads back all 100.
* **Automated:** `ProduceTest.a_record_with_no_key_reaches_the_topic` (jvm), with the broker side
  checked by `ci/b-06/run.sh`.

### Scenario: Records with the same key land on one partition
* **Given:** a producer against the test broker.
* **When:** 50 records with the key `k` are sent.
* **Then:** every returned `RecordMetadata` names the same partition, and that partition's end
  offset has grown by exactly 50.
* **Automated:** `ProduceTest.records_with_the_same_key_land_on_one_partition` (jvm).

### Scenario: Both actuals choose the same partition for the same key
* **Given:** the same 200 keys.
* **When:** they are produced by the JVM actual and by the native actual.
* **Then:** the partition chosen for each key is identical on both.
* **Automated:** `PartitionerAgreementTest` on both arms, compared by
  `ci/harness/compare-arms.sh`.
* *This is the differential oracle, and it earned its keep the first time it ran — five of eight
  keys disagreed ([research §2.2](../research/research-architecture.md)).*

### Scenario: A topic that does not exist is an error, not a silence
* **Given:** the test broker with auto-creation off.
* **When:** a record is sent to a topic that does not exist.
* **Then:** `send` throws, and the message names the topic.
* **Automated:** `ProduceTest.a_topic_that_does_not_exist_is_an_error_not_a_silence` (jvm).

### Scenario: A value is bytes, not text
* **Given:** a value that is not valid UTF-8.
* **When:** it is sent and read back by an independent consumer.
* **Then:** the bytes read are identical to the bytes sent.

### Scenario: Headers reach the broker with their bytes intact
* **Given:** a record carrying `trace=1`, `schema=kafkakn.v1`, `trace=2` — a duplicate name, in that
  order.
* **When:** it is produced on either arm.
* **Then:** an independent reader sees all three, in that order, with the same bytes.
* **Automated:** `HeadersTest.headers_reach_the_broker_with_their_bytes_intact`, read back by
  `ci/b-10/run.sh` with `kafka-console-consumer --property print.headers=true`; the two arms'
  renderings are held against each other as well.
* *Kafka's headers are an ordered sequence in which a name may repeat. A client that stored them in
  a map would answer this with two entries instead of three, and a consumer reading `lastHeader`
  would get a different value from one that iterates.*

### Scenario: A header with no value is not a header with an empty one
* **Given:** a record with `absent` carrying no value and `empty` carrying zero bytes.
* **When:** it is produced on either arm.
* **Then:** the two remain distinguishable to a reader.
* **Automated:** `HeadersTest.a_header_with_no_value_is_not_a_header_with_an_empty_one`; the script
  asserts that the two render **differently**, rather than asserting how either one renders.

### Scenario: acks reaches the broker and is honoured
* **Given:** a topic whose `min.insync.replicas` exceeds the in-sync set, and a producer with
  `acks=all`.
* **When:** a record is sent.
* **Then:** `send` throws carrying the broker's own refusal, naming the replicas.
* **And** the same topic accepts a record at `acks=1`, so the refusal was about `acks` and not about
  the topic being unusable.
* **Automated:** `ProduceTest.acks_reaches_the_broker_and_is_honoured` (jvm).
* *An **invalid** value would prove nothing: `kafka-clients` refuses `acks=99` at construction,
  before any broker sees it (B-06). The probe has to be a valid value the broker cannot satisfy.*

### Scenario: The producer describes a topic as the broker does
* **Given:** a topic of seven partitions — a count no other topic in the fixture has.
* **When:** `partitionsFor` is asked about it, and about a topic that does not exist.
* **Then:** the partitions, leaders, replicas and in-sync replicas are what `kafka-topics.sh
  --describe` says; the unknown topic fails; and neither call holds the caller's dispatcher.
* **Automated:** `TopicMetadataTest`, compared with the broker's description by `ci/b-29/run.sh`
  ([B-29](../backlog/B-29-topic-metadata.md)). How the unknown topic fails differs per arm and is in
  [producer-contract](../api/producer-contract.md).

### Scenario: Records in a transaction become visible together, or not at all
* **Given:** a transactional producer (`transactional.id`) on each arm.
* **When:** 50 records are committed in one transaction, and 50 more aborted in another — directly and
  through `inTransaction`.
* **Then:** under `read_committed` the committed 50 are all there and the aborted none; under
  `read_uncommitted` the aborted 50 are there too; the coordinator reports `CompleteCommit` and
  `CompleteAbort`.
* **Automated:** `TransactionTest`, counted by `ci/b-30/run.sh` through `kafka-console-consumer`
  under both isolation levels and `kafka-transactions.sh describe`
  ([B-30](../backlog/B-30-transactions.md)).

### Scenario: A fenced producer fails with one exception on both arms
* **Given:** a producer in an open transaction.
* **When:** a second producer with the same `transactional.id` initialises.
* **Then:** the first one's commit throws `ProducerFencedException`, and so does its next `send`; its
  open transaction is aborted.
* **Automated:** `TransactionTest.a_fenced_producer_fails_with_one_kafkakn_exception_on_both_arms`.
  Watched failing first on both arms with each client's own exception.

## 4. Quirks

- **The offsets are the oracle, never this library's own consumer.** A producer checked by its own
  consumer can be wrong in both directions at once
  ([test-broker](../services/test-broker.md)).
- **`acks` must be shown reaching the broker**, and an invalid value does not show it. A producer
  whose `acks` was silently dropped behaves identically to one that honoured it until something goes
  wrong — but `kafka-clients` rejects an invalid value at construction, so nothing is sent and
  nothing is proved. The suite uses a *valid* value the broker cannot satisfy.
- **The two clients do not partition keys the same way by default.** librdkafka uses CRC32
  (`consistent_random`), the Java producer uses murmur2. kafkakn sets librdkafka's
  `murmur2_random`, which its own documentation calls equivalent to the Java default. Found by the
  differential oracle on the first day both arms existed; neither arm could have noticed alone
  ([research §2.2](../research/research-architecture.md)).
- **The two arms refuse a bad configuration value in different places.** The JVM client validates at
  construction; librdkafka accepts and lets the broker decide. The contract promises only that it
  fails, and says so.

## 5. Code anchors

| What | Where |
|---|---|
| the interface | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt` |
| the scenarios above | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/ProduceTest.kt` |
| the broker fixture | `ci/broker/docker-compose.yml` |
