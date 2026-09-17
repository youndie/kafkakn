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
* *This is the differential oracle — [research §1.1](../research/research-architecture.md).*

### Scenario: A topic that does not exist is an error, not a silence
* **Given:** the test broker with auto-creation off.
* **When:** a record is sent to a topic that does not exist.
* **Then:** `send` throws, and the message names the topic.
* **Automated:** `ProduceTest.a_topic_that_does_not_exist_is_an_error_not_a_silence` (jvm).

### Scenario: A value is bytes, not text
* **Given:** a value that is not valid UTF-8.
* **When:** it is sent and read back by an independent consumer.
* **Then:** the bytes read are identical to the bytes sent.

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

## 4. Quirks

- **The offsets are the oracle, never this library's own consumer.** A producer checked by its own
  consumer can be wrong in both directions at once
  ([test-broker](../services/test-broker.md)).
- **`acks` must be shown reaching the broker**, and an invalid value does not show it. A producer
  whose `acks` was silently dropped behaves identically to one that honoured it until something goes
  wrong — but `kafka-clients` rejects an invalid value at construction, so nothing is sent and
  nothing is proved. The suite uses a *valid* value the broker cannot satisfy.
- **The two arms refuse a bad configuration value in different places.** The JVM client validates at
  construction; librdkafka accepts and lets the broker decide. The contract promises only that it
  fails, and says so.

## 5. Code anchors

| What | Where |
|---|---|
| the interface | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt` |
| the scenarios above | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/ProduceTest.kt` |
| the broker fixture | `ci/broker/docker-compose.yml` |
