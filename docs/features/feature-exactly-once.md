---
id: feature-exactly-once
title: Exactly-once read-process-write
type: feature
status: active
owner: unassigned
involved_services:
  - kafkakn-core
  - test-broker
client_entries: []
api:
  - producer-contract
  - consumer-contract
tags: [consumer, producer, transactions]
---

# Exactly-once read-process-write

## 1. Overview

A processor reads records, writes one output record per input record, and commits its progress
**inside the same transaction** as its output. If it stops at any point and a new instance takes over,
every input record appears in the committed output exactly once.

**Built and measured on both arms.** The loop is the caller's. kafkakn provides the parts:
- transactions on the producer ([B-30](../backlog/B-30-transactions.md));
- `groupMetadata()` on the consumer, and `sendOffsetsToTransaction` on the producer
  ([B-38](../backlog/B-38-exactly-once-read-process-write.md)).

## 2. Business rules

- The consumer reads with `isolation.level=read_committed`, the contract's default on both arms.
- Progress is committed with `sendOffsetsToTransaction(offsets, consumer.groupMetadata())`, where each
  offset is the next one to read. It is not committed with the consumer's `commit`.
- A new instance with the same `transactional.id` fences the old one, and whatever the old one left open
  is aborted.
- `groupMetadata()` is good only for a producer on the same arm, in the same process.

## 3. Code anchors

| Service | Code |
|---|---|
| kafkakn-core | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt`: `sendOffsetsToTransaction` |
| kafkakn-core | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.kt`: `groupMetadata` |
| kafkakn-core | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/Transactions.kt` |

## 4. Scenarios (BDD / test cases)

### Scenario: Every input record reaches the output exactly once across stops
* **Given:** an input topic that a third party writes 300 records into as the processor runs.
* **When:** the processor is stopped three times, each time after it has committed work, and each time
  after its output or after its offsets but before the commit. Each time a new instance takes over.
* **Then:** under `read_committed`, the output holds each of the 300 input records exactly once, and none
  twice. Under `read_uncommitted`, more records are visible: the aborted attempts. That shows the stops
  actually left transactions to abort.
* **Automated:** `ExactlyOnceTest.every_input_record_reaches_the_output_exactly_once_across_stops` on
  each arm, counted through `kafka-console-consumer` by `ci/b-38/run.sh`.

## 5. Out of scope

A built-in read-process-write loop: the loop is the caller's, and the test shows one. Exactly-once across
two clusters, or towards anything that is not Kafka.
