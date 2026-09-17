---
id: feature-backpressure-and-accounting
title: Backpressure, and accounting for every record
type: feature
status: draft
owner: unassigned
involved_services:
  - kafkakn-core
  - test-broker
client_entries: []
api:
  - producer-contract
tags: [producer, correctness]
---

# Backpressure, and accounting for every record

## 1. Overview

A producer that is faster than its broker must do something, and the thing it must **not** do is
lose records while reporting success. This feature exists as its own document because the underlying
native library makes that outcome the easy one to build by accident: a measured naive binding lost
**264 826 of 1 000 000** records with every indicator green
([research §1.4](../research/research-architecture.md)).

**`status: draft`: nothing here is built.** Every scenario is *target*.

## 2. Business rules

- **The unit of truth is what the caller asked to send.** Not what was enqueued, not what produced a
  delivery report.
- **A full queue is backpressure, not an error.** `send` suspends. It does not return a failure the
  caller may ignore, and it does not block the thread.
- **A delivery-report count is never presented as a success rate.** It answers a narrower question:
  how many of the records that were *queued* arrived.
- **`flush` returns when the outbound queue is empty**, measured as the queue length reaching zero —
  not as the return value of the underlying flush call, which is an error code.

## 3. Scenarios (BDD / test cases)

Every one is **target**: nothing is built.

### Scenario: Producing past the queue bound loses nothing
* **Given:** a producer whose queue bound is lowered to 1 000 records, and a broker slow enough that
  the queue fills.
* **When:** 100 000 records are sent.
* **Then:** every `send` returns normally or throws, and the topic's end offsets have grown by
  exactly 100 000.
* *The assertion that would have caught the measured loss of 264 826 records.*

### Scenario: A full queue suspends rather than failing
* **Given:** a producer at its queue bound.
* **When:** another record is sent.
* **Then:** the call has not returned; it returns once the queue drains.

### Scenario: Cancelling a suspended send leaves nothing queued
* **Given:** a `send` suspended on a full queue.
* **When:** its coroutine is cancelled.
* **Then:** the record is not delivered, and the end offsets do not move for it.

### Scenario: Both actuals account identically under backpressure
* **Given:** the same 100 000 records and the same queue bound.
* **When:** they are produced by the JVM actual and by the native actual.
* **Then:** both report the same number of successes, and the broker's end offsets agree with both.
* *The JVM client blocks on `max.block.ms` and librdkafka refuses; the contract flattens both into
  "suspends", and this is where that flattening is checked.*

### Scenario: flush waits for the queue, not for a return code
* **Given:** records in flight.
* **When:** `flush` is called.
* **Then:** it returns only after the outbound queue length is zero.

## 4. Quirks

- **A small round trip cannot find any of this.** The queue bound is 100 000 records by default; a
  2 000-record test never reaches it. The bound is lowered in the suite so the case is reachable in
  a test that finishes.
- **A record that was never queued produces no delivery report at all.** This is the mechanism that
  makes the loss invisible: the callback is correct, the count is correct, and the question they
  answer is the wrong one.
- **`rd_kafka_flush` returns an error code.** Reading it as a count printed `-185`, which is
  `RD_KAFKA_RESP_ERR__TIMED_OUT` in a field labelled "unflushed".

## 5. Code anchors

| What | Where |
|---|---|
| the suspending seam on the native side | `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/NativeProducer.kt` |
| the scenarios above | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/BackpressureTest.kt` |
| the contract they hold both actuals to | [producer-contract](../api/producer-contract.md) |
