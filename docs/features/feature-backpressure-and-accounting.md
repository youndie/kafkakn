---
id: feature-backpressure-and-accounting
title: Backpressure, and accounting for every record
type: feature
status: active
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

**Built and measured**, except where a scenario below says ***target***. The accounting is checked
against the broker's end offsets, and the check has been watched failing against a producer that
drops records ([research §2.7](../research/research-architecture.md)).

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

### Scenario: Producing past the queue bound loses nothing
* **Given:** a producer whose queue bound is lowered to 1 000 records, and a broker slow enough that
  the queue fills.
* **When:** 100 000 records are sent.
* **Then:** every `send` returns normally or throws, and the topic's end offsets have grown by
  exactly 100 000.
* **Automated:** `AccountingTest.the_broker_holds_every_record_the_caller_handed_in` and
  `BackpressureTest.producing_past_the_queue_bound_loses_nothing` on both arms, with the broker side
  checked by `ci/b-09/run.sh` and `ci/b-08/run.sh`. Run at 3 000 records against a queue bound of
  100, which overruns it many times over and finishes in a test.
* *The assertion that would have caught the measured loss of 264 826 records.* The end offsets are
  read by the script rather than asserted inside the test: a producer verified by a consumer of ours
  could be wrong in both directions at once, and there is no consumer here to be wrong with.
  Measured 2026-09-17: 3 000 handed in, end offsets 0 -> 3 000, on each arm.

### Scenario: A full queue suspends rather than failing
* **Given:** a producer at its queue bound.
* **When:** another record is sent.
* **Then:** the call has not returned; it returns once the queue drains.

### Scenario: Cancelling a send suspended on a full queue
* **Given:** a `send` suspended on a full queue, the broker not answering.
* **When:** its coroutine is cancelled at a deadline, and the broker answers later.
* **Then, native:** the caller is back at the deadline, the record is never queued, and the end offsets do
  not move for it.
* **Then, JVM (measured, not the promise this scenario first made):** the caller is back only when
  `kafka-clients` lets go of its blocking `send`: when there is room, or at `max.block.ms`. By then the
  record is queued, and it lands. This scenario used to promise "not delivered" for both arms, with no
  test behind it; B-73 measured the JVM doing otherwise.
* **Automated:** `CancelledSendTest.a_send_cancelled_while_it_waits_for_room_is_measured`, read against the
  broker by `ci/b-73/run.sh`.

### Scenario: Cancelling a send after its record was queued does not recall it
* **Given:** a `send` whose record is queued, the broker not answering.
* **When:** its coroutine is cancelled at a deadline, and the broker answers later.
* **Then:** the caller is back at the deadline, and the record lands, on both arms.
* **Automated:** `CancelledSendTest.a_send_cancelled_after_its_record_was_queued_still_lands`, read against
  the broker by `ci/b-73/run.sh`.

### Scenario: Both actuals account identically under backpressure
* **Given:** the same 100 000 records and the same queue bound.
* **When:** they are produced by the JVM actual and by the native actual.
* **Then:** both report the same number of successes, and the broker's end offsets agree with both.
* **Automated:** `AccountingTest.the_broker_holds_every_record_the_caller_handed_in`, per arm on its
  own topic, reconciled by `ci/b-09/run.sh`. Run at 3 000 records rather than 100 000.
* *The JVM client blocks on `max.block.ms` and librdkafka refuses; the contract flattens both into
  "suspends", and this is where that flattening is checked.*

### Scenario: The accounting is red when records are dropped
* **Given:** the same test, and a producer that counts an enqueue refusal and moves on — answering
  every `send` with a `RecordMetadata` for a record it never sent.
* **When:** the run is repeated against it.
* **Then:** the test fails on **both** arms, and the topic's end offsets are short by exactly the
  number the dropping producer admits to.
* **Automated:** the second pass of `ci/b-09/run.sh`, which fails if that pass is green. Measured
  2026-09-17: 100 of 3 000 landed on the jvm arm, 103 on the native arm, shortfall equal to the
  drops on both.
* *A guard nobody has watched fail is a guard whose failure mode is unknown, and four checks in this
  repository have passed while testing nothing.*

### Scenario: flush waits for the queue, not for a return code
* **Given:** records in flight.
* **When:** `flush` is called.
* **Then:** it returns only after the outbound queue length is zero.
* **Automated:** `BackpressureTest.flush_waits_for_the_queue_rather_than_for_a_return_code`.

## 4. Quirks

- **A serial caller cannot find any of this either.** `send` awaits the acknowledgement, so a caller
  that awaits each record has one in flight and never fills a queue of any size. The first version
  of the test was serial and exercised nothing; the vacuity guard is what said so
  ([research §2.5](../research/research-architecture.md)).
- **The two clients name the bound differently** — librdkafka counts records
  (`queue.buffering.max.messages`), the Java client counts bytes (`buffer.memory`) and waits
  (`max.block.ms`). The suite lowers it through a per-arm helper because there is no common spelling
  to give them ([research §2.6](../research/research-architecture.md)).
- **`runTest`'s virtual time skips the wait being tested.** The backpressure tests run on
  `Dispatchers.Default` for that reason.
- **A small round trip cannot find any of this.** The queue bound is 100 000 records by default; a
  2 000-record test never reaches it. The bound is lowered in the suite so the case is reachable in
  a test that finishes.
- **End offsets are a count only on a topic nothing transactional writes to.** A commit or abort
  marker takes an offset, and aborted records take theirs; on such a topic the reconciliation counts
  records under a named isolation level instead ([B-30](../backlog/B-30-transactions.md),
  [producer-contract](../api/producer-contract.md)). The accounting topic here is written by
  non-transactional producers only, which is what keeps the delta meaningful.
- **The accounting topic is fresh, and per arm.** The oracle is a delta on end offsets, and a delta
  is only attributable while nothing else writes to the topic — both arms run against one broker in
  one pass, so a shared topic would add their two counts into a number no assertion could check.
- **The deliberately naive producer is a test-only wrapper, not a second binding.** It simulates the
  refusal with a permit count, so it says nothing about librdkafka's refusal path; what it
  reproduces is the condition the guard catches, identically on both arms
  ([research §2.7](../research/research-architecture.md)).
- **A record that was never queued produces no delivery report at all.** This is the mechanism that
  makes the loss invisible: the callback is correct, the count is correct, and the question they
  answer is the wrong one.
- **`rd_kafka_flush` returns an error code.** Reading it as a count printed `-185`, which is
  `RD_KAFKA_RESP_ERR__TIMED_OUT` in a field labelled "unflushed".

## 5. Code anchors

| What | Where |
|---|---|
| the suspending seam on the native side | `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt` |
| the scenarios above | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/BackpressureTest.kt` |
| a cancelled send, both moments | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/CancelledSendTest.kt`, `ci/b-73/run.sh` |
| the accounting against the broker | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/AccountingTest.kt` |
| the producer that drops, so the guard can be seen failing | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/NaiveProducer.kt` |
| the reconciliation against end offsets | `ci/b-09/run.sh` |
| the surface may not offer a delivery count | `scripts/no_delivery_counters.py` |
| the contract they hold both actuals to | [producer-contract](../api/producer-contract.md) |
