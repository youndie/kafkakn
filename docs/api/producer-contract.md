---
id: producer-contract
title: The producer contract — the expect surface and what it promises
type: api_endpoints
status: active
services:
  - kafkakn-core
contract_source:
  - kafkakn:kafkakn-core io.github.youndie.kafkakn.KafkaProducer
parent_feature: feature-produce-a-record
---

# The producer contract

**`status: draft`: nothing below is built.** This is the contract the tests are written against —
[D5](../research/research-architecture.md) says a test cites a place here or in the Kafka protocol
documentation, and a test that cites neither is not accepted.

It is an `api` document rather than prose inside a feature because it is the thing two independent
implementations have to agree on. The JVM actual and the native actual are checked against *this*,
and against each other.

## The surface, as targeted

```kotlin
interface KafkaProducer {
    suspend fun send(record: ProducerRecord): RecordMetadata
    suspend fun flush()
    suspend fun close()
}

fun kafkaProducer(config: ProducerConfig): KafkaProducer   // expect
```

**Corrected 2026-09-17 while implementing [B-02](../backlog/B-02-expect-surface.md): not
`AutoCloseable`.** This document said it was, and it cannot be: `AutoCloseable.close` does not
suspend, while `close` here has to flush — and both ways of fitting into the interface are wrong.
Blocking a thread inside `close` is wrong on a runtime built around coroutines; dropping records
still in flight is the silent-loss shape this whole library exists to avoid. So `close` suspends and
the interface is its own. A `use`-shaped extension can be added when something needs it.

Construction is a top-level `expect fun` rather than an `expect class`: the interface stays ordinary
common code that both arms implement, and only the factory is platform-specific.

Nothing else is public in M1. `sendAll`, headers, transactions and partitioner overrides are absent
until an item asks for them.

## What each call promises

### `send`

| | |
|---|---|
| returns | `RecordMetadata` — topic, partition, offset — **after the broker has acknowledged** at the configured `acks` |
| suspends while | the record cannot yet be accepted, i.e. the producer is at its queue bound ([research §1.4](../research/research-architecture.md)) |
| throws | only for failures that are not retryable by the producer: an unknown topic with auto-creation off, an invalid configuration, a closed producer |
| never | returns having silently dropped the record |

**The promise that matters:** *every call to `send` that returns normally corresponds to one record
the broker acknowledged, and every call that does not return normally throws.* There is no third
outcome, and in particular there is no outcome in which the caller has to inspect a count to find
out whether their record survived.

This is the whole point of [feature-backpressure-and-accounting](../features/feature-backpressure-and-accounting.md).
The native implementation of it is not obvious — the underlying `rd_kafka_produce` has exactly the
third outcome this contract forbids — and that is why the contract is written down before the code.

### `flush`

Returns when every record handed to `send` on this producer has been acknowledged or has failed.
Implemented natively as `rd_kafka_outq_len` reaching zero, **not** as the return of
`rd_kafka_flush`, which is an error code ([research §1.4](../research/research-architecture.md)).

### `close`

Flushes, then releases. A record accepted by `send` before `close` is either acknowledged or its
`send` throws; `close` does not discard silently.

## Configuration

One map, keys named as Kafka names them, passed through to whichever client is underneath:
`bootstrap.servers`, `acks`, `compression.type`, `security.protocol`, `ssl.ca.location`,
`queue.buffering.max.messages`. Keys are **not** renamed into a Kotlin vocabulary — an operator
reading a kafkakn configuration should be able to search Kafka's documentation for the key they see.

A key neither actual honours is a **failure at construction**, not a silently ignored entry. The
prior art's sibling lesson applies: an option accepted and dropped looks identical to one that
worked, right up until it matters.

## Errors

| Situation | What the contract says |
|---|---|
| queue at its bound | `send` suspends; **not** an error |
| unknown topic, auto-creation off | `send` throws, and the message names the topic |
| the client refuses a configuration **value** | construction throws — see below |
| the broker refuses a configuration value | `send` throws, and the message carries the broker's own text |
| TLS peer not verifiable | construction or the first `send` throws, and the message names certificate verification |
| producer closed | `send` throws `IllegalStateException` |

Error **text** is not part of the contract; error **type** and the fact that something is thrown at
all are.

**Where a configuration value is refused is not the same on both arms, and the contract does not
pretend otherwise** (measured in [B-06](../backlog/B-06-jvm-actual.md)). `kafka-clients` validates
values at construction — `acks=99` raises its own `ConfigException` before any broker is contacted —
while librdkafka accepts the same value and lets the broker refuse it. The contract promises only
that an unusable value **fails**, and names construction as the earlier of the two places it may
happen. Failing earlier is better and neither arm is asked to become the other.

One consequence for testing, and it cost an iteration to find: **an invalid value proves nothing
about whether a setting reaches the broker**, because the JVM arm never sends it. The probe that
does is a *valid* value the broker cannot satisfy — `acks=all` against a topic whose
`min.insync.replicas` exceeds the in-sync set — with `acks=1` on the same topic as the control.

## What both actuals must agree on

This is the list the differential suite exists to check
([research §1.1](../research/research-architecture.md)):

1. the partition a record with a given key lands in;
2. the offset sequence a series of records produces on one partition;
3. which situations throw and which suspend;
4. the value of `RecordMetadata` for the same input;
5. behaviour at the queue bound — the JVM client blocks by `max.block.ms`, librdkafka refuses, and
   **this contract flattens both into "suspends"**. That flattening is the most likely place for the
   two arms to diverge and gets the densest tests.

## Code anchors

| What | Where |
|---|---|
| the interface | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt` |
| record and metadata types | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerRecord.kt` |
| configuration | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerConfig.kt` |
| the suite that holds both actuals to this document | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/` |
