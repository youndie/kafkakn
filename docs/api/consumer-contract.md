---
id: consumer-contract
title: The consumer contract — designed before it is built
type: api_endpoints
status: active
services:
  - kafkakn-core
contract_source:
  - "kafkakn:kafkakn-core io.github.youndie.kafkakn.KafkaConsumer (target — nothing is built yet)"
---

# The consumer contract

**Nothing here is built.** This document is [B-35](../backlog/B-35-the-consumer-designed-first.md):
the consumer designed before any of it is written, because the questions that decide its shape are
questions about the two clients underneath, and none of them is answered by writing a `poll` loop.
Every promise below is ***target*** until the item named beside it measures it; the facts it rests
on are marked **read**, with the address they were read at.

It is written in the shape of [producer-contract](producer-contract.md), and it inherits that
document's rules wholesale: bytes and not strings, a key neither client honours fails at
construction, TLS and SASL are spelled and translated as there, and a platform key is refused by
the other arm.

## 1. Threading: what each client demands of the thread that calls it

This is the decision every later consumer item inherits, and the producer's seam had to learn its
half of it twice ([research §2.3, §2.13](../research/research-architecture.md)).

| | kafka-clients 4.3.1 | librdkafka 2.13.0 |
|---|---|---|
| thread safety | **not thread-safe** — *"Un-synchronized access will result in `ConcurrentModificationException`"* | *"completely thread-safe … may call any of the API functions from any of its own threads at any time"* |
| what is actually checked | a light lock held **for the duration of a call**: `acquire()` records the calling thread's id, `release()` clears it when the call returns. **Sequential calls from different threads pass; overlapping calls throw.** | nothing — no per-thread state |
| interrupting a waiting call | `wakeup()`, the one method safe from another thread; the waiting call throws `WakeupException`. Thread interrupts work but are *discouraged* — they *"may cause a clean shutdown of the consumer to be aborted"* | a zero-timeout poll never waits, so there is nothing to interrupt |
| rebalance callbacks run | on the thread calling `poll` — *"rebalances will only occur during an active call to `poll`, so callbacks will also only be invoked during that time"* | queued callbacks *"will also be triggered by … `rd_kafka_consumer_poll()`"*, on its caller's thread; that `rebalance_cb` is among them is the documented example's usage, not a sentence — [B-37](../backlog/B-37-consumer-groups.md) confirms it |
| must come back within | `max.poll.interval.ms` (300 000) between polls, or the member leaves the group | the same key, the same default, the same consequence |

**Read at:** `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java`
(the "Multi-threaded Processing" section), `…/consumer/internals/ClassicKafkaConsumer.java` and
`…/consumer/internals/AsyncKafkaConsumer.java` (`acquire`, `release` — the same check in both);
`librdkafka-2.13.0.tar.gz!/INTRODUCTION.md` ("Threads and callbacks", and the offset store: *"updated by
`consumer_poll()` … to store the offset of the last message passed to the application"*).

**The decision (*target*, [B-36](../backlog/B-36-assign-and-poll.md)).**

- **JVM arm:** every call to the Java consumer runs on a lane of its own —
  `Dispatchers.IO.limitedParallelism(1)` per consumer. One lane means no two calls ever overlap, which
  is exactly what `acquire()` checks, and `IO` means a `poll` that waits holds a thread that exists
  for waiting rather than the caller's. It is **not** a single thread, and it does not need to be: the
  lock is per call, not per thread — read, not assumed.
- **Cancelling a waiting `poll` calls `wakeup()`**, from the cancelling side, and the `WakeupException`
  that follows is the cancellation, not a failure. Interrupting the lane's thread
  (`runInterruptible`) is the rejected alternative: the Java client's own documentation discourages
  it because it can abort a clean shutdown.
- **Native arm:** `rd_kafka_consumer_poll(rk, 0)` in a loop with `delay` between empty polls — the
  producer's delivery-report pump, again. Nothing waits inside C, so cancellation is immediate and no
  thread is held.
- **Neither arm hides `max.poll.interval.ms`.** A caller who takes longer than that between polls is
  removed from the group on both arms. That is Kafka's rule rather than a platform's, so the contract
  states it instead of working around it — and it is the reason for §2.

## 2. Shape: an explicit `poll` first, a `Flow` built on it later

```kotlin
interface KafkaConsumer {                                   // target
    suspend fun assign(partitions: List<TopicPartition>)   // B-36
    suspend fun seek(partition: TopicPartition, to: SeekTo) // B-36: beginning, end, offset, timestamp
    suspend fun poll(timeout: Duration): List<ConsumerRecord>
    suspend fun close()
}

data class ConsumerRecord(                                  // target
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long,
    val key: ByteArray?,
    val value: ByteArray?,          // null: a tombstone written by somebody else
    val headers: List<Header>,      // ordered, duplicates kept — as the producer sends them
)
```

- **`poll` returns what is available, up to a bound, or an empty list when `timeout` passes.** The
  JVM bounds a batch by `max.poll.records` (500); librdkafka returns one message per call, so the
  native arm drains up to the same 500 per `poll`. `max.poll.records` itself is a JVM key, refused on
  native by the configuration rule.
- **`value` is nullable** where `ProducerRecord.value` is not: this library cannot write a
  tombstone yet, but a consumer reads whatever anyone wrote.
- **The `Flow` comes later, as an extension over `poll`, not instead of it.** A cold `Flow` makes the
  collector's pace the poll's pace, and a collector slower than `max.poll.interval.ms` is evicted from
  its group — on both arms, with nothing in the `Flow`'s signature to say so. With an explicit
  `poll`, the time between calls is visible in the caller's code. The rejected alternative is the
  `Flow` as the first and only shape.
- **Groups come second** ([B-37](../backlog/B-37-consumer-groups.md)): `subscribe`, `commit`, and the
  rebalance callbacks, which run inside `poll` on both arms and therefore on the lane of §1.

## 3. Defaults: every key this contract names, read from both artefacts

**Read at:** `ConsumerConfig.configDef().defaultValues()` executed against `kafka-clients-4.3.1.jar`
on 2026-09-24, and `librdkafka-2.13.0.tar.gz!/CONFIGURATION.md`. Five of these rows were in
[research §1.8](../research/research-architecture.md); the rest are new here.

| Key | kafka-clients 4.3.1 | librdkafka 2.13.0 | kafkakn (*target*) | Why |
|---|---|---|---|---|
| `isolation.level` | `read_uncommitted` | `read_committed` | **`read_committed`, both arms** | see below |
| `enable.auto.commit` | `true` | `true` | **`false`, both arms** | same value, different meaning — see below |
| `allow.auto.create.topics` | `true` | `false` | **`false`, both arms** | reading a topic should not create it; the fixture refuses auto-creation for the same reason |
| `check.crcs` | `true` | `false` | **`true`, both arms** | corruption surfaces as an error instead of as bytes |
| `auto.offset.reset` | `latest` | `largest` | `latest`, travels | the same meaning; librdkafka accepts `earliest` and `latest` too, so those two spellings travel and the rest (`none`, `error`, `smallest`) are platform values |
| `partition.assignment.strategy` | `RangeAssignor`, `CooperativeStickyAssignor` | `range,roundrobin` | each arm's own; **platform key** | spelled as class names on one arm and as words on the other; the two defaults share `range`, which is what a mixed group will settle on — [B-37](../backlog/B-37-consumer-groups.md) measures that |
| `group.protocol` | `classic` | `classic` | `classic`, travels | `consumer` (KIP-848) is out of scope |
| `group.id` | none | none | travels; required by `subscribe` and `commit` | |
| `group.instance.id` | none | none | travels | static membership is not in the first consumer |
| `max.poll.interval.ms` | 300 000 | 300 000 | travels | §1 |
| `session.timeout.ms` | 45 000 | 45 000 | travels | |
| `heartbeat.interval.ms` | 3 000 | 3 000 | travels | |
| `fetch.min.bytes` | 1 | 1 | travels | |
| `max.partition.fetch.bytes` | 1 048 576 | 1 048 576 (alias of `fetch.message.max.bytes`) | travels | |
| `fetch.max.bytes` | 52 428 800 | 52 428 800 | travels | |
| `fetch.max.wait.ms` | 500 | *absent* — librdkafka calls it `fetch.wait.max.ms`, also 500 | **platform key on each side** | two names, one meaning; not translated until a caller needs it |
| `max.poll.records` | 500 | *absent* | JVM platform key; the native arm's per-`poll` bound is the same 500 | §2 |
| `enable.auto.offset.store` | *absent* | `true` | native platform key, left at `true` | with auto-commit off it only feeds `commit()`, which then commits what `poll` returned — the JVM's `commitSync()` semantics |
| `enable.partition.eof` | *absent* | `false` | native platform key, left at `false` | |
| `client.id` | `""` | `rdkafka` | each arm's own | cosmetic |

**`isolation.level`: the safe default, not the reference arm's.** B-25 gave both arms the Java client's
idempotence default because it was the one that writes nothing twice. The same rule here picks
librdkafka's: under `read_uncommitted` a consumer returns records from transactions that were
aborted — records that, as far as the producer's caller is concerned, never happened — and since
[B-30](../backlog/B-30-transactions.md) this library's own producer writes them. So the rule is
"the default that shows a caller nothing they did not ask for", and it lands on whichever arm has it.
The JVM arm sets `read_committed` unless the caller wrote the key.

**`enable.auto.commit`: one value, two meanings, so neither travels.** The Java consumer commits inside
`poll` and `close`, and the offsets it commits are those of the batch the previous `poll` returned —
at-least-once, *"but the requirement is that you must consume all data returned from each call to
`poll`"* before the next one. librdkafka commits in the background every `auto.commit.interval.ms`,
and what it commits is the offset store, which `enable.auto.offset.store` fills **as each message is
handed to the application** — before it is processed. A crash between the two commits a record that
was never processed on one arm and not on the other. The first consumer therefore commits only when
told to ([B-37](../backlog/B-37-consumer-groups.md)); a caller who turns auto-commit on gets each
client's own semantics, and the contract says which.

## 4. The oracle

- **Records are written by `kafka-console-producer`, not by this library**, and include what a
  text-shaped path would damage: non-UTF-8 values, null keys, null values, duplicate header names.
  A consumer checked against our own producer can be wrong in the same way as it and agree with
  itself — the reverse of the rule the producer lives by.
- **Both arms read the same partitions, and their readings are compared with each other and with
  `kafka-console-consumer`**, byte for byte and in order. A difference between the arms is a failure,
  not a warning.
- **Positions and committed offsets are read by `kafka-get-offsets.sh` and
  `kafka-consumer-groups.sh --describe`**, never by the consumer that moved or committed them.
- **Isolation is named in every read** — the lesson of [B-30](../backlog/B-30-transactions.md), where
  an unnamed level counted aborted records.

## 5. What the first consumer will not do

- **Group coordination** — `subscribe`, rebalances, `commit` — until [B-37](../backlog/B-37-consumer-groups.md).
  [B-36](../backlog/B-36-assign-and-poll.md) is assign and poll only.
- **Auto-commit by default**, on either arm (§3).
- **A `Flow` as the primary shape** (§2).
- **The `consumer` group protocol** (KIP-848), static membership, cooperative rebalancing chosen on
  the caller's behalf.
- **Exactly-once read-process-write** — [B-38](../backlog/B-38-exactly-once-read-process-write.md).
- **Deserializers.** Bytes in, bytes out, as for the producer.
- **Pause and resume, per-partition flow control, incremental fetch tuning.**
- **Metrics** — [B-41](../backlog/B-41-metrics-an-operator-can-read.md), for both clients at once.
- **More than one call in flight on one consumer.** Calls are serialised by the lane (§1); a caller
  who wants parallelism runs more consumers.

## 6. Code anchors

Nothing of the consumer exists yet, so the anchors are what it will be built from: the artefacts the
facts above were read in, and the producer code whose rules it inherits.

| What | Where |
|---|---|
| the Java consumer's thread check | `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ClassicKafkaConsumer.java` |
| the same check in the KIP-848 consumer | `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AsyncKafkaConsumer.java` |
| librdkafka's threading and consumer keys | `librdkafka-2.13.0.tar.gz!/INTRODUCTION.md`, `librdkafka-2.13.0.tar.gz!/CONFIGURATION.md` |
| the configuration rules a consumer inherits | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/TlsKeys.kt`, `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/SaslKeys.kt` |
| the TLS and SASL translations on the JVM arm | `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt` |
| the pump the native `poll` repeats | `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt` |
| the fixture and its third-party tools | `ci/harness/broker.sh` |
