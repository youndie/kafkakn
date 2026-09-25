---
id: consumer-contract
title: The consumer contract — designed before it is built
type: api_endpoints
status: active
services:
  - kafkakn-core
contract_source:
  - kafkakn:kafkakn-core io.github.youndie.kafkakn.KafkaConsumer
---

# The consumer contract

**Assign, seek and poll are built and measured** ([B-36](../backlog/B-36-assign-and-poll.md),
2026-09-24), and so are groups ([B-37](../backlog/B-37-consumer-groups.md), 2026-09-25). This document began as [B-35](../backlog/B-35-the-consumer-designed-first.md):
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

**The decision — built and measured in [B-36](../backlog/B-36-assign-and-poll.md).**

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
- **Measured, both arms**: eight coroutines polling one consumer at once read every record exactly
  once and nothing threw; with the JVM lane replaced by plain `Dispatchers.IO`, the Java client threw
  *"KafkaConsumer is not safe for multi-threaded access"* — the check read above, seen. A cancelled
  `poll` returned in 16 ms (JVM) and 0 ms (native), and the next call was served at once; with the
  `wakeup()` removed the caller was still released at once — and the next call waited 55 s behind the
  abandoned `poll`, which is why the test times the next call and not the cancellation. A waiting
  `poll` held a single-lane caller for under 500 ms on both arms; the native arm made to block in
  `rd_kafka_consumer_poll(timeout)` held it for 3.0 s.
- **Neither arm hides `max.poll.interval.ms`.** A caller who takes longer than that between polls is
  removed from the group on both arms. That is Kafka's rule rather than a platform's, so the contract
  states it instead of working around it — and it is the reason for §2.

## 2. Shape: an explicit `poll` first, a `Flow` built on it later

```kotlin
interface KafkaConsumer {                                   // B-36, B-37
    suspend fun assign(partitions: List<TopicPartition>)   // B-36
    suspend fun subscribe(topics: List<String>)             // B-37
    suspend fun commit()                                    // B-37
    suspend fun assignment(): List<TopicPartition>          // B-37
    suspend fun groupMetadata(): ConsumerGroupMetadata      // B-38
    suspend fun seek(partition: TopicPartition, to: SeekTo) // B-36: beginning, end, offset, timestamp
    suspend fun poll(timeout: Duration): List<ConsumerRecord>
    suspend fun close()
}

class ConsumerRecord(                                       // B-36
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
- **`value` is nullable**: a null value is a tombstone, whoever wrote it. `ProducerRecord.value` has
  been nullable too since [B-47](../backlog/B-47-a-producer-can-write-a-tombstone.md); until then this
  library could read a tombstone and not write one.
- **The `Flow` comes later, as an extension over `poll`, not instead of it.** A cold `Flow` makes the
  collector's pace the poll's pace, and a collector slower than `max.poll.interval.ms` is evicted from
  its group — on both arms, with nothing in the `Flow`'s signature to say so. With an explicit
  `poll`, the time between calls is visible in the caller's code. The rejected alternative is the
  `Flow` as the first and only shape.
- **Groups** ([B-37](../backlog/B-37-consumer-groups.md)): `subscribe(topics)`, `commit()` and
  `assignment()`. No rebalance callback is offered and none is installed: with auto-commit off, each
  client's own handling is the promise — a partition handed over resumes from its last commit.
  `commit()` is synchronous and commits the position after everything `poll` returned, for every
  partition held: `commitSync()` on the JVM, `rd_kafka_commit(rk, NULL, sync)` on native, whose stored
  offsets are exactly that.
- **`subscribe` and `commit` need a `group.id` the caller named**, on both arms. The native arm's
  private `kafkakn-assign-*` id exists only so librdkafka will `assign`; a subscription joining it, or a
  commit landing in it, would be a group nobody asked for.
- **A seek under a subscription is refused on both arms**, for now: the native arm seeks by
  re-assigning, which a subscription does not allow, and a seek only one arm could honour is the shape
  the configuration rule refuses.

### At-least-once, measured for loss

**Measured 2026-09-25, `ci/b-37/run.sh`**, with records written by a third party *while* the group
formed:

- a group of two members on each arm split four partitions ([0,1] and [2,3]), the leaving member
  left the way a crash would — one batch read and never committed — and the staying one ended with all
  four: 800 of 800 records seen, the abandoned record delivered again, commits at the log end for
  every partition, read by `kafka-consumer-groups.sh --describe`;
- **one group with a member on each arm**, in two processes at once: it formed, split ([0,1] to the
  JVM member, [2,3] to the native one), and when the native member left without committing its last
  batch the JVM member was given that batch and all four partitions: 600 of 600, commits at the log end;
- the check is for loss, and it is not blind: the abandoned batch is **not** counted as seen by the
  member that dropped it, so only redelivery can cover it. With the native `close()` made to commit
  first — a plausible "commit on close" — one record was lost in the native group and one in the mixed
  one, and the run went red.

The two arms' default assignment strategies differ (§3) and share `range`; the split observed is
range's shape. That is a reading of the shape, not a reading of the broker's own record of the
strategy.

### Two things librdkafka needs that the design did not know

Found by B-36, and each is the native arm doing more so that both arms mean the same.

- **`rd_kafka_assign` needs a `group.id`.** Without one it answers *"Local: Unknown group"*; the Java
  client assigns without. A caller who names no group gets a private one on the native arm,
  `kafkakn-assign-<random>`, which joins nothing and commits nothing — `kafka-consumer-groups.sh --list`
  shows no such group after the run.
- **A seek before fetching has started is refused**: *"Local: Erroneous state"*, as `rdkafka.h`
  warns — a seek is for partitions *"already assigned/consumed"*. The Java client seeks at any moment
  after `assign`. So the native arm seeks by **re-assigning**, with the partition moved and every other
  one at the next offset this side has handed out.

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
| `group.protocol` | `classic` | `classic` | `classic`, travels | `consumer` (KIP-848) was out of scope; planned since 2026-09-25 as [B-57](../backlog/B-57-the-kip-848-consumer-protocol.md) |
| `group.id` | none | none | travels; required by `subscribe` and `commit` | |
| `group.instance.id` | none | none | travels | static membership is not in the first consumer; [B-56](../backlog/B-56-static-membership.md) measures it |
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

- **Records are written by a third party, not by this library**, and include what a text-shaped path
  would damage: non-UTF-8 values, null keys, null values, duplicate header names, empty next to null.
  **Not `kafka-console-producer`**, which this section first named: the console tools carry text, and a
  value that is not UTF-8 comes back from them as U+FFFD. The third party is the Kafka distribution's
  own client, run as a program of its own (`ci/harness/Records.java`) that writes the records and reads
  them back as hex; kafkakn is not on its classpath. Measured, B-36: both arms read all twenty byte for
  byte as it does, and as each other.
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

- **Rebalance callbacks**, and a seek under a subscription (§2).
- **Auto-commit by default**, on either arm (§3).
- **A `Flow` as the primary shape** (§2).
- **The `consumer` group protocol** (KIP-848), static membership, cooperative rebalancing chosen on
  the caller's behalf.
- **Exactly-once as a built-in loop.** `groupMetadata()` and the producer's `sendOffsetsToTransaction` are
  in since [B-38](../backlog/B-38-exactly-once-read-process-write.md); the read-process-write loop
  around them is the caller's.
- **Deserializers.** Bytes in, bytes out, as for the producer.
- **Pause and resume, per-partition flow control, incremental fetch tuning.**
- **Metrics** — [B-41](../backlog/B-41-metrics-an-operator-can-read.md), for both clients at once.
- **More than one call in flight on one consumer.** Calls are serialised by the lane (§1); a caller
  who wants parallelism runs more consumers.

**Amended 2026-09-25.** At the owner's request, most of this list is now planned, each as its own item
with a differential test:
- the rebalance listener ([B-50](../backlog/B-50-a-rebalance-listener.md)), designed in this contract
  before it is built;
- a seek under a subscription ([B-51](../backlog/B-51-seek-under-a-subscription.md));
- pause and resume ([B-52](../backlog/B-52-pause-and-resume.md));
- consumer lag ([B-53](../backlog/B-53-consumer-lag-in-metrics.md));
- the `Flow` over `poll` ([B-54](../backlog/B-54-a-flow-over-poll.md));
- cooperative rebalancing, static membership and KIP-848 ([B-55](../backlog/B-55-cooperative-rebalancing.md)
  to [B-57](../backlog/B-57-the-kip-848-consumer-protocol.md));
- explicit commits and reading positions back ([B-48](../backlog/B-48-commit-explicit-offsets.md),
  [B-49](../backlog/B-49-committed-and-position.md)).

What stays out: auto-commit by default, exactly-once as a built-in loop, deserializers, and more than
one call in flight on one consumer. The list above is kept as it was written, because the reason each
entry was first left out is what its item has to answer.

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
