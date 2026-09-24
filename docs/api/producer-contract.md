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

**Built on both arms and published.** This is the contract the tests are written against —
[D5](../research/research-architecture.md) says a test cites a place here or in the Kafka protocol
documentation, and a test that cites neither is not accepted. Where the two arms turned out to
differ, it says so rather than promising the difference away; those places are marked **measured**
and carry the date.

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

**No member of this surface counts deliveries**, and that is a gate rather than a habit:
`scripts/no_delivery_counters.py` fails the build on a declaration in `commonMain` whose name pairs
a delivery word with a quantity word. A `sentCount` would be truthful about what was enqueued and
read as a success rate, which is precisely the number that reported complete success while 264 826
records of 1 000 000 had never been queued. The reconciliation lives in the suite, against the
broker's end offsets ([B-09](../backlog/B-09-accounting.md)).

### Headers

`ProducerRecord.headers` is a **list** of `RecordHeader(name, value)`, and every part of that
sentence is Kafka's shape rather than a convenience:

| | |
|---|---|
| ordered | the reader sees them in the order they were given |
| duplicate names allowed | `lastHeader(name)` and an iterating consumer legitimately disagree |
| `value` nullable | a null value is not an empty one, and a consumer can tell |

A `Map<String, ByteArray>` would drop entries — for tracing baggage and schema identifiers, exactly
the entries somebody added on purpose.

On the native side these travel through `rd_kafka_produceva`, which is **not** variadic and needs no
C of ours ([research §2.10](../research/research-architecture.md)); `rd_kafka_producev`, the one
§1.5 rules out, is a different function.

### TLS

| Key | Meaning | On native | On the JVM |
|---|---|---|---|
| `security.protocol=SSL` | verbatim, both arms | librdkafka's own key | the Java client's own key |
| `ssl.ca.location` | path to a PEM certificate authority | librdkafka's own key | translated to `ssl.truststore.location` + `ssl.truststore.type=PEM` |
| `ssl.endpoint.identification.algorithm` | `https` (the default) or `none` — **hostname** checking | librdkafka's own key, and its own spelling of off | `none` is translated to the empty string the Java client documents |

#### Idempotence is on by default on both arms — the reference arm's default, conditions included

**Decided 2026-09-24** ([B-25](../backlog/B-25-the-arms-disagree-on-idempotence.md)). The two
clients disagreed: `enable.idempotence` defaults to `true` in `kafka-clients` and to `false` in
librdkafka. Under a broker that loses acknowledgements the native arm wrote **200** records twice where
the JVM arm, left at its default, wrote none ([research §2.19](../research/research-architecture.md)).

The native arm now takes the Java client's default, and that default is conditional, measured
against `kafka-clients` 4.3.1:

| the caller set | idempotence |
|---|---|
| nothing | **on** |
| `acks` other than `all` | off, **silently** |
| `retries=0` | off, **silently** |
| `enable.idempotence` | what they set |
| `acks=1` and `enable.idempotence=true` | refused at construction |
| `max.in.flight.requests.per.connection` above 5 | **refused at construction**, even with idempotence unset |

The silent rows are the Java client's behaviour, kept on purpose: a default that was simply "on"
would make librdkafka refuse `acks=1`, which the reference accepts — a configuration that works on the
arm a caller runs locally and fails on the one they ship. The last row is the surprising one, and it
is also the reference's.

#### Certificate trust cannot be turned off, and hostname checking can

**`enable.ssl.certificate.verification` is refused at construction on both arms, by decision**
([B-18](../backlog/B-18-verification-cannot-be-turned-off.md)), and the message names the key.

The reason is the configuration rule above rather than a view about security. The key exists only in
librdkafka — `kafka-clients` has no equivalent, since
`ssl.endpoint.identification.algorithm` turns off hostname checking and never trust — so it is a
**platform** key, and its platform is the one with no oracle. A caller who turned trust off would be
alone with the implementation this whole project exists to check, and the suite could not tell them
anything about it. Refusing it on both arms is also what makes the README's sentence true rather than
nearly true.

A named convenience pointing at the same thing (`verifyCertificates = false`) is refused for the
older reason: a library that offers one gets it used in production.

**Hostname checking is a different question and gets the opposite answer.**
`ssl.endpoint.identification.algorithm` exists on both arms, carries Kafka's own name, and what it
does happens where the oracle can see it — so it travels. It is **the one remaining way to weaken
TLS through this API**, and it is named here so that "verification is on and cannot be turned off" is
read exactly as far as it is true: trust cannot be turned off, hostname matching can.

**Measured 2026-09-17, and it is the value rather than the key that differs.** Handed the empty
string that `kafka-clients` documents as "off", librdkafka answers *"Configuration property
`ssl.endpoint.identification.algorithm` cannot be set to empty value"*. The contract therefore spells
off as **`none`**, librdkafka's spelling, and the JVM arm translates it — the same shape as
`ssl.ca.location`, and for the same reason: a value that must be spelled differently per platform is
one the caller gets wrong on the arm they do not run locally. An empty value is refused on both arms,
which also happens to be what an environment variable that expanded to nothing looks like.

A peer that cannot be verified makes `send` throw, and the message **names the certificate**. That
is not free on the native side: the record is only enqueued, so it comes back as `Local: Message
timed out` like any other unreachable broker, and the sentence that explains it arrived on the error
callback ([research §2.9](../research/research-architecture.md)).

### `flush`

Returns when every record handed to `send` on this producer has been acknowledged or has failed.
Implemented natively as `rd_kafka_outq_len` reaching zero, **not** as the return of
`rd_kafka_flush`, which is an error code ([research §1.4](../research/research-architecture.md)).

### `close`

Flushes, then releases. A record accepted by `send` before `close` is either acknowledged or its
`send` throws; `close` does not discard silently.

**Measured 2026-09-17, both halves of it.** Twenty `SIGTERM`s at unplanned moments inside a real
service's ordered shutdown, through each of the two shapes a caller can have:

* the publisher that **awaits the acknowledgement inside the request** — 91 149 accepted events, none
  missing ([research §2.14](../research/research-architecture.md)). Nothing is ever outstanding when
  `close` runs, so what those rounds show is that an ordered shutdown does not cut a request
  mid-`send`;
* the publisher whose **`publish` returns first**, with a queue in front of the producer — 17 644
  accepted, none missing, and this is the shape in which `close` has records of its own to flush
  ([§2.17](../research/research-architecture.md), [B-23](../backlog/B-23-the-sink-that-does-not-wait.md)).

**What the second run also establishes is where this promise stops.** With the broker gone, its
control lost 129 records: **one** the producer had been asked for, and **127 that the service was
still holding and had not handed over** when its shutdown deadlines expired. The second number is not
about this contract — a record `send` was never called for is the caller's, and whether a service
should write its intent and reconcile later is an outbox question. The distinction is written here
because the sentence above covers both shapes and a reader will assume the harder one.

## Configuration

One map, keys named as Kafka names them, passed through to whichever client is underneath:
`bootstrap.servers`, `acks`, `compression.type`, `security.protocol`, `ssl.ca.location`. Keys are
**not** renamed into a Kotlin vocabulary — an operator reading a kafkakn configuration should be able
to search Kafka's documentation for the key they see.

That list is deliberately shorter than it was: `queue.buffering.max.messages` used to be in it, and
it is not portable — see below.

**`compression.type`, measured 2026-09-24** ([B-26](../backlog/B-26-compression-was-never-measured.md)).
It sat in this list for a week with no test behind it. `none`, `gzip`, `snappy`, `lz4` and `zstd`,
sent from each arm, are stored by the broker with exactly that codec — read out of the log segment by
`kafka-dump-log.sh`, not inferred from values that arrived, because an uncompressed batch arrives too.
It is the spelling that travels: librdkafka's own key is `compression.codec` and it accepts
`compression.type` as an alias. An unknown codec is refused at construction on both arms and the
message names the key the caller wrote; librdkafka's own sentence names `compression.codec`, so the
native arm puts the caller's key and value first.

A key neither actual honours is a **failure at construction**, not a silently ignored entry. The
prior art's sibling lesson applies: an option accepted and dropped looks identical to one that
worked, right up until it matters.

### A key honoured by exactly one arm is the harder case, and it is not portable

**Measured 2026-09-17.** `queue.buffering.max.messages` is librdkafka's, and the JVM arm refuses it
at construction because `ProducerConfig.configNames()` has never heard of it. A configuration that
works on native therefore cannot be handed to the oracle unchanged — and the oracle is the whole
argument of this project.

There is no third spelling. librdkafka bounds its queue by a **record count**; the Java client bounds
it by `buffer.memory` in **bytes** and waits `max.block.ms` for room. Inventing `maxQueuedRecords`
would mean this library deciding what the bound means on each side, which is exactly the "accepted
and quietly reinterpreted" shape the rule above exists to refuse.

So the contract splits the map in two, and says which half a key is in:

| | |
|---|---|
| **portable** | `bootstrap.servers`, `acks`, `compression.type`, `security.protocol`, `ssl.ca.location` — same name, same meaning, both arms |
| **platform** | everything else: `queue.buffering.max.messages`, `partitioner` (native); `buffer.memory`, `max.block.ms`, `linger.ms` (jvm) |

A platform key travels to the arm that owns it and is **refused by the other at construction**. That
is deliberate: a producer that accepted `buffer.memory` on native and ignored it would be lying
about a bound. Code meant to run on both arms passes the portable keys and supplies the platform
ones per target — the suite does exactly that through a per-arm helper
([research §2.6](../research/research-architecture.md)), and that helper is the honest shape rather
than a workaround.

## Errors

| Situation | What the contract says |
|---|---|
| queue at its bound | `send` suspends; **not** an error |
| unknown topic, auto-creation off | `send` throws, and the message names the topic |
| the client refuses a configuration **value** | construction throws — see below |
| the broker refuses a configuration value | `send` throws, and the message carries the broker's own text |
| TLS peer not verifiable | `send` throws and the message names certificate verification — but **not promptly on native**, see below |
| producer closed | `send` throws `IllegalStateException` |

Error **text** is not part of the contract; error **type** and the fact that something is thrown at
all are.

**How long an unverifiable peer takes to fail is not the same on the two arms, and the contract says
so rather than promising the faster one.** `rd_kafka_new` connects to nothing, and
`rd_kafka_produce` only enqueues, so on native the record waits out `message.timeout.ms` — **300 000
ms by default** — and comes back as `Local: Message timed out`. What makes it nameable is the error
callback, which keeps the last connection error that is not `_ALL_BROKERS_DOWN`, so the message ends
up carrying *"certificate verify failed: broker certificate could not be verified, verify that
`ssl.ca.location` is correctly configured"* ([research §2.9](../research/research-architecture.md)).
The JVM arm fails in seconds with `SslAuthenticationException`.

A caller who wants a native failure in seconds rather than minutes sets `message.timeout.ms`, which
is a platform key; the suite does exactly that. Failing pending sends the moment the error callback
reports an SSL error would remove the difference, and it is not done: it would mean this library
deciding that one class of librdkafka error is fatal, which is a policy librdkafka deliberately
leaves to the application.

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

1. **the partition a record with a given key lands in — and it took a decision to make true.** The
   default partitioners do not agree: librdkafka's is `consistent_random`, a CRC32 of the key, and
   the Java producer's is murmur2. Both are internally consistent, so neither implementation can
   notice on its own — the same key simply goes somewhere else depending on which arm produced it.
   librdkafka names the compatible option itself, `murmur2_random`, documented as "functionally
   equivalent to the default partitioner in the Java Producer", and **the native actual sets it as
   its default**. A caller who names `partitioner` keeps theirs. Found by the oracle on the first day
   it existed ([research §2.2](../research/research-architecture.md)).

   **Records with no key are excluded from this promise.** The Java client uses a sticky partitioner
   there — one partition per batch, switching when the batch is sent — and librdkafka picks at
   random. Neither is wrong and no setting reconciles them, so agreement is only claimed for keyed
   records and `PartitionerAgreementTest` only produces those.
2. the offset sequence a series of records produces on one partition;
3. which situations throw and which suspend;
4. the value of `RecordMetadata` for the same input;
5. **behaviour at the queue bound**, where the JVM client blocks and librdkafka refuses, and this
   contract flattens both into "suspends".

   That word is a claim about the **caller's thread**, not only about the outcome. `kafka-clients`
   waits inside `send` — for metadata, or for room in the accumulator up to `max.block.ms` — so a
   suspend signature wrapped straight around it blocks the thread it was called on. Measured
   2026-09-17: on a single-threaded dispatcher, three records waiting on metadata held the thread
   for **6 019 ms** while a coroutine asking for it every 2 ms got nothing. The JVM actual therefore
   does its waiting on `Dispatchers.IO`, and `JvmDispatcherSeamTest` is what keeps it there.

   The native arm reaches the same promise differently: `rd_kafka_produce` never blocks, and the
   suspension is a `delay` between attempts.
6. **`send` does not batch for you.** One `send` is one record and one acknowledgement, so a caller
   awaiting each one in turn has exactly one record in flight and gets one round trip per record.
   Throughput comes from calling it concurrently — both clients batch internally once records are in
   flight together. That is a property of a `send` that waits for an acknowledgement rather than an
   oversight, and it is also why the backpressure tests have to be concurrent to exercise anything
   at all ([research §2.5](../research/research-architecture.md)). No batching entry point is
   offered until an item asks for one.

## Code anchors

| What | Where |
|---|---|
| the interface | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt` |
| record and metadata types | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerRecord.kt` |
| configuration | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerConfig.kt` |
| the suite that holds both actuals to this document | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/` |
