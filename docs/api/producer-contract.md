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
    suspend fun partitionsFor(topic: String): List<PartitionInfo>   // B-29
    suspend fun initTransactions()                                 // B-30
    suspend fun beginTransaction()
    suspend fun sendOffsetsToTransaction(offsets: Map<TopicPartition, Long>, group: ConsumerGroupMetadata)   // B-38
    suspend fun commitTransaction()
    suspend fun abortTransaction()
    suspend fun metrics(): ProducerMetrics                         // B-41
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
until an item asks for them. (Headers arrived with B-10; an explicit partition and a timestamp with B-27 and B-28, below; `partitionsFor` with B-29; transactions with B-30.)

## What each call promises

### `send`

| | |
|---|---|
| returns | `RecordMetadata` — topic, partition, offset, timestamp — **after the broker has acknowledged** at the configured `acks` |
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

**That oracle does not survive transactions, and the contract says so here rather than leaving it to
be discovered.** A commit or abort marker occupies an offset of its own, so on a topic written by a
transactional producer the end offset is no longer a count of records — it is larger by one per
transaction per partition, and an aborted transaction's records are counted too. The reconciliation
there counts **records** read by `kafka-console-consumer`, and names the isolation level every time:
`read_committed` for the claim, `read_uncommitted` to show that what was aborted was written
([B-30](../backlog/B-30-transactions.md), `ci/b-30/run.sh`). An end-offset delta on such a topic
proves nothing either way.

### A null value is a tombstone

`ProducerRecord.value` is nullable since [B-47](../backlog/B-47-a-producer-can-write-a-tombstone.md).
A null value is a **tombstone**: on a topic with `cleanup.policy=compact`, compaction removes the key's
earlier values and, after the topic's `delete.retention.ms`, the tombstone itself. An **empty** value
is a value: compaction keeps it as the key's latest. That is the same distinction the header value
makes (below), and for the same reason: Kafka's protocol carries it and a reader can see it.

| | JVM | native |
|---|---|---|
| null value | `null` into the Java record, which `ByteArraySerializer` passes on as null | `RD_KAFKA_VTYPE_VALUE` with a null pointer and size 0 |
| empty value | an empty array | a zero-length value whose pointer is not null: librdkafka reads a null pointer as a null value |

*Measured* by `ci/b-47/run.sh` on both arms, with the distribution's own client as the reader. It reads
each key's last record: `~` for a null value, `x` for an empty one. Then it waits for the cleaner and
checks what compaction left: the tombstoned key holds nothing but its tombstone, and the emptied key
holds its empty value.

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
| `ssl.certificate.location` | path to the client's PEM certificate, for a broker that requires one ([B-31](../backlog/B-31-client-certificates.md)) | librdkafka's own key | **read**, and handed over as `ssl.keystore.certificate.chain` with `ssl.keystore.type=PEM` |
| `ssl.key.location` | path to that certificate's PEM private key — **with** the certificate or not at all | librdkafka's own key | **read**, and handed over as `ssl.keystore.key` |
| `ssl.key.password` | the key's password, if it is encrypted | librdkafka's own key | the Java client's own key — the one TLS key both clients spell alike |

#### A client certificate is two paths, and the Java client takes two contents

**The JVM translation of the client certificate is not a rename**, and that was read out of
`kafka-clients` 4.3.1 rather than assumed. Its PEM key store takes a *path* only as one file holding
the chain **and** the key — `FileBasedPemStore` hands the same contents to both — and takes two
separate things only as their contents, through `ssl.keystore.certificate.chain` and
`ssl.keystore.key` (`DefaultSslEngineFactory`). So the JVM arm reads both files at construction. The
rejected alternative, concatenating them into a temporary file, leaves a private key on disk where the
caller did not put one.

**The certificate and its key come together or not at all, on both arms.** Measured 2026-09-24:
librdkafka constructs a producer from a certificate with no key — it checks the pair only when a key
is set — and that producer presents nothing, so the refusal would arrive at the first handshake
against a listener that asks. The Java client refuses the same half at construction. The contract
refuses it at construction on both, and the message names both keys.

**The key must be PKCS#8 on both arms** — `-----BEGIN PRIVATE KEY-----` or
`-----BEGIN ENCRYPTED PRIVATE KEY-----` — and anything else is refused at construction with the
conversion in the message ([B-42](../backlog/B-42-a-pkcs1-key-works-on-one-arm.md)). **Measured
2026-09-25, before the rule:** librdkafka hands the file to OpenSSL and took the traditional PKCS#1 form
(`BEGIN RSA PRIVATE KEY`), plain and traditionally encrypted, and sent through the listener that
requires a client certificate; the Java client, which parses through
`PKCS8EncodedKeySpec`/`EncryptedPrivateKeyInfo`, refused both at construction with *"Invalid PEM
keystore configs"* over an `IOException` — not a sentence that says the form is the problem. One key
file, two answers. The rule answers once, on both arms, and names the way out, verified to produce a
PKCS#8 key that matches the certificate:
`openssl pkcs8 -topk8 -in <key> -out <new> -v2 aes-256-cbc`, or `-nocrypt` for an unencrypted one.
Converting on the JVM arm was the rejected alternative: it is this library's own cryptography,
OpenSSL's traditional encryption included, in the arm whose value is having none.
An encrypted PKCS#8 key with `ssl.key.password` is measured on both arms: the suite's own key is one.

**A certificate from an authority the broker does not trust is not sent at all — by either
client.** librdkafka's `rd_kafka_ssl_cert_callback` withholds a client certificate whose issuer is not
in the server's `certificate_authorities`, and the Java client sent none either — measured 2026-09-24:
the broker answered `certificate_required` to both arms for a certificate they had been given. (Which
part of JSSE decides that on the JVM side was not read here; the result was measured.) The caller meets *"certificate required"* for a certificate they did configure — the
right answer to a question they will not think to ask first.

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

### SASL

| Key | Meaning | On native | On the JVM |
|---|---|---|---|
| `security.protocol` | `SASL_PLAINTEXT` or `SASL_SSL` | librdkafka's own key | the Java client's own key |
| `sasl.mechanism` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` or `OAUTHBEARER` — **required** with a SASL protocol | an alias librdkafka accepts for its `sasl.mechanisms` | the Java client's own key |
| `sasl.username`, `sasl.password` | the credentials of those three mechanisms — **together** or not at all | librdkafka's own keys | **built into `sasl.jaas.config`**, the only place the Java client takes credentials |

Since [B-32](../backlog/B-32-sasl-plain-and-scram.md). The spelling is librdkafka's for the credentials
and both clients' for the mechanism; the rejected alternative was making `sasl.jaas.config` the
contract's only spelling, which is a Java string format a native caller would have to learn. A caller
who writes `sasl.jaas.config` themselves is left alone on the JVM arm — it is a platform key there,
and unknown to librdkafka — and one who writes it **and** the pair is refused: two answers to one
question.

**The JAAS string is where this translation can go wrong, and it is quoted the way the Java client
reads it back.** Its parser is `java.io.StreamTokenizer`: inside double quotes a backslash starts an
escape, a double quote ends the value, and so does a line break. All three are escaped. What decides
that it is right is the client's own parser — `JaasContext.loadClientContext` reading the password
back out in `TranslateForJavaTest` — and a broker user whose password holds a double quote and a
backslash, which both arms reach; the native arm sends that password raw, so its getting in is what
says the broker holds the password the test means.

**Three refusals at construction, on both arms,** each watched answered differently per arm before the
rule existed:

- a SASL protocol with **no mechanism**. Both clients default to `GSSAPI`, which the native bundle
  does not have (`--disable-gssapi`); librdkafka said *"No provider for SASL mechanism GSSAPI:
  recompile librdkafka with libsasl2 or openssl support"*, a sentence that does not name the key the
  caller forgot;
- `sasl.username` **without** `sasl.password`, or the reverse — librdkafka: *"sasl.username and
  sasl.password must be set"*; the JVM arm had not heard of either key;
- credentials for a mechanism that takes none, or for no mechanism at all — librdkafka would drop
  them, and the JVM arm has no login module to put them in.

**OAUTHBEARER takes a token the caller supplies** ([B-33](../backlog/B-33-sasl-oauthbearer.md)):
`sasl.mechanism=OAUTHBEARER` and an `OAuthBearerTokenProvider` in `ProducerConfig` — a suspending
function returning `OAuthBearerToken(value, principal, expiresAtMillis, extensions)` — come together
or not at all, refused at construction on both arms. It is the only shape both arms can honour: the
native bundle carries the mechanism and not librdkafka's OIDC fetcher, which needs curl. The library
asks for a token when it needs one and again at about eighty per cent of each token's life, on both
arms: `ExpiringCredentialRefreshingLogin` on the JVM, the refresh callback on native.

- **On the JVM the provider is found, not handed over.** The Java client instantiates its login
  callback handler by class name, so the producer registers its provider under a fresh id and writes the
  id into `sasl.jaas.config` as `kafkakn.provider`; kafkakn's handler reads it back in `configure`.
  A caller who also writes `sasl.jaas.config` or `sasl.login.callback.handler.class` is refused: two
  answers to where the token comes from.
- **On native the refresh callback runs inside `rd_kafka_poll`** — the producer's pump — and cannot
  suspend, so it starts a coroutine that asks the provider and answers with
  `rd_kafka_oauthbearer_set_token`.
- **A provider that throws reaches the caller in its own words.** On the JVM that took the documented
  channel: an exception out of the handler is replaced by *"An internal error occurred while retrieving
  token from callback handler"* (`OAuthBearerLoginModule.identifyToken`, 4.3.1, measured before it was
  read), while `OAuthBearerTokenCallback.error` becomes the `LoginException`'s message. The JVM arm fails
  at construction — the Java client logs in there; the native arm at the first `send`, after
  `message.timeout.ms`, with *"Failed to acquire SASL OAUTHBEARER token: …"* and the provider's words.
  The words travel; the exception object does not, on either arm.
- **Measured 2026-09-25**, `ci/b-33/run.sh`: 50/50 records with the caller's tokens on each arm; with
  twelve-second tokens and a broker that re-authenticates OAUTHBEARER connections every ten seconds,
  four tokens were issued in thirty seconds of sending on each arm, every send succeeding. With the
  native bridge made to overstate each token's life by an hour, nothing was refreshed, the broker
  refused the expired token at re-authentication, and the sends stalled until the test's timeout.

Only the producer takes a provider so far; `sasl.mechanism=OAUTHBEARER` on a consumer or an admin client
is refused at construction.

A wrong password makes `send` throw, and the message **names authentication**. As for an
unverifiable peer, the native arm reports it through the error callback — measured, *"… SASL
authentication error: Authentication failed: Invalid username or password"* — and waits out
`message.timeout.ms` first; the JVM arm throws `SaslAuthenticationException` at once.

### `partitionsFor`

`suspend fun partitionsFor(topic: String): List<PartitionInfo>` — partition id, leader, replicas and
in-sync replicas, ordered by partition, as the producer's own connection to the cluster sees them
([B-29](../backlog/B-29-topic-metadata.md)). `Producer.partitionsFor` on the JVM, `rd_kafka_metadata`
on native. `leader` is null when a partition has none — the Java client says that with a null or
`Node.noNode()`, librdkafka with `-1`.

**It waits off the caller's dispatcher on both arms**, and the test that says so was wrong first.
Both clients answer with a blocking call. Its first version measured the silence from inside a
ticker that could not start until the call was over, and passed on both arms against an
implementation that blocked the caller; corrected, it held a single-lane dispatcher for **20 s on the
JVM and 5 s on native** against a broker that was not there, and for well under the 500 ms it
tolerates once the call moved to `Dispatchers.IO`. Cancelling the caller stops it waiting; it does
not interrupt the client underneath.

**How long it waits is each client's own bound, and they are different keys.** The JVM arm waits
`max.block.ms`; the native arm passes the effective `socket.timeout.ms` — librdkafka's timeout for
network requests — because `max.block.ms` does not exist there.

**An unknown topic fails on both arms, and not alike.** Recorded, not promised, measured 2026-09-24:

| arm | how | after |
|---|---|---|
| native | `KafkaMetadataException: … Broker: Unknown topic or partition` — the broker's answer, read from the described topic's own error | 57 ms, in each of two runs |
| JVM | `TimeoutException: Topic … not present in metadata after 20000 ms` | 20 021 ms and 17 081 ms in two runs, with `max.block.ms` at 20 000 — the message names the bound, not the wait |

The native answer arrives as a *described topic carrying an error*, not as a failed call: an
implementation that read only the call's return code would report an unknown topic as a topic with
no partitions.

### Transactions

`initTransactions`, `beginTransaction`, `commitTransaction`, `abortTransaction` — Kafka's names, as
suspending functions — with `transactional.id` an ordinary configuration key that both clients spell
alike ([B-30](../backlog/B-30-transactions.md)). `inTransaction { … }` commits when its block
returns and aborts when it throws, and is built **on** the four: a caller who decides for themselves
when to abort needs the calls. Its abort runs even on cancellation, since an open transaction holds
`read_committed` readers back until it times out; an abort that fails too is attached as suppressed.
It does not retry or abort a failing commit — that is the caller's decision.

Measured 2026-09-24, `ci/b-30/run.sh`, on both arms: 50 records committed are all visible under
`read_committed`; 50 aborted are none of them, **and all 50 under `read_uncommitted`**, which is what
says the abort happened rather than the records never being sent; the transaction coordinator's own
`kafka-transactions.sh describe` reports `CompleteCommit` and `CompleteAbort` for the two ids.

**Init, commit and abort block inside both clients**, so they wait on `Dispatchers.IO`. The native
arm passes `-1` for every timeout, as librdkafka's header asks — twice `transaction.timeout.ms` for
init, the transaction's remaining time for commit and abort — because it warns that any other value
risks "internal state desynchronization". The JVM arm waits `max.block.ms`.

**A fenced producer throws `ProducerFencedException` — one type, on both arms — from its next call,
and from every call after it.** A second producer with the same `transactional.id` fences the first
by initialising, and aborts the first one's open transaction on the way (its 50 records: none under
`read_committed`, all 50 under `read_uncommitted`). What each client said underneath is the cause and
is recorded, not promised:

| arm | `commitTransaction` | the `send` after it |
|---|---|---|
| JVM | the client's own `ProducerFencedException`: *"There is a newer producer with the same transactionalId which fences the current one."* | *"Producer with transactionalId '…' and (producerId=…, epoch=0) has been fenced by another producer with the same transactionalId"* |
| native | `_FENCED (-144)`, fatal: *"Failed to end transaction: Local: This instance has been fenced by a newer instance"* | *"Local: This instance has been fenced by a newer instance"* |

A fenced producer is finished; the only call left that means anything is `close`.

### Exactly-once read-process-write

`sendOffsetsToTransaction(offsets, group)` commits a consumer's progress **inside** the open transaction,
so that the records sent in it and the input positions they came from become visible together or not
at all ([B-38](../backlog/B-38-exactly-once-read-process-write.md)). `offsets` are the next offset to
read per partition; `group` is `KafkaConsumer.groupMetadata()`, taken from the consumer that read them.
The loop around it is the caller's.

- **The group metadata is opaque**, and good only for a producer on the same arm in the same process.
  The JVM arm carries the Java consumer's own object; the native arm carries librdkafka's serialised form
  (`rd_kafka_consumer_group_metadata_write`, *"mainly for client binding use"*), so no C object has to
  outlive the call that needs it — the producer reads it back for `rd_kafka_send_offsets_to_transaction`
  and destroys it.
- **It depends on the consumer's `read_committed` default** ([consumer-contract](consumer-contract.md) §3):
  a loop that reads uncommitted input is not exactly-once whatever it does with its output.
- **Metadata the group has moved past is refused with `StaleGroupMetadataException`, on both arms**
  ([B-71](../backlog/B-71-send-offsets-after-the-group-moved-on.md)). Taken before a rebalance, it is refused
  as `ILLEGAL_GENERATION`; taken before the member left, as `UNKNOWN_MEMBER_ID`. The error is abortable,
  not fatal: **abort the transaction, and read again from the group's commit**. The partitions may belong to
  another member by now, and what this member processed will be processed again by whoever holds them. The
  producer stays usable: measured, the same member with fresh metadata commits right after the abort.
  - The Java client throws `CommitFailedException`, kept as the cause.
  - librdkafka returns the error codes above, marked *abortable*, and its sentence stays in the message.
  - [B-70](../backlog/B-70-kafkakn-soak.md)'s 37 minutes of chaos met this 17 times, each an instance woken from a
    freeze in the middle of a transaction, and not one record was lost or duplicated.

**Measured 2026-09-25**, `ci/b-38/run.sh`: 300 input records trickled in by a third party, the
processor stopped three times at random points — each after one to three committed batches, either
after its output or after its offsets, walking away with the transaction open — and restarted with
the same `transactional.id`. On each arm, under `read_committed` every input record reached the output
**exactly once** (300 of 300, 300 distinct); under `read_uncommitted` the aborted attempts showed (8 on
the JVM, 16 on native) — exactly the sizes of the batches the stops abandoned. With the native call made
to add no offsets, the output held 395 records for 300 inputs.

### `metrics`

`suspend fun metrics(): ProducerMetrics` — the machinery, never an outcome
([B-41](../backlog/B-41-metrics-an-operator-can-read.md)). A count of successes is the number that read
"complete success" while a quarter of the input was lost, and `scripts/no_delivery_counters.py` still
refuses one (its self-test still flags `deliveredCount`). Four values, each read from the arm's own
client, null where that source has not said:

| metric | JVM reads | native reads |
|---|---|---|
| `bufferedBytes` | `buffer-total-bytes` − `buffer-available-bytes` | `msg_size` |
| `requestsInFlight` | `requests-in-flight` | Σ brokers' `waitresp_cnt` |
| `brokerRoundTripMillis` | `request-latency-avg` | mean of brokers' `rtt.avg` (µs → ms) |
| `openConnections` | `connection-count` | brokers in state `UP` |

The native arm turns librdkafka's statistics on at one second unless the caller set
`statistics.interval.ms`; librdkafka's own default is off, and with it there would be no metrics.

**Same names, not quite the same measurements — measured 2026-09-25, `ci/b-41/run.sh`, one load, two
runs:**

| | JVM | native | the difference, and its tolerance |
|---|---|---|---|
| after `flush`: bytes, requests | 0, 0 | 0, 0 | none: exactly zero on both |
| under load: max bytes buffered | 245 760, 245 760 | 153 600, 163 840 | both above zero; the peaks are the clients' own batching |
| under load: round trip | 2.63, 9.92 ms | 2.13, 1.96 ms | within a factor of ten — the two runs gave ratios of 1.2 and 5.1, so a tighter bound would be a coin toss, and ten is what separates "the same order" from a defect |
| at rest / after: round trip | 6.0 / 2.62, 15.0 / 9.68 ms | null / null | librdkafka's `rtt` covers the last statistics interval only, and an idle one has none; the Java client averages over its thirty-second window |
| under load: max requests in flight | 5 | 0 | the Java value is read live, librdkafka's once a second at emission — not compared |
| open connections | 2 | 1 | one broker; the Java client appears to keep its bootstrap socket open beside the broker's, librdkafka does not — inferred from the counts, not traced. Tolerance: native ≤ JVM ≤ native + 1 |

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

### Idempotence is on by default on both arms — the reference arm's default, conditions included

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
| **portable** | `bootstrap.servers`, `acks`, `compression.type`, `security.protocol`, `ssl.ca.location`, `ssl.certificate.location`, `ssl.key.location`, `ssl.key.password`, `sasl.mechanism`, `sasl.username`, `sasl.password`, `transactional.id` — same name, same meaning, both arms |
| **platform** | everything else: `queue.buffering.max.messages`, `partitioner` (native); `buffer.memory`, `max.block.ms`, `linger.ms` (jvm) |

A platform key travels to the arm that owns it and is **refused by the other at construction**. That
is deliberate: a producer that accepted `buffer.memory` on native and ignored it would be lying
about a bound. Code meant to run on both arms passes the portable keys and supplies the platform
ones per target — the suite does exactly that through a per-arm helper
([research §2.6](../research/research-architecture.md)), and that helper is the honest shape rather
than a workaround.

## The admin client

A separate `kafkaAdmin(AdminConfig)`, not calls on the producer ([B-34](../backlog/B-34-a-minimal-admin.md)):
administration is a different lifecycle and usually a different set of permissions, and both clients
keep it apart.

```kotlin
interface KafkaAdmin {
    suspend fun createTopics(topics: List<NewTopic>)          // name, partitions, replication, topic config
    suspend fun deleteTopics(names: List<String>)
    suspend fun describeTopics(names: List<String>): Map<String, List<PartitionInfo>>
    suspend fun describeCluster(): ClusterDescription         // cluster id, controller, nodes
    suspend fun listConsumerGroups(): List<ConsumerGroupListing>                                // B-58
    suspend fun describeConsumerGroups(groupIds: List<String>): Map<String, ConsumerGroupDescription>  // B-58
    suspend fun listConsumerGroupOffsets(groupId: String): Map<TopicPartition, Long>                   // B-59
    suspend fun listOffsets(partitions: List<TopicPartition>, spec: OffsetSpec): Map<TopicPartition, Long?> // B-59
    suspend fun alterConsumerGroupOffsets(groupId: String, offsets: Map<TopicPartition, Long>)          // B-60
    suspend fun deleteConsumerGroupOffsets(groupId: String, partitions: List<TopicPartition>)          // B-60
    suspend fun deleteConsumerGroups(groupIds: List<String>)                                          // B-60
    suspend fun describeTopicConfigs(names: List<String>): Map<String, Map<String, TopicConfigEntry>> // B-61
    suspend fun alterTopicConfigs(name: String, set: Map<String, String>, delete: List<String>)      // B-61
    suspend fun createPartitions(topic: String, totalCount: Int)                                     // B-62
    suspend fun deleteRecords(beforeOffsets: Map<TopicPartition, Long>): Map<TopicPartition, Long>  // B-63
    suspend fun close()
}
```

`AdminConfig` is held to the producer's rules: a key neither client honours fails at construction —
the JVM arm against `AdminClientConfig.configNames()` — and the TLS and SASL keys are spelled,
refused and translated exactly as for a producer.

**No call holds the caller's dispatcher, and the two arms get there differently.** The JVM arm awaits
`Admin`'s own futures, which complete on the client's network thread. The native arm submits each
request to an event queue of its own and polls it with a zero timeout and a `delay` between polls —
the delivery report's seam, bridged the same way. A queue per request is what keeps two concurrent
calls from reading each other's answers. Held against a single-lane dispatcher with no broker, both
arms waited 5 s and held it for under the tolerated 500 ms; with the JVM arm blocking on `get()`
instead, the same test went red.

**An existing topic is `TopicExistsException` on both arms**, each client's own error as the cause:
the JVM client's `TopicExistsException`, librdkafka's per-topic `TOPIC_ALREADY_EXISTS`. Other failures
are each client's own — `KafkaAdminException` on native, the Java client's exceptions on the JVM — and
recorded, not promised. With no broker: the JVM *"Timed out waiting for a node assignment. Call:
listNodes"*, native *"Failed while waiting for controller: Local: Timed out"*, both after 5 s.

`controller` is whatever broker the cluster reports in that role. Under KRaft it is not necessarily a
member of the controller quorum; on the fixture both arms reported node 1.

**Consumer groups ([B-58](../backlog/B-58-list-and-describe-consumer-groups.md)).**
- `listConsumerGroups` lists consumer groups only. On the JVM that is `listGroups(ListGroupsOptions.forConsumerGroups())`:
  `listConsumerGroups()` is deprecated in 4.3.1, and share and streams groups are left out so that both
  arms answer the same question. On native it is `rd_kafka_ListConsumerGroups`.
- `describeConsumerGroups` gives, per group:
  - a state, in one `GroupState` for both clients' spellings (`STABLE` from the Java client, `Stable` or
    `PreparingRebalance` from librdkafka; a state only one client knows reads `UNKNOWN`);
  - the assignor the group settled on;
  - each member's id, client id, host (verbatim, `/127.0.0.1` on both) and assignment.
- **A group that does not exist is `DEAD` with no members, on both arms.** The Java client throws
  `GroupIdNotFoundException` for it, and librdkafka describes it as `DEAD` and cannot tell a missing group
  from a dead one. So the JVM arm maps the exception, per group, rather than fail the whole call. A group
  with no member left but with commits is `EMPTY`.
- *Measured* (`ci/b-58/run.sh`): a member that is not kafkakn, the distribution's console consumer, is
  described by both arms exactly as `kafka-consumer-groups.sh --describe --members --verbose` prints it.

**A group's offsets, and a partition's ([B-59](../backlog/B-59-consumer-group-offsets-and-lag.md)).**
- `listConsumerGroupOffsets(group)` is what the group committed, per partition: the next offset it reads, in
  topic-then-partition order, asked from outside the group. A partition never committed is absent, and a
  group that does not exist answers an empty map on both arms. JVM `listConsumerGroupOffsets(groupId)`,
  native `rd_kafka_ListConsumerGroupOffsets` with no partitions named.
- `listOffsets(partitions, spec)` is the broker's offset under `OffsetSpec.Earliest`, `Latest` (the offset
  after the last record) or `Timestamp(ms)` (the first record at or after it). **Null means no record is that
  late**, where both clients answer -1 and `kafka-get-offsets.sh` prints nothing for the partition. Read
  uncommitted, the default of both. The max-timestamp spec is left out: it answers a different question.
- **Lag is the caller's subtraction**, `listOffsets(…, Latest)` minus the commit. It is not a call of its own:
  a derived number the library could get subtly wrong, and one line for the caller.
- *Measured* (`ci/b-59/run.sh`): on each arm's own topic and group, the commits are what
  `kafka-consumer-groups.sh --describe` prints as CURRENT-OFFSET, and earliest, latest and four timestamps
  (before every record, exactly on one, between two, after all) are what `kafka-get-offsets.sh --time` prints.

**Moving and deleting a group's offsets, and deleting a group ([B-60](../backlog/B-60-reset-and-delete-group-offsets.md)).**
- `alterConsumerGroupOffsets(group, offsets)` moves an empty group's commits: what
  `kafka-consumer-groups --reset-offsets` does. A time-based reset is B-59's `listOffsets` with a
  `Timestamp`, fed to this call in the caller's code.
- `deleteConsumerGroupOffsets(group, partitions)` deletes the commits for those partitions, and
  `deleteConsumerGroups(ids)` deletes the groups and their commits.
- **A group with an active member is refused, and the refusal is the safety.** A live member would overwrite
  a moved offset with its next commit. The broker refuses three ways, and both arms throw one
  `GroupNotEmptyException` for all three, with the client's own error as the cause on the JVM:

  | Call | The broker's code | The Java client's type |
  |---|---|---|
  | alter | `UNKNOWN_MEMBER_ID` (25): the admin commits as no member | `UnknownMemberIdException` |
  | delete offsets | `GROUP_SUBSCRIBED_TO_TOPIC` (86) | `GroupSubscribedToTopicException` |
  | delete the group | `NON_EMPTY_GROUP` (68) | its own `GroupNotEmptyException`, a different class of the same name |

  A caller catches kafkakn's type. The Java client's is not kafkakn's type, and the JVM arm never lets it
  escape.
- Deleting a group that does not exist is refused on both arms, but each in its client's own type:
  `GroupIdNotFoundException` on the JVM, `KafkaAdminException` with `GROUP_ID_NOT_FOUND` (69) on native.
  This is recorded, not promised.
- *Measured* (`ci/b-60/run.sh`), on each arm's own group:
  - a moved group's commit is what `kafka-consumer-groups.sh --describe` prints;
  - the distribution's console consumer, joining the moved group, starts exactly at the moved offset;
  - a deleted group is absent from `--list`, next to a control group that is listed.

**A topic's configuration ([B-61](../backlog/B-61-topic-configs.md)).**
- `describeTopicConfigs(names)` returns every key the broker reports for each topic, in alphabetical
  order. Each key has its value (null when the broker calls it sensitive) and its source:
  - `TOPIC`: set on the topic;
  - `BROKER`: the broker's dynamic, dynamic-default and static sources, which are one here;
  - `DEFAULT`;
  - `UNKNOWN`.
- `alterTopicConfigs(name, set, delete)` is **incremental only**. The keys in `set` take their values, the
  keys in `delete` return to what the topic would have without them, and every other key is left alone.
  Both clients' non-incremental `alterConfigs` resets every key not named, and it is deliberately not
  offered.
- **What the broker refuses is refused with `IllegalArgumentException` on both arms, and nothing is
  changed.** That covers an unknown key and a value it cannot read (`INVALID_CONFIG`, 40). A call that
  pairs a bad value with a good one applies neither. The Java client's `InvalidConfigurationException` is
  the cause on the JVM. A topic that does not exist is each client's own failure: recorded, not promised.
- **A change is visible shortly after the call returns, not at that moment.** The controller accepts it,
  and the broker's view of the topic follows. A describe made at once showed the old value once on the
  native arm, and it is a race on either arm. Measured: visible after 7 to 110 ms. A caller who reads
  back what they set waits for it.
- *Measured* (`ci/b-61/run.sh`): on each arm's topic, after a set and a delete, all 33 keys, with their
  values and sources, are what `kafka-configs.sh --describe --all` reports. The tool gives no source of its
  own, only synonyms, and a key with none is one nobody set: the default, as both clients report it.

**Adding partitions ([B-62](../backlog/B-62-create-partitions.md)).**
- `createPartitions(topic, totalCount)` grows a topic to `totalCount`. A count equal to or below the current one
  is refused by the broker (`INVALID_PARTITIONS`, 37) with **`IllegalArgumentException` on both arms**, and the
  topic is unchanged. The Java client's `InvalidPartitionsException` is the cause on the JVM.
- **Keyed records move, and this library does not hide it.** The partitioner maps a key by the partition
  count, so a key's records written after the growth can land on a different partition from its earlier
  ones, and per-key order across the growth is lost. Measured: eight keys that all sat in partition 0 of a
  one-partition topic went to `1 0 2 3 1 0 0 3` of four afterwards. The two arms agree key for key, since both
  partition by murmur2.
- A producer made after the growth sees the new count. The grown count was visible to a describe within
  7 to 31 ms of the call.
- *Measured* (`ci/b-62/run.sh`), for each arm's topic:
  - `kafka-topics.sh --describe` reports `PartitionCount: 4`;
  - `kafka-get-offsets.sh` counts `0:11 1:2 2:1 3:2`, exactly where the arm said its records went.

**Deleting records ([B-63](../backlog/B-63-delete-records.md)).**
- `deleteRecords(beforeOffsets)` deletes every record before each partition's offset. It **returns the low
  watermark the broker reports**, in partition order: what happened, not what was asked for. Asked to
  delete before 2 on a partition that already starts at 4, both arms return 4. Asked for the end, the
  partition is emptied and starts where it ends.
- An offset past the end is refused by the broker (`OFFSET_OUT_OF_RANGE`, 1) with
  **`IllegalArgumentException` on both arms**, and nothing is deleted. On the JVM, the cause is the Java
  client's `OffsetOutOfRangeException`. A negative offset is refused before any request, as for a commit.
  Both clients read -1 as "the high watermark", which is not offered.
- *Measured* (`ci/b-63/run.sh`): each arm's returned watermarks are what `kafka-get-offsets.sh --time -2`
  reports.

**The suite does not build its fixtures with this client.** Everything it created is read back by
`kafka-topics.sh` and `kafka-configs.sh`, and the cluster id is compared with `kafka-cluster.sh` —
the same rule that keeps a producer from being checked by its own consumer.

## Errors

| Situation | What the contract says |
|---|---|
| queue at its bound | `send` suspends; **not** an error |
| unknown topic, auto-creation off | `send` throws, and the message names the topic |
| the client refuses a configuration **value** | construction throws — see below |
| the broker refuses a configuration value | `send` throws, and the message carries the broker's own text |
| TLS peer not verifiable | `send` throws and the message names certificate verification — but **not promptly on native**, see below |
| SASL credentials refused | `send` throws and the message names authentication — **not promptly on native**, for the same reason |
| an admin client creates a topic that exists | `TopicExistsException`, on both arms |
| an admin client alters or deletes the offsets of a group with an active member, or deletes the group | `GroupNotEmptyException`, on both arms |
| an admin client sets a topic configuration key the broker does not know, or a value it cannot read | `IllegalArgumentException`, on both arms, and nothing is changed |
| an admin client asks for a partition count that does not grow the topic | `IllegalArgumentException`, on both arms, and the topic is unchanged |
| an admin client deletes records before an offset past the partition's end | `IllegalArgumentException`, on both arms, and nothing is deleted |
| another producer took the `transactional.id` | every later call throws `ProducerFencedException`, on both arms |
| `sendOffsetsToTransaction` with group metadata the group has moved past | `StaleGroupMetadataException`, on both arms; abort and read again from the group's commit |
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

   **A record can also name its partition** (`ProducerRecord.partition`, measured 2026-09-24,
   [B-27](../backlog/B-27-a-record-can-name-its-partition.md)). Then the partitioner is not
   consulted, key or no key, and the record is where it says — read back by the broker's own
   consumer one partition at a time, 50 of 50 on the named partition and none elsewhere, on both
   arms. A negative partition is refused where the record is made. **A partition the topic does
   not have fails on both arms, and not in the same way:**

   | | what it says | how long it takes |
   |---|---|---|
   | JVM | *"Partition 99 of topic … with partition count 3 is not present in metadata after 20000 ms"* | **`max.block.ms`** — 20 s with the suite's setting, **60 s** at the client's default |
   | native | *"…: Local: Unknown partition"* | **4 ms** |

   The Java client waits for metadata that might yet grow the topic; librdkafka answers from the
   metadata it has. Neither is wrong, so this is recorded rather than equalised — but a caller on the
   JVM arm who mistypes a partition waits a minute to find out, and should know that.

   **A record can name its time** (`ProducerRecord.timestamp`, epoch milliseconds, measured
   2026-09-24, [B-28](../backlog/B-28-a-record-carries-its-timestamp.md)), and
   `RecordMetadata.timestamp` is **the time the broker kept** — the record's own on an ordinary topic,
   **the broker's clock on a topic configured with `message.timestamp.type=LogAppendTime`**, which is
   the only way a caller learns their time was replaced. Read back by the broker's own consumer:
   `CreateTime:1600000000000` for the time named, and `LogAppendTime:<the broker's clock>` on the
   other topic, on both arms. A record that names none carries the client's clock at `send`; a
   negative timestamp is refused where the record is made.

   **There is no field saying which of the two times it is, by decision.** librdkafka reports the
   type with the value; the Java client's `RecordMetadata` exposes the value alone, so a type here
   would be a field one arm fills and the other guesses.
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
