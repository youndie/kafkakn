# kafkakn

[![check](https://github.com/youndie/kafkakn/actions/workflows/check.yaml/badge.svg)](https://github.com/youndie/kafkakn/actions/workflows/check.yaml)
[![suite](https://github.com/youndie/kafkakn/actions/workflows/suite.yaml/badge.svg)](https://github.com/youndie/kafkakn/actions/workflows/suite.yaml)
[![license](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A Kafka client for Kotlin Multiplatform: a producer, a consumer with groups and exactly-once, an admin client, and
Schema Registry serializers for `@Serializable` types.

- **On `linuxX64`** it is librdkafka 2.13.0, linked statically into your binary. A service compiled with Kotlin/Native
  talks to Kafka with no JVM and no extra shared libraries.
- **On the JVM** it is the official `org.apache.kafka:kafka-clients` 4.3.1.

One API in common code, the same behaviour on both targets. The whole test suite runs on both against one broker, and
the native side is correct when it agrees with the official client. Where the two clients underneath genuinely
differ, the [contracts](#documentation) say so rather than hide it.

## Getting it

Published to `https://reposilite.kotlin.website/snapshots`, not to Maven Central. Every release is a version of its own,
`0.1.0.<n>`, never overwritten, and tagged `v0.1.0.<n>` in this repository; the newest is the highest tag. There is
no compatibility promise between releases yet.

```kotlin
plugins {
    kotlin("multiplatform") version "2.4.20"
}

repositories {
    maven("https://reposilite.kotlin.website/snapshots") {
        content { includeGroupAndSubgroups("io.github.youndie") }
    }
    mavenCentral()
}

kotlin {
    jvm()
    linuxX64 {
        binaries.executable { entryPoint = "main" }
    }

    sourceSets.commonMain.dependencies {
        implementation("io.github.youndie.kafkakn:kafkakn-core:0.1.0.14")
        // Optional: Schema Registry, JSON Schema and Protobuf.
        implementation("io.github.youndie.kafkakn:kafkakn-schema-registry:0.1.0.14")
    }
}
```

- **Use Kotlin 2.4.20.** A klib carries compiler metadata, and a build on another Kotlin version may refuse it.
- **Nothing to configure for the native link.** The native artefact carries librdkafka, OpenSSL, zlib and zstd inside
  the klib.
- **The first native build downloads the Kotlin/Native toolchain.** From a container with only a JDK and Gradle, this
  build file got to a record on a topic in 106–109 s, of which 74–78 s was the toolchain arriving.

## Producing

```kotlin
val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to "kafka:9092", "acks" to "all"))
try {
    val written: RecordMetadata = producer.send(
        ProducerRecord(
            topic = "orders",
            value = payload,                       // ByteArray: Kafka values have no encoding
            key = orderId.encodeToByteArray(),
            headers = listOf(RecordHeader("trace", traceId.encodeToByteArray())),
        ),
    )
    println("${written.partition}@${written.offset}")
} finally {
    producer.close()
}
```

**`send` returns when the broker has acknowledged the record, or throws.** There is no third outcome: holding a
`RecordMetadata` is the acknowledgement. What follows from that:

- **A full queue is backpressure, not an error.** `send` suspends until there is room, for up to `max.block.ms`, then
  throws `RecordNotQueuedException`: the record was never queued, and retrying cannot write it twice.
- **Throughput comes from concurrency.** One `send` is one record and one acknowledgement. Call it from many
  coroutines at once, and the client batches what is in flight.
- **Two steps when you need a deadline.** `enqueue(record)` returns a `Delivery` once the record is queued, and
  `delivery.await()` waits for the broker. If `enqueue` throws, nothing was written. If it returned, the record goes
  on whatever you do next: cancelling `await` does not recall it.
- **`close(timeout)` bounds a shutdown.** A record not acknowledged in time fails with
  `ClosedBeforeAcknowledgedException`. It may still have been written, so retrying it may write it twice.
- **Idempotent by default, on both targets,** with the Java client's conditions: it turns off if you set `acks` to
  anything but `all`, or `retries=0`.
- **Transactions:** set `transactional.id`, call `initTransactions()` once, then `producer.inTransaction { ... }`,
  which aborts on any failure, cancellation included.

The reason for this shape: `rd_kafka_produce` only enqueues, and a record it refuses produces no delivery report at
all. A naive binding that counted delivery reports lost 264 826 of 1 000 000 records while reporting complete success.

## Consuming

```kotlin
val consumer = kafkaConsumer(
    ConsumerConfig("bootstrap.servers" to "kafka:9092", "group.id" to "billing", "auto.offset.reset" to "earliest"),
)
consumer.subscribe(listOf("orders"))
consumer.records().collect { record ->         // a cold Flow over poll()
    handle(record.key, record.value)
    consumer.commit()
}
```

- **Commits are yours.** `enable.auto.commit` is `false` on both targets, because the two clients mean different
  things by `true`. `commit()` commits the position after everything `poll` has returned; `commit(offsets)` commits
  exactly the offsets you name.
- **`poll(timeout)`** returns up to 500 records, or an empty list once `timeout` passes. It never holds the caller's
  dispatcher, and the consumer is still usable after a cancelled `poll`.
- **Without a group:** `assign(partitions)`, and `seek(partition, SeekTo.Beginning)` (or `End`, `Offset`,
  `Timestamp`).
- **Rebalances:** `subscribe(topics, listener)` with `onAssigned`, `onRevoked` and `onLost`. The listener runs inside
  `poll`, so it commits and seeks through the scope it is handed, not through the consumer. Cooperative rebalancing
  (`partition.assignment.strategy=cooperative-sticky`), static membership (`group.instance.id`) and the KIP-848
  protocol (`group.protocol=consumer`) are supported. The default protocol stays each client's own, `classic`.
- **Exactly once, read-process-write:** a transactional producer and `sendOffsetsToTransaction(offsets,
  consumer.groupMetadata())`. Readers get `isolation.level=read_committed` by default on both targets.

## Administering

```kotlin
val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to "kafka:9092"))
admin.createTopics(listOf(NewTopic("orders", partitions = 6, replicationFactor = 3)))
```

It covers:
- **topics:** create, delete, describe, add partitions, read and change their configuration;
- **records:** delete up to an offset, and look up offsets (`listOffsets`);
- **the cluster:** describe it;
- **consumer groups:** list and describe them, read, move and delete their offsets, delete a group.

- **Errors you can catch in common code:**
  - `TopicExistsException`;
  - `UnknownTopicException`;
  - `GroupNotEmptyException`;
  - `IllegalArgumentException` for an argument the broker refused;
  - `KafkaAdminException` for everything else.

  On the JVM, the Java client's own exception is the `cause`.
- **A topic is not visible the moment `createTopics` returns.** Described at once, a new topic was unknown in 199 of
  200 tries on both targets, and visible within 134 ms, typically 25–35 ms. That is Kafka. If you describe what you
  have just created, wait for it.

## Schema Registry

```kotlin
@Serializable
data class Order(val id: Long, val total: Double, val note: String? = null)

val registry = SchemaRegistry("http://schema-registry:8081")
val orders = registry.jsonSchemaSerde<Order>(valueSubject("orders"))   // or protobufSerde<Order>(...)

orders.register()                                    // optional: fail at startup, not at the first record
producer.send(ProducerRecord("orders", orders.encode(Order(1, 9.5))))
val order: Order = orders.decode(record.value!!)
```

- **`@Serializable` needs the serialization compiler plugin** in your build: `kotlin("plugin.serialization")`, at the
  Kotlin version above.
- **Confluent's own serializers read these bytes, and kafkakn reads theirs.** Measured both ways against
  `KafkaJsonSchemaSerializer` and `KafkaProtobufSerializer` 8.3.2, on both targets. A Java or Go service on the same
  topic needs nothing from you.
- **The schema is generated from the type**, and registered once. The JSON Schema is draft-07 and closed
  (`additionalProperties: false`): that is what lets a type gain an optional field under the registry's default
  `BACKWARD` compatibility. The Protobuf schema is the `.proto` kotlinx.serialization generates.
- **A change the subject refuses** (a required field removed, say) throws `IncompatibleSchemaException` with the
  registry's own reason, before any record is made.
- **Not supported:**
  - Avro, which has no Kotlin Multiplatform library;
  - polymorphic types;
  - schema ids by GUID or in record headers.
- **HTTPS works on the JVM**, through the default CIO client or any `HttpClient` you pass.
- **From native, reach the registry over HTTP**, in-cluster or through a TLS-terminating sidecar. Ktor's CIO has no TLS
  on Kotlin/Native, and its Curl engine carries its own OpenSSL, which does not link beside kafkakn's.

## Configuration

**Kafka's own keys, verbatim:** `bootstrap.servers`, `acks`, `group.id`, `security.protocol`. There is no
kafkakn-specific spelling.

- **An unknown key fails at construction.** A setting that was accepted and silently dropped would look exactly like
  one that worked.
- **Some keys exist on one target only.** Examples: `queue.buffering.max.messages` on native, `buffer.memory` and
  `linger.ms` on the JVM. The other target refuses them at construction. Code for both targets passes the common keys
  and adds the platform ones per target. The [producer](docs/api/producer-contract.md) and
  [consumer](docs/api/consumer-contract.md) contracts list which keys work on both, and each target's defaults.
- **TLS:** `security.protocol=SSL` and `ssl.ca.location` (a PEM file). For client certificates, also
  `ssl.certificate.location`, `ssl.key.location` and `ssl.key.password`. The same keys work on both targets.
  **Certificate verification cannot be turned off.** Hostname checking can, with
  `ssl.endpoint.identification.algorithm=none`.
- **SASL:** `PLAIN` and `SCRAM-SHA-256`/`SCRAM-SHA-512`, through `sasl.mechanism`, `sasl.username` and
  `sasl.password`. `OAUTHBEARER` takes an `OAuthBearerTokenProvider` you supply: kafkakn does not fetch OIDC tokens
  itself.

## The native binary

Measured on a binary built from the published artefact, against the same binary built without it:

- **The same shared libraries, exactly.** Linking kafkakn adds none: `libc`, `libm`, `libpthread`, `libdl`, `librt`,
  `libresolv`, `libutil`, `libcrypt`, `libgcc_s` and the loader, all there before.
- **glibc 2.17 or newer.** This is the one thing kafkakn raises, because the C bundle is built in a `manylinux2014`
  image.
- **librdkafka 2.13.0 with one local patch.** `rdrand.c` includes a header glibc 2.17 does not have. The patch is
  re-applied and re-tested on every librdkafka upgrade.
- **The default partitioner is the Java client's.** Native sets librdkafka's `murmur2_random`, so a key lands on the
  same partition whichever target produced it. librdkafka's own default, a CRC32, would not.

## Where the two targets differ

Both clients are the reference on their own platform, and kafkakn does not paper over the differences. The ones worth
knowing:

- **An invalid value** (`acks=99`) fails at construction on the JVM, and at the broker on native.
- **`poll`'s timeout** is counted on the wall clock on the JVM (the Java client's timer), and on the monotonic clock on
  native. On a host whose clock jumps forward, a JVM `poll` can return early.
- **A burst of concurrent sends into a topic created a moment before** can leave the Java client's first batch stuck
  until `delivery.timeout.ms`, with `OutOfOrderSequenceException` at the broker. Native does not do this. Sending and
  awaiting one record per partition first avoids it.
- **With every broker down**, both targets refuse a record for a topic they know under the default
  `metadata.recovery.strategy=rebootstrap`, and both queue it under `none`.

The contracts list the rest, each with how it was measured.

## Not supported

| | |
|---|---|
| `linuxArm64`, macOS | built and tested on request, not published |
| Windows, `musl`, Kafka Streams, Avro | not planned |
| ACLs and broker configuration in the admin client | not planned |
| OIDC token fetching for `OAUTHBEARER` | not planned: you supply the token |
| HTTPS to a Schema Registry from a native binary | use HTTP, see above |

## Documentation

- [Producer contract](docs/api/producer-contract.md): what `send`, the configuration, transactions and the admin
  client promise, and where the targets differ.
- [Consumer contract](docs/api/consumer-contract.md): polling, groups, rebalances, commits, exactly-once.
- [Schema Registry](docs/services/kafkakn-schema-registry.md): the module, its wire format, and its limits.
- [`ci/downstream`](ci/downstream/src/commonMain/kotlin/Main.kt): a separate build that resolves the published
  artefact, links a native binary and produces with it.

To work on kafkakn itself, start at [docs/README.md](docs/README.md) and [CLAUDE.md](CLAUDE.md).

## License

MIT.
