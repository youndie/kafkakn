# kafkakn

[![license](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A Kafka **producer** for Kotlin Multiplatform — so that a service compiled to a single
Kotlin/Native binary can produce to Kafka without a JVM anywhere, and so that the native
implementation can be proved right against the official one.

```kotlin
val producer = kafkaProducer(
    ProducerConfig(
        "bootstrap.servers" to "kafka:9092",
        "acks" to "all",
    ),
)
val where = producer.send(
    ProducerRecord(
        topic = "orders",
        value = payload,                                  // bytes, not text
        key = orderId.encodeToByteArray(),
        headers = listOf(RecordHeader("trace", traceId.encodeToByteArray())),
    ),
)                                                         // returns when the broker acknowledged
producer.close()
```

Something very close to that is [`ci/consumer`](ci/consumer/src/commonMain/kotlin/Main.kt), which is
a build of its own: it resolves the published artefact from the network, links a native binary and
runs it, and an independent reader counts what arrived.


## Getting it

Snapshots only — no release, no Maven Central, no version promise
([D7](docs/research/research-architecture.md)).

```kotlin
repositories {
    maven("https://reposilite.kotlin.website/snapshots") {
        content { includeGroupAndSubgroups("io.github.youndie") }
    }
}

dependencies {
    implementation("io.github.youndie.kafkakn:kafkakn-core:0.1.0-SNAPSHOT")
}
```

Three coordinates, because a KMP module has one per target: `kafkakn-core` (metadata),
`kafkakn-core-jvm`, `kafkakn-core-linuxx64`. The native one carries librdkafka and its TLS stack
**inside the klib**, so a downstream link needs no configuration of its own.

## The idea

One `expect` surface, two actuals:

| Target | Behind it | Why |
|---|---|---|
| `linuxX64` | librdkafka 2.13.0 through cinterop, linked statically | the reason the project exists — a producer inside a single binary, with no runtime dependency beyond libc |
| `jvm` | `org.apache.kafka:kafka-clients` 4.3.1 | the reference implementation, and therefore **the oracle**: one `commonTest` suite runs on both arms against one broker, and the native arm is correct when it agrees |

The only published Kafka client for Kotlin/Native (`com.icemachined:kafka-client` 0.2.0, October
2022) ships three native artefacts and **no JVM variant**, so it has no way to check itself except by
believing what a broker tells it. That asymmetry is this project's whole argument — and it earns its
keep: the oracle found a real defect on the first day it ran, and an external consumer found another
the library's own suite could not see.

## The thing worth knowing before reading the code

`rd_kafka_produce` only **enqueues**, and when its queue is full it refuses — which is backpressure,
not an error. **A record that was never queued produces no delivery report**, so a producer that
counts delivery reports sees a perfect success rate while losing records. A measured naive binding
lost **264 826 of 1 000 000** this way, with a successful flush.

Everything about this API follows from that:

- **`send` suspends** rather than returning a failure the caller may ignore. It returns when the
  broker has acknowledged, so holding a `RecordMetadata` *is* the acknowledgement — there is no
  third outcome to inspect.
- **The reconciliation is against what the caller handed in**, and it lives in the test suite rather
  than in the library.
- **No public API exposes a delivery-report count**, and that is a build gate
  (`scripts/no_delivery_counters.py`), not a habit.
- **`send` does not batch for you.** One call is one record and one acknowledgement; throughput
  comes from calling it concurrently, and both clients batch internally once records are in flight
  together.

## What it deliberately does not do

| Not done | Why |
|---|---|
| **Consumers, consumer groups, rebalancing** | where most of a Kafka client's difficulty lives; a thin consumer shipped for symmetry would be worse than none |
| **Transactions, exactly-once** | out of scope until asked |
| **Admin API, Schema Registry, Streams** | out of scope |
| **SASL** | out of scope; TLS is in, with verification on and no way to turn it off from this API |
| **`linuxArm64`** | designed for and not built: a line in the build and a row in the matrix, and the klib's C archives would be built a second time |
| **macOS, Windows, `musl`** | out of scope. The spike measured `macosArm64` and it links; the cost of leaving it out is that a contributor on a Mac cannot run the native arm locally and has to use the Linux box or CI |

## Documentation

Start at [docs/README.md](docs/README.md) — the research says why the architecture is what it is,
[the producer contract](docs/api/producer-contract.md) says what both arms are held to, and
[backlog.md](backlog.md) is the queue. The interesting reading is the places where the two arms do
**not** agree and the contract says so instead of promising it away: the partitioner, the queue
bound, where a configuration value is refused, and how long an unverifiable TLS peer takes to fail.

## Checks

```bash
make check              # the documents
./gradlew ktlintCheck   # the code
```

Both run in CI. The suite itself needs the C bundle and a broker and runs on a Linux box — each
item's `ci/b-NN/run.sh` is what runs it, and `ci/consumer` is a build outside this one that resolves
the published artefact and runs it.

## License

MIT.
