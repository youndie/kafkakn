# kafkakn

[![check](https://github.com/youndie/kafkakn/actions/workflows/check.yaml/badge.svg)](https://github.com/youndie/kafkakn/actions/workflows/check.yaml)
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
([D7](docs/research/research-architecture.md)). What would end that is a **condition, not a date**:
somebody outside this portfolio turning up and wanting the library. Nothing is being done to bring
one — the repository is not announced anywhere, and the item that would have tested demand was
dropped for that reason ([B-21](docs/backlog/B-21-does-anyone-want-this.md)) — so the honest reading
is that snapshots are where this stays until that happens by itself.

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

### What the native artefact requires

Measured 2026-09-17 by [`ci/b-16/run.sh`](ci/b-16/run.sh), on the binary a downstream build
(`ci/consumer`) links against one built the same way with the dependency removed — not on this
repository's own test binary, which was once linked with options no stranger had.

- **Shared libraries: the same set, exactly.** `linux-vdso`, the loader, `libc`, `libcrypt`, `libdl`,
  `libgcc_s`, `libm`, `libpthread`, `libresolv`, `librt`, `libutil` — and every one of those is there
  before kafkakn is. Linking it adds none and removes none. (The earlier wording here, "no runtime
  dependency beyond libc", was false in the first word that matters: `libgcc_s` is not libc, and one
  `ldd` refutes it.)
- **glibc 2.17 or newer**, and this is the one thing kafkakn does raise: the same binary without it
  references nothing above 2.14. 2.17 is `manylinux2014`'s, which is where the C bundle is built
  ([D4](docs/research/research-architecture.md)) — the floor is the bundle's build image, by
  decision, and the script fails if it ever moves.
- **The bundle carries a one-line local patch.** librdkafka 2.13.0 includes `<sys/random.h>` from
  `rdrand.c` under `#ifndef _WIN32` only, while the `getentropy` call it is there for is guarded by
  `HAVE_GETENTROPY`; the header arrived in glibc 2.25, so on a 2.17 image the include has to go.
  **There is no upstream pull request and there will not be one** — nothing from this project is
  filed anywhere — so the patch is a file in this repository, re-applied and re-tested on every
  librdkafka bump ([B-03](docs/backlog/B-03-c-bundle-old-glibc.md)).

## The idea

One `expect` surface, two actuals:

| Target | Behind it | Why |
|---|---|---|
| `linuxX64` | librdkafka 2.13.0 through cinterop, linked statically | the reason the project exists — a producer inside a single binary, and **`ldd` on it is the same set as on the same binary built without kafkakn** (measured, see below) |
| `jvm` | `org.apache.kafka:kafka-clients` 4.3.1 | the reference implementation, and therefore **the oracle**: one `commonTest` suite runs on both arms against one broker, and the native arm is correct when it agrees |

The only published Kafka client for Kotlin/Native (`com.icemachined:kafka-client` 0.2.0, October
2022) ships three native artefacts and **no JVM variant**, so it has no way to check itself except by
believing what a broker tells it. That asymmetry is this project's whole argument, and it has earned
its keep twice:

- **The two clients put the same key on different partitions.** librdkafka hashes keys with CRC32,
  the Java producer with murmur2; of eight keys, five landed differently depending on which arm
  produced them. Each arm was individually consistent with the broker, so neither could have noticed
  alone — and the symptom would have surfaced in somebody else's consumer, as records for one key
  arriving out of order ([B-07](docs/backlog/B-07-native-actual.md),
  [research §2.2](docs/research/research-architecture.md)).
- **The published klib carried the bindings and not the C.** Eleven items passed over it: this
  project's own test binaries were linked with `linkerOpts` naming archives on the build machine, so
  the suite was green while a stranger's link failed with 14 undefined symbols. It took a build that
  was not this one to find it ([B-15](docs/backlog/B-15-native-klib-carries-no-c.md), §2.12).

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
