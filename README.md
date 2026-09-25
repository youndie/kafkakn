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

Something very close to that is [`ci/downstream`](ci/downstream/src/commonMain/kotlin/Main.kt), which is
a build of its own: it resolves the published artefact from the network, links a native binary and
runs it, and an independent reader counts what arrived.


## Getting it

Snapshots only — no release, no Maven Central, no version promise
([D7](docs/research/research-architecture.md)). What would end that is a **condition, not a date**:
somebody outside this portfolio turning up and wanting the library. Nothing is being done to bring
one — the repository is not announced anywhere, and the item that would have tested demand was
dropped for that reason ([B-21](docs/backlog/B-21-does-anyone-want-this.md)) — so the honest reading
is that snapshots are where this stays until that happens by itself.

The whole of a build file that links it, because the two halves that usually get shown on their own
do not compose into a working one — measured, see [`ci/b-20/run.sh`](ci/b-20/run.sh):

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
    linuxX64("native") {
        binaries.executable { entryPoint = "main" }
    }

    sourceSets.commonMain.dependencies {
        implementation("io.github.youndie.kafkakn:kafkakn-core:0.1.0-SNAPSHOT")
    }
}
```

Three coordinates, because a KMP module has one per target: `kafkakn-core` (metadata),
`kafkakn-core-jvm`, `kafkakn-core-linuxx64`. The native one carries librdkafka and its TLS stack
**inside the klib**, so a downstream link needs no configuration of its own.

**The Kotlin version is part of the instructions, not a detail.** A klib carries metadata that a
build on another compiler refuses, so `2.4.20` above is the version this is known to work with rather
than a placeholder. `send` suspends, and the coroutines runtime arrives with the dependency — the
build file above compiles a caller that uses `runBlocking` with nothing else added.

**Measured from an empty machine**: a container with a JDK, Gradle and nothing else — no clone, no
Gradle cache, no `~/.konan` — gets from that build file to a record on a topic in **106 and 109
seconds** across two runs, of which **74–78 s is the Kotlin/Native toolchain downloading** and about
31 s is everything else ([`ci/b-20/run.sh`](ci/b-20/run.sh)). Most of a first build is Kotlin/Native
arriving, and that price is not this library's.

### What the native artefact requires

Measured 2026-09-17 by [`ci/b-16/run.sh`](ci/b-16/run.sh), on the binary a downstream build
(`ci/downstream`) links against one built the same way with the dependency removed — not on this
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

## What it does not do yet

**None of this exists today.** Since 2026-09-24 most of it is planned, in an order that keeps the hard
part last ([backlog.md](backlog.md), stages 5 to 9) — and every item arrives with a test that holds
the two arms to one answer, because both clients underneath already implement all of it and the gap
is surface, not implementation.

| Not done yet | Where it stands |
|---|---|
| **Topic metadata** | planned — [B-29](docs/backlog/B-29-topic-metadata.md). An explicit partition and a record timestamp are in since [B-27](docs/backlog/B-27-a-record-can-name-its-partition.md) and [B-28](docs/backlog/B-28-a-record-carries-its-timestamp.md) |
| **OIDC token fetching** | not planned — OAUTHBEARER is in since [B-33](docs/backlog/B-33-sasl-oauthbearer.md) with a token the caller supplies, since the native bundle has no curl. PLAIN and SCRAM are in since [B-32](docs/backlog/B-32-sasl-plain-and-scram.md), TLS since B-11, client certificates since [B-31](docs/backlog/B-31-client-certificates.md), and **certificate trust cannot be turned off** — the key that would do it is refused on both arms, because it exists only on the one without an oracle ([B-18](docs/backlog/B-18-verification-cannot-be-turned-off.md)). Hostname checking is the one thing that can be relaxed, with `ssl.endpoint.identification.algorithm=none` |
| **Admin beyond topics and the cluster** | listing and describing consumer groups is in since [B-58](docs/backlog/B-58-list-and-describe-consumer-groups.md), and reading a group's committed offsets and a partition's offsets since [B-59](docs/backlog/B-59-consumer-group-offsets-and-lag.md). Planned since 2026-09-25 — resetting and deleting group offsets ([B-60](docs/backlog/B-60-reset-and-delete-group-offsets.md)), topic configuration ([B-61](docs/backlog/B-61-topic-configs.md)), adding partitions ([B-62](docs/backlog/B-62-create-partitions.md)) and deleting records ([B-63](docs/backlog/B-63-delete-records.md)). ACLs are not planned. Creating, deleting and describing topics and describing the cluster are in since [B-34](docs/backlog/B-34-a-minimal-admin.md) |
| **Rebalance callbacks, `Flow`, the KIP-848 group protocol** | planned — a rebalance listener ([B-50](docs/backlog/B-50-a-rebalance-listener.md)), a `Flow` over `poll` ([B-54](docs/backlog/B-54-a-flow-over-poll.md)), cooperative rebalancing, static membership and KIP-848 ([B-55](docs/backlog/B-55-cooperative-rebalancing.md) to [B-57](docs/backlog/B-57-the-kip-848-consumer-protocol.md)). A consumer that assigns and seeks ([B-36](docs/backlog/B-36-assign-and-poll.md)) and joins groups with manual commits ([B-37](docs/backlog/B-37-consumer-groups.md)) is in, built to a contract designed first ([B-35](docs/backlog/B-35-the-consumer-designed-first.md)) — and a group with one member on each arm is measured |
| **`linuxArm64` as a shipped target** | not planned: not published and not run in CI, by decision. It is built on request (`-Pkafkakn.linuxArm64`) and run on arm64 hardware since [B-39](docs/backlog/B-39-linux-arm64.md): the native suite passes there and agrees with the JVM arm (93 tests, 17 observations, measured 2026-09-25). Its binaries need glibc 2.17, the same as x64, and have been run on it ([B-44](docs/backlog/B-44-arm64-glibc-floor.md)) |
| **macOS as a shipped target** | not published. `macosArm64` exists for contributors since [B-40](docs/backlog/B-40-macos-for-contributors.md), declared only on a Mac: the native suite runs there against the broker and agrees with the JVM arm (92 tests, 17 observations, measured 2026-09-25). No `macosX64`, no Windows, no `musl` |
| **Schema Registry, Streams, Windows, `musl`** | not planned. The first two are in neither client underneath, so they are not a gap between kafkakn and what it wraps |

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
item's `ci/b-NN/run.sh` is what runs it, and `ci/downstream` is a build outside this one that resolves
the published artefact and runs it.

## License

MIT.
