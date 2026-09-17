---
id: kafkakn-core
title: kafkakn-core — the library module
type: service
repo_url: https://github.com/youndie/kafkakn
module: kafkakn-core
tech_stack: [Kotlin Multiplatform, Kotlin/Native, cinterop, librdkafka, kafka-clients]
owner: unassigned
depends_on:
  - test-broker
publishes:
  - io.github.youndie:kafkakn-core (snapshots, reposilite)
---

# kafkakn-core

The one published module: a Kafka producer behind a single `expect` surface, with two actuals.
Nothing here exists yet — `status` of every layer document says what is built, and today the answer
is nothing. The shape below is the **target**.

## Targets

| Target | Status | Actual | Why |
|---|---|---|---|
| `jvm` | mandatory | `org.apache.kafka:kafka-clients` 4.3.1 | the reference implementation, and therefore the oracle ([research §1.1](../research/research-architecture.md)) |
| `linuxX64` | mandatory | librdkafka 2.13.0 through cinterop, linked statically | the reason the project exists: a producer inside a single binary |
| `linuxArm64` | designed for, not built | same as `linuxX64` | [D6](../research/research-architecture.md) — a build-matrix row, claimed nowhere until it is one |

Deliberately absent: macOS, Windows, `musl`, JS, Wasm. The spike measured `macosArm64` and it works;
it is not in this project's scope and no claim is made here.

## The module cut, and why it is one module

One published artefact, `kafkakn-core`. A `kafkakn-testing` module is expected later for the
fixtures an external consumer needs ([B-13](../backlog/B-13-external-consumer-acceptance.md)); it is
not split out before there is a consumer to need it, because a module with one in-tree caller
accumulates API nobody has used.

Package: `io.github.youndie.kafkakn`. Group: `io.github.youndie`.

## The source-set layout

```
kafkakn-core/src/
  commonMain/      the expect surface, the record types, the configuration
  commonTest/      THE SUITE - every producer assertion, run on both actuals
  jvmMain/         actual over kafka-clients
  nativeMain/      actual over librdkafka; the cinterop seam
  nativeInterop/cinterop/rdkafka.def
```

`commonTest` is the point. A test that can only be written in `jvmTest` or `nativeTest` is either
about the platform seam itself or a sign the `expect` surface leaked a platform's shape — see
[H1](../research/research-architecture.md).

## Build and publication

- Snapshots to `reposilite.kotlin.website/snapshots` under a **content filter**, so an outage there
  cannot fail the resolution of anything else ([D7](../research/research-architecture.md)).
- **No Maven Central**, no release, no version promise.
- **Three coordinates, not one.** A KMP module has as many as it has targets, and a route that
  covers one covers none of the others:

  | Coordinate | What a consumer gets |
  |---|---|
  | `io.github.youndie:kafkakn-core` | the metadata module — what common code asks for |
  | `io.github.youndie:kafkakn-core-jvm` | the jvm variant |
  | `io.github.youndie:kafkakn-core-linuxx64` | the native variant, with the cinterop klib beside it |

  `ci/publish/run.sh` names all three after a publish and then compiles a **separate build** against
  them from a cache purged of this group — and requires that same probe to fail against an empty
  repository, because "it resolved" says nothing about where it resolved from.
- The coordinate lives in `gradle.properties` and nowhere else. Gradle applies `group` and `version`
  to every project, so a module cannot publish under a different one by forgetting to set it.

## Quirks — the ones that will bite

- **The C bundle is not built by Gradle.** It is produced by a script into a cache outside the
  source tree and consumed by cinterop as static archives. Building it inside the Gradle graph
  would put a multi-minute Docker build on every clean checkout.
- **Publishing the native variant needs the C bundle.** `-Pkafkakn.noKafkaC` produces a module
  without the cinterop — a different artefact wearing the same coordinate. It is a measurement aid
  and never a publication route, which is why `.github/workflows/publish.yaml` builds the bundle
  rather than dropping it.
- **The cinterop definition names no target.** Paths arrive from the build script, so a second
  native target is a matrix row ([D6](../research/research-architecture.md)).
- **The native actual cannot use `rd_kafka_producev`** — it is variadic
  ([research §1.5](../research/research-architecture.md)) — so per-message headers need their own
  mechanism and are not in M1.
- **`rd_kafka_flush` returns an error code, not a count.** Anything that treats its return as "how
  many are left" is wrong; the count is `rd_kafka_outq_len`.

## Code anchors

| What | Where |
|---|---|
| the expect surface | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/` |
| the common suite | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/` |
| the native actual and the cinterop seam | `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/` |
| the cinterop definition | `kafkakn-core/src/nativeInterop/cinterop/rdkafka.def` |
| the C bundle build | `ci/librdkafka/` |
