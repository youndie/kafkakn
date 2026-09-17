# kafkakn

[![license](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A Kafka **producer** for Kotlin Multiplatform — so that a service compiled to a single
Kotlin/Native binary can produce to Kafka without a JVM anywhere, and so that the native
implementation can be proved right against the official one.

> **Status: design only.** Nothing is implemented, nothing is published, and there is no build yet.
> This repository holds the research, the contract and the backlog. Start at [docs/](docs/README.md).

## The idea

One `expect` surface, two actuals:

| Target | Behind it | Why |
|---|---|---|
| `linuxX64` | librdkafka 2.13.0 through cinterop, linked statically | the reason the project exists — a producer inside a single binary, with no runtime dependency beyond libc |
| `jvm` | `org.apache.kafka:kafka-clients` 4.3.1 | the reference implementation, and therefore **the oracle**: the same test suite runs on both arms against one broker, and the native arm is correct when it agrees |

`linuxArm64` is designed for and not built.

The only published Kafka client for Kotlin/Native (`com.icemachined:kafka-client` 0.2.0, October
2022) ships three native artefacts and **no JVM variant**, so it has no way to check itself except by
believing what a broker tells it. That asymmetry is this project's whole argument.

## What it will deliberately not do

| Not done | Why |
|---|---|
| **Consumers, consumer groups, rebalancing** | where most of a Kafka client's difficulty lives; a thin consumer shipped for symmetry would be worse than none |
| **Transactions, exactly-once** | out of scope until asked |
| **Admin API, Schema Registry, Streams** | out of scope |
| **SASL** | out of scope; TLS is in |
| **macOS, Windows, `musl`** | out of scope |

## The thing worth knowing before reading the code

`rd_kafka_produce` only **enqueues**, and when its queue is full it refuses — which is backpressure,
not an error. **A record that was never queued produces no delivery report**, so a producer that
counts delivery reports sees a perfect success rate while losing records. A measured naive binding
lost **264 826 of 1 000 000** this way, with a successful flush.

Everything about this library's API follows from that: `send` suspends rather than failing, the
reconciliation is against what the caller handed in, and no public API exposes a delivery-report
count.

## Documentation

Start at [docs/README.md](docs/README.md). The backlog is [backlog.md](backlog.md).

## Checks

```bash
make check
```

## License

MIT.
