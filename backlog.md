# Backlog: a Kafka producer a Kotlin/Native service can link in

> Role of this document: the backlog. **One file per item in [`docs/backlog/`](docs/backlog/)** —
> `B-NN-<slug>.md`. What lives here is the index (generated) and everything that is not an item: the
> goal, the stages, and the decisions.
>
> New item: copy [`docs/templates/backlog-item.md`](docs/templates/backlog-item.md), take the next
> free `B-NN`, and run `python3 scripts/backlog_index.py` after editing.

## Goal

A Kafka **producer** that a Kotlin/Native service can link into a single binary, with a JVM arm that
exists so the native arm can be proved right. The architecture and the evidence behind it are in
[docs/research/research-architecture.md](docs/research/research-architecture.md); read it before
taking an item, because the obvious thing here is wrong in at least four documented ways.

Nothing is built yet. Every layer document says `status: draft` and means it.

## Stages

| Stage id | Stage | What it is |
|---|---|---|
| `stage-0-it-builds` | It builds, and a test can fail | Targets, the `expect` surface, the C bundle, the broker, and the differential harness — before any producer exists. |
| `stage-1-produce` | It produces, and loses nothing | Both actuals, backpressure, and the accounting that catches the defect this project is shaped around. |
| `stage-2-real-use` | Usable against a real deployment | Headers and TLS. |
| `stage-3-usable-by-others` | Someone else can use it | Snapshots, and a consumer outside this repository that proves it. |

## Marks

`[ ]` open · `[~]` in progress · `[x]` done · `[?]` open question · `[-]` dropped

<!-- BEGIN INDEX -->

## Open (1)

| Task | | Priority | Size | Blocked by |
|---|---|---|---|---|
| [B-13](docs/backlog/B-13-external-consumer-acceptance.md) `[~]` | Acceptance from outside: a consumer project that uses the published artefact | P1 | M | B-12, B-15 |

## Closed (14)

**It builds, and a test can fail**

- [B-01](docs/backlog/B-01-gradle-skeleton.md) `[x]` - Gradle skeleton: jvm and linuxX64 targets, pinned catalogue
- [B-02](docs/backlog/B-02-expect-surface.md) `[x]` - The expect surface, compiling and throwing
- [B-03](docs/backlog/B-03-c-bundle-old-glibc.md) `[x]` - The C bundle, built against glibc 2.17, consumed by cinterop
- [B-04](docs/backlog/B-04-broker-fixture.md) `[x]` - The broker fixture: KRaft, three partitions, auto-create off
- [B-05](docs/backlog/B-05-differential-harness.md) `[x]` - The differential harness: one suite, both actuals, one broker
- [B-14](docs/backlog/B-14-ci-workflow.md) `[x]` - CI: make check on every pull request

**It produces, and loses nothing**

- [B-06](docs/backlog/B-06-jvm-actual.md) `[x]` - The JVM actual over kafka-clients
- [B-07](docs/backlog/B-07-native-actual.md) `[x]` - The native actual: produce and delivery reports across the callback seam
- [B-08](docs/backlog/B-08-suspend-on-backpressure.md) `[x]` - Suspend on backpressure instead of failing
- [B-09](docs/backlog/B-09-accounting.md) `[x]` - Account for every record the caller handed in

**Usable against a real deployment**

- [B-10](docs/backlog/B-10-record-headers.md) `[x]` - Record headers without rd_kafka_producev
- [B-11](docs/backlog/B-11-tls.md) `[x]` - TLS on both arms

**Someone else can use it**

- [B-12](docs/backlog/B-12-publish-snapshots.md) `[x]` - Publish snapshots to reposilite
- [B-15](docs/backlog/B-15-native-klib-carries-no-c.md) `[x]` - The published native klib does not carry its C dependency

<!-- END INDEX -->

## Decisions worth not re-litigating

**The JVM arm is the oracle, and it is built first.** The only published Kafka client for
Kotlin/Native has no JVM variant, so it can only check itself against its own understanding of the
protocol. Here the same `commonTest` suite runs on both arms against one broker, and the native arm
is correct when it agrees with `org.apache.kafka:kafka-clients`. That is why
[B-05](docs/backlog/B-05-differential-harness.md) is in stage 0 and not at the end.

**First a test from the specification, then the code; the test passing is the gate.** A test cites a
place in [the producer contract](docs/api/producer-contract.md) or in Kafka's own documentation.
A test written from common sense is not accepted — the common sense here says a produce call that
returns has sent something, and on the native side that is false.

**Producer only.** No consumer, no group coordination, no transactions, no Admin API, no SASL. Group
coordination is where most of a Kafka client's difficulty lives, and a thin consumer shipped for
symmetry would be the worse outcome. The fence moves when somebody asks, not because the shape looks
incomplete.

**A record that was never queued yields no delivery report.** This is the mechanism behind a measured
loss of 264 826 records in 1 000 000 with every indicator green. Everything in
[feature-backpressure-and-accounting](docs/features/feature-backpressure-and-accounting.md) follows
from it, and the reconciliation that catches it lives in the **suite**, against the broker's own
offsets — never in the library counting its own successes.

**The C bundle is built against an old glibc, not against the host.** Both routes were measured. This
one trades three `konan.properties` keys JetBrains may change at any patch release for a pinned
build image and a one-line patch this project carries — the difference being *when* the breakage
lands: on somebody else's release, or on ours.

**Snapshots to reposilite, never Maven Central.** No release, no version promise. Publication is a
decision nobody has taken.
