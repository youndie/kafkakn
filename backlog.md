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

**Stages 0 to 3 are closed**: both arms produce, the differential suite runs on one broker, and
snapshots are on reposilite with a build outside this one that resolves them, links a native binary
and runs it.

Stage 4 is a different kind of question and is budgeted like one — **three working days of build,
hard stop**. What the project lacks is not a second target but a first real user, and two of its
claims had never met one: that `close` keeps its promise inside an ordered shutdown, and that a
stranger can get from the README to a running binary.

**Both are answered.**

**The first** ([B-19](docs/backlog/B-19-close-under-a-real-shutdown.md), 2026-09-17):
forty `SIGTERM`s at unplanned moments inside a real service's ordered shutdown, 221 830 accepted
events, none missing from the topic, and a positive control that loses exactly one record per
concurrent sender. It also found the half of its own sentence it could not reach — a publisher whose
`send` has already returned — which is [B-23](docs/backlog/B-23-the-sink-that-does-not-wait.md) and
is outside this budget.

**The second** ([B-20](docs/backlog/B-20-a-strangers-first-ten-minutes.md), 2026-09-17): from a
machine with a JDK, Gradle and nothing else, the README's own build file reaches a record on a topic
in 106 and 109 seconds — against ten minutes, and about 70% of that is the Kotlin/Native toolchain
downloading. It was **red first**: the README's snippet did not compile in the only kind of project
that can link the artefact, which is the missing step the item existed to find.

The third question — whether anybody outside this portfolio wants it — **is not asked**. It had one
mechanism, announcing, and nothing from this project is posted anywhere; the item that held it is
dropped with the cost of dropping it written down ([B-21](docs/backlog/B-21-does-anyone-want-this.md)).

### Stages 5 to 9: toward the clients underneath

**Opened 2026-09-24 at the owner's request** — bring kafkakn closer to what other Kafka clients do.
[D2](docs/research/research-architecture.md) is amended rather than removed: the fence becomes an
ordered roadmap, and the order is the part of it that survives.

The unusual thing about this distance is where it lies. **Both arms already are full clients** —
librdkafka and `kafka-clients` each consume, coordinate groups, run transactions, administer topics
and speak SASL ([research §1.8](docs/research/research-architecture.md), read out of the artefacts).
What kafkakn lacks is surface and tests, so every item below arrives with a differential test, and
each is a place the two arms could disagree without either noticing.

The first thing §1.8 found is not a missing feature: **the arms disagree by default on
`enable.idempotence`** — `true` on the JVM, `false` on librdkafka — which is why the stage opens with
[B-25](docs/backlog/B-25-the-arms-disagree-on-idempotence.md) at P0, ahead of anything new.

### Kill criteria for stage 4

They are written down before the work so that a bad result is a result rather than a
disappointment:

1. **RQ-A red** — a record accepted by the publisher and missing from the topic. Stop, fix the
   contract, publish the negative result. Nothing is announced about a library that can lose a
   record on shutdown.
2. **RQ-C red twice** — the artefact is not shippable. Nobody is being invited to try it either way,
   which makes this a correctness stop rather than a reputational one.
3. **Day three ends with RQ-A unmeasured** — the integration is too big for the budget; fall back to
   a smaller publisher and re-plan rather than extending.

The expected outcome is written down too, so that it cannot be claimed afterwards: **RQ-A green,
and demand unknown** — correct, and nobody asked. That is a fine place to leave a repository; what
it is not is evidence either way, and the backlog says so rather than letting silence read as a
verdict.

## Stages

| Stage id | Stage | What it is |
|---|---|---|
| `stage-0-it-builds` | It builds, and a test can fail | Targets, the `expect` surface, the C bundle, the broker, and the differential harness — before any producer exists. |
| `stage-1-produce` | It produces, and loses nothing | Both actuals, backpressure, and the accounting that catches the defect this project is shaped around. |
| `stage-2-real-use` | Usable against a real deployment | Headers and TLS. |
| `stage-3-usable-by-others` | Someone else can use it | Snapshots, and a build outside this repository that proves it. |
| `stage-4-a-real-user` | Somebody actually runs it | A publisher in a deployment no test harness controls, a stranger's first ten minutes, and whether anyone outside the portfolio wants it. |
| `stage-5-producer-parity` | The producer other clients have | What already ships and was never measured, then explicit partition, timestamp, topic metadata and transactions. |
| `stage-6-real-deployments` | It connects where it is deployed | Client certificates, SASL, and the metrics an operator reads. |
| `stage-7-admin` | It can manage what it writes to | Topics and the cluster, created and described through the same two arms. |
| `stage-8-consume` | It reads — designed before it is built | The consumer, in the order that keeps group coordination last: a design, assign-and-poll, groups, exactly-once. |
| `stage-9-targets` | Where it runs | `linuxArm64`, and macOS so a contributor can run the native arm locally. |

## Marks

`[ ]` open · `[~]` in progress · `[x]` done · `[?]` open question · `[-]` dropped

<!-- BEGIN INDEX -->

## Open (10)

| Task | | Priority | Size | Blocked by |
|---|---|---|---|---|
| [B-35](docs/backlog/B-35-the-consumer-designed-first.md) `[ ]` | The consumer, designed before it is built: a contract document and the defaults it starts from | P2 | M | - |
| [B-36](docs/backlog/B-36-assign-and-poll.md) `[ ]` | A consumer without a group: assign partitions, seek, and read as a Flow | P2 | L | B-35 |
| [B-37](docs/backlog/B-37-consumer-groups.md) `[ ]` | Consumer groups: subscribe, rebalance, commit — and a group with one consumer from each arm | P2 | XL | B-36 |
| [B-39](docs/backlog/B-39-linux-arm64.md) `[ ]` | linuxArm64: settle H5 — does a second native target cost a matrix row and no code? | P2 | M | - |
| [B-42](docs/backlog/B-42-a-pkcs1-key-works-on-one-arm.md) `[ ]` | A PKCS#1 client key: read in the source as working on one arm only | P2 | S | B-31 |
| [B-43](docs/backlog/B-43-native-flush-may-hold-the-callers-thread.md) `[ ]` | The native flush calls a blocking rd_kafka_flush on the caller's thread | P2 | S | - |
| [B-33](docs/backlog/B-33-sasl-oauthbearer.md) `[ ]` | SASL OAUTHBEARER with a token the caller supplies | P3 | M | B-32 |
| [B-38](docs/backlog/B-38-exactly-once-read-process-write.md) `[ ]` | Exactly-once read-process-write: offsets committed inside the producer's transaction | P3 | L | B-30, B-37 |
| [B-40](docs/backlog/B-40-macos-for-contributors.md) `[ ]` | macOS, so a contributor can run the native arm without the Linux box | P3 | M | - |
| [B-41](docs/backlog/B-41-metrics-an-operator-can-read.md) `[ ]` | Metrics an operator can read — without the library counting its own successes | P3 | M | - |

## Closed (33)

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
- [B-24](docs/backlog/B-24-the-central-guard-times-out.md) `[x]` - The central guard's red pointed at coroutines, because a default named a topic nothing creates

**Usable against a real deployment**

- [B-10](docs/backlog/B-10-record-headers.md) `[x]` - Record headers without rd_kafka_producev
- [B-11](docs/backlog/B-11-tls.md) `[x]` - TLS on both arms
- [B-18](docs/backlog/B-18-verification-cannot-be-turned-off.md) `[x]` - Certificate verification: the README and the contract disagree
- [B-22](docs/backlog/B-22-a-dead-patch-must-say-so.md) `[x]` - A bump that makes the patch dead must say so, not merely fail

**Someone else can use it**

- [B-12](docs/backlog/B-12-publish-snapshots.md) `[x]` - Publish snapshots to reposilite
- [B-13](docs/backlog/B-13-external-downstream-acceptance.md) `[x]` - Acceptance from outside: a downstream project that uses the published artefact
- [B-15](docs/backlog/B-15-native-klib-carries-no-c.md) `[x]` - The published native klib does not carry its C dependency
- [B-16](docs/backlog/B-16-readme-says-what-was-measured.md) `[x]` - The README says what was measured, not what sounded right
- [B-17](docs/backlog/B-17-consumer-is-the-wrong-word-here.md) `[x]` - `ci/downstream` is the wrong word in a Kafka repository

**Somebody actually runs it**

- [B-19](docs/backlog/B-19-close-under-a-real-shutdown.md) `[x]` - RQ-A: does close() keep its promise inside a real ordered shutdown?
- [B-20](docs/backlog/B-20-a-strangers-first-ten-minutes.md) `[x]` - RQ-C: does the artefact resolve and link on a machine that has never seen this repository?
- [B-21](docs/backlog/B-21-does-anyone-want-this.md) `[-]` - RQ-B: does anyone outside this portfolio want it?
- [B-23](docs/backlog/B-23-the-sink-that-does-not-wait.md) `[x]` - The publish that returns before the acknowledgement — the shape RQ-A could not reach

**The producer other clients have**

- [B-25](docs/backlog/B-25-the-arms-disagree-on-idempotence.md) `[x]` - The arms disagree on idempotence by default — a retried record can be written twice by one and once by the other
- [B-26](docs/backlog/B-26-compression-was-never-measured.md) `[x]` - compression.type is named portable in the contract and no test has ever set it
- [B-27](docs/backlog/B-27-a-record-can-name-its-partition.md) `[x]` - A record can name its partition, as it can in every other client
- [B-28](docs/backlog/B-28-a-record-carries-its-timestamp.md) `[x]` - A record carries its timestamp, and the metadata says which time the broker kept
- [B-29](docs/backlog/B-29-topic-metadata.md) `[x]` - partitionsFor: what a topic looks like, from the producer that writes to it
- [B-30](docs/backlog/B-30-transactions.md) `[x]` - Transactions: records that become visible together, or not at all

**It connects where it is deployed**

- [B-31](docs/backlog/B-31-client-certificates.md) `[x]` - Client certificates: a broker that asks who is connecting gets an answer
- [B-32](docs/backlog/B-32-sasl-plain-and-scram.md) `[x]` - SASL PLAIN and SCRAM: most managed Kafka will not talk to a client without them

**It can manage what it writes to**

- [B-34](docs/backlog/B-34-a-minimal-admin.md) `[x]` - A minimal admin client: create, delete and describe topics, describe the cluster

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

**Producer first, and the consumer last.** Until 2026-09-24 this read *producer only*: no consumer,
no group coordination, no transactions, no Admin API, no SASL, and "the fence moves when somebody
asks". Somebody asked — the owner, to bring kafkakn closer to the clients underneath it — and the
fence became an order rather than a wall ([D2](docs/research/research-architecture.md), amended).
What it kept is the reason: group coordination is where most of a Kafka client's difficulty lives, so
the consumer comes after everything else, and its first item is a design document rather than code.
A thin consumer shipped for symmetry is still the worse outcome; it is now avoided by sequencing.

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
