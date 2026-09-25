---
id: research-architecture
title: kafkakn — architecture research
type: research
status: active
date: 2026-09-17
---

# Research: the architecture of kafkakn

kafkakn is a Kafka **producer** for Kotlin Multiplatform: one `expect` surface with two actuals —
librdkafka through cinterop on Kotlin/Native, and the official Apache Kafka java client on the JVM.
The niche it occupies is not "a Kafka client for Kotlin"; there are several. It is **a producer a
Kotlin/Native service can link into a single binary**, with a JVM arm that exists so the native arm
can be proved right.

This document records **verified facts** (read in an artefact or measured, with the address),
**decisions** with the alternative rejected, and **hypotheses** with the item that will settle them.
It is the entry point: the layer documents say what the library does, this one says why it is built
this way.

---

## 1. Verified facts

### 1.1 The niche is not empty, and what is in it has no oracle

Verified against Maven Central's index on 2026-09-17.

| Fact | Where verified |
|---|---|
| `com.icemachined:kafka-client` 0.2.0, published **2022-10-21**, is the only published Kafka client for Kotlin/Native | `search.maven.org` index, group `com.icemachined` |
| It ships **four** artefacts: the metadata module plus `-linuxx64`, `-macosx64`, `-mingwx64` | same listing |
| **There is no `jvm` variant** | same listing — the four names are exhaustive |
| It wraps librdkafka via cinterop against a **system-installed** library, and predates the current Kotlin/Native memory model (Kotlin 1.7.10) | its own README and build, 0.2.0 |

**Consequence 1.** A Kotlin/Native-only Kafka client has no way to tell a wrong wire assumption from
a right one except by running against a broker and believing what it sees. Every such project is its
own oracle, which is the weakest position a protocol implementation can be in.

**Consequence 2, and it is this project's reason to exist.** Publishing a JVM actual that delegates
to `org.apache.kafka:kafka-clients` turns the same test suite into a **differential** one: the
native arm must agree with the reference implementation on the same broker, for the same inputs.
That is a property the prior art could not have had, and it is cheap only because the JVM arm is not
our code.

**Consequence 3.** The JVM arm is therefore not a portability afterthought to be added later. It is
part of the test strategy, so it is built in M0 and not in M3.

### 1.2 Linking librdkafka into a Kotlin/Native binary works, and what it costs

Measured during a five-day feasibility spike, September 2026. The numbers below are quoted with the
configuration they were taken from; each was produced by a run whose log was kept.

| Fact | How it was established |
|---|---|
| librdkafka 2.13.0 links statically into a `linuxX64` binary; the binary's `ldd` set is **identical** to a Kafka-free baseline built the same way | `ldd` on both, diffed |
| stripped size delta **8 656 416 B (8.26 MB)**, of which TLS + zlib + zstd are **5 982 304 B (5.71 MB)** and librdkafka alone 2 674 112 B | three binaries from one build, razves attribution |
| the Kotlin side of the producer is about **20 KB** | razves origin split: `KOTLIN` 1 865 639 → 1 886 208, the rest is `C` |
| TLS works through that linked OpenSSL (`security.protocol=SSL`, `ssl.ca.location`) and **`ldd` does not change** | produce + consume over an `SSL://` listener, offsets verified over a plaintext path |
| 1.8M messages over 233 s: no crash, no deadlock, RSS flat (−1.9%) at a flat thread count, valgrind byte-identical at 10× the messages | soak on an isolated two-core box |

**Consequence.** The premise holds: a Kotlin/Native service can carry a Kafka producer without
acquiring a runtime dependency. Almost the whole size cost is OpenSSL, which is unavoidable for
anyone who needs TLS — so there is no size argument for a TLS-less build, and none is offered.

**Re-measured 2026-09-17 on the published artefact**, [B-16](../backlog/B-16-readme-says-what-was-measured.md),
`ci/b-16/run.sh`, because the README was about to repeat a spike number as if it were a current one.
The subject is the binary a build outside this repository links, held against the same build with the
dependency removed:

| | with kafkakn | without |
|---|---|---|
| `ldd` | `linux-vdso`, the loader, `libc libcrypt libdl libgcc_s libm libpthread libresolv librt libutil` | **the same set, exactly** |
| highest versioned glibc symbol | `GLIBC_2.17` | `GLIBC_2.14` |

The spike's row above reproduces. The **glibc floor does not**: kafkakn raises it from 2.14 to 2.17,
which is `manylinux2014`'s, the image the C bundle is built in — so the floor is a consequence of D4
rather than of Kotlin/Native, and "kafkakn adds nothing above Kotlin/Native's own floor" was wrong in
a way only the measurement could show. The script pins 2.17 so the README and the artefact cannot
drift apart.

**The first version of that comparison attributed four libraries to the wrong thing.** Its baseline
was a hello-world, so it differed from the downstream build in *two* ways — kafkakn and kotlinx-coroutines —
and it reported `libcrypt`, `libresolv`, `librt` and `libutil` as kafkakn's. They are the coroutines
runtime's. That list was one edit away from being written into the README as a measured fact; what
kept it out was asking what else the two binaries differed by.

### 1.3 Kotlin/Native ships a glibc 2.19 sysroot, and that decides how the C side is built

Measured in the same spike.

A librdkafka compiled on a current distribution references symbols that sysroot does not have — the
C11 thread family (glibc 2.28), `strlcpy` (2.38), `getentropy` (2.25), and the `__isoc23_strto*`
redirections the 2.38 headers emit — and the link fails. **This is not specific to Kafka**; it is
what any C library built on a current distribution meets.

Two routes exist, and both were measured:

| Route | What it costs |
|---|---|
| point the toolchain at the host sysroot — `targetSysRoot`, `crtFilesLocation`, `libGcc` plus a `-L` | three `konan.properties` keys **JetBrains documents as unstable between patch releases** |
| build the C bundle in an old-glibc image (`manylinux2014`, glibc 2.17), where librdkafka falls back to the **tinycthread it already bundles** | a pinned build image, and today a one-line local patch: `rdrand.c` includes `<sys/random.h>` under `#ifndef _WIN32` only, while the `getentropy` call it exists for is guarded by `HAVE_GETENTROPY`; the header arrived in glibc 2.25 |

**Consequence, and it is a decision this project has to take rather than inherit:** see D4. Nothing
is being reported upstream, so the second route carries a patch this project maintains.

**Note for anyone measuring this again.** `nm -u` on a static archive lists undefined symbols *per
member*, including ones another member satisfies — by that reading librdkafka "needs" `mtx_lock`
from libc when built against 2.17, where in fact it defines it itself. Ask the pair, with anchored
names: is the symbol defined anywhere in this archive (`[TtWw] sym$`), and is it referenced
undefined (`U sym$`).

### 1.4 `rd_kafka_produce` enqueues, and a refused message never produces a delivery report

Measured in the spike, and it is the single most consequential fact in this document.

`rd_kafka_produce` only **enqueues**. At `queue.buffering.max.messages` (100 000 by default) it
refuses with `RD_KAFKA_RESP_ERR__QUEUE_FULL`. That refusal is **backpressure, not an error** — and a
message that was never queued **never yields a delivery report**, so a caller watching delivery
reports sees a perfect success rate.

A naive binding lost **264 826 of 1 000 000** messages this way, with `failed = 0` and a successful
flush. A 2 000-message round trip never reaches the queue bound and cannot find it.

**Consequence 1.** The unit of truth is *what the caller asked to send*, never *what was enqueued*.

**Consequence 2.** A produce call must be able to **suspend** on backpressure. Returning a failure
the caller may ignore reproduces the defect in a new place; blocking the thread is wrong on a
runtime built around coroutines.

**Consequence 3.** Delivery-report counts answer "how many of the messages that were queued
arrived". No API here presents that number as a success rate.

**Consequence 4.** Completion of a batch is `rd_kafka_outq_len` reaching zero. `rd_kafka_flush`
returns an **error code**, and reading it as a count of unsent messages printed `-185` —
`RD_KAFKA_RESP_ERR__TIMED_OUT` wearing a quantity's clothes.

These four are why [feature-backpressure-and-accounting](../features/feature-backpressure-and-accounting.md)
exists as a feature of its own rather than a paragraph inside "produce a record".

### 1.5 `rd_kafka_producev` cannot be used through cinterop

It is variadic, and a variadic C function has no usable shape through cinterop. `rd_kafka_produce`
is the non-variadic equivalent and is what the native actual calls.

**Consequence.** The native arm cannot mirror librdkafka's own recommended API, and per-message
headers — which `producev` is the natural vehicle for — need a different mechanism. Recorded as H2.

### 1.6 The callback arrives on librdkafka's own threads

Delivery reports are invoked from threads librdkafka owns. Under the current memory model this
works: `staticCFunction` for the callback, atomics for what it touches, exercised over 1.8M messages
with no crash and no deadlock.

**Consequence.** The seam between "a C callback on a foreign thread" and "a suspended Kotlin
coroutine" is the load-bearing part of the native actual, and it is where the tests should be
densest.

### 1.7 Versions, read from the registry on 2026-09-17

| | |
|---|---|
| Kotlin | 2.4.20 |
| coroutines | 1.11.0 |
| `org.apache.kafka:kafka-clients` | 4.3.1 |
| librdkafka | 2.13.0 |
| broker image | `apache/kafka:4.3.1` |

The JVM client and the broker are the same release, which removes one variable from the
differential suite.

---

### 1.8 What the two clients underneath already do, and how much of it kafkakn lets through

Read out of the artefacts on 2026-09-24, because the question "how far is this from other Kafka
clients" has an unusual answer here: **both arms already are full clients.** librdkafka 2.13.0 and
`kafka-clients` 4.3.1 each implement producing, consuming, groups, transactions, administration and
SASL. What kafkakn lacks is not an implementation — it is the part of its `expect` surface that would
let those through, and the tests that would hold the two arms to one answer for each.

| Capability | librdkafka 2.13.0 | kafka-clients 4.3.1 | kafkakn today |
|---|---|---|---|
| transactions | `rd_kafka_init_transactions`, `…begin…`, `…send_offsets_to…`, `…commit…` | `Producer.initTransactions`, `beginTransaction`, `sendOffsetsToTransaction`, `commitTransaction`, `abortTransaction` | the four since [B-30](../backlog/B-30-transactions.md) — §2.23; `sendOffsetsToTransaction` is B-38 |
| explicit partition, timestamp | `rd_kafka_produceva` fields | `ProducerRecord(topic, partition, timestamp, key, value, headers)` | partition since [B-27](../backlog/B-27-a-record-can-name-its-partition.md), timestamp since [B-28](../backlog/B-28-a-record-carries-its-timestamp.md) |
| topic metadata | `rd_kafka_metadata` | `Producer.partitionsFor(topic)` | `partitionsFor` since [B-29](../backlog/B-29-topic-metadata.md) — §2.22 |
| consumer, assign and poll | `rd_kafka_assign`, `rd_kafka_consumer_poll`, `rd_kafka_seek_partitions`, `rd_kafka_offsets_for_times`, `rd_kafka_query_watermark_offsets` | `Consumer.assign`, `poll`, `seek`, `offsetsForTimes`, `endOffsets` | assign, seek and poll since [B-36](../backlog/B-36-assign-and-poll.md) — §2.25 |
| consumer groups | `rd_kafka_subscribe`, `rd_kafka_incremental_assign`, `rd_kafka_commit` | `Consumer.subscribe` (+ rebalance listener), `commitSync` | subscribe and commit since [B-37](../backlog/B-37-consumer-groups.md) — §2.26 |
| administration | `rd_kafka_CreateTopics`, `DeleteTopics`, `CreatePartitions`, `DescribeCluster`, `ListOffsets`, `DescribeConsumerGroups` | `org.apache.kafka.clients.admin.Admin` | create, delete and describe topics, describe the cluster since [B-34](../backlog/B-34-a-minimal-admin.md); the rest is not planned |
| compression | `gzip`, `snappy`, `lz4`, `zstd`, all compiled into the bundle | the same four; `zstd-jni`, `lz4-java`, `snappy-java` resolve at runtime | named portable in the contract and never measured until [B-26](../backlog/B-26-compression-was-never-measured.md): **all four, both arms, stored as asked** |
| SASL | `PLAIN`, `SCRAM` and `OAUTHBEARER` compiled in; GSSAPI **not** (`--disable-gssapi`); OIDC **not** (`--disable-curl`) | all of them | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` since [B-32](../backlog/B-32-sasl-plain-and-scram.md) — §2.21; OAUTHBEARER is B-33 |
| client certificate (mTLS) | `ssl.certificate.location`, `ssl.key.location` | a keystore | since [B-31](../backlog/B-31-client-certificates.md), librdkafka's spelling, read into a PEM key store on the JVM — §2.20 |
| metrics | `rd_kafka_conf_set_stats_cb` (JSON every `statistics.interval.ms`) | `Producer.metrics()` | absent |

| Fact | Where verified |
|---|---|
| every librdkafka function in the first table is declared | `librdkafka-2.13.0.tar.gz!/src/rdkafka.h` — `rd_kafka_subscribe` at line 4186, `rd_kafka_offsets_for_times` at 3376, `rd_kafka_query_watermark_offsets` at 3318 (declared at the start of a line, which a pattern expecting a space before the name misses) |
| SASL `PLAIN`, `SCRAM`, `OAUTHBEARER` are in our bundle; Cyrus/GSSAPI and the OIDC token refresher are not | `nm --defined-only librdkafka-static.a` from `ci/librdkafka/build.sh`: `rd_kafka_sasl_plain_provider`, `…_scram_provider`, `…_oauthbearer_provider` defined; `rd_kafka_sasl_cyrus_provider`, `rd_kafka_sasl_oauthbearer_oidc_token_refresh_cb` absent |
| all four codecs are in our bundle | the same archives: snappy inside `librdkafka-static.a`, `LZ4_compress_default` bundled there, `ZSTD_compress` in `libzstd.a`, `deflate` in `libz.a` |
| every JVM method in the first table exists | `javap` against `kafka-clients-4.3.1.jar!/org/apache/kafka/clients/producer/Producer.class`, `…/ProducerRecord.class`, `…/consumer/Consumer.class`, `…/admin/Admin.class` |
| the JVM arm resolves the codec libraries | `./gradlew :kafkakn-core:dependencies --configuration jvmRuntimeClasspath`: `zstd-jni` 1.5.6-10, `lz4-java` 1.10.2, `snappy-java` 1.1.10.7 |

**And the defaults do not agree, in one place that matters.** Read from the same two artefacts:

| Key | kafka-clients 4.3.1 | librdkafka 2.13.0 |
|---|---|---|
| **`enable.idempotence`** | **`true`** | **`false`** |
| `max.in.flight.requests.per.connection` | 5 | 1 000 000 |
| `acks` | `all` | `-1` (all) |
| `linger.ms` | 5 | 5 |
| `retries` | 2 147 483 647 | 2 147 483 647 |
| `compression.type` | `none` | `none` |

*Verified:* `ProducerConfig.configDef().defaultValues()` executed against `kafka-clients-4.3.1.jar`;
the default column of `librdkafka-2.13.0.tar.gz!/CONFIGURATION.md`.

**Consequence 1, and it is the same shape as §2.2.** The JVM arm is idempotent by default and the
native arm is not, with retries unbounded on both. A record whose acknowledgement is lost and which is
retried can therefore be written **twice by one arm and once by the other**, and each arm is
individually consistent with the broker. That the defaults differ is read out of the artefacts; that
it produces a duplicate is **H6**, and it is not measured yet.

**The consumer side has its own disagreement**, read the same way — `ConsumerConfig.configDef()` and
the same `CONFIGURATION.md`:

| Key | kafka-clients 4.3.1 | librdkafka 2.13.0 |
|---|---|---|
| **`isolation.level`** | **`read_uncommitted`** | **`read_committed`** |
| `partition.assignment.strategy` | `RangeAssignor`, `CooperativeStickyAssignor` | `range,roundrobin` |
| `auto.offset.reset` | `latest` | `largest` (the same thing, spelled differently) |
| `enable.auto.commit` | `true` | `true` |
| `group.protocol` | `classic` | `classic` |

A consumer on one arm would therefore see records from aborted transactions that the same consumer
on the other arm hides. It is written down now, before any consumer exists, so that the design item
starts from it rather than meeting it in a test.

**And one thing is simpler than it looked.** librdkafka accepts **`sasl.mechanism`**, singular, as an
alias of its own `sasl.mechanisms` — the row reads *"Alias for `sasl.mechanisms`"* — so the mechanism
is portable under the Java client's name. What differs is the credentials: librdkafka takes
`sasl.username` and `sasl.password`; the Java client takes one `sasl.jaas.config` string. Both default
the mechanism to `GSSAPI`, which our bundle does not contain.

**Consequence 2.** Closing the distance to other clients is mostly surface and tests, not
implementation — which is also why each capability has to arrive with its own differential test.
Everything in the first table is a place where the two arms could disagree without either noticing.

## 2. Decisions

**D1 — the JVM arm is the oracle, and it ships in M0.** One `expect` surface, two actuals; the
common test suite runs on both. The native arm is correct when it agrees with
`org.apache.kafka:kafka-clients` against the same broker. *Rejected:* native first, JVM "later" —
which is how the prior art ended up with no way to check itself (§1.1).

**D2 — producer only, and the scope is a fence rather than a roadmap.** No consumer, no group
coordination, no transactions, no exactly-once, no Admin API, no Schema Registry, no Streams, no
SASL. Group coordination in particular is where most of a Kafka client's difficulty lives, and
pretending otherwise by shipping a thin consumer would be the worse outcome. *Rejected:* a
"minimal consumer" for symmetry.

**Amended 2026-09-24, at the owner's request: the fence becomes an ordered roadmap.** The request was
to bring kafkakn closer to what other Kafka clients do, and §1.8 is what that means here — both arms
already implement it, so the distance is surface and tests. The order is the part of the old decision
that survives, and its reason is unchanged:

1. **what already ships and has never been measured** — the idempotence default the arms disagree
   on, and a compression key the contract calls portable with no test behind it;
2. **the producer surface other clients have** — explicit partition, timestamp, topic metadata,
   transactions;
3. **what a real deployment needs to connect at all** — client certificates and SASL;
4. **a minimal administration surface**;
5. **the consumer, last, and designed before it is built.** "Group coordination is where most of a
   client's difficulty lives" is still true, which is why it comes after everything else and why its
   first item produces a document rather than code. A thin consumer shipped for symmetry is still the
   outcome to avoid; the difference is that it is now avoided by sequencing rather than by exclusion.

Schema Registry and Streams stay out: neither is in either client underneath, so neither is a gap
between kafkakn and the clients it wraps.

**D3 — `produce` suspends.** The API is `suspend fun send(record): RecordMetadata`, and backpressure
is expressed by the call not returning yet. *Rejected:* returning `Result` and letting the caller
retry — §1.4 is a defect report about exactly that shape.

**D4 — the C bundle is built in an old-glibc image, not against the host sysroot.** It trades three
toolchain keys JetBrains may change at any patch release for a pinned image plus a one-line patch
this project carries. The deciding argument is *where the breakage lands*: an unstable
`konan.properties` key breaks on a Kotlin upgrade, at a moment nobody chose, while a patch against a
pinned librdkafka breaks when this project bumps librdkafka, deliberately. **This is a decision, not
a measurement** — both routes were shown to work. Re-checked whenever librdkafka is bumped (H3).

**D5 — first a test from the specification, then the code; the test passing is the gate.** Taken
from the sibling project bochka. A test cites the place in
[the producer contract](../api/producer-contract.md) or in the Kafka protocol documentation that it
encodes; a test written "from common sense" is not accepted, because the common sense here is wrong
in at least the four ways §1.4 lists.

**D6 — `linuxX64` and `jvm` are mandatory; `linuxArm64` is designed for and not built.** Every
native-specific decision is written so a second native target costs a build-matrix row rather than a
redesign — no target name appears in common code, and the cinterop definition is target-agnostic.
It is not in the gate, and no claim is made about it until it is.

**D7 — snapshots to reposilite only.** `reposilite.kotlin.website/snapshots`, group
`io.github.youndie.kafkakn` — the **project's** namespace rather than the account's, so every
artefact of this project sits under one directory. The repository is declared under a content filter
so an outage there cannot fail resolution of anything else. **No Maven Central**, and no release:
publication is a decision nobody has taken; the group is chosen so that the decision stays available
without a rename ([§2.11](#211-a-per-artefact-route-is-a-token-per-target)).

Amended 2026-09-17: the group was `io.github.youndie` until the publishing token made the cost of
that visible.

**D8 — the repository is public, so CI runs on GitHub's standard runners.** `make check` on every
pull request, the same target a contributor runs. The label matters and is not a default taken
blindly: on this account a job on `ubuntu-latest` in a **private** repository does not start at all
(the paid minutes are exhausted), and the self-hosted runners available are registered to an
organisation rather than to this account — pointing at them from here would leave runs queued
indefinitely, and a run that never finishes looks exactly like one that passed. Public repositories
get standard runners for free, which is what makes the ordinary answer the right one here.
Item: [B-14](../backlog/B-14-ci-workflow.md).

**D9 — nothing from this repository names a private project.** It is public, and the work it starts
from was done in a repository that is not. The measurements in §1 are quoted as measurements with
their method and their configuration; the repository they were taken in is not named, linked, or
described in a way that identifies it. A reader can re-run any of them from what is written here.

---

## 3. Hypotheses, and where each is settled

| # | Hypothesis | Settled by |
|---|---|---|
| H1 | ~~The same common test suite can express every producer assertion in a way both actuals satisfy~~ — **settled 2026-09-17, with a qualification: see §2.1** | [B-05](../backlog/B-05-differential-harness.md) `done` |
| H2 | ~~Per-message headers can be carried without `rd_kafka_producev` (§1.5)~~ — **settled 2026-09-17: `rd_kafka_produceva` takes the same fields as an array, see §2.10** | [B-10](../backlog/B-10-record-headers.md) `done` |
| H3 | The old-glibc route (D4) survives a librdkafka bump without a new patch | re-checked at every bump; first at [B-03](../backlog/B-03-c-bundle-old-glibc.md) |
| H4 | A suspending `send` over librdkafka's callback seam has no throughput cost worth reporting against the blocking shape | **not measured, deliberately — §2.4** |
| H5 | `linuxArm64` costs a matrix row and no code (D6) — **questioned 2026-09-25: four places in the C-bundle build already assume x86_64; see B-39's first iteration. Running it waits on a decision** | [B-39](../backlog/B-39-linux-arm64.md) `question` |
| H6 | ~~Without idempotence, a retried record whose acknowledgement was lost is written twice by the native arm and once by the JVM arm (§1.8)~~ — **settled 2026-09-24: yes, and systematically; see §2.19** | [B-25](../backlog/B-25-the-arms-disagree-on-idempotence.md) `done` |
| H7 | ~~A consumer can be expressed as one `expect` surface both arms honour without leaking either client's threading model~~ — **settled 2026-09-24 for assign and poll: yes, see §2.25; groups are B-37's** | [B-36](../backlog/B-36-assign-and-poll.md) `done` |

### 2.1 H1, settled: three kinds of assertion, and only one of them needs machinery

Building the harness ([B-05](../backlog/B-05-differential-harness.md)) split the question in a way
the hypothesis did not anticipate. "Can `commonTest` express every producer assertion" has three
answers, not one:

**Assertions whose truth belongs to the broker** — how many records landed, on which partition, at
which offset, what an independent reader sees. These go in `commonTest` unchanged. Each arm is
checked against the same third party, and agreement between the arms follows from both being right
rather than being asserted directly. This is most of the suite.

**Assertions about what only the client knows** — which partition *its own* partitioner chose for a
key, which error type it raises for a given failure, how it normalises a configuration value. These
are expressible in `commonTest`, but comparing them **across** arms cannot happen inside a test: the
two are separate processes on separate platforms, and `commonTest` is compiled twice rather than run
once. Each arm records what it saw, and `ci/harness/compare-arms.sh` diffs the two files afterwards.
That is the differential oracle's actual mechanism, and it is where the two implementations can
differ while each looks correct on its own.

**Assertions about the platform seam itself** — that the cinterop archives link, that a delivery
report arriving on librdkafka's thread resumes the right coroutine. These belong in `linuxX64Test`
and that is **not** a leak of platform shape into the contract; it is the one place where the thing
under test is the platform. `CinteropLinkTest` is the first of them.

**Consequence.** "A test that can only be written in one arm's source set" is a warning sign for the
first two kinds and the normal case for the third, so the rule in `CLAUDE.md` is stated that way
rather than as a prohibition.

**Consequence 2.** The comparison needs its own vacuity guard and has one: two files that do not
exist agree perfectly, and so do two empty ones, so absence and emptiness are failures rather than
agreement. Without that, a suite that never ran would produce the strongest possible "the arms
agree".

### 2.2 What the oracle caught on the first day it existed

**The two clients do not partition keys the same way.** librdkafka's default `partitioner` is
`consistent_random` — a **CRC32** hash of the key — while the Java producer uses murmur2. Measured
2026-09-17 in [B-07](../backlog/B-07-native-actual.md): of eight keys, five landed on different
partitions depending on which arm produced them.

Neither implementation is wrong, and **neither could have noticed on its own**. Each one's records
go where that one's partitioner says, every assertion each arm makes about its own records passes,
and the broker is happy in both cases. A single-implementation client would ship this and the
symptom would appear in a consumer somewhere else entirely, as records for one key arriving out of
order across a rebalance.

librdkafka names the compatible option itself: `murmur2_random`, documented as "functionally
equivalent to the default partitioner in the Java Producer". kafkakn sets it, because one library
that puts a key in two different places depending on the platform is not one library. A caller who
sets `partitioner` explicitly keeps their choice.

**Consequence.** This is the concrete answer to "why two arms", and it arrived on day one. It is
also the argument for the observation mechanism rather than broker-based checking: the broker cannot
tell you the arms disagree, because each arm is individually consistent with it.

### 2.3 An exception on librdkafka's thread hangs the caller; it does not crash

Measured 2026-09-17 while building the control that was supposed to show a crash being noticed. A
failure thrown inside the delivery-report callback — on a thread librdkafka owns, outside any `try`
the caller wrote — **does not terminate the process and surfaces nowhere at all**. It leaves the
continuation parked, and the caller stays suspended for ever.

That is worse than a crash, because a crash is reported and a hang is not: the test that caught it
did so as `UncompletedCoroutinesError`, a timeout, and only because the suite had a timeout.

**Consequence.** The callback unparks the continuation **before** doing anything else and wraps
everything after it in a catch that resumes exceptionally. Whatever goes wrong in there, the caller
is waiting, and resuming it with the failure is the only outcome that is not a hang.

### 2.4 H4 is not measured, and the reason is better than a number would be

The hypothesis asked whether the suspending `send` costs throughput against the blocking shape. It
is recorded as **not measured**, for two reasons that are worth more than the figure:

**There is no second arm of the comparison.** The blocking shape was replaced, not kept. Measuring
it would mean maintaining two implementations of the central path in order to compare them, and the
one that exists is the one the contract requires — a blocking retry loop holds a thread on a runtime
built around coroutines, which is wrong regardless of what it measures.

**The available stand cannot support the claim.** Both suites end to end take about two minutes on a
shared machine whose timings move by more than the difference being asked about. D5 already says
throughput here is a ratio with a spread or nothing, and a ratio needs the second arm this project
has deliberately not got.

**What it would take**, if the question becomes live: an isolated host, the blocking shape restored
behind a build flag, paired interleaved runs, and the ratio published with its spread and the
absolute figures beside it. That is a measurement item, not a line in a producer item, and nobody
has asked for it.

### 2.5 A serial `send` cannot experience backpressure, and that shapes how it is used

Found by a vacuity guard in [B-08](../backlog/B-08-suspend-on-backpressure.md): the first
backpressure test sent 3 000 records one at a time and never filled a queue of 100, because `send`
awaits the broker's acknowledgement and a caller awaiting each record has exactly one in flight.

**Consequence for the test.** Backpressure is only reachable with concurrent sends, so the suite
launches them concurrently. Without the guard the test would have passed while exercising nothing —
the exact shape of the defect this project exists to prevent.

**Consequence for the API.** A caller who wants throughput has to supply the concurrency. That is a
real property of a `send` that waits for an acknowledgement, not an accident, and it is the argument
somebody will make for a batching entry point later. None is offered today.

### 2.6 The two clients name the queue bound differently

librdkafka bounds its outbound queue by `queue.buffering.max.messages` — a **record count**. The
Java client bounds it by `buffer.memory` in **bytes** and waits `max.block.ms` for room. There is no
common spelling to give them, and the contract keeps Kafka's own names rather than inventing a
third.

**Consequence.** "The same configuration on both arms" is achievable for most keys and not for this
one. The suite makes the queue small through a per-arm helper, which is the honest shape: a common
test with a platform-specific fixture, rather than a common key that silently means something
different on each side.

### 2.7 The accounting guard, measured against a producer that drops

[B-09](../backlog/B-09-accounting.md). Both arms handed 3 000 records to a topic created empty for
the run, and the topic's end offsets grew to exactly 3 000 on each — measured 2026-09-17 by
`ci/b-09/run.sh`, reading `kafka-get-offsets.sh`, not the library.

**The number that makes it mean something is the second one.** The same test was run again against a
deliberately naive producer that counts an enqueue refusal and moves on, and it went red on both
arms: the jvm arm landed 100 of 3 000 and the native arm 103, while every `send` had returned a
`RecordMetadata`. The shortfall equalled the drops the control admits to, exactly, on both arms —
so the oracle sees the whole of the loss and not a part of it.

**Why the control is a wrapper and not a second binding.** It simulates the refusal with a permit
count rather than taking it from librdkafka, so it says nothing about librdkafka's own refusal path
— that is what §1.4 and [B-08](../backlog/B-08-suspend-on-backpressure.md) are for. What it
reproduces exactly is the condition the guard exists to catch: records handed in, answered with a
result-shaped value, that no broker ever saw. Being identical on both arms is the gain; a control
only one arm can run leaves the other arm's guard unproven.

**Consequence for the oracle.** The topic is created fresh per run and **per arm**. Both arms run
against one broker in one pass, so a shared topic would add their two counts into a single number no
assertion could attribute, and a reused topic makes the delta somebody else's.

### 2.8 A fresh topic configuration silently dropped every topic-level property

[B-11](../backlog/B-11-tls.md), measured 2026-09-17. The native arm created its topic handles with
`rd_kafka_topic_conf_new()` and set only the partitioner on them. In librdkafka a **topic**
configuration built that way starts from the defaults and inherits nothing from the global `conf`,
so every topic-level property the caller had set went nowhere: `message.timeout.ms`, `acks`,
`compression.codec`, `request.required.acks`.

**It was invisible because the default agreed with the test.** librdkafka's default for
`request.required.acks` is `-1`, which is `acks=all`, so the suite's `acks=all` assertions passed
while the value never left the global configuration. What exposed it was a TLS test asking to fail
in twenty seconds and hanging for a minute: `message.timeout.ms=20000` was being dropped and the
default of five minutes applied instead.

**The fix is to stop building one.** Every property goes on the global `conf`, where librdkafka
applies topic-level ones to the default topic configuration it creates implicitly, and the topics
are opened with `rd_kafka_topic_new(handle, name, NULL)` so that configuration is the one in force.
The partitioner default rides along with the rest.

**Consequence beyond the bug.** A configuration key that is accepted and dropped is the exact shape
this project refuses everywhere else, and it was inside the library for four items. The refusal path
was well guarded — an unknown key throws on both arms — and the silent path was not guarded at all,
because it never looked like a key being refused.

### 2.9 librdkafka's last error is the one that explains least

`_ALL_BROKERS_DOWN` arrives after the error that caused it, and it is a summary: librdkafka's own
header calls it informational and says not to treat it as fatal. A producer that keeps "the last
error" therefore reports `Local: All broker connections are down: 1/1 brokers are down` for a
certificate that cannot be verified, a port with nothing on it, and a broker that is genuinely
down — the three cases a caller most needs to tell apart.

Keeping the last error that is **not** that code gives, for the same failure:

> `Local: SSL error: ssl://127.0.0.1:9094/bootstrap: SSL handshake failed: ... certificate verify
> failed: broker certificate could not be verified, verify that ssl.ca.location is correctly
> configured or root CA certificates are installed (install ca-certificates package)`

**Consequence.** The error callback is not optional machinery for a producer over librdkafka. A
record bound for a broker it cannot reach comes back as `Local: Message timed out` whatever the
reason was, and the reason exists only on that callback.

**Known limitation.** The slot is one per process: a `staticCFunction` captures nothing, so there is
nowhere per-producer to write. It is cleared when a producer is constructed. Per-producer
attribution means handing each producer's identity through `rd_kafka_conf_set_opaque` and keeping a
second registry — the shape of the fix, if two producers ever fail at once here.

### 2.10 H2, settled: the non-variadic path exists, and no C of ours is needed

`rd_kafka_produceva(rk, vus, cnt)` takes exactly the fields `rd_kafka_producev` takes, as an
**array** of `rd_kafka_vu_t`, and is an ordinary function. Verified in `rdkafka.h` 2.13.0 and then by
producing through it on both arms ([B-10](../backlog/B-10-record-headers.md)).

**§1.5 is narrower than it was being read.** It says `rd_kafka_producev` is variadic and unusable
through cinterop, which is true; the conclusion drawn from it — that headers need a C shim of our
own — does not follow. The item's fallback was a small C wrapper compiled into the bundle. It is not
needed, and this project still contains no C of its own.

**Consequences.**

- The whole produce path moved to `produceva`, not only the header case. Two paths, one of them
  taken by the few callers who use headers, is how the less-travelled one rots.
- `produceva` names the topic directly, so the topic-handle cache is gone — and with it the last of
  the machinery that dropped topic-level configuration in §2.8.
- It returns an `rd_kafka_error_t *` rather than `-1` plus `rd_kafka_last_error()`: no global to
  read, and a queue-full refusal is the same value every time.
- `RD_KAFKA_VTYPE_HEADER` per header rather than one `VTYPE_HEADERS` list. librdkafka takes
  ownership of a `rd_kafka_headers_t` **on success only**, so every error path would have to destroy
  it — and a queue-full retry loop is exactly where that is forgotten. Mixing the two returns
  `_CONFLICT`, so one has to be chosen.

**And the headers themselves.** A duplicate name survives, in order, on both arms; a null value and
an empty one stay distinguishable through the broker, on both arms. Kafka's headers are an ordered
sequence rather than a map, so `RecordHeader` is carried in a list: a client that stored them in a
map would have answered a three-header record with two.

### 2.11 A per-artefact route is a token per target

The publishing credential is issued by a job that turns Maven coordinates into Reposilite routes,
and **a Reposilite route is a raw string prefix with a trailing slash**: a route at
`/snapshots/io/github/youndie/kafkakn-core/` grants nothing under `…/kafkakn-core-jvm/`, which is a
sibling directory. Under a group of `io.github.youndie` this project therefore needed three routes —
one per coordinate — and a fourth the day `linuxArm64` is added, which is a credential to re-issue
for a build-matrix row.

**Under `io.github.youndie.kafkakn` all of them live in one directory**, so one route covers the
project and every target it ever grows. Same trailing slash, same guarantee that it reaches no
sibling project.

**The group was changed for this**, at the point where the first token was about to be issued and
not before — the cost was invisible while nothing had been published. It also happens to be the
namespace form to ask Maven Central for later, which is the reason it is worth doing once rather
than after somebody has depended on the old coordinate.

### 2.12 The klib carried the bindings and not the implementation

[B-15](../backlog/B-15-native-klib-carries-no-c.md), found by
[B-13](../backlog/B-13-external-downstream-acceptance.md). A build that is not this one resolved the
published `kafkakn-core-linuxx64`, compiled against it, and could not link: **14 undefined symbols**,
`rd_kafka_produceva` and `rd_kafka_poll` among them. The same source ran on the jvm arm from the same
version, so the API was fine and the artefact was not.

**Why every check here was green anyway.** The archives were named in `build.gradle.kts` as
`linkerOpts` on **this project's own test binaries**, at absolute paths in `~/.cache/kafkakn`. The
suite linked because the build file told it how; the published klib said nothing about
`librdkafka-static.a`, and nothing in the gate compares the two. Eleven items passed over it.

**The fix is `staticLibraries` in `rdkafka.def`**, with the directories passed as `-libraryPath`
from Gradle so the file stays machine-independent. cinterop then copies the archives into the klib
and a downstream link needs no configuration at all.

| | before | after |
|---|---|---|
| `kafkakn-core-linuxx64-…-cinterop-rdkafka.klib` | 76 952 bytes | 11 280 633 bytes |
| a stranger's `linkReleaseExecutableLinuxX64` | 14 undefined symbols | a 9.6 MB binary that runs |

The 11 MB is the compressed form of 46 MB of archives — librdkafka, OpenSSL, zlib, zstd — and it is
the same 46 MB the link was always going to consume. What changed is where they live.

**The guard is the removal, not an addition.** `linkerOpts` is gone from `build.gradle.kts`: this
project's own test binaries now link the way a stranger's does, so an artefact that cannot be linked
cannot pass the suite either. A check that exercises a path only the library's own build knows about
is a check that cannot see this class of defect, and this is the second one it hid
([§2.8](#28-a-fresh-topic-configuration-silently-dropped-every-topic-level-property) was the first).

### 2.13 "Suspends" was a claim about the thread, and on the JVM arm it was false

The contract flattens two behaviours at the queue bound into one word: `kafka-clients` blocks,
librdkafka refuses, and both are promised as "suspends". That word is about the **caller's thread**,
and the JVM actual was not keeping it — `delegate.send(...)` ran on whatever dispatcher the caller
was on, and `kafka-clients` waits inside `send` for metadata it does not have and for room in the
record accumulator.

**Measured 2026-09-17**, on a single-threaded dispatcher with no broker at the address: three records
held the thread for **6 019 ms** while a coroutine asking for it every 2 ms got nothing. `flush` and
`close` were worse — they wait outright.

**Why the suite could not see it.** `BackpressureTest` produces 3 000 records concurrently on
`Dispatchers.Default`, a pool: the blocking is invisible until every thread in it is taken, and the
broker kept draining faster than that. A guard for this has to name the resource it is about — one
thread — which is why `JvmDispatcherSeamTest` builds its own dispatcher.

**Two fixtures failed before one worked**, and both failed by being green:

1. the ordinary topic — the broker acknowledged faster than records could pile up, so nothing ever
   waited;
2. the strict topic (`min.insync.replicas=2`, where nothing can be acknowledged) — the broker
   **refuses** those records immediately, and a refusal drains the accumulator exactly as well as an
   acknowledgement does.

What holds the client is metadata it cannot get. That fixture needs no broker at all.

**And the first assertion was wrong too.** It counted ticks and asked for more than one, which is
true of a thread that was held for six seconds: between two blocking calls the dispatcher comes free
for a moment and the ticker gets its turn. The measurement is the **gap** — the longest silence —
not the count.

**Consequence.** The JVM actual does its waiting on `Dispatchers.IO`. That is what "suspends" can
honestly mean over a blocking client: the caller's dispatcher stays free, and the wait happens on a
thread that exists for waiting. The native arm reaches the same promise by never blocking at all.
*(Questioned 2026-09-24: its `flush` begins with a blocking `rd_kafka_flush` — read, not measured;
[B-43](../backlog/B-43-native-flush-may-hold-the-callers-thread.md).)*

### 2.14 RQ-A, measured: the shutdown order held, and what that green does not cover

**The subject is a service this repository does not own** — a webhook gateway on Kotlin/Native with
an HTTP ingress, a SQLite database, `kore`'s ordered stop and a Kafka sink behind a flag. That is the
point of it: every earlier test of `close` was written by the same hands as the promise and decided
for itself when the shutdown happened.

**The oracle is the gateway's own table, not the producer.** Its `events` rows record what it
accepted before this library was involved; the topic's keys, read by `kafka-console-consumer`, say
what arrived. A producer asked whether it delivered what it delivered answers yes.

Two things were fixed in writing before any round ran, because settling them afterwards would settle
the result:

* **the row is committed before the publish is attempted**, which is what makes a loss visible at
  all — a row with nothing behind it;
* **`SIGTERM` only, never `SIGKILL`.** A process killed outright cannot run `close`, and the gap
  between the row and the send is then an **outbox** question rather than a question about this
  library. Conflating the two would let an outbox defect be reported as a kafkakn one.

**Measured 2026-09-17**, `ci/b-19/run.sh`, release binary, broker in Docker, `acks=all`, three
partitions, a fresh topic and a fresh database per round, the signal landing at a random point three
to eight seconds into a steady stream:

| | |
|---|---|
| rounds | 20, plus one positive control |
| accepted events | **91 149** (3 049–5 973 per round) |
| accepted and missing from the topic | **0** |
| shutdown, signal to exit | **2.26 s** in 19 rounds, 5.85 s in one, against a 30 s grace period |
| positive control (broker stopped before the signal) | **8 missing, and the service named all 8** |

The shutdown time is almost entirely `preDrainWait`: two seconds the gateway spends announcing that
it is going away before it drains anything. Nothing in the release stages came near its deadline, so
the pre-registered **amber** — no loss but an overrun — did not occur.

**The same twenty rounds again with eight times the concurrency**, because a drain that finishes
instantly may simply never have been asked anything: 64 concurrent senders instead of 8, **130 681**
accepted events (2 748–10 441 per round), **0** missing, the same 2.26 s shutdown, and a drain of
**12–82 ms** every time.

**The control of that sweep is what says how much those milliseconds are worth.** With the broker
stopped, it lost exactly **64** records — one per concurrent sender, each named by the service. So
the set of requests sitting inside `send` when the world changes is the concurrency, and the
ordinary rounds were draining up to 64 of them each: about **1 280 requests caught mid-`send`**
across the sweep, every one of them finished before the producer was closed. The drain is short
because a publish is milliseconds, not because there was nothing in it.

**What this green does not cover, and the reason is in the sink rather than in the library.** The
gateway's publish awaits the broker's acknowledgement inside the request, so a record is either
inside somebody's `send` or finished — it is never sitting in the producer with its `send` already
returned, which is the state `close` exists to answer for. What these twenty rounds exercise is therefore the
**order** — that the drain finishes before the producer is closed, so a request mid-`send` is not cut
— and not `close` rescuing records already queued. A sink that returned before the acknowledgement
would be the one that tests the flush, and that sink is the outbox shape the item ruled out of M2. It
is worth writing down which of the two was measured, because the contract sentence covers both and a
reader would reasonably assume the harder one. The second shape is
[B-23](../backlog/B-23-the-sink-that-does-not-wait.md), filed and not started: M2 bought three days
and this spent them.

**The harness was wrong twice before it was right, and both times it was wrong by being green.** A
topic named after the round alone replayed the *previous* run's records, and the second run reported
2 612 records with no row behind them — a finding entirely of the harness's own making. And a port is
not free the moment its process is: the control round died of `EADDRINUSE` seconds after a clean
exit, and a round that never started reads exactly like a round that passed. Both are why the control
round exists at all: a reconciliation that has never come out non-zero has not been shown able to.

### 2.15 RQ-C, measured: the README did not compile, and the toolchain is most of the wait

**Measured 2026-09-17**, [B-20](../backlog/B-20-a-strangers-first-ten-minutes.md), `ci/b-20/run.sh`.
The machine is a container with a JDK, Gradle and nothing else: no clone, no Gradle cache, no
`~/.konan`, and no knowledge of this repository beyond what the README prints. The project it builds
is **extracted from the README itself** rather than written here, because a scaffold written by this
repository is exactly the part a stranger does not have.

**The first run was red in 25 seconds, and the reason was the README.** Its *Getting it* block showed
a `repositories { }` fragment and a top-level `dependencies { implementation(...) }` one. There is no
`implementation` configuration at the top level of a multiplatform project — which is the only kind
of project that can link the native artefact — so the build failed at script compilation:
`Unresolved reference 'implementation'`. The block was also missing `mavenCentral()`, so nothing else
would have resolved either. The README now carries a **whole build file**, and the check pastes it
verbatim rather than into a scaffold; a scaffold would have hidden this for ever.

**Then green, twice:**

| | run 1 | run 2 |
|---|---|---|
| total, `gradle` to a record on the topic | **106 s** | **109 s** |
| of which the Kotlin/Native toolchain arriving | 74 s | 78 s |
| everything after it | 32 s | 31 s |

Against a ten-minute budget, with **~70% of it Kotlin/Native downloading its own LLVM, sysroot and
libffi**. The split is why that is legible: a first run that overran because of the download would
otherwise be argued about afterwards rather than read off the log.

**Two things this number is not.** It is a container on the build machine, so the CPU and the network
are that machine's and not a stranger's — it is a floor, not a universal figure. And the toolchain
lands inside the container rather than in the mounted home, which is what keeps every run cold; a
stranger's second build does not pay the 74 s again.

**The red run is also the positive control, and it arrived without being arranged.** A check whose
subject is "did it work" passes just as well when it is not looking, and this one was watched failing
for a real reason before it was watched passing.

### 2.16 `patch -R` does not mean "is this already applied?"

[B-22](../backlog/B-22-a-dead-patch-must-say-so.md) needed the bundle build to tell two opposite
situations apart on a librdkafka bump: *upstream fixed it, delete the patch* and *upstream moved the
code, rewrite the patch*. `patch --batch --forward` gives both the same sentence —
`Ignoring previously applied (or reversed) patch.` — and exit 1.

The obvious probe is `patch --dry-run --reverse`: if the change is already in the source, reversing
it works. **It does not answer that question.** Measured 2026-09-17 with GNU patch 2.7.6 against
librdkafka 2.13.0's own `src/rdrand.c`, patched and unpatched:

| | unpatched | already patched |
|---|---|---|
| `patch -R --dry-run` | **exit 0** | exit 0 |
| `patch -R --dry-run --forward` | exit 1 | exit 0 |
| `patch --dry-run` | exit 0 | **exit 0** |
| `patch --dry-run --forward` | exit 0 | exit 1 |

Given `-R` on a patch that is *not* applied, patch prints `Unreversed patch detected!  Ignoring -R.`
and applies it forward, exiting 0. Without `--forward`, **neither probe can say no**: both rows are
`0 0`, and the first version of `ci/librdkafka/apply-patches.sh` shipped with exactly that — it
reported "already applied, delete this patch" against a pristine source. That is the one answer whose
cost is asymmetric: a live patch deleted on a bump, and a build that then fails somewhere else.

It was caught by running the script against the real source rather than by reading it, and then by
`ci/b-22/run.sh`, which holds the script against four fixtures and requires four different answers.

**Also visible in that run and worth knowing:** the patch applies to 2.13.0 **with fuzz 1 and an
offset** — its context was written against a slightly different neighbourhood. `patch`'s own output
is no longer swallowed by the bundle build, because "succeeded with fuzz 1" is how a patch says its
context has begun to drift, long before it becomes one of the three refusals.

### 2.17 The other half of `close`, measured — and the control is where the answer is

[§2.14](#214-rq-a-measured-the-shutdown-order-held-and-what-that-green-does-not-cover) ended by
naming what its twenty rounds did not cover: the publisher awaited the acknowledgement inside the
request, so a record was either inside somebody's `send` or finished, never sitting in the producer
with its `send` already returned — which is the state `close` exists to answer for.
[B-23](../backlog/B-23-the-sink-that-does-not-wait.md) is the same harness with a bounded queue in
front of the producer: `publish` hands the record over and returns, a coroutine of the sink's own
calls `send`, and `close` drains what is queued.

**Two kinds of loss become possible, and they wear one shape from outside** — a row in the service's
table with nothing on the topic behind it. So the classification was fixed **before the run**, and
the classifier is a line the service prints at the time rather than a reading taken afterwards: the
sink announces every event id immediately before it asks the producer.

**Measured 2026-09-17**, `ci/b-23/run.sh`, 64-deep queue, 64 concurrent senders, on a machine
restarted minutes earlier:

| | 20 rounds | the control (broker stopped before the signal) |
|---|---|---|
| accepted | **17 644** (525–1 266 per round) | 1 259 |
| asked of the producer and lost | **0** | **1** |
| accepted, queued, never asked | **0** | **127** |
| refused (`send` threw) | 0 | 1 |
| shutdown | 2.26 s, drain 12–40 ms | **15.07 s, DEADLINE_EXCEEDED** |

**The green is the smaller half of the result.** What the control says is the part worth keeping:
with the broker gone, the drain stage ran to exactly **10.000 s** and the release stage to exactly
**3.000 s** — both their deadlines — kore cut them, and the **127 records still in the queue were
never handed to the producer at all**. One record was in the producer and lost; one `send` threw.

Without the split, that control reads *"129 records lost"* and the number gets attributed to this
library. **Two of the 129 are about the producer. The other 127 are an outbox question** — whether a
service should write its intent and reconcile later — and they are not about kafkakn at all. That
distinction is the whole reason the item insisted the classification be pre-registered; decided
afterwards, it would have been an opinion.

**What the queued shape costs, unasked but visible.** The same harness at the same concurrency
accepted **17 644** events across twenty rounds against §2.14's **130 681**: a single consumer in
front of the producer serialises what was 64 concurrent `send`s, so the service's own throughput
falls by roughly 7×. That is a property of the shape, not of the library — and it is the trade a
service makes when it decides a webhook's `200` must not wait for Kafka.

**Consequence for the contract.** Both halves of the `close` sentence have now been exercised: a
record the producer holds when the signal arrives is flushed (twenty rounds, zero lost), and a record
the *service* holds and has not yet handed over is the service's problem, which the contract never
claimed otherwise but which nothing had ever separated before.

### 2.18 A default in the harness named a topic nothing creates

[B-24](../backlog/B-24-the-central-guard-times-out.md). `AccountingTest` is the guard this project is
shaped around, and run outside `ci/b-09/run.sh` it went red after sixty seconds of silence with
`UncompletedCoroutinesError` — a sentence about coroutines, for a project whose central claim is
about lost records. It was filed as a timeout under load. It was neither a timeout nor load.

`Observations.accountingTopic` fell back to `"kafkakn-acct"` when the environment did not name a
topic, and **the script passes a per-run name**, so the fallback invented one nothing creates. The
Java client then waited `max.block.ms` for metadata that was never coming and `runTest`'s own
one-minute watchdog fired first. The accessor's KDoc said "the harness creates both" two lines above
the default that contradicted it.

**Three things are worth keeping out of this.** A fallback that manufactures its own subject is the
same shape as a lookup test that can never fail, and it is at its worst in the machinery *around* a
test rather than in the test. `runTest`'s default timeout **masked** the honest error: raising it to
ten minutes is what made `Topic … not present in metadata after 60000 ms` visible. And the first
diagnosis — a slow box — was the plausible one and the wrong one, on a box whose load average
genuinely was 11 at the time.

The fix is the default's removal, not a new check: 60 s of silence becomes an immediate failure
naming the script that creates the topic, and `testTopic` and `strictTopic` keep their defaults
because those fall back to exactly the names the scripts use.

### 2.19 H6, settled: every record in flight when the acknowledgement is lost is written twice

[B-25](../backlog/B-25-the-arms-disagree-on-idempotence.md). The default disagreement of §1.8 —
`enable.idempotence` `true` on the JVM, `false` on librdkafka — was a fact about two configuration
tables. Whether it costs anything was H6, and it needed a fault that loses an acknowledgement on
purpose.

**The fault** is `docker pause` on the broker for six seconds, five times, while a driver outside this
build produces unique values from fifty concurrent senders against a two-second client request
timeout. A batch the broker appended just before the freeze gets no acknowledgement in time; the
client retries it after the thaw; without idempotence the retry is appended again.

**One detail decided whether the fixture could see anything at all.** In librdkafka,
`request.timeout.ms` *"is only enforced by the broker"* — which is frozen. The client gives up on a
request after `socket.timeout.ms`, sixty seconds by default, longer than any pause here. Setting only
the key the Java client uses would have left the native arm never retrying anything, and its zero
would have been a zero for the wrong reason.

**Measured 2026-09-24**, `ci/b-25/run.sh`, the candidate published from the branch and resolved by a
driver the way a stranger's build resolves it:

| run | records on the topic | distinct | written twice |
|---|---|---|---|
| native, idempotence off — the control, first | 105 966 | 105 766 | **200** |
| JVM, idempotence off | 116 300 | 116 050 | **250** |
| native, default — after this item | 107 184 | 107 184 | **0** |
| JVM, default | 153 874 | 153 874 | **0** |

In every run *distinct* equals the number of records the driver handed in, and nothing was refused:
the fault lost no record, it only duplicated some. **200 and 250 are multiples of fifty — the
driver's concurrency.** The reading, and it is a reading rather than a measurement: each freeze that
caught requests in flight caught one per sender, and **every one of them was written twice** — four
of five freezes on the native control, five of five on the JVM. The duplicate is not a rare race the
fixture was lucky to hit; it is what happens to everything in flight when an acknowledgement is lost.

So before this item, the same program would have written a quarter-thousand records twice on the
native arm and none on the JVM arm under the same broker failure, and each arm would have been
individually consistent with the broker — the shape of §2.2, found by the same method.

**The default kafkakn now takes is the reference arm's, conditions included** — measured against the
jar, not assumed: on unless the caller set `acks` below all or `retries` to zero, when it turns off
without a word; and more than five requests in flight is refused even with idempotence unset. A
native default that was simply "on" would have made librdkafka refuse `acks=1`, which the reference
accepts. `IdempotenceTest` holds all seven rows against both arms.

### 2.20 A client certificate is two paths on one arm and two contents on the other

[B-31](../backlog/B-31-client-certificates.md). The item's premise left one thing open on purpose:
whether the Java client's PEM key store takes two separate files. **It does not**, and that was read
in the source rather than tried:

| Fact | Where verified |
|---|---|
| a PEM key store *path* is one file holding the chain **and** the key — the same contents are handed to both | `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/common/security/ssl/DefaultSslEngineFactory.java` — `FileBasedPemStore.load` |
| two separate things are taken only as contents: `ssl.keystore.certificate.chain` + `ssl.keystore.key`; a location beside them is refused (*"Both SSL key store location and separate private key are specified"*) | the same file, `createKeystore` |
| the key is decoded as PKCS#8 only — `PKCS8EncodedKeySpec`, or `EncryptedPrivateKeyInfo` with `ssl.key.password` | the same file, `PemStore.privateKey` |
| librdkafka reads two paths and hands them to OpenSSL; it checks the pair only when a key is set | `librdkafka-2.13.0.tar.gz!/src/rdkafka_ssl.c` — `SSL_CTX_use_certificate_chain_file`, `check_pkey` |
| librdkafka withholds a client certificate whose issuer is not in the server's `certificate_authorities` | the same file, `rd_kafka_ssl_cert_callback` |
| the image's `configure` script, given a global `KAFKA_SSL_CLIENT_AUTH=required`, demands a trust store file **and** a password file — which a PEM trust store refuses | `apache/kafka@4.3.1!/docker/resources/common-scripts/configure` |

**Consequences.** The JVM arm reads both files at construction and passes their text; a temporary
concatenated file was rejected because it leaves a private key on disk where the caller put none.
The certificate and key are refused at construction unless both are set — measured on native, which
constructed from a certificate alone and would have failed only at the first handshake. The broker
fixture's third listener is configured with listener-scoped keys, so the image script never sees a
global demand for client certificates.

**Measured 2026-09-24**, `ci/b-31/run.sh`: the listener's own tools refused no certificate and the
wrong authority's before anything else ran; 200/200 records with an encrypted PKCS#8 key on each arm,
counted over plaintext; and **both** arms refused with `certificate_required` for a certificate from
the wrong authority — the Java client withholds it too, which was measured and not read. What a
PKCS#1 key does on the JVM arm was read and not measured — [B-42](../backlog/B-42-a-pkcs1-key-works-on-one-arm.md).

### 2.21 Credentials are two keys on one arm and a Java string on the other

[B-32](../backlog/B-32-sasl-plain-and-scram.md). `sasl.mechanism` needed nothing — the Java client's
key, and an alias librdkafka accepts (§1.8). The credentials did: librdkafka takes `sasl.username` and
`sasl.password`, the Java client has neither key and takes credentials only inside `sasl.jaas.config`.

| Fact | Where verified |
|---|---|
| the JAAS string is parsed by `java.io.StreamTokenizer` — escapes inside quotes, a line break ends a quoted value | `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/common/security/JaasConfig.java` |
| `JaasContext.loadClientContext` is the public door to that parser, and the one the client uses | `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/common/security/JaasContext.java` |
| the image `ensure`s `KAFKA_OPTS` once a SASL listener is advertised | `apache/kafka@4.3.1!/docker/resources/common-scripts/configure` |
| with no mechanism, librdkafka answers *"No provider for SASL mechanism GSSAPI"*; with half the pair, *"sasl.username and sasl.password must be set"* | measured 2026-09-24, `SaslTest` red on native before the rules |
| both SCRAM mechanisms in one `kafka-configs --add-config` are refused: *"A user credential cannot be altered twice in the same request"* | measured 2026-09-24 against `apache/kafka:4.3.1` |

**Consequences.** The JVM arm builds the JAAS string and escapes the backslash, the double quote and
line breaks; the proof is the client's own parser reading the value back, not a string compare. Three
rules move to common code, because each was answered in different words per arm and none named the
key: a SASL protocol needs a mechanism, the username and password come together, and credentials
belong to PLAIN or SCRAM only.

**Measured 2026-09-24**, `ci/b-32/run.sh`: the broker's own tools refused a wrong password for PLAIN
and SCRAM first; then 100/100 records on each arm for PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and
SCRAM-SHA-512 over `SASL_SSL`, counted over plaintext; and 100/100 for a password holding a double
quote and a backslash — the native arm sending it raw, which is what says the broker holds it.

### 2.22 A seam test that started its clock after the silence

[B-29](../backlog/B-29-topic-metadata.md). `partitionsFor` is a blocking call on both arms, so it was
written first as one — on the caller's thread — to watch the dispatcher test go red. **It stayed
green on both arms.** The ticker that measures the silence ran on the same single lane as the call,
started its clock on its first turn, and its first turn came after the call returned; it was then
cancelled before measuring a single gap. The silence it existed to see happened before it started
looking.

Corrected — the mark taken before the call and read once more after it — the same blocking
implementation held the lane for **20.0 s on the JVM and 5.0 s on native**, each arm's whole bound
(`max.block.ms`, `socket.timeout.ms`), against a broker that was not there. On `Dispatchers.IO` it
stays under the tolerated 500 ms. `JvmDispatcherSeamTest` does not have this defect only because its
sends are `launch`ed and the ticker gets the thread first.

**Consequence.** A dispatcher test is watched red against a blocking implementation before it is
believed; the order in which the ticker and the call get the lane is part of the fixture. And the
same reading turned up a claim in §2.13 that no test holds —
[B-43](../backlog/B-43-native-flush-may-hold-the-callers-thread.md).

| Fact | Where verified |
|---|---|
| an unknown topic comes back from `rd_kafka_metadata` as a described topic with `err` set, the call itself succeeding | measured 2026-09-24, native `TopicMetadataTest`: *"Broker: Unknown topic or partition"* in 57 ms |
| the Java client waits for an unknown topic's metadata and throws `TimeoutException` naming `max.block.ms` | measured, 20 021 ms and 17 081 ms in two runs at `max.block.ms=20000` |
| both arms describe a 7-partition topic exactly as `kafka-topics.sh --describe` does | `ci/b-29/run.sh` |

### 2.23 Transactions retire the end-offset oracle on the topics they touch

[B-30](../backlog/B-30-transactions.md). Every accounting check since B-09 reconciled against end
offsets. A transaction's commit or abort marker takes an offset, and an aborted transaction's
records take theirs, so on a transactional topic the end offset is not a count of anything a caller
asked for. The oracle there is records read by `kafka-console-consumer` under a **named** isolation
level — `read_committed` for the claim, `read_uncommitted` as the positive control that an abort's
records were written — and the coordinator's own `kafka-transactions.sh describe`.

| Fact | Where verified |
|---|---|
| `-1` is the timeout librdkafka recommends for init (twice `transaction.timeout.ms`), commit and abort (the remaining time); other values "risk internal state desynchronization" | `librdkafka-2.13.0.tar.gz!/src/rdkafka.h`, the `rd_kafka_*_transaction` declarations |
| fencing reaches the native caller as `_FENCED` from the transactional call, and as a fatal state afterwards | `librdkafka-2.13.0.tar.gz!/src/rdkafka.h`; measured, `TransactionTest` |
| `kafka-console-consumer --consumer-property` still works in 4.3.1 and prints its deprecation notice **on stdout**, so it is counted as a record | measured 2026-09-24: 201 lines for 200 committed records; `--command-property` gives 200 |

**Measured**, `ci/b-30/run.sh`, both arms: committed 50/50 under `read_committed`; aborted 0 under
`read_committed` and 50 under `read_uncommitted`; `CompleteCommit` and `CompleteAbort` from the
coordinator; a fenced producer's commit and next `send` both `ProducerFencedException`.

### 2.24 The consumer, read before it is written — and H7 restated

[B-35](../backlog/B-35-the-consumer-designed-first.md) produced
[consumer-contract](../api/consumer-contract.md) and no code. Three readings decided its shape.

| Fact | Where verified |
|---|---|
| the Java consumer's thread check is a lock held **for one call**: `acquire()` takes the caller's thread id, `release()` clears it; overlapping calls throw `ConcurrentModificationException`, sequential calls from different threads do not | `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ClassicKafkaConsumer.java`, and the same in `…/AsyncKafkaConsumer.java` |
| `wakeup()` is the one cross-thread call; interrupts are discouraged because they can abort a clean shutdown | `apache/kafka@4.3.1!/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java` |
| librdkafka is thread-safe throughout; its offset store is filled as each message is handed to the application | `librdkafka-2.13.0.tar.gz!/INTRODUCTION.md` |
| six more consumer defaults disagree or exist on one side only: `allow.auto.create.topics` (`true`/`false`), `check.crcs` (`true`/`false`), `fetch.max.wait.ms` vs `fetch.wait.max.ms`, `max.poll.records` (JVM only), `enable.auto.offset.store` and `enable.partition.eof` (native only) | `ConsumerConfig.configDef().defaultValues()` run against `kafka-clients-4.3.1.jar`; `librdkafka-2.13.0.tar.gz!/CONFIGURATION.md` |

**Consequences, all *target* until B-36 and B-37 measure them.** The JVM arm confines each consumer to
a `Dispatchers.IO.limitedParallelism(1)` lane — not one thread, because the check is not per thread —
and cancels a waiting `poll` with `wakeup()`. The native arm polls with a zero timeout and `delay`, as
the producer's pump does. `enable.auto.commit` is off on both arms because the same `true` means
"commit what the previous `poll` returned" on one and "commit what was handed over, processed or
not" on the other. `isolation.level` becomes `read_committed` on both — the safe default, which this
time is librdkafka's.

**H7 restated.** The reading says each client's threading model can be absorbed behind one surface:
the JVM's constraint is "no overlap", which a serial lane gives, and librdkafka has none. What cannot
be absorbed is `max.poll.interval.ms` — but it is the same rule on both arms, so it belongs to the
contract rather than leaking from one platform. That is an argument, and H7 is settled only when
[B-36](../backlog/B-36-assign-and-poll.md) shows three things on both arms: two coroutines calling one
consumer concurrently never produce a `ConcurrentModificationException`; cancelling a `poll` that is
waiting returns promptly and leaves the consumer usable; and neither holds the caller's dispatcher,
held with the mark taken before the call (§2.22).

### 2.25 H7, settled for assign and poll — and two things the reading did not predict

[B-36](../backlog/B-36-assign-and-poll.md) built the surface §2.24 designed and made the three
measurements it named, on both arms, each watched red against a mutant:

| Measurement | Result | The mutant that turned it red |
|---|---|---|
| eight coroutines polling one consumer at once | every record once, nothing thrown | JVM lane replaced by `Dispatchers.IO`: *"KafkaConsumer is not safe for multi-threaded access"* |
| a waiting `poll` cancelled, then the next call | released in 16 ms (JVM) / 0 ms (native); next call at once | JVM without `wakeup()`: released at once, **next call 55 s later** |
| a waiting `poll` against a single-lane caller | under 500 ms of silence | native blocking in `rd_kafka_consumer_poll(timeout)`: 3.0 s |

**The second row's first version passed its mutant.** It timed the cancellation, which returns at once
either way because the waiting call is detached from its caller; what the missing `wakeup()` breaks is
the *next* call, queued behind a `poll` nobody is waiting for. §2.22's lesson again, in a new shape:
the fixture measured the moment next to the one that matters.

**Two librdkafka requirements the design did not know**, each answered by the native arm doing more:
`rd_kafka_assign` needs a `group.id` (*"Local: Unknown group"*; the native arm supplies a private one
that joins nothing — `kafka-consumer-groups.sh --list` confirms it never appears), and a seek before
fetching has started is refused (*"Local: Erroneous state"*, as `rdkafka.h` warns; the native arm seeks
by re-assigning).

**And one about the fixture.** Records stamped in 2023, so that a seek to a time has one right
answer, were deleted by the broker within minutes: time-based retention reads the records' time. The
consumer's topic is created with `retention.ms=-1`.

### 2.26 One group, one member on each arm — and a loss check that was blind at first

[B-37](../backlog/B-37-consumer-groups.md). D2 kept groups out as the hard part; what the measurement
found hard was the test, not the arms.

**The mixed group formed on the first run.** A JVM member and a native member in one classic group, in
two processes at once, split four partitions and handed them over when the native one left: 600 of 600
records, commits at the log end as `kafka-consumer-groups.sh` reads them. The two defaults' only shared
assignor is `range`, and the split had range's shape — read from the shape, not from the broker.

**The loss check's first version could not see the loss it was for.** The leaving member abandons one
batch uncommitted, the way a crash would; the first version counted that batch as *seen*, so the
group's union covered it whether or not anyone received it again. Moved out of `seen`, the batch can
only be covered by redelivery — and a native `close()` made to commit first then lost one record in
the native group and one in the mixed one, and the run went red. The same lesson as §2.22 and §2.25,
in a third shape: what the check counted included the thing it was checking.

**When the member that stays may stop was the hardest question, and it was answered wrong twice.**
A fixed run time made the result depend on how fast Gradle started: the same run passed, and then
"lost" 465 records when the JVM member left before the writer finished. "Nothing for five seconds"
stopped a member that had joined before the writer started: 565 "lost". Neither was the consumer
losing anything — each was the fixture leaving early — and both read exactly like the failure the
test exists to find. What holds is data: a member stays until it holds every partition and the last
record of each has been seen by the group as its own process knows it.

**Records arrive while the group forms.** Written all at once, whichever member joined first read
everything before the second arrived, and a split would never meet a record; the third party writes a
record every 40 ms instead.

## 4. Risks, with the machinery that would catch them

**A wrong wire assumption that both arms share.** The differential oracle catches disagreement
between the arms, not a shared misunderstanding of Kafka. Mitigation: the broker's own end offsets
and `kafka-console-consumer` are used as a third party in acceptance, never the library reading back
its own writes.

**Silent message loss returning under a new shape.** §1.4 is one instance; the general form is any
path where the library's count of "sent" comes from anywhere but the caller's input. Mitigation: a
test that produces past `queue.buffering.max.messages` is in the gate from M1, not an afterthought —
the defect is invisible below the bound.

**The patch in D4 rotting against a new librdkafka.** Mitigation: the bump procedure re-applies and
re-tests it (H3), and the patch is a file in this repository rather than a sed in a script, so a
conflict is visible. Since [B-22](../backlog/B-22-a-dead-patch-must-say-so.md) the bundle build also
says *which* rot it is — obsolete, moved, or a file that is gone — because one message for two
opposite actions is how a dead patch gets carried for years (§2.16).

**A green suite over a library nobody has called.** Mitigation: an acceptance item that uses the
published artefact from an external downstream project, not the sources.
