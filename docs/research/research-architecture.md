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
`io.github.youndie`, with the repository declared under a content filter so an outage there cannot
fail resolution of anything else. **No Maven Central**, and no release: publication is a decision
nobody has taken.

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
| H2 | Per-message headers can be carried without `rd_kafka_producev` (§1.5) | [B-10](../backlog/B-10-record-headers.md) |
| H3 | The old-glibc route (D4) survives a librdkafka bump without a new patch | re-checked at every bump; first at [B-03](../backlog/B-03-c-bundle-old-glibc.md) |
| H4 | A suspending `send` over librdkafka's callback seam has no throughput cost worth reporting against the blocking shape | **not measured, deliberately — §2.4** |
| H5 | `linuxArm64` costs a matrix row and no code (D6) | not scheduled; claimed nowhere until it is |

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
conflict is visible.

**A green suite over a library nobody has called.** Mitigation: an acceptance item that uses the
published artefact from an external consumer project, not the sources.
