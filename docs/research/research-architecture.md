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
[B-13](../backlog/B-13-external-consumer-acceptance.md). A build that is not this one resolved the
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
