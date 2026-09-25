# CLAUDE.md — kafkakn

A Kafka client for Kotlin Multiplatform — a producer, a consumer and a minimal admin client: one
`expect` surface each, two actuals — librdkafka through cinterop on Kotlin/Native,
`org.apache.kafka:kafka-clients` on the JVM. Targets `jvm` and `linuxX64` are mandatory; `macosArm64`
exists for contributors on a Mac and is not published; `linuxArm64` is built and run on request
(`-Pkafkakn.linuxArm64`, `ci/b-39/run.sh`) and not published.

**State (2026-09-26): stages 0 to 14 are closed; the backlog has no open item.** The producer has parity with the clients
underneath — explicit partition, timestamp, metadata, idempotence by default, compression, transactions,
TLS with client certificates, SASL PLAIN/SCRAM/OAUTHBEARER, metrics; the consumer assigns, seeks, joins
groups and commits, with exactly-once read-process-write; the admin client creates, deletes, describes,
configures and grows topics, deletes records, and reads and moves consumer groups and their offsets. Everything is measured against the broker's own tools, and the contracts in
`docs/api/` say where the two arms differ. `linuxArm64` runs on arm64 hardware down to glibc 2.17 (B-39,
B-44), built on request and not published. This line used to
say *nothing is implemented*, and said it for twenty-four closed items after it stopped being true; it
is dated now so that its age is visible. Read before writing code — the obvious design is wrong in four
documented ways.

## Where to start a session

1. [docs/research/research-architecture.md](docs/research/research-architecture.md) — the decisions
   and the evidence under them. Without it the items look like "do the obvious thing", and here the
   obvious thing is wrong:
   - **`rd_kafka_produce` only enqueues, and a record it refuses produces no delivery report at
     all** (§1.4). A naive binding lost 264 826 of 1 000 000 records with `failed = 0` and a
     successful flush. The unit of truth is what the *caller* asked to send.
   - **A full queue is backpressure, not an error** (§1.4, D3). `send` suspends. Returning a failure
     the caller may ignore rebuilds the same defect somewhere new.
   - **`rd_kafka_flush` returns an error code, not a count** (§1.4). Reading it as "how many are
     left" printed `-185`, which is a timeout wearing a quantity's clothes. The count is
     `rd_kafka_outq_len`.
   - **`rd_kafka_producev` is variadic and unusable through cinterop** (§1.5). The library cannot
     mirror librdkafka's own recommended API.
2. [docs/api/producer-contract.md](docs/api/producer-contract.md) — what a test cites.
3. [backlog.md](backlog.md) — the queue, the stages, the decisions.

## The rule that governs everything here

> **First a test from the specification, then the code. The test passing is the GATE.**

A test cites a concrete place in the producer contract or in Kafka's own documentation — in its name
or in a comment above it. A test "from common sense" is not accepted, because the common sense here
says a produce call that returned has sent something.

Order inside a task: read the place in the contract → write the red test → **confirm it is red for
the intended reason** → implement → green.

## The second rule

> **The JVM arm is the oracle, not a portability afterthought.**

The same `commonTest` suite runs on both arms against one broker. The native arm is correct when it
agrees with the reference implementation.

Three kinds of assertion, and the difference matters (research §2.1):

- **the broker's truth** — offsets, partitions, what an independent reader sees. `commonTest`, each
  arm checked against the same third party. Agreement follows from both being right.
- **what only the client knows** — the partitioner's choice for a key, the error type, how a config
  value is normalised. `commonTest` too, but comparing them *across* arms cannot happen inside a
  test: record with `recordObservation`, and `ci/harness/compare-arms.sh` diffs the two files. This
  is where the arms can differ while each looks right alone.
- **the platform seam** — that the archives link, that a callback on librdkafka's thread resumes the
  right coroutine. `linuxX64Test`, and that is correct rather than a leak.

So a test that can only live in one arm's source set is a warning sign for the first two and the
normal case for the third. If it is one of the first two, the `expect` surface has probably leaked a
platform's shape, and that is a finding for the research document rather than something to work
around.

## What not to do

- **Do not let the library count its own successes.** The reconciliation lives in the suite, against
  the broker's end offsets. A producer that reports its own delivery rate is the shape that failed.
- **Do not verify a produce with our own consumer.** The oracles are the broker's end offsets and
  `kafka-console-consumer`. A producer checked by its own consumer can be wrong in both directions
  at once.
- **Do not test below the queue bound and call it covered.** The default bound is 100 000 records; a
  2 000-record test cannot reach the case that matters. Lower the bound in the suite.
- **Do not type keys or values as `String`.** Kafka's value space has no encoding, and the first
  non-UTF-8 payload is what finds out.
- **Do not add a configuration key the actuals do not honour.** An option accepted and dropped looks
  identical to one that worked. Unknown keys fail at construction.
- **Do not name a private project.** This repository is public. The work it starts from was done in
  one that is not: quote the measurements with their method, never the repository.
- **Do not set `status: draft` on the default branch.** It is reserved for a document in an open
  pull request and the checker rejects it on `main`. A document that is not built yet is `active`
  with its unbuilt parts marked ***target*** in the text — see [docs/README.md](docs/README.md).
- **Do not drop a *target* marker without a test behind it.** `main` describes what exists, and the
  marker is where that is said.
- **Do not trust a gotcha inherited from a sibling project without re-checking it at the pinned
  version.** Several in the research document came from measurement here; the rest say where they
  came from.

## The loop merges its own pull requests, but only on green

An iteration that closes a backlog item opens its pull request **and** merges it (squash, the item
id in the footer, branch deleted), then moves on. Without this the loop stalls after one or two
items: statuses on `main` change only when a pull request merges, so every later item stays blocked
on a blocker that still reads `open`.

**It merges only when the checks have PASSED**, which is not the same statement as "the checks have
finished". A run that is still queued, one that was cancelled, and one that never started all look
alike from a distance, and this repository is public precisely so that its checks actually run —
wait for `conclusion=success`, not for the absence of red.

What does not change: the gate is green before the merge, an item is `done` only if its acceptance
was exercised, and a `question` item waits for a person. A pull request opened by anything other
than this loop — a dependency bot included — is not the loop's to merge.

## Where the work runs

`linuxX64` builds, the C bundle and the broker run on the Linux box through `wsl-run`; the mutagen
session is named `kafkakn`. **Edits are made in this checkout on the Mac** — the Linux side is a
one-way replica, so anything written there is erased on the next cycle, including build output,
certificates and logs. A log that matters is captured on the Mac by redirecting the remote command,
never written on the far side.

Because the session exists, `git`, `make` and the documentation scripts in this repository need a
`LOCAL=1` prefix: they belong on the Mac, and git on the replica is meaningless anyway — the session
is `--ignore-vcs` and there is no `.git` over there.

**A Mac runs the native arm too** ([B-40](docs/backlog/B-40-macos-for-contributors.md)).
`macosArm64` is declared only on a Mac, from a bundle `ci/librdkafka/build-macos.sh` builds there, and
`ci/b-40/run.sh` runs the native suite on the Mac against the broker on the Linux box, through a
tunnel, then diffs it against the JVM arm there. It is a contributor's loop; `linuxX64` stays the
target that is built, measured and published.

**The Mac is also the arm64 host** ([B-39](docs/backlog/B-39-linux-arm64.md)). Docker Desktop there is
an aarch64 Linux, so the `linuxArm64` C bundle is built on the Mac (`KAFKAKN_ARCH=aarch64
ci/librdkafka/build.sh`) and the binaries run there in a stock `ubuntu:24.04` container. Kotlin is
cross-compiled on the Linux box. `ci/b-39/run.sh`, run on the Mac, does all of it. The build box has no
arm64 emulation, and the loop does not register any.

## Read the file back instead of believing the edit

Three separate no-ops happened in one item on 2026-09-17 and each was reported as done: a shell that
failed to parse a command before the edit inside it ran; a second edit that could not find text the
first was supposed to have written; and the commit-message hook rejecting a **whole** command for a
73-character subject, so the status change and index regeneration chained in front of the commit
never ran either — and retrying only the commit merged the work with the item still `wip`.

The hook intercepts **before** execution. Anything chained after a bad commit message silently does
not happen. So: after editing, read the file back; after a merge, check the item's status on `main`.
A command that printed nothing did not necessarily do nothing quietly — it more often did nothing at
all.

## Checks

```bash
make check          # the documents
./gradlew ktlintCheck   # the code
```

**Two halves, and CI runs both** ([B-14](docs/backlog/B-14-ci-workflow.md)): `make check` needs
python and takes a second, `ktlintCheck` needs a JDK and takes a minute. Neither runs the suite —
that needs the C bundle and a broker, and every item's own `ci/b-NN/run.sh` is what runs it on the
Linux box.

The formatter is the portfolio's, pinned by `io.github.youndie.sborka.lint`, and its rule set is not
only about layout: it refuses a `catch (e: Throwable)` that swallows `CancellationException`, and it
was right twice on the day it arrived.

## Language

Everything in this repository is in English: documents, code, KDoc, test names, exception messages,
commit messages, pull request titles and bodies. Commits follow Conventional Commits.

Kafka's own names are verbatim as Kafka writes them: `bootstrap.servers`, `acks`,
`queue.buffering.max.messages`, `security.protocol`, `ssl.ca.location`.
