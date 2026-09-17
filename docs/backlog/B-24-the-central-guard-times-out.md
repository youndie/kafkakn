---
id: B-24
title: "REFUTED: the accounting guard does not time out — it was run without its fixture"
status: question
priority: P1
size: S
stage: stage-1-produce
---

# B-24 — the accounting guard times out, and that reads like the defect it guards

`AccountingTest.the_broker_holds_every_record_the_caller_handed_in` fails on the jvm arm with
`UncompletedCoroutinesError: After waiting for 1m, the test body did not run to completion`.

**Observed 2026-09-17 on `main`**, not only on a branch: it was found while running the suite for
[B-18](B-18-verification-cannot-be-turned-off.md), then reproduced twice on `main` itself with
nothing of that branch in the tree, on a box at load average ~11. The native arm was not reached in
those runs, so whether it shares the failure is unknown.

The number is `runTest`'s default: sixty seconds. The test hands in 3 000 records at `acks=all`
through a queue bound lowered to 100, so it is *meant* to spend its time waiting — that is the
backpressure it exists to exercise — and on a busy machine the waiting exceeds the default.

- **Why this is worse than an ordinary flake.** This is the guard the whole project is shaped
  around, and its failure text is about coroutines rather than about records. A reader who sees it
  red will either believe records were lost or learn to ignore the one test that must never be
  ignored. Both are worse outcomes than a slow test.
- **The decision to take.** Give it a timeout that is a property of the work rather than of
  `runTest`'s default, and make it fail with a sentence that says which of the two it is: the
  records were accounted for and the clock ran out, or a record went missing. A number picked here
  is a guess until somebody measures the test's own duration on an idle box and on a loaded one —
  both, because a timeout set from the idle figure is the same bug again.
- The rejected alternative is lowering `RECORDS`. The count is what carries the test past the queue
  bound many times over; trading it for speed trades away the mechanism
  ([research §2.5](../research/research-architecture.md)).
- Not covered: whether the native arm times out too. It is the first thing the item measures.

- AC: the test's own wall-clock duration measured on both arms, idle and under load, and written
  down before any number is chosen.
- AC: red for a timeout says so in its message and does not read as a lost record.
- AC: the naive control still goes red for the reason it always did — a timeout that swallowed the
  control would make the guard pass for the wrong reason.
- Anchors: `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/AccountingTest.kt`,
  `ci/b-09/run.sh`.

## The premise was wrong, and this is what was measured

**The guard does not time out.** Run the way it is meant to be run — `ci/b-09/run.sh`, which creates
the topics first — it passes on both arms, 2026-09-17:

```
  jvm       handed in 3000   answered-without-sending 0   end offsets 0 -> 3000
  linuxX64  handed in 3000   answered-without-sending 0   end offsets 0 -> 3000
  ... the control run is red, as it must be
  jvm       dropped 2900 of 3000   end offsets 0 -> 100
  linuxX64  dropped 2891 of 3000   end offsets 0 -> 109
```

31 tests on the jvm arm and 28 on the native one, no failures, and the naive control red for the
reason it has always been red.

**What actually happened.** The failure was produced by running `./gradlew :kafkakn-core:jvmTest
--tests "*AccountingTest*"` directly — which is not how this test runs. Its topic is created by
`ci/b-09/run.sh`; without it the Java client waits `max.block.ms` for metadata that will never
arrive, and `runTest`'s own one-minute watchdog fires first:

```
UncompletedCoroutinesError: After waiting for 1m, the test body did not run to completion
```

Raising that watchdog to ten minutes does not fix anything, and it is how the real cause became
visible — the run then failed at 120 s with the truthful sentence instead:

```
org.apache.kafka.common.errors.TimeoutException: Topic kafkakn-acct-jvm not present in metadata after 60000 ms.
```

So `runTest`'s default was **masking** the honest error, not causing the failure. The load average
(~11) had nothing to do with it either; it was the first thing suspected and the wrong one.

The three acceptance criteria above are therefore not achievable as written: there is no timeout to
measure idle and under load, and no timeout message to improve.

## The question, and it is for a person

**Option 1 — close this as refuted.** The guard is fine, the repository already says how the suite
runs (`CLAUDE.md`: "each item's `ci/b-NN/run.sh` is what runs it"), and a contributor who reads that
never meets this. Cost: the next person to run one test directly gets sixty seconds of silence and
then a sentence about coroutines, and this file is where they would otherwise have found out why.

**Option 2 — re-aim it at what is actually wrong**: a suite run outside its fixture blames the
library. The test could answer in a second instead of sixty, with a sentence naming
`ci/b-09/run.sh`, by checking its topic exists before it produces. Cost: every test that needs a
fixture then needs the same guard, or the one that has it becomes the exception nobody extends —
and this is the kind of helpfulness that grows into a second harness inside the suite.

Whoever decides should know the third fact: the misattribution is **not** the library reporting a
loss. It is a Kafka client timeout and a coroutines watchdog, and no reconciliation ever claimed a
record was missing. The original worry — that a red on this guard would be read as lost records —
was reasonable and turned out not to be what this red said.
