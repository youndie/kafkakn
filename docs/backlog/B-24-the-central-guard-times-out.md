---
id: B-24
title: "The accounting guard times out on a loaded box, and a timeout reads as a lost record"
status: wip
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
