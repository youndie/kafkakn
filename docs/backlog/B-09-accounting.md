---
id: B-09
title: "Account for every record the caller handed in"
status: done
priority: P0
size: S
stage: stage-1-produce
blocked_by: [B-08]
---

# B-09 — Account for every record the caller handed in

The guard that would have caught the defect this project is shaped around: the number of records the
caller asked to send, the number the broker acknowledged, and the topic's end offsets must agree.

- **The decision and its reason.** The reconciliation lives in the **suite**, not in the library. A
  library that counts its own successes and reports them is exactly the shape that failed; the check
  belongs outside it, against the broker.
- The rejected alternative is a `sentCount` property on the producer. It answers the narrower
  question — how many records were *queued* — and invites the caller to read it as a success rate.
- Not covered: at-least-once redelivery and idempotence. Out of scope
  ([D2](../research/research-architecture.md)).

- AC: a test produces past the queue bound and asserts the end offsets grew by exactly the number of
  records handed in, on both arms.
- AC: the same test is shown **failing** against a deliberately naive implementation that counts
  enqueue refusals and moves on — so the guard is known to catch the defect it exists for.
- AC: no public API exposes a delivery-report count.
- Anchors: `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/AccountingTest.kt`.

## Outcome — 2026-09-17

Closed by `ci/b-09/run.sh`, run twice on the Linux box against a KRaft broker.

| | jvm | linuxX64 |
|---|---|---|
| handed in | 3 000 | 3 000 |
| answered without being sent | 0 | 0 |
| end offsets, on a topic created empty | 0 -> 3 000 | 0 -> 3 000 |
| suite | 15 tests, 0 failures | 17 tests, 0 failures |

**The control, which is what makes the row above mean anything.** The same test against
`NaiveProducer` — which counts an enqueue refusal and moves on, answering every `send` with a
`RecordMetadata` — was **red on both arms**, and the script fails if it is not. The jvm arm landed
100 of 3 000 and the native arm 103, and in both cases the shortfall was exactly the number the
control admits to dropping: the oracle sees the whole of the loss, not a part of it.

**The third criterion is a gate, not a review note.** `scripts/no_delivery_counters.py` fails on a
`commonMain` declaration pairing a delivery word with a quantity word, strips comments first so the
documents may still explain what is forbidden and why, and carries a `--selftest` that runs the rule
against a sample it must flag and one it must not. It was also watched failing on the real surface
with a `deliveredCount` injected into `KafkaProducer`.

**Deliberately not done.** The naive producer is a test-only wrapper, not a second binding: it
simulates the refusal with a permit count and therefore says nothing about librdkafka's own refusal
path, which is [B-08](B-08-suspend-on-backpressure.md)'s subject. The gain is that the control runs
identically on both arms — one that only the native arm could run would leave the jvm arm's guard
unproven. Recorded as [research §2.7](../research/research-architecture.md).
