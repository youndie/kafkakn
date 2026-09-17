---
id: B-09
title: "Account for every record the caller handed in"
status: open
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
