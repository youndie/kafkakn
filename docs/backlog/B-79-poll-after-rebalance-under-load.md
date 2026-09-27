---
id: B-79
title: "PollAfterRebalanceTest's JVM fill expires its records in whole-suite runs on a loaded machine"
status: open
priority: P2
size: S
stage: stage-18-what-the-consumer-and-the-harness-found
blocked_by: []
---

# B-79 — `PollAfterRebalanceTest`'s JVM fill expires its records in whole-suite runs on a loaded machine

On 2026-09-27 the test failed on the JVM three times, in three whole-suite runs (`ci/b-09`, `ci/b-11`, and
`jvmTest linuxX64Test --continue`). The message was *"Expiring 16 record(s) for kafkakn-poll-rebalance-jvm-…:
120000 ms has passed since batch creation. The request has not been sent, or no server response has been received
yet."* The fill writes 2 × 10 000 records of 1 000 bytes, a thousand concurrent sends at a time, and one batch
went two minutes (`delivery.timeout.ms`) without an answer. The shared build machine was loaded then (46 users,
load average about 3). The test passed alone, together with `EnqueueMetadataTest`, and in a whole JVM suite on
`main` and on the branch afterwards.

Refuted on the way: that the consumer bridge's container shares the broker. mostik runs its own, `mostik-broker`,
and stops only that one.

- **The decision and its reason.** Measure before changing anything. A fill that passes by being slower, or a
  longer `delivery.timeout.ms`, would hide the one fact worth having: whether the broker stalled, or the client's
  network thread did.
- AC: a run of the whole suite that reproduces it records, around the fill, the broker's own view (request
  latency, the log for that minute) and the Java client's (`producer.metrics()` for the batch's partition), so the
  two are told apart.
- AC: if it is the broker or the machine, the fill is sized to what the fixture can take, and the item says so. If
  it is the client, that is a finding for the contract, not a test change.
- Anchors: `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/PollAfterRebalanceTest.kt`.
