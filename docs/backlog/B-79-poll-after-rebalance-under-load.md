---
id: B-79
title: "PollAfterRebalanceTest's JVM fill expires its records: the Java client's burst into a fresh topic"
status: done
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

## Iteration log

- 2026-09-27, B-80's regressions: a fourth time, in `ci/b-09/run.sh`'s control pass (the whole suite run against
  the dropping producer), with the same message. Pass 1 of the same run, the whole suite, was green.

## Findings (2026-09-27)

**Not load.** The broker's log for the fourth failure showed the mechanism. 0.1 s after the topic was created, the
idempotent producer re-sent sequence 0 of partition 0 after the broker had accepted 0 to 79, then kept re-sending
it: `OutOfOrderSequenceException`, 107 times against an end sequence of 999, for the two minutes of
`delivery.timeout.ms`.

*Measured*, `ci/b-79/run.sh` (`FreshTopicBurstTest`, ten rounds each, load average 1.4):

| | at once | one acknowledged record per partition first |
|---|---|---|
| JVM arm | 9 of 10 stuck: 16 records lost each, 952 once, after 120 s; 121–122 `OutOfOrderSequence` in the broker's log per round | 10 of 10 clean, 66–224 ms |
| native arm | 10 of 10 clean, 2.3–3.4 s | 10 of 10 clean |

**The Java client alone reproduces it** (`ci/b-79/Burst.java`, no kafkakn on the classpath): from 1 thread, 4 of 5
rounds lost records; from 64 threads, 3 of 5. So it is `kafka-clients` 4.3.1, not the JVM arm's concurrent `send`.
The first burst in a process was clean in every series. That is why the test passed alone and failed in
whole-suite runs, where other fresh topics come first. Load only changed which run it happened in.

- **Done.** The fill sends record 0 of each partition alone, acknowledged, before the burst. The record count is
  unchanged. The contract records the behaviour under "What both actuals must agree on". Nothing is filed upstream.
- **Verified** on 2026-09-28: the whole JVM suite three times in a row with the fix, and `PollAfterRebalanceTest`
  passed in each. Before, it failed in four of six whole-suite runs, so three passes by chance would be about 1 in
  27. An ordered mutant, where the test is the second burst in its process, could not be arranged: Gradle does not
  order test classes. The control for the method is the `warm=1` rows above, where rounds 2 to 10 were clean.
- The `mostik-broker` hypothesis was refuted earlier: that container is mostik's own.
