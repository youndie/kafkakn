---
id: B-101
title: "AdminPartitionsTest reads a topic it has just created and is told it is unknown (native, one suite run)"
status: done
priority: P1
size: S
stage: stage-18-what-the-consumer-and-the-harness-found
blocked_by: []
---

# B-101 — `AdminPartitionsTest` reads a topic it has just created and is told it is unknown

Found by B-100's whole-suite run on the Linux box (2026-09-29, load average 0.8):
`AdminPartitionsTest.new_partitions_land_on_the_broker_named_and_an_unknown_broker_is_refused` failed on `linuxX64`
with *"describeTopics: kafkakn-assigned-linuxX64-…: Broker: Unknown topic or partition (3)"*. The test creates the
topic, grows it, and then describes it in a loop that waits for partition 1 (B-90). The loop retries a description
that lacks the partition, not one that fails because the topic is not in the broker's metadata yet.

The same test failed on the JVM in CI once before (run 36450407379), which is what B-90 answered.

- **The decision and its reason.** Measure where it fails before changing the wait: at the first `describeTopics`, or
  after `createPartitions`. If the broker answers `createTopics` before its metadata has the topic, that is Kafka's
  behaviour (the create is the controller's, the description a broker's cache) and a fact for the admin contract; the
  test then waits for it. If kafkakn's native `createTopics` returns before librdkafka's result says the topic exists,
  it is a defect of ours.
- AC: the failure's step is recorded, and the admin contract says whether a topic is visible when `createTopics`
  returns, on both arms.
- AC: the test waits for what the contract says, and its wait is bounded.
- Anchors: `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/AdminPartitionsTest.kt`.

## Findings

- **Done, 2026-09-29. It is Kafka's, on both arms.** `ci/b-101/run.sh`, 200 rounds per arm (`CreateThenDescribeTest`,
  which asserts nothing): described at once after `createTopics`, 199 of 200 new topics were unknown on the JVM and
  199 of 200 on native; all were visible within 74 ms (JVM) and 134 ms (native), typically 25 to 35 ms. After
  `createPartitions` on a visible topic, the old count came back at once in 6 of 200 (JVM) and 21 of 200 (native),
  and the new count within 16 and 38 ms. The contract says so under the admin client.
- **Why the test passed almost always:** it grows the topic before describing it, and the grow call is usually long
  enough for the topic to reach the broker's description. On 2026-09-29 it was not.
- **The test waits now,** for an unknown topic as for a missing partition, within the same bound
  (`describedOnce`). Mutant, the retry on an unknown topic taken out: the test failed by name on native in 1 of 6
  runs (`AdminPartitionsTest.new_partitions_land_on_the_broker_named_and_an_unknown_broker_is_refused[linuxX64]`),
  as rare as the original failure. With the wait, 10 of 10 runs green, five per arm.
- **Found on the way:** the unknown topic is a different type on each arm, and `KafkaAdminException` exists only on
  native (B-103).
