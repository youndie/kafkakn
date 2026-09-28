---
id: B-100
title: "The native poll handed over 486 records of a partition a callback took inside that poll: drop them"
status: wip
priority: P1
size: S
stage: stage-18-what-the-consumer-and-the-harness-found
blocked_by: []
---

# B-100 — the native poll handed over 486 records of a partition a callback took inside that poll

B-68's suspected mechanism, seen. The `suite` workflow on
[#123](https://github.com/youndie/kafkakn/pull/123) (run 36488070956, a GitHub-hosted runner, 2026-09-28) failed
`PollAfterRebalanceTest.a_poll_returns_no_record_of_a_partition_given_up_during_it` on `linuxX64` only:

- `poll.rebalance.strays=487`: partition 0, offsets 1500 onward, *not held*, and 1500 also *fetched before the
  revocation*. The JVM arm had none.
- `poll.rebalance.mid.drain=1/486`: B-68's counter. One rebalance callback took partitions inside a `poll` that had
  already collected records, and 486 of them were of the partitions taken. So every stray is that mechanism: the
  records `drain()` collected before `rd_kafka_consumer_poll` ran the revocation reached the caller after
  `onRevoked` had run for their partition.

B-68 was left at option 3 because the counter read 0 wherever it was looked for; its rule was "2, then by the
result", and the result is now the counter moving with the strays. The fix B-68 described is option 1.

The PR the run belonged to changed only `kafkakn-schema-registry` and was merged in spite of the red `suite`: the
merge watcher decided "all green" with `grep -qv`, which on the Mac (ugrep) answers "no line differs" when one does.
That is fixed in the loop's watcher, not here.

- **The decision and its reason.** After a callback gives partitions up inside a `drain`, drop the records already
  collected for those partitions, as the Java client drops a revoked partition's fetched records. Records collected
  for partitions still held stay.
- The rejected alternative, as in B-68: stop the drain at the callback and return what was collected. Those records
  would still reach the caller after its listener gave their partition up.
- AC: a `linuxX64Test` of the filtering itself, which fails with the filter removed.
- AC: `PollAfterRebalanceTest` stays the integration guard on both arms, and the counter now says how many records
  were dropped.
- AC: the consumer contract's §2a says, for both arms, that `poll` returns only records of partitions held after
  that poll's callbacks, and replaces the "once, unexplained" note with what was measured.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.native.kt`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/PollAfterRebalanceTest.kt`.
