---
id: B-90
title: "B-87's test read the topic before the growth was visible, and failed once on CI"
status: done
priority: P1
size: S
stage: stage-20-what-waited-for-a-caller
epic: feature-administer-topics
blocked_by: [B-87]
---

# B-90 — B-87's test read the topic before the growth was visible

The `suite` workflow on B-89's pull request failed in
`AdminPartitionsTest.new_partitions_land_on_the_broker_named_and_an_unknown_broker_is_refused` on the JVM:
*"Collection contains no element matching the predicate."* The test described the topic at once after
`createPartitions`, and the new partition was not in the description yet. The growth reaches the broker's view of
the topic a moment later: the test above it in the same file waits for exactly that (`VISIBLE_WITHIN`), and so do
B-61's configuration tests. B-87's test was written without the wait. It passed on the Linux box and on B-87's own
CI run, and failed on the next.

- **Done.** The test waits, bounded by the same `VISIBLE_WITHIN`, until partition 1 is described, then checks its
  replicas.
- AC: the test is green on both arms on the box, and the `suite` workflow is green.
- Anchors: `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/AdminPartitionsTest.kt`.
