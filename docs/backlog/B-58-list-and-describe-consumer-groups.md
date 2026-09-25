---
id: B-58
title: "List and describe consumer groups"
status: open
priority: P2
size: M
stage: stage-13-admin
---

# B-58 — list and describe consumer groups

The admin client creates, deletes and describes topics, and describes the cluster
([B-34](B-34-a-minimal-admin.md)). It cannot see a single consumer group, and groups are what an
operator asks about most. Both clients can: `Admin.listConsumerGroups`/`listGroups` and
`describeConsumerGroups`, and `rd_kafka_ListConsumerGroups`/`rd_kafka_DescribeConsumerGroups` in
librdkafka 2.13.0's `rdkafka.h`. Until 2026-09-25 the README said consumer-group administration was *not planned*;
the owner reversed that.

- **The decision and its reason.** `listConsumerGroups()` and `describeConsumerGroups(ids)`:
  - the group id, its state, its protocol, and its members;
  - each member's id, client id, host, and assigned partitions.

  Only what both clients report, as `describeTopics` did. The Java client's `listGroups` (4.x) also
  lists share and streams groups; this item lists consumer groups only, so that both arms answer the
  same question.
- Not covered: offsets and lag ([B-59](B-59-consumer-group-offsets-and-lag.md)), and changing
  anything.

- AC: for a group with one member on each arm, both arms' descriptions agree with each other and with
  `kafka-consumer-groups --describe --members`.
- AC: an empty group and a group that does not exist are reported the same way on both arms, as the
  contract states.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`,
  `docs/api/producer-contract.md` (the admin section).
