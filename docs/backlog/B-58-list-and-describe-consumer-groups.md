---
id: B-58
title: "List and describe consumer groups"
status: done
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

## Findings (2026-09-25)

- **AC: a group described alike on both arms, and as `kafka-consumer-groups --describe --members` says.**
  The member held against the broker's tool is the distribution's console consumer, so kafkakn is on
  neither side of the comparison. Member id, host, client id and assignment are identical on both arms and
  in the tool's output. The arms agree on all six observations.
- **AC: an empty and a missing group reported the same way on both arms.** The missing one was not, at
  first. The Java client threw `GroupIdNotFoundException`, and librdkafka described it as `DEAD` with no
  members. It is `DEAD` on both now: librdkafka cannot tell missing from dead, and a portable caller could
  not name the Java exception. The JVM arm maps it per group. A group with no member left but with
  commits is `EMPTY` on both.
- **`listConsumerGroups()` is deprecated in 4.3.1**, and warnings are errors here, so the JVM arm uses
  `listGroups(ListGroupsOptions.forConsumerGroups())`.
- **Two faults in my runner, both about reading the broker's tool:** it stripped the host's leading `/`,
  which the tool prints just as both clients do. And it read the sixth column, which in 4.3.1's `--verbose`
  is `CURRENT-EPOCH`, not the assignment; it printed `-` and looked like a group still assigning.
- **Mutants:**
  - native reporting no members: killed by name;
  - the JVM rethrowing a missing group: killed by name;
  - lower-casing the state name: **not a mutant**, the mapping is case-insensitive by construction. It
    pointed at a real gap instead: nothing tested the two-word states. `both_clients_spellings_of_a_state_read_as_one`
    now does, and the mutant that drops the camel-case split is killed by it on both arms.
