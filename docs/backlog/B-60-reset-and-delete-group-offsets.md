---
id: B-60
title: "Reset a group's offsets, delete them, and delete a group"
status: done
priority: P2
size: M
stage: stage-13-admin
blocked_by: [B-59]
---

# B-60 — reset a group's offsets, delete them, and delete a group

Replaying a topic for a consumer service means moving its group's offsets while the group is empty,
which today takes `kafka-consumer-groups --reset-offsets`. Both clients do it:
`Admin.alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets` and `deleteConsumerGroups`, and
librdkafka's `rd_kafka_AlterConsumerGroupOffsets`, `rd_kafka_DeleteConsumerGroupOffsets` and
`rd_kafka_DeleteGroups`.

- **The decision and its reason.** The three calls, with the broker's own refusals kept as errors:
  altering the offsets of a group that has members is refused by the broker, and that refusal is the
  safety.
- Not covered: a `--to-datetime`-style convenience. [B-59](B-59-consumer-group-offsets-and-lag.md)'s
  `listOffsets` plus this call is that convenience, in the caller's code.

- AC: each arm moves an empty group's offsets, and a member that joins afterwards reads from exactly
  there, as the broker counts.
- AC: altering the offsets of a group with an active member is refused on both arms with the same
  exception.
- AC: a deleted group no longer appears in `kafka-consumer-groups --list`.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.

## Findings (2026-09-25)

- **The shape.** `alterConsumerGroupOffsets(groupId, offsets)`, `deleteConsumerGroupOffsets(groupId,
  partitions)` and `deleteConsumerGroups(groupIds)`, and one `GroupNotEmptyException` for the broker's
  refusal. None of the three Java calls is deprecated in 4.3.1 (`javap -v`).
- **The broker refuses an active group three ways, measured the same on both clients:**
  - an altered offset is refused with `UNKNOWN_MEMBER_ID` (25), because the admin commits as no member;
  - a deleted offset with `GROUP_SUBSCRIBED_TO_TOPIC` (86);
  - a deleted group with `NON_EMPTY_GROUP` (68).

  They were measured raw first: the JVM threw `UnknownMemberIdException`,
  `GroupSubscribedToTopicException` and its own `GroupNotEmptyException`; native threw `KafkaAdminException`
  with the three codes. Both arms now throw kafkakn's `GroupNotEmptyException`. On native, the alter and
  delete-offsets refusals arrive per partition, and the group refusal per group.
- **AC: each arm moves an empty group's offsets, and a member that joins afterwards reads from exactly
  there.** It read 7 on both arms. The broker's `--describe` prints `0:7` for each arm's group, and the
  distribution's console consumer, joining the moved group, starts partition 0 at 7 (`ci/b-60/run.sh`).
- **AC: altering an active group is refused on both arms with the same exception.** So are deleting its
  offsets and deleting the group, and the member's commit is untouched.
- **AC: a deleted group no longer appears in `kafka-consumer-groups --list`.** The runner checks this next
  to a control group, the moved one, which the same listing shows.
- **Deleting a group that does not exist is refused on both arms in different types.** The JVM throws
  `GroupIdNotFoundException`; native throws `KafkaAdminException` with `GROUP_ID_NOT_FOUND` (69). The
  arms are compared on "refused" only, and the contract records the difference without promising either.
- **A mutant found a hole in the test.** It left the Java client's `GroupNotEmptyException` unmapped, and it
  survived, because the test compared exception names and that Java class has the same simple name as
  kafkakn's. The test now checks the type with `is`. With that, all seven mutants are killed, each by a
  named test:
  - native: each of the three codes dropped from the mapping; a per-partition error ignored; the altered
    offsets replaced by 0;
  - JVM: `UnknownMemberIdException` or the Java `GroupNotEmptyException` left unmapped; the deleted
    partitions replaced by none.
