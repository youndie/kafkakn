---
id: B-59
title: "A consumer group's committed offsets and its lag, read by the admin client"
status: done
priority: P2
size: M
stage: stage-13-admin
blocked_by: [B-58]
---

# B-59 — a consumer group's committed offsets and its lag, read by the admin client

An operator's question is *how far behind is this group*, asked from outside the group. That needs the
group's committed offsets and each partition's end offset. Both clients have both:
`Admin.listConsumerGroupOffsets` and `listOffsets`, and `rd_kafka_ListConsumerGroupOffsets` and
`rd_kafka_ListOffsets`.

- **The decision and its reason.** `listConsumerGroupOffsets(group)` returning committed offsets, and
  `listOffsets(partitions, spec)` for earliest, latest or a timestamp. Lag is left to the caller as
  end minus committed: a derived number the library could get subtly wrong, and one line of code for
  the caller.
- `listOffsets` also answers "where does this partition start and end" for everyone else. On the
  consumer side that has been a seek away until now.

- AC: for a group that has committed, both arms' offsets agree with each other and with
  `kafka-consumer-groups --describe`.
- AC: `listOffsets` for earliest, latest and a timestamp agrees with `kafka-get-offsets` on both arms.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaAdmin.kt`.

## Findings (2026-09-25)

- **The shape.** `listConsumerGroupOffsets(groupId): Map<TopicPartition, Long>` and
  `listOffsets(partitions, spec): Map<TopicPartition, Long?>` with `OffsetSpec.Earliest`, `Latest` and
  `Timestamp(ms)`. A null offset means no record is as late as the timestamp: both clients answer -1 there,
  and `kafka-get-offsets.sh` prints nothing for that partition. The max-timestamp spec is left out, because
  it answers a different question. None of the four Java calls is deprecated in 4.3.1 (`javap -v` on the
  broker's own `kafka-clients-4.3.1.jar`).
- **Red first:** with both arms stubbed empty, both tests failed on the JVM on their assertions.
- **AC: committed offsets agree with each other and with `kafka-consumer-groups --describe`.** On each arm's
  own group, `0:4 1:3` from the admin is `0:4 1:3` in the tool's CURRENT-OFFSET column
  (`ci/b-59/run.sh`). A group that does not exist answers an empty map on both arms, compared across them.
- **AC: `listOffsets` agrees with `kafka-get-offsets` on both arms.** The runner checked earliest, latest
  and four timestamps: before every record, exactly on one, between two, and after all of them. It checked
  every partition, including an empty one, against the tool's `--time` on each arm's topic. Nine
  observations agree across the arms.
- **The fixture keeps its records for ever (`retention.ms=-1`).** Its timestamps are from 2001, a fixed
  answer the tool can be asked about. Under the default retention, a segment whose newest record is from
  2001 would be deleted at the first retention check.
- **Mutants (both arms):**
  - Killed, each by `a_partitions_start_end_and_offset_for_a_time_are_the_brokers` on its arm:
    - native: earliest asked as latest; the -1 kept as an offset; a timestamp asked as latest;
    - JVM: a timestamp asked as latest; the -1 kept.
  - **Two survived, and both are equivalent under this API.** They drop the "no commit" filter: the native
    negative-offset filter, and the JVM's null `OffsetAndMetadata` check. Asked for a whole group, the broker
    answers only the partitions that have a commit, so neither filter can be reached through a call that
    names no partitions. They stay as guards on each client's documented shape. The JVM one is also what
    the nullable Java type requires.
