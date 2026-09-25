---
id: B-51
title: "Seek under a subscription, within the partitions the group gave"
status: done
priority: P2
size: M
stage: stage-11-everyday-gaps
blocked_by: [B-50]
---

# B-51 — seek under a subscription, within the partitions the group gave

`seek` is refused under a subscription on both arms (`KafkaConsumer.jvm.kt`: *"seek is refused under a
subscription, on both arms: the group decides positions"*). The reason was the native arm: it seeks by
re-assigning, and a subscription does not allow that. The Java client seeks any assigned partition at
any time. Replaying from a timestamp, or skipping a poison record, are ordinary things to do in a
group.

- **The decision and its reason.** Allow `seek` for a partition the group currently assigns to this
  member, and refuse it for any other. On native, `rd_kafka_seek_partitions` on the assigned
  partitions, which librdkafka accepts once they are being fetched. Whether a seek right after
  assignment is refused there, as `assign` was, is the first thing to measure (consumer contract §2).
- Needs the listener ([B-50](B-50-a-rebalance-listener.md)): a seek on assignment is the common use.

- AC: in a group, each arm seeks a held partition to an offset and reads from exactly there, as the
  broker's offsets say.
- AC: a seek of a partition this member does not hold is refused on both arms.
- AC: a seek made from the listener on assignment takes effect before the first record of that
  partition is returned.
- Anchors: `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.jvm.kt`,
  `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.native.kt`.

## Findings (2026-09-25)

- **The third AC needed §2a extended, within its own rule.** A listener cannot call the consumer, so
  `RebalanceScope` gained `seek`, and `onAssigned` gained a form that is given the scope. That form
  defaults to the one-argument one, so existing listeners compile unchanged. `seek` from `onRevoked` is
  refused on both arms.
- **The first thing the item said to measure decided the native design.** librdkafka refuses a seek right
  after an assignment (B-36's *"Erroneous state"*). So native runs `onAssigned` before `rd_kafka_assign`,
  and writes the seek into the assigned list as the starting offset. Outside a callback, a seek in a group
  is `rd_kafka_seek_partitions`, and `position` answers it until a record is read.
- **AC, measured on both arms (`ci/b-51/run.sh`, arms agreeing):**
  - a held partition sought to 7 reads position 7, and its next record is 7;
  - a seek of a partition not held is refused with `IllegalStateException`;
  - a seek from `onAssigned` to 12 makes 12 the first record returned.
- **B-37's refusal test is renamed**, to `a_seek_before_the_group_has_assigned_anything_is_refused_on_both_arms`,
  because that is what it checks and what stays refused.
- **Mutants, each caught by its own test:**
  - native ignoring the seek on assignment;
  - native recording a group seek without doing it;
  - a JVM scope whose seek does nothing;
  - native without the held check.
