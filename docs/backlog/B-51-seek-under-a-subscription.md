---
id: B-51
title: "Seek under a subscription, within the partitions the group gave"
status: open
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
