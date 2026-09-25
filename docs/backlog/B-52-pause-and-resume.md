---
id: B-52
title: "Pause and resume partitions without leaving the group"
status: open
priority: P2
size: M
stage: stage-11-everyday-gaps
---

# B-52 — pause and resume partitions without leaving the group

A consumer whose downstream is slow for one partition can only stop polling, and a consumer that stops
polling for `max.poll.interval.ms` is removed from its group. Both clients pause per partition and
keep the member alive: `Consumer.pause`/`resume`, and `rd_kafka_pause_partitions`/`resume_partitions`.
The consumer contract lists it under what the first consumer will not do (§5).

- **The decision and its reason.** `pause(partitions)`, `resume(partitions)`, `paused()`. It is the
  consumer's backpressure: the producer's backpressure (D3) is a suspending `send`, and the consumer's
  is to keep polling while fetching nothing for a partition.
- The trap: librdkafka may already hold fetched records for a partition when it is paused. Whether they
  are still returned is exactly where the arms could differ, and the test asks.

- AC: a partition paused on each arm returns no records while the others do, and the member stays in
  its group past `max.poll.interval.ms` while it keeps polling.
- AC: after `resume`, the partition continues from its position, and no record is skipped or repeated,
  counted against the broker.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.kt`,
  `docs/api/consumer-contract.md`.
