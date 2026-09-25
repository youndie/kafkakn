---
id: B-52
title: "Pause and resume partitions without leaving the group"
status: done
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

## Findings (2026-09-25)

- **The trap did not spring, on either arm.** Partition 0 was paused after its first batch, with most of
  its 2000 records still to come, and returned 0 records in 10 s of polling on both arms. The arms agree.
- **AC: a paused partition returns nothing while the others do, and the member stays past
  `max.poll.interval.ms`.** Partition 1 was written during the pause and returned its records. After 10 s
  against a 6 s interval, the member still held both partitions and a commit was accepted.
- **AC: after resume, nothing skipped or repeated.** 2000 read, 2000 distinct, and the broker holds 2000,
  on both arms (`ci/b-52/run.sh`).
- **Beyond the AC: a seek keeps a pause.** Native's `assign`-mode seek re-assigns every partition, and
  librdkafka starts an assignment unpaused. So native re-pauses what is still held, and
  `a_seek_does_not_undo_a_pause` tests it. The mutant that drops the re-pause is caught only by that test.
- **Mutants, each caught by name:**
  - native pause that does not pause;
  - native resume that does not resume;
  - JVM pause that does not pause;
  - native re-assign that does not re-pause.
