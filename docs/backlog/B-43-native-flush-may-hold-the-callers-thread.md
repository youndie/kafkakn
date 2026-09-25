---
id: B-43
title: "The native flush calls a blocking rd_kafka_flush on the caller's thread"
status: wip
priority: P2
size: S
stage: stage-5-producer-parity
---

# B-43 — native `flush` and the caller's thread

Found while doing [B-29](B-29-topic-metadata.md), by reading, not by measuring. [Research
§2.13](../research/research-architecture.md) says the native arm keeps the "suspends" promise "by
never blocking at all". Its `flush` begins with `rd_kafka_flush(handle, FLUSH_MS)`, `FLUSH_MS` being
30 000 — a call that blocks until the queue drains or the timeout passes, on whatever thread the
caller's dispatcher gave it. Only after that does it poll `rd_kafka_outq_len` with `delay`, which is
the suspending part.

B-29's own test showed how easily this hides: a silence measured from inside a ticker that cannot
start until the blocking call is over reads as no silence at all. `BackpressureTest`'s vacuity guard,
which §2.13 names as the native side of the claim, is about `send`, not `flush`.

- **The decision this item makes, not the one it assumes.** If `rd_kafka_flush` does hold the thread
  for records that cannot drain, either drop it — the `rd_kafka_outq_len` loop that follows already is
  the completion condition — or move it to `Dispatchers.IO` like `partitionsFor`. The first removes a
  blocking call; the second keeps whatever `rd_kafka_flush` does that polling does not, which is the
  question to read in librdkafka before choosing.
- AC: `flush` with records that cannot be delivered (no broker, a short `message.timeout.ms`) held
  against a single-lane dispatcher the way `TopicMetadataTest` holds `partitionsFor` — with the mark
  taken before the call — measured before and after.
- AC: §2.13's sentence is corrected or confirmed in writing.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/TopicMetadataTest.kt`.
