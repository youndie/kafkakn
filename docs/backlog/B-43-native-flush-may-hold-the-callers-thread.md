---
id: B-43
title: "The native flush calls a blocking rd_kafka_flush on the caller's thread"
status: done
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

## Findings (2026-09-25)

**Measured: it did hold the thread.** `NativeFlushSeamTest` sent five records to a port with no broker
(`message.timeout.ms=6000`) and called `flush` on a single-lane dispatcher, the mark taken before the
call. The dispatcher was held for 5.5 s, which is all of the wait. On `Dispatchers.IO` the silence
stays under the tolerated 500 ms (the green run's figures are in `logs/b-43/`).

**Moved, not dropped.** `rdkafka.c` shows why the call is kept: `rd_kafka_flush` sets `rk_flushing` for
its own duration, which makes the broker threads treat `linger.ms` as zero. The `rd_kafka_outq_len`
loop alone would make a caller with a long linger wait that linger on every `flush`.

**The fixture, found on the second try.** The strict topic was tried first. It fails on both arms: the
broker refuses those records at once, and the refusal drains the queue exactly as an acknowledgement
would. `JvmDispatcherSeamTest` had already written that down. A port with no broker keeps records
pending on native only: the Java client queues nothing without metadata. So the test is a platform-seam
test in `linuxX64Test`; the JVM arm's `flush` is on `Dispatchers.IO` by construction.

**§2.13 is corrected in writing**, at the sentence that was wrong.
