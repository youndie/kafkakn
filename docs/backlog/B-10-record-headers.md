---
id: B-10
title: "Record headers without rd_kafka_producev"
status: open
priority: P2
size: M
stage: stage-2-real-use
blocked_by: [B-07]
---

# B-10 — Record headers without `rd_kafka_producev`

Headers are how tracing and schema identifiers travel, so a producer without them is not usable in
most deployments. The natural vehicle on the native side is `rd_kafka_producev`, which is variadic
and therefore unusable through cinterop ([research §1.5](../research/research-architecture.md)).

- **The decision and its reason.** Settle **H2**: find the mechanism librdkafka offers that is not
  variadic — `rd_kafka_headers_new` plus a produce path that accepts them — and check the arms agree
  on what the broker stores.
- The rejected alternative is a small C shim compiled into the cinterop bundle that wraps `producev`.
  It works and it adds C of our own to a project whose whole argument is that it writes none; kept
  as a fallback if the non-variadic path does not exist.
- Not covered: header-based partitioning and any interceptor mechanism.

- AC: **H2 settled in writing** in [research §3](../research/research-architecture.md), either way.
- AC: a record with headers round-trips, and an independent reader sees the same header bytes.
- AC: both arms agree on header ordering and on what a duplicate key does.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerRecord.kt`,
  `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`.
