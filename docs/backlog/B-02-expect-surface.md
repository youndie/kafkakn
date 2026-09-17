---
id: B-02
title: "The expect surface, compiling and throwing"
status: wip
priority: P0
size: S
stage: stage-0-it-builds
blocked_by: [B-01]
---

# B-02 — The expect surface, compiling and throwing

The interface from [producer-contract](../api/producer-contract.md), with both actuals present and
both throwing `NotImplementedError`. This is what makes the first test red for the reason it is
supposed to be red rather than red because nothing compiles.

- **The decision and its reason.** Write the surface from the contract document **before** either
  implementation, so neither platform's shape leaks into it. `send` suspends
  ([D3](../research/research-architecture.md)); values are bytes, not `String`.
- The rejected alternative is deriving the interface from whichever actual gets written first —
  which is how an `expect` ends up mirroring `kafka-clients`' `Future` or librdkafka's callback.
- Not covered: headers, `sendAll`, transactions, partitioner overrides. Absent until an item asks.

- AC: `KafkaProducer`, `ProducerRecord` and `RecordMetadata` exist in `commonMain` and compile on
  both targets.
- AC: a `commonTest` test calls `send` and asserts it throws `NotImplementedError` — on both
  targets. The suite is then known to reach both actuals.
- AC: no type in the surface names a platform, and `commonMain` imports nothing from either client.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/SurfaceTest.kt`.
