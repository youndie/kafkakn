---
id: B-80
title: "Native forgets the topics it described when every broker is down, as the Java client's rebootstrap does"
status: wip
priority: P1
size: M
stage: stage-18-what-the-consumer-and-the-harness-found
epic: feature-backpressure-and-accounting
blocked_by: [B-77]
---

# B-80 — native forgets the topics it described when every broker is down, as the Java client's rebootstrap does

[B-77](B-77-a-stopped-broker-and-a-known-topic.md) measured it. With every broker unreachable and a topic
already written to, the Java client refuses the record at `max.block.ms`. Its default `metadata.recovery.strategy`,
`rebootstrap`, replaces the metadata with the bootstrap addresses, so the topic is unknown again. Native queues
the record. The owner chose option 1 on 2026-09-27: native follows the oracle.

- **The decision and its reason.** When librdkafka reports every broker down (`_ALL_BROKERS_DOWN` on the error
  callback), and the effective `metadata.recovery.strategy` is `rebootstrap`, the producer forgets the topics it
  has described (B-76's cache). The next `enqueue` to one of them waits for metadata, and refuses at
  `max.block.ms` if none arrives, as the JVM's does. `metadata.recovery.strategy=none` keeps the topics on native,
  as it keeps the metadata on the JVM. So it is one key with one meaning on both arms, and the caller who prefers
  "queued" sets it.
- AC: `ci/b-77/run.sh`, both arms, as they ship: every `enqueue` while the broker is stopped throws
  `RecordNotQueuedException`, and none of those records is in the topic afterwards.
- AC: with `metadata.recovery.strategy=none`, both arms queue every record, and each lands once the broker is back.
- AC: after the broker is back, a topic forgotten this way is described again and the next `enqueue` is queued.
  The accounting run (`ci/b-09/run.sh`) and B-74's and B-76's runners stay green.
- AC: the contract names `metadata.recovery.strategy` as a key both arms honour, with the measured behaviour.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`,
  `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/Statistics.native.kt`, `docs/api/producer-contract.md`.
