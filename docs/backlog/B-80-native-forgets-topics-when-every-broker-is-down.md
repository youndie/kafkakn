---
id: B-80
title: "Native forgets the topics it described when every broker is down, as the Java client's rebootstrap does"
status: done
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

## Findings (2026-09-27)

- **Done as decided.** The topics the producer has described (B-76's cache) moved into `HandleContext`, which the
  error callback reaches through the handle's opaque. On `_ALL_BROKERS_DOWN`, when the effective
  `metadata.recovery.strategy` is `rebootstrap`, the callback empties it. librdkafka refuses any other value but
  `none` at construction.
- *Measured*, `ci/b-77/run.sh`, both arms, `docker stop`, `max.block.ms` 5 000. As they ship, both refused at 5 and
  20 s (native also at 0 s), each after 5 001–5 002 ms, and the topic held `warm back`. With `none`, both queued
  every record and the topic held `warm r-0 r-1 r-2 back`. In both passes the first `enqueue` after the broker was
  back was queued and landed. The arms agree on every observation. Red before the fix: native queued at 5 and
  20 s.
- **The moment of the stop is not asserted.** The JVM answered both ways at 0 s, refused in B-77's run and queued
  in this item's first run, so the test asserts from 5 s on and records 0 s.
- **Mutants.** Not clearing the cache is the red run above. Clearing it whatever the strategy was killed by
  `StoppedBrokerTest.a_record_enqueued_while_every_broker_is_down_is_answered_alike_on_both_arms` in the `none`
  pass.
- **Regressions**, green: `ci/b-76`, `ci/b-74`, `ci/b-09` (whole suite and accounting on both arms).
- The contract's configuration table was stale: it listed `max.block.ms` as a JVM key, which it stopped being in
  B-74. It and `metadata.recovery.strategy` are portable now.
