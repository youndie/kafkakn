---
id: B-53
title: "Consumer metrics, lag first"
status: wip
priority: P2
size: M
stage: stage-11-everyday-gaps
---

# B-53 — consumer metrics, lag first

[B-41](B-41-metrics-an-operator-can-read.md) gave the producer four measures of machinery. The consumer
has none, and the first number an operator asks of a consumer is lag. Both clients report it:
`Consumer.currentLag(tp)` and the `records-lag` metrics on the JVM, and `consumer_lag` per partition in
librdkafka's statistics JSON, which the native arm already parses for the producer.

- **The decision and its reason.** Lag per held partition, plus what B-41's rule allows: machinery,
  not success counts. Lag is a distance to the end of the log, not a count of anything handled, so it
  passes `scripts/no_delivery_counters.py`. The gate runs anyway.
- The two arms measure at different moments: librdkafka once per statistics interval, the Java client
  on each fetch. The tolerance is stated in the contract, as B-41 did for the round trip.

- AC: under one load, each arm's lag per partition is within the stated tolerance of what
  `kafka-consumer-groups --describe` computes from the broker's offsets.
- AC: a consumer that has read everything reads lag 0 on both arms.
- AC: `no_delivery_counters.py` passes, and its self-test still fails.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/Statistics.native.kt`,
  `scripts/no_delivery_counters.py`, `docs/api/consumer-contract.md`.
