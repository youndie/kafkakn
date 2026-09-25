---
id: B-53
title: "Consumer metrics, lag first"
status: done
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

## Findings (2026-09-25)

- **The two clients' "lag" were two different numbers, read in librdkafka's own `STATISTICS.md`.**
  `consumer_lag` is measured from the committed offset, and `consumer_lag_stored` from the stored offset,
  which is the position. The Java client's `currentLag` is measured from the position. So the native arm
  reads `consumer_lag_stored`. A reading taken *before* any commit tells the two apart, and the test has
  one: the mutant that reads `consumer_lag` fails it.
- **No tolerance was needed, because the lag was frozen before it was read.** The partition was paused
  (B-52) at position 500, and the member kept polling past two statistics intervals.
- **AC: each arm's lag against `kafka-consumer-groups --describe`.** 1500 on both arms, before the commit
  and after, and the broker's LAG 1500. The tolerance the item expected is stated in the contract as what
  it is: the arms agree on a lag that is not moving, and sample at different moments while it moves.
- **AC: a consumer that read everything reads 0**, on both arms.
- **AC: `no_delivery_counters.py` passes, and its self-test still flags the counter.**
- **Native plumbing:** the consumer turns statistics on once a second. Its statistics callback keeps the
  latest document in the rebalance bridge, because that is the consumer's opaque since B-50.
- **Mutants, each caught by name:**
  - native reading `consumer_lag`;
  - native without the statistics callback;
  - JVM lag off by one.
- **Found in my own runner:** a note in it claimed the broker's LAG would read 0 after the member resumed.
  It read 1500, because the member never moves its commit after resuming. The runner now compares the
  broker's LAG with the members' figure directly.
