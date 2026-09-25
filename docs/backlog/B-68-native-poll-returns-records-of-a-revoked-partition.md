---
id: B-68
title: "The native poll can return records of a partition revoked or lost during that same poll"
status: open
priority: P1
size: S
stage: stage-14-unmeasured-promises
blocked_by: [B-65]
---

# B-68 — the native poll can return records of a partition revoked or lost during that same poll

Found by [B-65](B-65-onlost-when-the-session-expires.md). Its member counts a record as a *stray* when it
arrives for a partition the member's own listener says it does not hold. In 5 rounds where the native member
was frozen past its session, there was 1 stray. In 5 rounds with the JVM member frozen, there were 0.

The likely mechanism, read in the code, not yet observed step by step: the native `poll`'s `drain()` calls
`rd_kafka_consumer_poll(handle, 0)` in a loop, collecting records into one list. A rebalance callback runs
**inside** one of those calls. So records of a partition collected earlier in the same `poll` are returned
to the caller **after** `onRevoked` or `onLost` has run for that partition. The Java client runs its
callbacks before it fetches, and returns only records of partitions the member still holds.

By the project's rule the native arm is wrong here, since it disagrees with the reference. The cost is
duplicates, not loss, and only for a caller who commits on revocation, which is what B-50 teaches:
- the listener commits what was processed;
- the returned records are processed after that commit;
- the next owner starts from the commit and processes them again.

- **The decision and its reason.** After the drain, drop the records of any partition that a callback run
  during that drain took away, as the Java client drops the fetched records of a revoked partition.
  Also forget their positions. Confirm the mechanism first: record which record was the stray and when,
  against the callback's time.
- The rejected alternative is stopping the drain at the first callback, returning what was collected
  before it. The records would still reach the caller after the listener ran, which is the defect itself.

- AC: the mechanism is observed, not only read. A stray's offset and time are placed against the `onLost`
  or `onRevoked` that took its partition.
- AC: `ci/b-65/run.sh` asserts zero strays on both arms, over repeated runs.
- AC: the consumer contract's §2 says, for both arms, that `poll` returns only records of partitions held
  after that poll's callbacks.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaConsumer.native.kt`,
  `ci/b-65/run.sh`.
