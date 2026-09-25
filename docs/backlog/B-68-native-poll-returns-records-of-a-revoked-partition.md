---
id: B-68
title: "The native poll can return records of a partition revoked or lost during that same poll"
status: question
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

## Iteration 1 (2026-09-25): not reproduced

- **Evidence for the defect: 1 stray in about 26 native freezes.** B-65 saw it once, in one of its first 5
  frozen rounds. Its offset and time were not recorded then; they are recorded now. Since then:
  - B-65's later rounds: 0;
  - `ci/b-68/run.sh`, 8 freezes at 20 records a second: 0;
  - 8 more freezes at the trickle's full rate (133 a second, about 2 000 records piling up per 15 s
    freeze): 0.

  The JVM had 0 in every run.
- **An ordinary eager rebalance does not reproduce it either:** `PollAfterRebalanceTest` gave 0 strays on
  both arms. The member there read its backlog first at full speed, then slowly with 300 ms between polls.
- **So the mechanism is still a reading of the code, not an observation.** `drain()` does collect across
  several `rd_kafka_consumer_poll` calls, and a callback can run in a later one. The zeros suggest the
  window is narrow: librdkafka appears to serve a rebalance ahead of records queued before it, so a stray
  needs the rebalance to arrive while a `poll` is part-way through collecting. That priority is inferred
  from the zeros, not read in librdkafka's source.
- **What exists, and is merged:**
  - `ci/b-68/run.sh`: repeated freezes, and each stray placed against the member's last listener event;
  - `PollAfterRebalanceTest`: the promise for an ordinary rebalance, green on both arms;
  - `SessionExpiryTest`: now records every stray's offset and time.

## Question: fix by reasoning, instrument first, or leave it?

The native arm diverged from the reference once, and the fix is cheap. But its acceptance cannot be
exercised as long as the defect cannot be produced on demand. A mutant reverting the fix would survive every
test here. The owner decides between:

1. **Fix by reasoning.** Drop the records of any partition that a callback run during that `drain` took away,
   as the Java client drops a revoked partition's fetched records. Test the filtering itself in `linuxX64Test`,
   and let the freeze runners guard the integration statistically. Cost S. Residual risk: it may not be the
   stray's cause.
2. **Instrument first.** Count, inside the native `drain`, each callback that takes away a partition whose
   records are already collected. Show the count only to the test runs, and run the freezes again. That
   observes the mechanism directly, or rules it out. Then choose 1 or 3. Cost S, plus a debug-only hook in
   the product code.
3. **Leave it.** Record in the contract that a native member resuming from a lost session was once handed a
   record of a partition it had just lost (1 in about 26), and keep the runner recording strays. Cost XS.

My recommendation is 2 then 1: it is the only path where the change is measured rather than believed.
