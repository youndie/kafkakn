---
id: B-65
title: "onLost when a member's session expires, measured on both arms"
status: done
priority: P2
size: S
stage: stage-14-unmeasured-promises
---

# B-65 — `onLost` when a member's session expires, measured on both arms

The consumer contract says two things about `onLost`, and they disagree. §2 records it as measured: a
collector slower than `max.poll.interval.ms` is evicted, and both arms report `+[0] ![0] +[0] -[0]`
([B-64](B-64-native-poll-throws-where-the-jvm-rejoins.md)). §2a still says *"Not measured: `onLost`. It
needs a member removed for missing `session.timeout.ms`, which no run here arranges yet."* The first road
is measured. The second, a member whose heartbeats stop while it is not polling either, has never been
run.

- **The decision and its reason.** Freeze a member's process with `SIGSTOP` for longer than
  `session.timeout.ms`, then `SIGCONT` it, on each arm and with the other arm as the member that takes
  over. A frozen process is what a GC pause, a stopped container or a suspended VM looks like to the
  broker. It stops the heartbeat thread too, which a slow collector does not. The listener's promise is
  the same on both roads, and only one of them has been checked.
- The rejected alternative is a network cut (iptables, `docker network disconnect`). It changes the broker
  side as well, needs privileges on the build box, and does not isolate the member.
- Not covered: `onLost` under the KIP-848 protocol ([B-57](B-57-the-kip-848-consumer-protocol.md)), unless
  the same run can ask for it at no extra cost.

- AC: a member frozen past `session.timeout.ms` hears `onLost` for what it held when it resumes, and then
  `onAssigned`. The other member takes the partitions over while it is frozen. This holds on both arms and
  is compared across them.
- AC: nothing is lost across the freeze, against the broker's end offsets. What is processed twice is
  counted and recorded: a lost member cannot commit, and the contract says so.
- AC: §2a's "Not measured" sentence is replaced by what was measured, and credits B-64 for the eviction
  road.
- Anchors: `docs/api/consumer-contract.md`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/RebalanceListenerTest.kt`.

## Findings (2026-09-25)

- **AC: a member frozen past `session.timeout.ms` hears `onLost` when it resumes, and then `onAssigned`,
  and the other member takes over meanwhile.** Both arms heard `! +` after the freeze, compared across
  them. The broker's log names the removal *"on heartbeat expiration"* inside the freeze window, which
  shows this is the session road and not B-64's eviction. The taker was assigned the frozen member's
  partitions during the freeze.
- **It comes back from exactly the group's commit**, at the hand-back: `2:186/186 3:186/186` and
  `0:189/189 1:189/189`. My first reading of an earlier run said the JVM member resumed from its own old
  position. That was inferred from gaps in a list, and the direct measurement refuted it.
- **AC: nothing lost.** 0 of 1 200 lost in every round, and the group's commits reach every end.
  Processed twice: 34 to 365 a round, all of it work the lost member had processed and could not commit.
  The count depends on how long it held its partitions uncommitted, not on the arm.
- **AC: §2a's "Not measured" sentence** is replaced by what was measured, crediting B-64 for the eviction
  road.
- **Found: a native divergence, now [B-68](B-68-native-poll-returns-records-of-a-revoked-partition.md).**
  One native member, in one of 5 frozen rounds, was handed a record of a partition its listener had just
  given up. The JVM had none in 5 rounds. The likely mechanism is read in the code: `drain()` collects
  records across several `rd_kafka_consumer_poll` calls, and a callback can run in a later one. The runner
  records native strays, with offset and time, and asserts them only on the JVM until B-68.
- **Fixed on the way, in the test and the runner, not the product:**
  - The member kept a revoked partition's processed offset, and committed it again on a later revocation.
    Once that overwrote the other member's 300 with a stale 187. It is now dropped on revocation.
    `CooperativeTest`'s member (B-55) has the same bookkeeping and was not changed here. It can only bite
    when a member is given a partition back and reads nothing before giving it up again.
  - The runner's windows had two problems:
    - an event made on `SIGCONT` can carry a time a millisecond before the runner's own "resumed";
    - the JVM member once took 3 s to rejoin, past a fixed 2 s cutoff.

    Both are now cut at the freeze and at the hand-back itself.
- **Mutants:** both killed, each by name:
  - native `rd_kafka_assignment_lost` read as false: the native member failed with *"commit: Broker:
    Unknown member"*;
  - the JVM's `onPartitionsLost` override removed: the JVM member failed with `CommitFailedException`.

  In both, the lost partitions arrived as revoked, and a commit from a member no longer in the group failed.
