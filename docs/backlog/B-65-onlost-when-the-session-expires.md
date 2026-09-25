---
id: B-65
title: "onLost when a member's session expires, measured on both arms"
status: open
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
