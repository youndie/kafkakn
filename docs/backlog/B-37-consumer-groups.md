---
id: B-37
title: "Consumer groups: subscribe, rebalance, commit — and a group with one consumer from each arm"
status: done
priority: P2
size: XL
stage: stage-8-consume
blocked_by: [B-36]
---

# B-37 — consumer groups

The part D2 warned about. Subscribing to topics, taking part in a rebalance, and committing offsets —
`subscribe` and `commitSync` on the JVM, `rd_kafka_subscribe`, `rd_kafka_incremental_assign` and
`rd_kafka_commit` on native ([research §1.8](../research/research-architecture.md)).

- **The decision and its reason.** At-least-once is the promise: a record may be delivered twice
  across a rebalance, and it is never skipped. The test for that is loss, not duplication — the
  inverse of the producer's accounting, and just as easy to get backwards.
- **The differential test here is unusual and it is the point.** One group with a JVM consumer and a
  native consumer in it. Both speak the same group protocol to the same coordinator, and their
  default assignment strategies differ (§1.8) — so the group either converges on one both support or
  fails to form, and either answer is worth more than any single-arm test.
- Committed offsets are read by `kafka-consumer-groups.sh --describe`, never by the consumer that
  committed them.
- The rejected alternative is auto-commit only. It makes "processed" and "committed" different events
  the caller cannot order, which is the at-least-once promise with a hole in it.
- Not covered: exactly-once ([B-38](B-38-exactly-once-read-process-write.md)), the consumer group
  protocol `consumer` (KIP-848) — `classic` is the default on both arms.

- AC: two consumers in one group split the partitions; stopping one hands its partitions to the other
  and **no record is skipped**, counted against what `kafka-console-producer` wrote.
- AC: a mixed group — one consumer per arm — forms, splits, rebalances and loses nothing.
- AC: manual commit, read back by `kafka-consumer-groups.sh`, matches what the caller committed on
  both arms.
- Anchors: `ci/harness/broker.sh`, `docs/research/research-architecture.md`.

## Findings (2026-09-25)

**Measured, `ci/b-37/run.sh`.** The records were written by a third party while the group formed.
- **A group of two on each arm:** it split four partitions ([0,1] and [2,3]). The leaving member left
  the way a crash would, reading one batch and never committing it, and the staying member ended with
  all four. 800 of 800 records were seen, the abandoned record was delivered again, and commits reached
  the log end on every partition (`kafka-consumer-groups.sh --describe`).
- **A mixed group, one member per arm, two processes at once:** it formed and split ([0,1] JVM,
  [2,3] native). When the native member left without committing, the JVM member was given its batch
  and all four partitions. 600 of 600, commits at the log end.

**The loss check was blind at first.** The abandoned batch was counted as seen by the member that
dropped it, so the union covered it with or without redelivery. It now has to come back. With the
native `close()` made to commit first, one record was lost in the native group and one in the mixed
one, and the run went red (`logs/b-37/`). The run script's summary lines are now printed only when
true: in the mutant's log they had claimed success under the failures.

**The fixture's stop rule, wrong twice before it was right.** The staying member first stayed a
fixed time: the run passed once, then "lost" 465 records when Gradle started faster and the member left
before the writer finished. Next it stayed until five quiet seconds: a member that joined before the
writer started left at once, and 565 were "lost". Both losses were the harness's, not the consumer's.
It now stays until it holds every partition and the last record of each has been seen. A third
timing trap was the writer finishing before the native members joined, while that task compiled and
linked. The run now builds first and starts the native members from their test binary.

**Decisions on the way**, in [consumer-contract](../api/consumer-contract.md) §2:
- `subscribe` and `commit` need a `group.id` the caller named, on both arms.
- No rebalance callback is installed.
- A seek under a subscription is refused on both arms for now.

**Not covered, as the item says.** Exactly-once is B-38, and the `consumer` group protocol (KIP-848)
stays out.
