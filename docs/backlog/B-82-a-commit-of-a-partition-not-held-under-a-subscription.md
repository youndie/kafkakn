---
id: B-82
title: "A commit of a partition the member does not hold, under a subscription"
status: done
priority: P2
size: M
stage: stage-20-what-waited-for-a-caller
epic: feature-consume-records
blocked_by: []
---

# B-82 — a commit of a partition the member does not hold, under a subscription

The consumer contract (§2a) says a commit of a partition the consumer does not hold is accepted by both arms, and
the broker stores it. That was measured with `assign`, where the group has no generation to check against. Under a
subscription the broker may judge such a commit against the member's generation. That was left for after the
rebalance listener ([B-50](B-50-a-rebalance-listener.md)), which is done, and it was never measured.

- **The decision and its reason.** Measure first, on both arms, against the broker's own `kafka-consumer-groups`
  view of the committed offsets. If the arms agree, the contract states the measured behaviour. If they differ, the
  item ends as a `question`.
- AC: a member of a two-member group commits an offset for a partition the other member holds. For each arm, what
  `commit` answers and what the broker stores are recorded, and the group's own member reads on from the right
  offset.
- AC: the contract's sentence *"That is not measured yet"* is replaced by the measurement, or by the question.
- Anchors: `docs/api/consumer-contract.md`, `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/`.

## Findings (2026-09-28)

*Measured*, `ci/b-82/run.sh` (`ForeignCommitTest`) on the Linux box. A fresh topic of two partitions with 20 records
each, and two members of one group holding one partition each. B read its partition to the end and committed
nothing. A committed offset 5 for B's partition.

| | classic protocol | KIP-848 (`group.protocol=consumer`) |
|---|---|---|
| A's `commit`, JVM and native | returned | returned |
| stored, by `kafka-consumer-groups.sh` | 5, on both arms | 5, on both arms |
| a new member resumed that partition from | 5, on both arms | 5, on both arms |

The arms agree on all four observations. The broker judges a commit by the member and its generation or epoch,
not by the partitions it holds. So a commit from the wrong member is not refused; it overwrites. The consumer
contract's *"That is not measured yet"* is replaced by this, with the rule that follows for a caller: commit only
what you hold. kafkakn adds no check of its own. Neither client has one, and a check against the assignment would
race with a rebalance.
