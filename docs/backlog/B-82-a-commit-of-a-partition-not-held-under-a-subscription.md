---
id: B-82
title: "A commit of a partition the member does not hold, under a subscription"
status: open
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
