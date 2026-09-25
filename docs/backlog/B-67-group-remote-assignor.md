---
id: B-67
title: "group.remote.assignor under the KIP-848 protocol, measured"
status: wip
priority: P3
size: S
stage: stage-14-unmeasured-promises
blocked_by: [B-57]
---

# B-67 — `group.remote.assignor` under the KIP-848 protocol, measured

[B-57](B-57-the-kip-848-consumer-protocol.md) refuses `partition.assignment.strategy` under
`group.protocol=consumer`, and its message tells the caller to *"name the assignor with
group.remote.assignor"*. That key travels to both clients unchanged, and no run has ever set it. The
library points the caller at a key it has not measured.

- **The decision and its reason.** Set `group.remote.assignor` on a member of each arm, and read the
  assignor the group settled on from the broker's tool. The fixture broker (`apache/kafka:4.3.1`) settled
  on `uniform` by default in B-57. Measure a name the broker knows and one it does not, on both arms.
- Not covered: writing a server-side assignor, or changing the broker's `group.consumer.assignors`.

- AC: with a named assignor the broker knows, `kafka-consumer-groups --describe --state` shows it as the
  group's ASSIGNMENT-STRATEGY, on both arms.
- AC: a name the broker does not know is refused the same way on both arms: the same type, or recorded
  in the contract where it cannot be.
- Anchors: `docs/api/consumer-contract.md`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/ConsumerProtocolTest.kt`.
