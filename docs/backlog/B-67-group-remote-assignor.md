---
id: B-67
title: "group.remote.assignor under the KIP-848 protocol, measured"
status: done
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

## Findings (2026-09-25)

- **AC: with a named assignor the broker knows, the broker's tool shows it as the group's
  ASSIGNMENT-STRATEGY, on both arms.** `ci/b-67/run.sh` names each arm's group, holds its member for 15 s,
  and reads `kafka-consumer-groups.sh --describe --state` meanwhile: `range (Stable, 1 member)` on both
  arms. The admin client's description agrees, and so do the arms.
- **Found in the reading, not the product: an empty group shows the broker's default.** The first run read
  the tool after the member had left and saw `uniform`, which made the run red. B-57 had also seen `uniform`
  on its empty group. The contract now says to read a live group.
- **AC: a name the broker does not know is refused the same way on both arms.** Measured raw first:
  - the JVM threw `UnsupportedAssignorException` (*"Supported assignors: uniform, range"*);
  - native threw `KafkaConsumeException` from a fatal `UNSUPPORTED_ASSIGNOR` (112).

  Both now throw `IllegalArgumentException` at the first `poll`: the caller's argument, refused by the
  broker, as B-61 treats a refused configuration.
- **Mutants:** all four killed, each by name:
  - each arm's mapping switched off: killed by the refusal test;
  - `group.remote.assignor` dropped before it reaches each client: native was killed by both tests; the
    JVM by the refusal test, which then read `done`, and by the named test.
