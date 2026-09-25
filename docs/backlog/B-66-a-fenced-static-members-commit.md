---
id: B-66
title: "What a fenced static member's commit does, on both arms"
status: wip
priority: P2
size: S
stage: stage-14-unmeasured-promises
---

# B-66 — what a fenced static member's commit does, on both arms

[B-56](B-56-static-membership.md) measured fencing for `poll` only. A second member with the same
`group.instance.id` fences the first, and the first's next `poll` throws `ConsumerFencedException` on both
arms. A service that commits offsets it has already processed calls `commit` as well, and nothing says what
that does on a fenced member:
- whether it throws, and with which type on each arm;
- whether the broker accepts the offsets anyway.

The contract says in so many words: *"Measured for `poll` only. A commit made by a fenced member is not."*

- **The decision and its reason.** Measure `commit()` and `commit(offsets)` on a fenced member, and read
  the group's offsets back with `kafka-consumer-groups --describe`. The refusal must be one type on both
  arms, as fencing on `poll` is. The broker must be seen to reject the fenced member's commit, not merely
  the client to report it.
- Not covered: fencing under the KIP-848 protocol, where static membership works differently.

- AC: on a fenced member, `commit` throws the same exception on both arms, and `kafka-consumer-groups
  --describe` shows the group's offsets untouched by it.
- AC: the contract's B-56 paragraph drops "measured for `poll` only" in favour of what was measured.
- Anchors: `docs/api/consumer-contract.md`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/StaticMembershipTest.kt`.
