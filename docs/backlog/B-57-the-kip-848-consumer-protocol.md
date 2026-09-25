---
id: B-57
title: "The KIP-848 consumer protocol, on both arms and in a mixed group"
status: wip
priority: P3
size: L
stage: stage-12-group-protocols
blocked_by: [B-50, B-55]
---

# B-57 — the KIP-848 consumer protocol, on both arms and in a mixed group

Kafka's new group protocol (KIP-848) moves assignment to the broker and replaces the join and sync
barrier with heartbeats. Both clients underneath support it:
- the Java client has `GroupProtocol.CONSUMER` in 4.3.1;
- librdkafka's `CHANGELOG.md` says *"Starting with librdkafka 2.12.0, the next generation consumer group
  rebalance protocol defined in KIP-848 is production-ready"*, and `group.protocol` takes `classic` or
  `consumer`.

It is off by default on both. The consumer contract called it out of scope (§3, §5) until the owner
reversed that on 2026-09-25.

- **The decision and its reason.** `group.protocol=consumer` becomes a supported value on both arms,
  measured, with the contract's rebalance promises re-read under it. librdkafka's changelog warns of
  *"contract change associated with the new protocol"*, and those are what the listener, commit and
  seek items have to survive.
- Whether the test broker accepts the new protocol with its current configuration is a hypothesis,
  checked first; the fixture changes if it does not.
- Not covered: making it the default. The default is the clients' own, `classic`, until both of them
  change it.

- AC: a group of one member on each arm, both with `group.protocol=consumer`, forms and reads every
  record once. `kafka-consumer-groups --describe` reports the group as a consumer-protocol group.
- AC: the listener ([B-50](B-50-a-rebalance-listener.md)) and explicit commits
  ([B-48](B-48-commit-explicit-offsets.md)) keep their promises under it, or the contract says where
  they do not.
- Anchors: `docs/api/consumer-contract.md`, `ci/broker/`.
