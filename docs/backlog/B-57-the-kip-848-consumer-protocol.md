---
id: B-57
title: "The KIP-848 consumer protocol, on both arms and in a mixed group"
status: done
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

## Findings (2026-09-25)

- **The hypothesis held: the fixture needs no change.** `apache/kafka:4.3.1` accepts `group.protocol=consumer`
  as configured. The distribution's console consumer formed such a group first, and `kafka-groups.sh --list`
  named it `Consumer consumer`, with the broker's `uniform` assignor. The anchor `ci/broker/` is unchanged.
- **AC: a group of one member on each arm under the new protocol forms and reads every record once.** This
  is `ci/b-57/run.sh`, B-55's mixed run with the members switched: a JVM member, a native member and a
  native third joining 30 s later. Across 1 200 records, 0 were lost and 0 processed twice. The commits
  reach every end, and nobody gave up everything mid-stream. The broker's tool lists the group as
  `Consumer consumer`, which rules out a quiet fallback to `classic`.
- **AC: the listener and explicit commits keep their promises.** On each arm alone, and compared across
  the arms:
  - `+[0] -[0]`;
  - every record once;
  - an explicit commit of 12 reads back;
  - the commit made in `onRevoked` at `close` (17) is what the broker holds afterwards.

  In the mixed group, members commit only on revocation, and no record is repeated. One difference for a
  listener is recorded in the contract: assignments arrive in pieces (`+[2, 5]`, then `+[0, 1, 3, 4]`).
- **Found: the classic keys were refused by both clients in different types.** The Java client threw
  `ConfigException`; librdkafka's `rd_kafka_new` failed, which native reported as `IllegalStateException`.
  The keys are `partition.assignment.strategy`, `session.timeout.ms` and `heartbeat.interval.ms`. They are
  now refused in common code, first, with `IllegalArgumentException` on both arms, as B-55 refuses a
  strategy only one arm understands.
- **Native works because of B-55's switch.** librdkafka reports the new protocol as `COOPERATIVE`, and the
  rebalance callback's incremental assign serves it.
- **Mutants:** all four killed, each by name:
  - the check skipped: killed by the keys test (JVM);
  - the check's call removed on native, and on the JVM: killed by the keys test on each arm;
  - native's incremental switch forced off: killed by the lone-member test.
