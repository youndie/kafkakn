---
id: B-56
title: "Static membership: a member that restarts keeps its partitions"
status: open
priority: P3
size: M
stage: stage-12-group-protocols
---

# B-56 — static membership: a member that restarts keeps its partitions

`group.instance.id` travels today, and nothing tests it (consumer contract §3: *"static membership is
not in the first consumer"*). With it, a member that restarts within `session.timeout.ms` rejoins
under the same identity and gets its partitions back without a rebalance. That is what makes a rolling
restart of a consumer service cheap. Both clients document it: librdkafka's `CONFIGURATION.md` row for
`group.instance.id` says *"Static group members are able to leave and rejoin a group within the
configured `session.timeout.ms` without prompting a group rebalance"*.

- **The decision and its reason.** Measure it, since the key already passes through: restart a static
  member on each arm, and in a mixed group. If it holds, write it into the contract with its limits.
  If not, the key stops travelling until it does.
- The trap: `close` on a static member does not leave the group, by design, so a test that closes and
  reopens must expect no rebalance, not a clean leave.

- AC: a static member on each arm is closed and reopened within the session timeout. The group's
  generation, as `kafka-consumer-groups --describe --members` reports it, does not change, and the
  member holds the same partitions.
- AC: two members with the same `group.instance.id` fence each other the way the broker says, on both
  arms.
- Anchors: `docs/api/consumer-contract.md`, `ci/b-37/run.sh`.
