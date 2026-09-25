---
id: B-66
title: "What a fenced static member's commit does, on both arms"
status: done
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

## Findings (2026-09-25)

- **Measured raw first, the arms parted, as they had on `poll` before B-56.** Both refused both commits,
  and neither commit reached the broker. But:
  - the JVM threw `FencedInstanceIdException` twice;
  - native threw `KafkaConsumeException`: *"Broker: Static consumer fenced by other consumer with same
    group.instance.id"* the first time, and a bare *"Local: Fatal error"* the second. Once librdkafka's
    consumer is fenced, it is in a fatal state, and only `rd_kafka_fatal_error` still names the reason.
- **AC: on a fenced member, `commit` throws the same exception on both arms, and the group's offsets are
  untouched.** Both forms, `commit(offsets)` and `commit()`, now throw `ConsumerFencedException` on both
  arms. Native recognises both roads: `FENCED_INSTANCE_ID` itself, and `_FATAL` whose reason is fencing.
  The group's offsets read `0:6`, the new member's, through the admin client and through
  `kafka-consumer-groups.sh --describe` (`ci/b-66/run.sh`). The fenced member's attempted 9 is nowhere.
- **AC: the contract's B-56 paragraph** now describes the commits instead of saying "measured for `poll`
  only".
- **In the runner:** the tool lists the closed static member's partition 1 with `-`. A static member does
  not leave on `close`, B-56's trap once more. The runner reads only committed partitions.
- **Mutants:** all three killed, each by `a_fenced_members_commit_is_refused_and_changes_nothing` on its arm:
  - native mapping off;
  - native mapping without the `_FATAL` road;
  - JVM mapping off.
