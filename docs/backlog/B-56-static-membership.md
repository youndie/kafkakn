---
id: B-56
title: "Static membership: a member that restarts keeps its partitions"
status: done
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

## Findings (2026-09-25)

- **It holds, so the key keeps travelling.** A static member closed and reopened within the session
  timeout gets partition `[0]` back as `[0]`. The other member's listener hears nothing in the window, on
  each arm in one process and in a mixed group both ways (JVM stayer and native restarter, and the reverse).
  The positive control, the same restart without `group.instance.id`, is a rebalance the other member hears
  on both arms.
- **Deviation from the AC: the generation comes from the broker's log, not from `kafka-consumer-groups
  --describe --members`.** For a classic group the tool prints `GROUP-EPOCH` and the member epoch as `-`,
  so it cannot show a generation at all. The broker logs every "Preparing to rebalance group G" and
  "Stabilized group G generation N". `ci/b-56/run.sh` cuts that log to the restart's window with
  `docker logs --since/--until`:
  - static groups: nothing in the window, on each arm and in the mixed group;
  - dynamic control: generations 3 and 4 in its window;
  - every group the runner read does appear in the log.
- **AC: two members with one `group.instance.id` fence each other the way the broker says, on both arms.**
  The newer member holds both partitions, and the older one's next `poll` throws. Measured raw first, the
  two arms parted:
  - the JVM threw `FencedInstanceIdException`, naming the reason;
  - native threw `KafkaConsumeException: poll: Local: Fatal error`, which named nothing. The reason sat
    behind `rd_kafka_fatal_error` as `FENCED_INSTANCE_ID`.

  Now both throw `ConsumerFencedException`, modelled on `ProducerFencedException`. It is deliberately not
  named after the Java class, the lesson of B-60's same-named exception. A fenced member's `close`
  completes without an error on both arms.
- **The trap the item named was met on the test's own side.** The first run reported `-[1]` heard during
  a static restart on both arms. That was the stayer's own revocation at its `close`, inside a watch window
  that never closed. Revocations at `close` happen for a static member too, though the group hears nothing.
  The window now shuts before the stayer stops. The dynamic control depended on the same fix to mean
  anything.
- **Mutants:** all four killed, each by name on its arm:
  - `group.instance.id` dropped from each arm's configuration: killed by the restart test and by the
    fencing test;
  - the fenced mapping removed on each arm: killed by the fencing test.
- **Found on the way, not a product defect:** Gradle binds `--tests` only to the task just before it, so a
  combined `jvmTest linuxX64Test --tests X` ran the whole JVM suite during development in B-59 and B-60. The
  runners call each task separately and were not affected.
