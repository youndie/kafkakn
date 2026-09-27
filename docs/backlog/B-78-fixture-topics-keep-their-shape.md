---
id: B-78
title: "A fixture topic in the wrong shape stops the harness, instead of failing the suite for the wrong reason"
status: done
priority: P1
size: S
stage: stage-18-what-the-consumer-and-the-harness-found
blocked_by: []
---

# B-78 — a fixture topic in the wrong shape stops the harness

On 2026-09-27 the test broker's `kafkakn` topic had one partition. The suite assumes three, the ones
`broker.sh topic` creates. Nobody knows what created it. `broker.sh` creates every fixture topic with
`--create --if-not-exists`, which keeps whatever is there. So the first sign was `ExplicitPartitionTest`
failing on both arms in a whole-suite run, with "Unknown partition" on native and a timeout on the JVM. That is a
fixture defect that reads as a library one.

- **Done.** `ensure_topic <name> <partitions> [--config k=v ...]` in `ci/harness/broker.sh` creates the topic if it
  is absent. If it exists with another partition count, or without a `--config` it names, it refuses with the
  command that removes it. It does not recreate the topic itself, because a topic of that name may hold what
  someone else is reading. `up` checks every fixture topic with it, including the suite's two defaults, `kafkakn`
  and `kafkakn-strict`. `up` is where the check cannot be skipped: runners call `topic` and `strict-topic` behind
  `| head -1`, which throws away their exit status. Both now exit non-zero on a mismatch as well.
- AC: `up` on the fixture as it is: exit 0.
- AC: `kafkakn` recreated with one partition, the state found on 2026-09-27. `up` exits 1 with
  *"FIXTURE TOPIC kafkakn IS NOT THE ONE THE SUITE ASSUMES: 1 partitions, not 3"* and the removal command. After
  that command, `up` exits 0 and `kafkakn` has three partitions.
- AC: a scratch topic of one partition, asked for through `topic` (three): exit 1. With `PARTITIONS=1`: exit 0.
  Through `strict-topic`: exit 1 with *"no min.insync.replicas=2"*.
- Measured on the Linux box, 2026-09-27: all three, as stated.
- Anchors: `ci/harness/broker.sh`.
