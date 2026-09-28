---
id: B-89
title: "kafkakn-soak for an hour on the KIP-848 consumer protocol"
status: done
priority: P3
size: M
stage: stage-20-what-waited-for-a-caller
epic: feature-exactly-once
blocked_by: []
---

# B-89 — kafkakn-soak for an hour on the KIP-848 consumer protocol

[B-70](B-70-kafkakn-soak.md) ran the exactly-once service for an hour under chaos, on the classic protocol. Its
findings say *"the KIP-848 protocol (a later run can switch it on)"*. The protocol is supported
([B-57](B-57-the-kip-848-consumer-protocol.md)), and never ran long or under kills and freezes.

- AC: `ci/b-70/run.sh` with `group.protocol=consumer` for an hour, on both arms. Input equals output, exactly once,
  checked the way B-70 checks it, and the native RSS shows no growth.
- AC: whatever differs from the classic run is recorded, and a defect becomes an item.
- Anchors: `kafkakn-soak/`, `ci/b-70/run.sh`.

## Findings (2026-09-28)

*Measured*, `GROUP_PROTOCOL=consumer FREEZE_FOR=60 DURATION=3600 ci/b-70/run.sh` on the Linux box, freshly rebooted.
`kafka-groups.sh` showed the group as type `Consumer`, protocol `consumer`, with two `rdkafka` members and two Java
members holding the six partitions. The freezes were 60 s, past the broker's 45 s KIP-848 session.

- **Exactly once:** 309 600 input records, 309 600 in the output, none missing, none twice, read by the Java client
  under `read_committed`. The group's commits reached 51 600/51 600 on every input partition.
- **Chaos:** 28 kills and 34 freezes across the four slots, and 12 exits by the instances themselves.
- **Memory:** native RSS stayed at 17–20 MB in each long-lived process (the longest, 15.2 min: 18.9 → 17.1 MB). The
  JVM's grew within its `-Xmx512m`, as on the classic run.
- **Why instances exited by themselves.** StaleGroupMetadataException, 7 times, on both arms: an instance woken from
  a freeze whose group had moved on, B-71's refusal, as on the classic run. Also a transactional call timing out
  after a 60 s freeze, which outlasts `transaction.timeout.ms`: on native, `sendOffsetsToTransaction` with
  `_TIMED_OUT … [retriable]` 3 times, and once `Local: Message timed out`; on the JVM, once `TimeoutException …
  EndTxn … within max.block.ms`. The crash-only service exits, is restarted, and exactly-once holds. The types
  differ between the arms, which the contract already records for timeouts ("each client's own type"); nothing new
  is promised.
- The soak takes the group protocol from `SOAK_GROUP_PROTOCOL`. Under `consumer` it sets no `session.timeout.ms`,
  because that is the broker's under KIP-848 and the Java client refuses it from a member.
