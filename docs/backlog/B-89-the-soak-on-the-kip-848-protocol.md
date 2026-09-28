---
id: B-89
title: "kafkakn-soak for an hour on the KIP-848 consumer protocol"
status: wip
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
