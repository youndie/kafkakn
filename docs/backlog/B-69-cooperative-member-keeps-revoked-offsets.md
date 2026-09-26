---
id: B-69
title: "CooperativeTest's member commits a revoked partition's stale offset"
status: done
priority: P2
size: XS
stage: stage-14-unmeasured-promises
---

# B-69 — `CooperativeTest`'s member commits a revoked partition's stale offset

Found by [B-65](B-65-onlost-when-the-session-expires.md). B-65's member kept each partition's processed
offset after committing it on revocation. When the partition came back to it later, and it gave the
partition up again before reading anything, it committed that stale offset once more, over whatever the
owner in between had committed. It happened once: a stale 187 overwrote another member's 300, and the
group's commits stopped short of the end. B-65's member was fixed. `CooperativeTest`'s member
([B-55](B-55-cooperative-rebalancing.md), also B-57's mixed group) has the same bookkeeping and was not.

The defect is in the test's member, not in the product. It can make `ci/b-55/run.sh` or `ci/b-57/run.sh`
red for the wrong reason ("the group's commits do not reach every end"), and a red run for the wrong reason
teaches people to rerun rather than read.

- **The decision and its reason.** Drop a partition's offset from the member's `processed` map once it is
  committed on revocation, and on loss, as B-65's member does now. Nothing else in the member changes.
- Not covered: the product, which is not involved.

- AC: `CooperativeTest`'s member forgets a revoked or lost partition's offset.
- AC: `ci/b-55/run.sh` and `ci/b-57/run.sh` stay green.
- Anchors: `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/CooperativeTest.kt`.

## Findings (2026-09-26)

- **AC: the member forgets a revoked or lost partition's offset.** After committing on revocation it drops
  those partitions from `processed`, and on loss it drops them without committing, as B-65's member does.
- **AC: the runners stay green.**
  - `ci/b-55/run.sh`: 1 200 records, 0 lost, 0 processed twice, commits `200/200` on all six partitions.
  - `ci/b-57/run.sh`: the same numbers under the KIP-848 protocol.

  Both ran with `GRADLE_OPTS=-Dorg.gradle.daemon=false`, which keeps the test worker out of other sessions'
  limited scopes.
- **Not mutation-checked, and why.** The stale commit needs a member to get a partition back and give it up
  again before reading anything from it. No run arranges that on demand; B-65 met it once. Reverting the fix
  would pass every run here, so a kill cannot be shown. The item rests on the one observation in B-65 and on
  reading the member.
