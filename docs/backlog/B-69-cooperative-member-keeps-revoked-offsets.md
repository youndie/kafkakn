---
id: B-69
title: "CooperativeTest's member commits a revoked partition's stale offset"
status: wip
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
