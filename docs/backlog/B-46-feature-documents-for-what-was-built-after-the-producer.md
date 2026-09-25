---
id: B-46
title: "Feature documents for consuming, exactly-once and administration"
status: wip
priority: P2
size: M
stage: stage-10-housekeeping
blocked_by: [B-45]
---

# B-46 — feature documents for consuming, exactly-once and administration

Stages 5 to 9 added a consumer, exactly-once read-process-write and an admin client. None of them
has a feature document. `docs/features/` holds three: producing, backpressure and accounting, and a
secure connection. The consumer's behaviour is written down only in `docs/api/consumer-contract.md`
and in the items. The contract says what each call promises. A feature document's scenarios are what
a user does and what they must see, and they carry the `**Automated:**` lines that `bdd_report.py`
reads. So today the report cannot see any of the consumer's tests.

- **The decision and its reason.** Three documents, from the template, each with the scenarios the
  code already passes, and an `**Automated:**` line naming the test and the harness script behind it:
  - `feature-consume-records`: assign, seek, poll, groups, commit;
  - `feature-exactly-once`: read-process-write across stops;
  - `feature-administer-topics`: create, delete, describe, and describe the cluster.
- Written from the tests and the harness scripts, not from the contract. A scenario no test exercises
  is marked ***target***, not written as fact.
- Not covered: behaviour that does not exist yet. The items in stages 11 to 13 add their scenarios to
  these documents as they land.

- AC: `python3 scripts/bdd_report.py` lists the three features, and every scenario it counts as
  automated names a test that exists.
- AC: `make check` passes, and the coverage map names the three documents.
- Anchors: `docs/features/`, `docs/api/consumer-contract.md`, `kafkakn-core/src/commonTest/`.
