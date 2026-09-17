---
id: B-04
title: "The broker fixture: KRaft, three partitions, auto-create off"
status: open
priority: P0
size: S
stage: stage-0-it-builds
---

# B-04 — The broker fixture: KRaft, three partitions, auto-create off

`apache/kafka:4.3.1` in KRaft mode, one node, with the properties
[test-broker](../services/test-broker.md) describes — and with its own oracles wired up, because
every later item's assertions are made through them.

- **The decision and its reason.** A real broker, not a fake. A fake encodes this project's
  understanding of Kafka and then checks that understanding against itself, which is blind to the one
  class of defect this library most needs to catch.
- The rejected alternative is an in-memory double for speed. It costs the differential oracle its
  meaning: two actuals agreeing against a fake agree about the fake.
- Not covered: multi-broker, replication beyond factor 1, and any durability claim.

- AC: the topic is created with **3 partitions**, auto-creation is **off**, and the tag is pinned.
- AC: a helper reads the topic's summed end offsets, and a helper reads messages back through
  `kafka-console-consumer` — the two third parties every later assertion uses.
- AC (positive control): the fixture is shown **failing** — the same check against a port with
  nothing on it — so that a later green means something.
- AC (positive control): a produce through the console tools is shown writing nothing when stdin is
  not attached, and the end offsets are what reveal it. The failure mode is documented in
  [test-broker](../services/test-broker.md) and this is where it is demonstrated.
- Anchors: `ci/broker/docker-compose.yml`, `ci/harness/broker.sh`.
