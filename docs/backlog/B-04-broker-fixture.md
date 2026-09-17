---
id: B-04
title: "The broker fixture: KRaft, three partitions, auto-create off"
status: done
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

---

## Findings — 2026-09-17

**Done.** The fixture stands, and both oracles are shown able to say yes **and** no.

| | |
|---|---|
| topic | `PartitionCount: 3`, `ReplicationFactor: 1`, leaders and ISR listed |
| auto-creation | off |
| image | `apache/kafka:4.3.1`, pinned |
| offsets oracle | `0 -> 1` on one produce, and `0` for a pattern never produced |
| consumer oracle | found 1, and 0 for the pattern never produced |
| control 1 | the broker check **fails** against a dead port |
| control 2 | a produce with no stdin attached exits **zero** and writes nothing; only the offsets reveal it |

### `docker compose up --wait` called a crash-looping container Healthy

The first run of this item failed with an empty topic list and a produce that wrote nothing, and the
cause was neither: `KAFKA_CONTROLLER_LISTENER_NAMES` was missing, the broker aborted in
`StorageTool` before it ever listened, the container restarted with exit 1 in a loop — and compose
printed `Container kafkakn-broker Healthy` regardless.

A fixture that reports success while its subject is dead makes every test above it meaningless, and
it fails them in a way that looks like their own defect. So **`up` does not trust `--wait`**: it
polls until the broker answers a real request, and on timeout prints the broker's own exception
rather than a timeout of its own. Recorded in
[test-broker](../services/test-broker.md)'s quirks.

### The second control is the one worth keeping

`docker exec` without `-i` attaches no stdin, so a console producer piped into it reads EOF, exits
**zero**, and writes nothing. The script demonstrates it deliberately and asserts that the offsets
did **not** move — which is the only witness there is. The harness's own `produce` helper uses
`docker exec -i` and the comment says why, with the note that `-i` after the container name makes
`-i` the command.

### Not covered

TLS ([B-11](B-11-tls.md)), multi-broker, replication beyond factor 1. Single node means `acks=all` is
a durable write to one in-sync replica — the property the suite needs, and not a durability claim
about a cluster.
