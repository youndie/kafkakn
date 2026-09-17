---
id: test-broker
title: test-broker — the broker the suite runs against
type: service
repo_url: https://github.com/youndie/kafkakn
module: ci/broker
tech_stack: [Apache Kafka, KRaft, Docker]
owner: unassigned
depends_on: []
publishes: []
---

# test-broker

A real broker, not a fake. Every acceptance assertion in this project is made against
`apache/kafka:4.3.1` in KRaft mode, single node, and the same broker serves both arms of the
differential suite — which is what makes their disagreement mean something.

## Why a real broker rather than a fake

A fake encodes this project's understanding of Kafka, and then the suite checks that understanding
against itself. The one class of defect this library most needs to catch — a wrong wire assumption —
is invisible to it. The cost is that the suite needs Docker; that is accepted.

## Shape

| | |
|---|---|
| image | `apache/kafka:4.3.1`, pinned — a moving tag would silently re-point the environment between runs |
| mode | KRaft, single node, controller and broker in one process |
| topic | 3 partitions, replication factor 1 |
| listeners | `PLAINTEXT` for the ordinary suite; an `SSL` listener for [feature-secure-connection](../features/feature-secure-connection.md) |
| auto-create | **off**, so a test against a topic that does not exist fails instead of quietly succeeding |

Single node means replication factor 1, so `acks=all` is a durable write to **one** in-sync replica.
That is the property the suite needs; it is not a durability claim about a real cluster, and no
document here makes one.

## The third party

Assertions do not read back through this library. The oracles are the broker's own end offsets
(`kafka-get-offsets.sh`) and `kafka-console-consumer`. A producer verified by its own consumer can
be wrong in both directions at once.

## Quirks

- **`docker compose up --wait` reports a crash-looping container as `Healthy`.** Measured
  2026-09-17: with `KAFKA_CONTROLLER_LISTENER_NAMES` missing, the broker aborted in `StorageTool`
  before it listened at all, the container restarted with exit 1 in a loop, and compose printed
  `Container kafkakn-broker Healthy`. Everything downstream then failed for reasons that looked like
  its own. `ci/harness/broker.sh up` therefore does not trust `--wait`: it polls
  `kafka-broker-api-versions.sh` until the broker **answers**, and on timeout prints the broker's own
  exception rather than a timeout of its own.
- **`docker exec` without `-i` attaches no stdin.** A console producer piped into it reads EOF,
  exits zero and writes nothing. It cost a day in the spike; the end offsets are what caught it.
- **Certificates must live outside any synchronised tree** on a machine where one is in use, or they
  are erased between the fixture being written and the broker reading it.
- **The SSL port must be published, not merely configured.** A listener declared inside the
  container with no published port refuses every connection, which looks exactly like a broken TLS
  configuration.

## Code anchors

| What | Where |
|---|---|
| compose definition | `ci/broker/docker-compose.yml` |
| the TLS overlay and its certificates | `ci/broker/docker-compose.tls.yml`, `ci/broker/certs.sh` |
| the harness that starts it for the suite | `ci/harness/` |
