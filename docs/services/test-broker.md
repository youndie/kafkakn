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
| listeners | `PLAINTEXT` on 9092, `SSL` on 9094 **and** `MTLS` on 9095 — SSL that requires a client certificate ([B-31](../backlog/B-31-client-certificates.md)) — all always up |
| server keystore | PKCS12; the clients read a PEM CA |
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
  are erased between the fixture being written and the broker reading it. They are generated into
  `~/.cache/kafkakn/tls` on the machine that runs the broker, beside the C bundle — a file copied in
  from the Mac also arrives at mode 0600, which this container's non-root user cannot read.
- **The SSL port must be published, not merely configured.** A listener declared inside the
  container with no published port refuses every connection, which looks exactly like a broken TLS
  configuration.
- **There is no plaintext-only mode.** Both listeners come up together, because a fixture with two
  modes gives the suite a mode in which its TLS scenarios quietly do not run, and a scenario that
  did not run reads exactly like one that passed.
- **The broker refuses a PEM keystore here**, though Kafka 4.x supports PEM: `SSL key store password
  cannot be specified with PEM format, only key password may be specified`. The image gives no way
  to not specify one — its `configure` script `ensure`s `KAFKA_SSL_KEYSTORE_CREDENTIALS` the moment
  an SSL listener is advertised, exports the file's contents as the keystore password, and exits on
  `${!1}: unbound variable` if it is missing. So the **server** uses PKCS12 and both **clients** read
  a PEM CA, which is the half this library has to get right.
- **The client-certificate listener is configured with listener-scoped keys**
  (`KAFKA_LISTENER_NAME_MTLS_SSL_CLIENT_AUTH`, `…_TRUSTSTORE_TYPE`, `…_TRUSTSTORE_LOCATION`). Set
  globally, `KAFKA_SSL_CLIENT_AUTH=required` makes the image's `configure` script demand a trust store
  password file, and a PEM trust store refuses any password. The key store is the global one: Kafka
  falls back to the unprefixed `ssl.*` for whatever a listener does not override.
- **`broker.sh mtls-selftest` asks the listener to say no first** — no certificate, then a
  certificate from the wrong authority — with the broker's own tools, and only then to say yes. A
  listener that quietly does not ask is as green as one that works for every good certificate.
- **Regenerating the certificates under a running broker breaks it silently.** The broker loads its
  keystore once, at startup; new certificates leave it presenting one no client trusts, and the
  symptom is an SSL handshake failure that looks exactly like a misconfigured client. Measured, and
  it cost a run. `certs.sh` is therefore idempotent — it keeps a valid set and regenerates only on
  `FORCE=1`, which then needs the container recreated.

## Code anchors

| What | Where |
|---|---|
| compose definition | `ci/broker/docker-compose.yml` |
| the TLS overlay and its certificates | `ci/broker/docker-compose.tls.yml`, `ci/broker/certs.sh` |
| the harness that starts it for the suite | `ci/harness/` |
| the fixture's own positive control | `ci/harness/broker.sh tls-selftest` — the right CA connects, the wrong one does not, asked with the broker's own tools |
