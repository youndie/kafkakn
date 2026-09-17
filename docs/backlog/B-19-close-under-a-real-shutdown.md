---
id: B-19
title: "RQ-A: does close() keep its promise inside a real ordered shutdown?"
status: open
priority: P0
size: L
stage: stage-4-a-real-user
---

# B-19 — RQ-A: does `close()` keep its promise inside a real ordered shutdown?

The contract promises that **a record accepted by `send` before `close` is either acknowledged or
its `send` throws**, and that `close` discards nothing. Everything that has tested that promise so
far was a test that decided when the shutdown happened. The first place it meets a `SIGTERM`
arriving mid-stream, in a process that also has an ingress, a database and an ordered shutdown of
its own, is a real service.

- **The decision and its reason.** The publisher is an existing Kotlin/Native service rather than a
  new example: a service with a real ingress, real storage and `kore`'s ordered shutdown exercises
  exactly the surface a test harness never does. **xyk** is the candidate — every accepted webhook
  produced to a topic, key the delivery id, headers carrying the trace id, the sink **off unless
  `kafka.bootstrap.servers` is set**. If xyk is closed for changes, a service of the same shape from
  `keel`'s examples is the fallback, and taking the fallback is a budget decision rather than a
  defeat. **What the fallback loses is the real ingress** — traffic arriving from outside on its own
  schedule rather than from a generator the test starts — so a green on the example is a weaker
  green and the write-up says so in the same sentence as the result.
- The rejected alternative is another harness in this repository. It would be written by the same
  hands that wrote the promise, and the one thing it could not supply is a shutdown nobody arranged
  for it.
- Not covered: throughput as an absolute number ([H4](../research/research-architecture.md) stays
  not measured), consumers, and anything that makes the publisher a product.

**The oracle is the service's own accepted-delivery log, not the producer.** The service's SQLite
table records what it accepted before the producer was involved; the topic's end offsets say what
arrived. A producer asked whether it delivered what it delivered answers yes.

**Two things about that log are pre-registered, because deciding them afterwards would decide the
result.**

- **The row is written BEFORE `send`.** That is what makes a loss visible — a row with no offset
  behind it. It also means a crash between the row and the `send` reads as a loss while being
  nothing of the kind, so:
- **`SIGTERM` only, never `SIGKILL`.** A process killed outright cannot run `close`, and what the
  gap between the row and the send then shows is an **outbox** question — whether a service should
  write its intent and reconcile it later — not a question about this library's `close`. That is a
  real question and it is **out of M2**; conflating the two would let an outbox defect be reported
  as a kafkakn one, or the reverse.

- AC: under a steady stream, **20 × `SIGTERM` at random points**; after each, the topic's end
  offsets reconcile against the service's accepted-delivery log. Broker in Docker, `acks=all`, three
  partitions, as in the spike.
- AC: **green** is 20/20 with zero records accepted by the service and absent from the topic, and
  shutdown inside `kore`'s configured deadline.
- AC: **amber** — zero loss, deadline exceeded — is a `close()` timing defect and is recorded as
  one, not rounded to green.
- AC: **red** — any accepted record missing — goes into `docs/research/` as a negative result
  **before anything else is done**, and stops [B-21](B-21-does-anyone-want-this.md).
- Anchors: `docs/api/producer-contract.md`, `docs/research/`.
