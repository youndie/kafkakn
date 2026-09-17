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
  defeat.
- The rejected alternative is another harness in this repository. It would be written by the same
  hands that wrote the promise, and the one thing it could not supply is a shutdown nobody arranged
  for it.
- Not covered: throughput as an absolute number ([H4](../research/research-architecture.md) stays
  not measured), consumers, and anything that makes the publisher a product.

**The oracle is the service's own accepted-delivery log, not the producer.** xyk's SQLite table
records what it accepted before the producer was involved; the topic's end offsets say what arrived.
A producer asked whether it delivered what it delivered answers yes.

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
