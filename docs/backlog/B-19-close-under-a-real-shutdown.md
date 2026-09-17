---
id: B-19
title: "RQ-A: does close() keep its promise inside a real ordered shutdown?"
status: done
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
- Anchors: `ci/b-19/run.sh`, `ci/b-19/generator.py`, `docs/api/producer-contract.md`,
  `docs/research/research-architecture.md`.

## What happened

**The publisher is xyk**, the fallback was not taken, and the real ingress is therefore in the
measurement rather than named as a loss. Its sink is off unless a broker is configured and absent
from the binary on a target kafkakn publishes nothing for; the row is committed before the publish,
the producer is closed after the engine drains, and a refused publish is named on stdout with its
event id instead of failing a request that was already stored.

**Measured 2026-09-17** with `ci/b-19/run.sh`, release binary, broker in Docker, `acks=all`, three
partitions, a fresh topic and database per round, the signal at a random point three to eight seconds
into a steady stream:

| sweep | rounds | accepted | missing | shutdown | drain |
|---|---|---|---|---|---|
| 8 concurrent senders | 20 | 91 149 | **0** | 2.26 s (one at 5.85 s) | — |
| 64 concurrent senders | 20 | 130 681 | **0** | 2.26 s | 12–82 ms |
| control at 8, broker stopped before the signal | 1 | 5 511 | 8, **all 8 named by the service** | 8.28 s | — |
| control at 64, same | 1 | 7 711 | 64, **all 64 named** | 8.28 s | 6.08 s |

**Green as pre-registered**: 20/20 with zero accepted records absent from the topic, and the
shutdown inside kore's deadlines — no release stage came near one, so the amber case did not arise.
The 2.26 s is almost all `preDrainWait`, two seconds the gateway spends announcing that it is going
away before it drains anything.

**Each control lost exactly one record per concurrent sender**, which is what says how much the
green is worth: the set of requests inside `send` when the world changes is the concurrency, so the
ordinary rounds at 64 were draining up to 64 of them each — about **1 280 requests caught mid-`send`**
across that sweep, all finished before the producer was closed.

**What the green does not cover, found by the measurement rather than argued at it.** xyk's publish
awaits the acknowledgement inside the request, so a record is either inside somebody's `send` or
finished; it is never sitting in the producer with its `send` already returned, which is the state
`close` exists to answer for. These rounds exercise the **order**: the drain finishes before the
producer is closed, so a request mid-`send` is not cut. The
contract's sentence also covers `close` answering for records already queued, and that half needs a
caller that returns before the acknowledgement. Filed as
[B-23](B-23-the-sink-that-does-not-wait.md), not started: M2 bought three days and this spent them.
The contract now says which half has been measured, at the sentence itself.

**The harness was wrong twice, and both times by being green.** A topic named after the round alone
replayed the previous run's records — 2 612 records with no row behind them, a finding entirely of
its own making; and a port is not free the moment its process is, so the first control round died of
`EADDRINUSE` after a clean exit, which reads exactly like a round that passed. Both are in the
script's comments, next to the lines that fix them.

The publisher's side is [youndie/xyk#5](https://github.com/youndie/xyk/pull/5).
