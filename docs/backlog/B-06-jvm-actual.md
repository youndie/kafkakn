---
id: B-06
title: "The JVM actual over kafka-clients"
status: open
priority: P0
size: M
stage: stage-1-produce
blocked_by: [B-05]
---

# B-06 — The JVM actual over kafka-clients

`org.apache.kafka:kafka-clients` 4.3.1 behind the `expect` surface. It is written **first**, because
until it works the differential harness has nothing to compare against.

- **The decision and its reason.** Delegate, do not reimplement. The value of this arm is that it is
  the reference implementation; every line of our own logic in it is a line the oracle no longer
  vouches for.
- The rejected alternative is a hand-rolled JVM producer sharing code with the native arm. It would
  make the two arms agree by construction and prove nothing.
- Not covered: exposing `kafka-clients` types in the public API. The surface stays platform-free
  ([B-02](B-02-expect-surface.md)).

- AC: the scenarios of [feature-produce-a-record](../features/feature-produce-a-record.md) pass on
  `jvm`.
- AC: `send` bridges the client's `Future` into a suspension that resumes on acknowledgement —
  without blocking a thread, and cancellation propagates.
- AC: a configuration key neither actual honours **fails at construction**, per
  [producer-contract](../api/producer-contract.md). Shown failing.
- AC: `acks` is shown reaching the broker — a value the broker must refuse comes back refused. An
  option silently dropped looks identical to one honoured.
- Anchors: `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/JvmProducer.kt`.
