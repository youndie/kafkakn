---
id: B-35
title: "The consumer, designed before it is built: a contract document and the defaults it starts from"
status: done
priority: P2
size: M
stage: stage-8-consume
---

# B-35 — the consumer, designed first

D2 kept the consumer out because group coordination is where most of a Kafka client's difficulty
lives; the amendment of 2026-09-24 brings it in **last and designed first**. This item produces a
document, not code — the consumer contract and the research beneath it — and it settles H7.

- **The decision and its reason.** Design before code, because the questions that decide the shape
  are all about the two clients underneath and none is answerable by writing a `poll` loop:
  - **threading.** What each client requires of the thread that calls it, read out of
    `kafka-clients` 4.3.1 and librdkafka 2.13.0 rather than remembered, and what that forces on a
    suspending API — the producer's seam had to learn this twice (§2.3, §2.13);
  - **the shape.** A cold `Flow` of records against an explicit `poll`, and what "suspends" means for
    a read that may wait indefinitely;
  - **the defaults the arms already disagree on** — `isolation.level` is `read_uncommitted` on the JVM
    and `read_committed` on librdkafka, and the assignment strategies differ
    ([research §1.8](../research/research-architecture.md)); the document decides which travels;
  - **the oracle.** Records written by `kafka-console-producer`, read by both arms and compared with
    each other and with `kafka-console-consumer`; committed offsets read back by
    `kafka-consumer-groups.sh`. A consumer checked by our own producer is the same mistake as the
    reverse.
- The rejected alternative is building assign-and-poll first and designing around it. The first
  version's threading decision is the one every later item inherits.
- Not covered: implementation. [B-36](B-36-assign-and-poll.md) is the first code.

- AC: a consumer contract document in the api layer, in the shape of `producer-contract.md`, with
  every promise marked *target* until an item measures it.
- AC: the defaults table for every consumer key the contract names, read from both artefacts, with the
  decision for each disagreement and its reason.
- AC: an explicit list of what the first consumer will **not** do.
- AC: H7 settled in the research, or restated with what would settle it.
- Anchors: `docs/api/producer-contract.md`, `docs/research/research-architecture.md`.

## Findings (2026-09-24)

**Delivered:** [consumer-contract](../api/consumer-contract.md), and research §2.24. No code, as the
item says.

- **Threading, read rather than remembered.** The Java consumer's "not thread-safe" is a lock held for
  one call, not an affinity to one thread. So a serial lane on `Dispatchers.IO` satisfies it and a
  dedicated thread is not needed. `wakeup()` is the documented way to cancel; interrupts are
  discouraged by the client's own documentation.
- **The defaults table has twenty rows.** Research §1.8 had five. The six new disagreements include
  two where kafkakn picks one value for both arms: `allow.auto.create.topics=false` and
  `check.crcs=true`.
- **Two decisions that go against a default.**
  - `isolation.level=read_committed` on both arms: the safe default, which here is librdkafka's where
    B-25's was the JVM's.
  - `enable.auto.commit=false` on both arms: the same `true` commits different things on the two
    clients.
- **Shape:** an explicit `poll` first, and a `Flow` later over it. A `Flow` hides
  `max.poll.interval.ms`, which evicts a slow collector on both arms.
- **H7** is restated with the three measurements B-36 must make (research §2.24). The hypothesis moved
  to B-36.
