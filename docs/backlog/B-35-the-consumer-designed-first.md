---
id: B-35
title: "The consumer, designed before it is built: a contract document and the defaults it starts from"
status: open
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
