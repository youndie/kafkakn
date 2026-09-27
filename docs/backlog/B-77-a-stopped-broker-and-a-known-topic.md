---
id: B-77
title: "A stopped broker and a known topic: the JVM refuses the record, native queues it"
status: open
priority: P1
size: M
stage: stage-18-what-the-consumer-and-the-harness-found
epic: feature-backpressure-and-accounting
blocked_by: [B-76]
---

# B-77 — a stopped broker and a known topic: the JVM refuses the record, native queues it

[mostik](https://github.com/youndie/mostik) measured this on 2026-09-27 (its B-08). The topic's metadata was known
from an earlier publish, and the broker was stopped with `docker stop`, not paused. Then five records were
enqueued:

| | JVM | native |
|---|---|---|
| `enqueue` | `RecordNotQueuedException`, five of five (mostik answers `429`) | queued, five of five (mostik answers `504`) |

Both answers are allowed by the contract, which does not promise the arms agree while the broker is unreachable.
They are still two answers to one situation, and [B-76](B-76-enqueue-without-metadata.md) removed exactly that
for missing metadata. B-74 measured a **paused** broker (`docker pause`: connections open, nothing answered),
and there both arms queued. So what differs here is a stopped broker, whose connections are refused. **Why the
Java client refuses is not known.** One guess is that it drops a known topic's metadata once every connection
fails, and then waits `max.block.ms` for it again. That is a hypothesis until it is measured.

- **The decision and its reason.** Measure first, decide after, as with B-68. Which arm is moved to which answer
  depends on why the JVM refuses, and that is the owner's call once it is known.
- AC: `ci/b-77/run.sh` stops the broker (`docker stop`), not pauses it, with a topic already described on both
  arms. For each arm it records whether `enqueue` returned or threw, after how long, with what message, and
  whether the record is in the topic once the broker is back.
- AC: the Java client's reason is read, not guessed. That means what it logs, and the code path in
  `kafka-clients` 4.3.1 that throws, cited with an address inside the artefact.
- AC: the findings end in a question for the owner, with the options it opens: native refuses as well, the JVM
  arm keeps the record, or the contract names the difference.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`,
  `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt`, `docs/api/producer-contract.md`.
