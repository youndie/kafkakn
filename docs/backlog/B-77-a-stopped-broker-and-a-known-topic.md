---
id: B-77
title: "A stopped broker and a known topic: the JVM refuses the record, native queues it"
status: question
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

## Findings (2026-09-27)

*Measured*, `ci/b-77/run.sh` on the Linux box. A topic was described with one acknowledged record, the broker was
stopped with `docker stop -t 1`, and each arm enqueued at 0, 5 and 20 s after the stop, with `max.block.ms` 5 000.
The broker was then started again, and every queued record's fate was read, and the topic dumped with the broker's
own reader:

| pass | at 0 s | at 5 s | at 20 s | the topic afterwards |
|---|---|---|---|---|
| JVM, as it ships | refused after 5 034 ms | refused after 5 001 ms | refused after 5 001 ms | `warm` |
| JVM, `metadata.recovery.strategy=none` | queued in 2 ms | queued in 0 ms | queued in 0 ms | `warm r-0 r-1 r-2` |
| native | queued in 0 ms | queued in 0 ms | queued in 0 ms | `warm r-0 r-1 r-2` |

The JVM's refusal said *"Topic … not present in metadata after 5000 ms"*: it had **forgotten** a topic it had just
written to. Each record queued on either arm landed once the broker was back.

**Why, read in `kafka-clients` 4.3.1, not guessed.** The hypothesis was confirmed by the control above and by the
source:
- `metadata.recovery.strategy` defaults to `rebootstrap`
  (`kafka-clients-4.3.1-sources.jar!/org/apache/kafka/clients/CommonClientConfigs.java`, line 242);
- when the client needs metadata and no node is available or connecting, it rebootstraps at once
  (`kafka-clients-4.3.1-sources.jar!/org/apache/kafka/clients/NetworkClient.java`, lines 1228–1231);
- rebootstrapping replaces the metadata snapshot with one holding only the bootstrap addresses
  (`kafka-clients-4.3.1-sources.jar!/org/apache/kafka/clients/Metadata.java`, lines 306–315, `bootstrap`,
  `rebootstrap`);
- so the topic is unknown again, and `send` waits `max.block.ms` for its metadata before it throws. That is
  B-76's path, reached from a known topic.

librdkafka 2.13.0 has the same key, with the same default (`CONFIGURATION.md`, `metadata.recovery.strategy`,
`metadata.recovery.rebootstrap.trigger.ms` = 300 000). But its produce path queues a record for a topic
regardless, and kafkakn's own cache of described topics (B-76) is never cleared. So native queues.

## Question for the owner

Both answers are true under the contract. Which one should both arms give while every broker is unreachable?

1. **Native follows the oracle** (recommended). When librdkafka reports every broker down (`_ALL_BROKERS_DOWN` on
   the error callback), and `metadata.recovery.strategy` is `rebootstrap`, the default, the producer forgets the
   topics it has described. The next `enqueue` waits for metadata and, like the JVM's, refuses at `max.block.ms`.
   Both arms answer "never queued" during an outage. `metadata.recovery.strategy=none` is a key both clients
   already know, and it gives "queued" on both. That makes the choice the caller's, one key with one meaning on
   each arm. The cost is that during an outage every `enqueue` takes `max.block.ms` to refuse, on native as it
   already does on the JVM.
2. **The JVM arm defaults to `metadata.recovery.strategy=none`**, as kafkakn already sets the native partitioner to
   match the Java client. Both arms queue, and the records land after the outage. The cost is moving the oracle away
   from its own default, and giving up rebootstrap: a client whose brokers all changed address would no longer
   find them again through `bootstrap.servers`.
3. **The contract names the difference** and changes nothing. mostik keeps answering `429` on the JVM and `504` on
   native for the same outage.
