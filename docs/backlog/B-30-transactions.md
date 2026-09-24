---
id: B-30
title: "Transactions: records that become visible together, or not at all"
status: done
priority: P2
size: L
stage: stage-5-producer-parity
blocked_by: [B-25]
---

# B-30 — transactions

Both clients underneath implement Kafka transactions — `initTransactions`, `beginTransaction`,
`commitTransaction`, `abortTransaction` on the JVM, the `rd_kafka_*_transaction` family on native
([research §1.8](../research/research-architecture.md)). Transactions require idempotence, which is
why this waits for [B-25](B-25-the-arms-disagree-on-idempotence.md).

- **The decision and its reason.** The four calls as suspending functions, with Kafka's names, and
  `transactional.id` as an ordinary configuration key. A scoped helper that commits on success and
  aborts on an exception is worth having, and it is built **on** the four rather than instead of them,
  because a caller who has to decide when to abort needs the calls.
- **This breaks the project's own oracle, and the item has to replace it first.** Every accounting
  check so far compares the records handed in against the topic's **end offsets**. Commit and abort
  markers occupy offsets, so on a transactional topic the end offset is no longer a count. The oracle
  becomes `kafka-console-consumer --isolation-level read_committed`, counting records rather than
  offsets.
- **Fencing is where the arms are most likely to differ.** A second producer with the same
  `transactional.id` fences the first; what the fenced one's next call throws is observed per arm and
  mapped onto one kafkakn exception.
- The rejected alternative is transactions without the consumer half. It is the right first step, and
  it is only a step: `sendOffsetsToTransaction` needs a consumer's group metadata and is
  [B-38](B-38-exactly-once-read-process-write.md).
- Not covered: `sendOffsetsToTransaction`, and anything read-side.

- AC: records in a committed transaction are all visible under `read_committed`, and records in an
  aborted one are none of them — on both arms.
- AC: under `read_uncommitted` the aborted records **are** visible, which is what shows the abort
  happened rather than the records never being sent.
- AC: a fenced producer fails with one kafkakn exception on both arms, and the underlying errors are
  recorded.
- AC: the accounting oracle's limitation is written into the contract, not discovered by the next
  person.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.kt`,
  `docs/api/producer-contract.md`, `ci/b-09/run.sh`.

## Findings (2026-09-24)

**Measured, `ci/b-30/run.sh`, both arms.** 50 records committed: all 50 under `read_committed`,
directly and through `inTransaction`. 50 aborted: none under `read_committed`, **all 50 under
`read_uncommitted`** — the positive control that the abort happened to records that were written.
The coordinator's `kafka-transactions.sh describe` reports `CompleteCommit` and `CompleteAbort`.

**Fencing, watched red first.** Before the mapping the JVM arm threw the client's own
`org.apache.kafka.common.errors.ProducerFencedException` and the native arm
`_FENCED (-144) … [fatal]`; now both throw `ProducerFencedException`, from the commit and from the
next `send`, with each client's sentence as the cause — recorded in
[producer-contract](../api/producer-contract.md). The fenced producer's open transaction is aborted
by its successor's init: 0 under `read_committed`, 50 under `read_uncommitted`.

**The oracle's limitation is in the contract**, beside the end-offset reconciliation it limits, and
in [feature-backpressure-and-accounting](../features/feature-backpressure-and-accounting.md).

**On the way.** `kafka-console-consumer --consumer-property` prints its deprecation notice on
stdout in 4.3.1, as a line of the output: a 200-record topic counted 201. The harness now names the
isolation level with `--command-property`.
