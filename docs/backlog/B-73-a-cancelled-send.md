---
id: B-73
title: "A cancelled send: what the contract promises, measured on both arms"
status: wip
priority: P0
size: M
stage: stage-16-a-deadline-on-send
epic: feature-backpressure-and-accounting
---

# B-73 — a cancelled `send`: what the contract promises, measured on both arms

`send` can be cancelled on both arms, and nothing public says what that means for the record. The JVM arm
suspends in `suspendCancellableCoroutine` on `Dispatchers.IO` (`KafkaProducer.jvm.kt:241`). The native arm
suspends in `enqueue`'s backpressure loop and then in `slot.await()` (`KafkaProducer.native.kt:421`). Both
KDocs say the same sentence: cancelling stops the caller waiting and does **not** recall a record the client
has already accepted. The contract does not say it. In `producer-contract.md` the word *cancel* appears only
for `partitionsFor` and the transactional calls. No test cancels a `send`.

A caller who bounds the wait with `withTimeout` is the first one to depend on that sentence. An HTTP bridge
built on keel is being planned: it answers only after the broker's acknowledgement, and it answers something
else once a deadline passes. For such a caller the unwritten sentence decides whether its answer is true.
"Not written, try again" is a lie if the record lands after the deadline, and the retry then writes it twice.

- **The decision and its reason.** Write the two moments into the contract, per arm, and measure each one
  against the broker's end offsets, the oracle every other item here uses:
  - **cancelled before the record is queued.** On native this is the backpressure loop, and the KDoc at
    `KafkaProducer.native.kt:447` calls it "the one moment at which cancellation is clean": `enqueue`
    unparks the slot and rethrows. On the JVM the waiting happens *inside* `kafka-clients`' `send`, which
    blocks for metadata or buffer room up to `max.block.ms`. A coroutine cancelled there cannot interrupt
    it: once `send` returns, the record is in the accumulator. So the JVM arm may have no clean moment
    except before the dispatch to `Dispatchers.IO`. That is read from the code, not measured, and this
    item measures it.
  - **cancelled after the record is queued.** The record goes on; its delivery report arrives later and
    completes a slot nobody awaits. The claim to measure is that the record **is** in the topic afterwards.
    A sentence that says "may" without a run that shows "does" is the kind this contract refuses.
- Rejected: making `send` non-cancellable after enqueue (`NonCancellable`). That turns a caller's deadline
  back into `message.timeout.ms`, 300 000 ms by default on native, which is the wait the caller cancelled
  to escape.
- Not covered: giving the caller a way to *tell* the two moments apart. That is
  [B-74](B-74-a-cut-wait-says-whether-the-record-was-queued.md), and it needs this item's measurements first.

- AC: `producer-contract.md` gains a *Cancellation* part under `send` that names both moments on each arm
  and what each means for the record, marked **measured** with the date.
- AC: a test on both arms cancels a `send` after the record is queued, against a broker that has stopped
  answering (paused, not stopped), then lets the broker answer. The record is found in the topic by end
  offset.
- AC: a test cancels a `send` while it waits for room (native: the queue at its bound). After the queue
  drains, the record is **not** in the topic. On the JVM the same test states what was measured, whichever
  way it went.
- AC: after the delivery reports of cancelled sends have arrived, the native arm holds no parked slot
  (`waiting` is empty). Cancelling must not leak.
- Anchors: `docs/api/producer-contract.md`,
  `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt`,
  `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/`, `ci/b-73/run.sh` if the pause needs a
  runner.
