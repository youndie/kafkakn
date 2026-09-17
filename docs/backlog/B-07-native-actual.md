---
id: B-07
title: "The native actual: produce and delivery reports across the callback seam"
status: done
priority: P0
size: L
stage: stage-1-produce
blocked_by: [B-03, B-06]
---

# B-07 — The native actual: produce and delivery reports across the callback seam

librdkafka behind the same surface. The load-bearing part is not the produce call; it is the seam
where a C callback arriving on librdkafka's own thread resumes a Kotlin coroutine
([research §1.6](../research/research-architecture.md)).

- **The decision and its reason.** `staticCFunction` for the delivery-report callback, atomics for
  what it touches, and a registry keyed by the message opaque so a report finds its continuation.
  Measured over 1.8M messages with no crash and no deadlock — the mechanism is known to hold; what
  is unknown is our use of it.
- The rejected alternative is polling for completion without a callback. It costs a thread and
  cannot report per-record outcomes, which the contract requires.
- Not covered: backpressure ([B-08](B-08-suspend-on-backpressure.md)) and accounting
  ([B-09](B-09-accounting.md)) — deliberately separate items, because folding them in here is how
  they end up half-done.

- AC: `rd_kafka_produce`, not `rd_kafka_producev` — the latter is variadic and unusable through
  cinterop ([research §1.5](../research/research-architecture.md)).
- AC: the scenarios of [feature-produce-a-record](../features/feature-produce-a-record.md) pass on
  `linuxX64`, **and** the cross-arm scenario passes.
- AC: a deliberately crashing callback is shown crashing the run, so "no crash" is a statement the
  harness could contradict.
- AC: `flush` is implemented as `rd_kafka_outq_len` reaching zero, never as the return of
  `rd_kafka_flush` — which is an error code.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/NativeProducer.kt`,
  `kafkakn-core/src/nativeInterop/cinterop/rdkafka.def`.

---

## Findings — 2026-09-17

**Done, and the differential oracle earned its keep on the first day both arms existed.**

| | |
|---|---|
| suite | `jvmTest` 12, `linuxX64Test` 14, 0 failures, both against the same broker |
| the arms | **agree on all 14 observations** — after a fix the comparison forced |
| `rd_kafka_producev` | used 0 times outside comments; it is variadic and has no usable shape through cinterop |
| `flush` | `rd_kafka_outq_len` reaching zero, never the return of `rd_kafka_flush` |
| control | a failure on librdkafka's thread reaches the caller — see below, because the control was wrong first |

### The finding: the two clients partition keys differently

The first comparison **disagreed**. Of eight keys, five went to different partitions depending on
which arm produced them:

```
< partitioner.beta=2      > partitioner.beta=1
< partitioner.gamma=1     > partitioner.gamma=2
< partitioner.zeta=1      > partitioner.zeta=0
```

librdkafka's default `partitioner` is `consistent_random` — a **CRC32** hash of the key — while the
Java producer uses murmur2. Read out of librdkafka's own `CONFIGURATION.md`, which names the
compatible option in the same sentence: `murmur2_random`, "functionally equivalent to the default
partitioner in the Java Producer".

**Neither arm could have noticed alone.** Each one's records go where its own partitioner says,
every assertion each arm makes about its own records passes, and the broker is content either way.
A single-implementation client ships this and the symptom appears somewhere else entirely — a
consumer seeing one key's records split across partitions after a client is rewritten.

kafkakn sets `murmur2_random` on the native arm, because one library that puts a key in two
different places depending on the platform is not one library. A caller who sets `partitioner`
explicitly keeps their choice. Recorded as [research §2.2](../research/research-architecture.md).

### The control was wrong, and being wrong found something worse than what it looked for

The item asked for "a deliberately crashing callback shown crashing the run". It does not crash.
An exception thrown inside the delivery callback — on a thread librdkafka owns, outside any `try`
the caller wrote — **does not terminate the process and surfaces nowhere**. It left the continuation
parked and the caller suspended for ever, and the only reason anything noticed was that `runTest`
has a timeout: the symptom was `UncompletedCoroutinesError`, not a crash.

A hang is worse than a crash, because a crash is reported. So the producer now **unparks the
continuation before doing anything else** and wraps the rest in a catch that resumes exceptionally:
whatever goes wrong in there, the caller is waiting, and resuming it with the failure is the only
outcome that is not a hang. The control asserts that consequence instead of the one the item
guessed. [Research §2.3](../research/research-architecture.md).

The affordance that injects the failure is `internal` and set only from the test — not an
environment variable, because "crash on demand" should not be a switch anyone can find in a library.

### The seam, as built

The callback is a `staticCFunction` and cannot capture, so the way back to a suspended caller is a
top-level atomic registry keyed by the id handed to librdkafka as `msg_opaque`. A poll coroutine
calls `rd_kafka_poll`, which is the only thing that makes delivery reports run at all.

`NativeArmStillAStubTest` and the `ProduceTest` filter it guarded are both **deleted** — the guard
failed first, exactly as designed, and that failure is what says the exclusion is no longer
warranted.

### Not covered

Backpressure is handled crudely here — `QUEUE_FULL` drains with `rd_kafka_poll` inside the produce
loop — and [B-08](B-08-suspend-on-backpressure.md) replaces it. Headers
([B-10](B-10-record-headers.md)), TLS ([B-11](B-11-tls.md)). The byte-fidelity scenario remains
unticked: it needs a byte-exact independent reader.
