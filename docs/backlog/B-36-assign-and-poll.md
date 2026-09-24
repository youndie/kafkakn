---
id: B-36
title: "A consumer without a group: assign partitions, seek, and read as a Flow"
status: done
priority: P2
size: L
stage: stage-8-consume
blocked_by: [B-35]
---

# B-36 — assign and poll

The first consumer code, and deliberately the half without group coordination: assign partitions
explicitly, seek to the beginning, the end, an offset or a time, and read. Both clients underneath
have every piece — `assign`, `poll`, `seek`, `offsetsForTimes` on the JVM; `rd_kafka_assign`,
`rd_kafka_consumer_poll`, `rd_kafka_seek_partitions`, `rd_kafka_offsets_for_times` on native
([research §1.8](../research/research-architecture.md)).

- **The decision and its reason.** Built to the contract [B-35](B-35-the-consumer-designed-first.md)
  produces, with its threading decision; nothing here re-decides it. Groups are left out so that the
  first thing measured is whether both arms read the same records in the same order — without a
  rebalance that could explain a difference away.
- **The oracle is a third party on both ends.** Records are written by `kafka-console-producer`, not
  by kafkakn, and include what a text-shaped path would damage: non-UTF-8 values, null keys, null
  values, duplicate header names.
- The rejected alternative is reading back what this library's own producer wrote. It would pass for
  a consumer and a producer that are wrong in the same way.

- AC: both arms read every record of an assigned partition, in order, byte for byte against what
  `kafka-console-consumer` reads.
- AC: seeking to beginning, end, an offset and a timestamp lands where the broker's own
  `kafka-get-offsets.sh` says it should, on both arms.
- AC: the two arms' readings are compared with each other, and a difference is a failure rather than a
  warning.
- Anchors: `ci/harness/broker.sh`, `docs/research/research-architecture.md`.

## Findings (2026-09-24)

**Measured, `ci/b-36/run.sh`, both arms.**
- Twenty records written by a third party were read byte for byte and in order, identically to the
  oracle and to each other. Among them: a null key, a tombstone, a value that is not UTF-8, duplicate
  header names with a null header value, and an empty key and value next to null ones.
- Seeks landed where `kafka-get-offsets.sh` says: beginning 0, offset 7, timestamp → 6. A seek to the
  end read nothing.

**The third party is not `kafka-console-producer`**, which the item named. The console tools carry
text, so a non-UTF-8 value cannot survive them. The records are written, and read back as hex, by the
Kafka distribution's own client, run as its own program (`ci/harness/Records.java`); kafkakn is not on
its classpath. [consumer-contract](../api/consumer-contract.md) §4 says so.

**H7, settled for assign and poll** ([research §2.25](../research/research-architecture.md)). Each of
the three measurements was watched red against a mutant. The cancellation test first passed its
mutant: it timed the cancellation, not the next call. It now times the next call, and with the JVM's
`wakeup()` removed that call took 55 s.

**Two librdkafka requirements the design did not know**, both absorbed by the native arm:
- `rd_kafka_assign` needs a `group.id`. The arm supplies a private one, and the cluster never lists it.
- A seek before fetching starts is refused. The arm seeks by re-assigning.

**The fixture's own trap.** Records stamped in 2023 were deleted by time-based retention within
minutes. The consumer topic now has `retention.ms=-1`, and `broker.sh up` recreates it if it was
emptied.
