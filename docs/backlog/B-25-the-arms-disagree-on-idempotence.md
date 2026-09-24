---
id: B-25
title: "The arms disagree on idempotence by default — a retried record can be written twice by one and once by the other"
status: done
priority: P0
size: M
stage: stage-5-producer-parity
---

# B-25 — the arms disagree on idempotence by default

Read out of the artefacts, not reasoned: `enable.idempotence` defaults to **`true`** in
`kafka-clients` 4.3.1 and to **`false`** in librdkafka 2.13.0, with retries unbounded on both and
`max.in.flight.requests.per.connection` at 5 against 1 000 000
([research §1.8](../research/research-architecture.md)). A record whose acknowledgement is lost and
which is then retried can therefore be written **twice by the native arm and once by the JVM arm**,
and each arm is individually consistent with the broker. That is the shape of the partitioner
finding (§2.2), and it is in what ships today — which is why this item is P0 and comes before any new
surface.

- **The decision and its reason.** The native arm sets `enable.idempotence=true` unless the caller
  named the key, the same rule and the same reason as `partitioner=murmur2_random`: one library that
  duplicates on one platform and not on the other is not one library, and the reference arm's default
  is the one that travels. A caller who sets it keeps their value.
- **Measured first, not assumed.** The consequence is H6 and nothing has measured it. The fixture has
  to lose an acknowledgement on purpose — the broker paused past `request.timeout.ms` while records
  are in flight, then resumed — and it is itself the positive control: with idempotence **off**, the
  native arm must be shown writing a duplicate, or the rounds after the change prove nothing.
- The rejected alternative is setting it off on the JVM arm instead, to agree the other way. That
  makes the oracle the one that duplicates.
- Not covered: transactions, which require idempotence and are
  [B-30](B-30-transactions.md).

- AC: with `enable.idempotence=false` on the native arm, the fault fixture produces **at least one
  duplicate**, counted per value by `kafka-console-consumer` — watched red before anything changes.
- AC: after the change, the same rounds produce **zero duplicates on both arms**, with the default
  observed per arm by `recordArmFact` rather than assumed.
- AC: a caller who sets `enable.idempotence=false` gets exactly that on both arms.
- AC: the contract's *Configuration* section records the default, the disagreement it replaces, and
  its reason; H6 is settled in the research either way.
- Anchors: `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/IdempotenceTest.kt`,
  `ci/b-25/run.sh`, `docs/api/producer-contract.md`, `ci/harness/broker.sh`.

## What happened

**The decision in this item was half right, and measuring the reference arm before copying it is what
showed which half.** "The native arm sets `enable.idempotence=true` unless the caller named it" is
not what the Java client does. Measured against `kafka-clients-4.3.1.jar`, by constructing its own
`ProducerConfig` and reading the value it settled on:

| the caller set | the Java client |
|---|---|
| nothing | on |
| `acks=1` | **off, silently** |
| `retries=0` | **off, silently** |
| `acks=1` and `enable.idempotence=true` | refuses |
| `max.in.flight.requests.per.connection=10` | **refuses, with idempotence unset** |

A native default that was only "on" would have made librdkafka refuse `acks=1`. The native arm now
takes the whole rule, and `IdempotenceTest` holds all seven rows against both arms, reading each
client's own effective value — `ProducerConfig` on one arm, `rd_kafka_conf_get` off the constructed
handle on the other — rather than what kafkakn asked for.

**Red first, on exactly two rows**: the default, and `max.in.flight=10`, which the native arm accepted
where the reference refuses. The other five passed before the change only because the native default
was already off; removing the `acks` condition afterwards fails exactly the `acks` row, which is what
shows they are guarding the rule rather than passing by default.

**H6, measured** with `ci/b-25/run.sh`: a driver built outside this repository against the branch's
candidate, fifty concurrent senders, the broker frozen five times for six seconds against a two-second
request timeout.

| run | records | distinct | written twice |
|---|---|---|---|
| native, idempotence off — the control, first | 105 966 | 105 766 | **200** |
| JVM, idempotence off | 116 300 | 116 050 | **250** |
| native, default | 107 184 | 107 184 | **0** |
| JVM, default | 153 874 | 153 874 | **0** |

Nothing was lost or refused in any run. 200 and 250 are multiples of the concurrency, which reads as
**every record in flight at a freeze written twice** — systematic, not a race the fixture was lucky
to hit.

**One line decided whether the fixture could see anything.** librdkafka's `request.timeout.ms` *"is
only enforced by the broker"*, which is frozen; the client's own limit is `socket.timeout.ms`, sixty
seconds by default. With only the Java client's key set, the native arm would never have retried and
its zero would have meant nothing. The driver sets each arm's own key.

**And the fault could not live in the suite.** Every item runner runs the whole suite, and a test that
needs the broker frozen under it would either run unfrozen everywhere else or fail everywhere else.
It is a driver instead — the same shape as `ci/downstream` — built against a candidate published to a
repository on disk, so the code measured is this branch's and the path is a stranger's.
