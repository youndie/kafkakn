---
id: B-25
title: "The arms disagree on idempotence by default — a retried record can be written twice by one and once by the other"
status: wip
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
  `docs/api/producer-contract.md`, `ci/harness/broker.sh`.
