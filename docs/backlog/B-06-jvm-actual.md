---
id: B-06
title: "The JVM actual over kafka-clients"
status: done
priority: P0
size: M
stage: stage-1-produce
blocked_by: [B-05]
---

# B-06 — The JVM actual over kafka-clients

`org.apache.kafka:kafka-clients` 4.3.1 behind the `expect` surface. It is written **first**, because
until it works the differential harness has nothing to compare against.

- **The decision and its reason.** Delegate, do not reimplement. The value of this arm is that it is
  the reference implementation; every line of our own logic in it is a line the oracle no longer
  vouches for.
- The rejected alternative is a hand-rolled JVM producer sharing code with the native arm. It would
  make the two arms agree by construction and prove nothing.
- Not covered: exposing `kafka-clients` types in the public API. The surface stays platform-free
  ([B-02](B-02-expect-surface.md)).

- AC: the scenarios of [feature-produce-a-record](../features/feature-produce-a-record.md) pass on
  `jvm`.
- AC: `send` bridges the client's `Future` into a suspension that resumes on acknowledgement —
  without blocking a thread, and cancellation propagates.
- AC: a configuration key neither actual honours **fails at construction**, per
  [producer-contract](../api/producer-contract.md). Shown failing.
- AC: `acks` is shown reaching the broker — a value the broker must refuse comes back refused. An
  option silently dropped looks identical to one honoured.
- Anchors: `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/JvmProducer.kt`.

---

## Findings — 2026-09-17

**Done.** The JVM arm delegates to `kafka-clients`, and the broker agrees with everything it claims.

| | |
|---|---|
| suite | `jvmTest` 10 tests, 0 failures |
| end offsets | `300 -> 450`, grew by **150**, which is the 100 + 50 the suite said it sent |
| independent consumer | 100 of 100 with no key, 50 of 50 with a key |
| the oracle can still say no | a stamp never produced: 0 |
| recorded for the other arm | `partitioner.key-k.partition=2` — the first observation B-07 will be compared against |

### The `acks` probe was wrong, and being wrong is the finding

The item asked for "a value the broker rejects". `acks=99` is such a value — and
**`kafka-clients` refuses it at construction**, with its own
`ConfigException: Invalid value 99 for configuration acks: String must be one of: all, -1, 0, 1`,
before any broker is contacted. So the probe proved nothing about whether `acks` travels; it proved
the client validates.

That matters twice over:

- **as a test**, the probe had to become a *valid* value the broker cannot satisfy — `acks=all`
  against a topic with `min.insync.replicas=2` on a single-broker cluster, which comes back
  `NOT_ENOUGH_REPLICAS` — with `acks=1` on the same topic as the control, so the refusal is known to
  be about `acks` rather than about the topic being unusable;
- **as a contract question**, the two arms refuse a bad value in **different places**: the JVM client
  at construction, librdkafka by letting the broker decide. [The contract](../api/producer-contract.md)
  now says only that an unusable value fails, names construction as the earlier of the two, and
  declines to make either arm imitate the other. Failing earlier is better.

### An unknown key fails where the caller is still holding it

`kafka-clients` logs unknown configuration at WARN and carries on, which is the shape this project
refuses. The arm validates against `ProducerConfig.configNames()` — **the client's own set**, not a
list maintained here, because a hand-written list beside a growing set goes stale on the first Kafka
release. Shown failing for `bootstrapServers`, the plausible-looking wrong spelling.

### Two of B-02's tests were true then and false now

They asserted that the factory returns the stub and that `send` throws `NotImplementedError` — on
**both** arms. That was right when nothing was implemented and became a lie the moment one arm was.
They are gone rather than qualified: what an implemented arm does is asserted by its own feature
tests, and what an unimplemented one does is not interesting.

### The native arm is excluded from `ProduceTest`, and the exclusion is guarded

There is no native producer until [B-07](B-07-native-actual.md), so `ProduceTest` is filtered out of
the native run. An exclusion with only a comment behind it is how a suite quietly stops testing an
arm, so `NativeArmStillAStubTest` asserts the native factory **still returns the stub**. The day
B-07 lands, that test fails, and the only way to make it pass is to delete it together with the
filter it guards.

### Scenarios still not ticked, and why

- *Both actuals choose the same partition for the same key* — needs the native arm
  ([B-07](B-07-native-actual.md)). The observation it will be compared against is already being
  recorded.
- *A value is bytes, not text* — needs a byte-exact independent reader; `kafka-console-consumer`
  gives text. Not attempted rather than approximated, and the scenario stays unticked.

### Not covered

Headers, transactions, partitioner overrides, and exposing any `kafka-clients` type in the public
API — `commonMain` still has no imports at all.
