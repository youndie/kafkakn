---
id: B-02
title: "The expect surface, compiling and throwing"
status: done
priority: P0
size: S
stage: stage-0-it-builds
blocked_by: [B-01]
---

# B-02 — The expect surface, compiling and throwing

The interface from [producer-contract](../api/producer-contract.md), with both actuals present and
both throwing `NotImplementedError`. This is what makes the first test red for the reason it is
supposed to be red rather than red because nothing compiles.

- **The decision and its reason.** Write the surface from the contract document **before** either
  implementation, so neither platform's shape leaks into it. `send` suspends
  ([D3](../research/research-architecture.md)); values are bytes, not `String`.
- The rejected alternative is deriving the interface from whichever actual gets written first —
  which is how an `expect` ends up mirroring `kafka-clients`' `Future` or librdkafka's callback.
- Not covered: headers, `sendAll`, transactions, partitioner overrides. Absent until an item asks.

- AC: `KafkaProducer`, `ProducerRecord` and `RecordMetadata` exist in `commonMain` and compile on
  both targets.
- AC: a `commonTest` test calls `send` and asserts it throws `NotImplementedError` — on both
  targets. The suite is then known to reach both actuals.
- AC: no type in the surface names a platform, and `commonMain` imports nothing from either client.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/`,
  `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/SurfaceTest.kt`.

---

## Findings — 2026-09-17

**Done.** The surface exists in `commonMain`, both arms resolve it, and every test is red for the
reason it is supposed to be red.

| | |
|---|---|
| tests | `jvmTest` 5, `linuxX64Test` 5, 0 failures — read from the result files |
| mutation | making the stub's `send` return a value instead of throwing turned **both** arms red, one failure each; restored |
| `commonMain` imports | **none at all** — the surface depends on nothing |

### The contract said `AutoCloseable` and it cannot be

`AutoCloseable.close` does not suspend, and `close` here has to flush. Both ways of fitting into the
interface are wrong: blocking a thread inside `close` on a coroutine runtime, or dropping records
still in flight — which is the silent-loss shape this library exists to avoid. So `close` suspends
and `KafkaProducer` is its own interface. [The contract](../api/producer-contract.md) is corrected in
the same change, with the reason, rather than the code quietly differing from it.

Construction is a top-level `expect fun kafkaProducer(config)` rather than an `expect class`: the
interface stays ordinary common code that both arms implement, and only the factory is
platform-specific. That keeps the *contract* out of the expect/actual mechanism entirely.

### The check for AC 3 was wrong before it was right, and is now a gate

"No type in the surface names a platform" was first checked by grepping the sources — which reported
`KafkaProducer.kt`, because its KDoc explains what `rd_kafka_produce` does. Documentation that names
the thing it warns about is what a good comment looks like; a checker that cannot tell it from an
import teaches people to delete the comment.

Corrected to strip comments first, and promoted from a one-off command to
`scripts/common_is_platform_free.py` in `make check` — the invariant erodes by one convenient import
and nobody notices. **Shown failing**: a file importing `org.apache.kafka` is reported, then removed.

### Two decisions inside the types

- **Key and value are `ByteArray`.** Kafka's key space has no encoding; a `String` API imposes UTF-8
  and the first payload that is not valid UTF-8 finds out.
- **`ProducerRecord` is deliberately not a `data class`.** The generated `equals` compares
  `ByteArray` by identity, so two records with identical bytes would be unequal and nothing would
  say so until a test compared them.

### Not covered

Headers, `sendAll`, transactions, partitioner overrides — absent until an item asks. Validation of
unknown configuration keys is named in the contract and not implemented: it arrives with the first
arm that can say what it honours ([B-06](B-06-jvm-actual.md)).
