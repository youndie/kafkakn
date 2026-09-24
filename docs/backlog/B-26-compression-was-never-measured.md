---
id: B-26
title: "compression.type is named portable in the contract and no test has ever set it"
status: done
priority: P1
size: S
stage: stage-5-producer-parity
---

# B-26 — compression is named portable and was never measured

The contract's *Configuration* section lists `compression.type` among the keys passed through to both
clients. **Nothing in the suite sets it.** It is the only key in that list with no test behind it, and
a claim in a table that nobody measured is the claim that turns out to be wrong
([research §1.8](../research/research-architecture.md) confirms all four codecs are in both arms; that
is a fact about the artefacts, not about what arrives at the broker).

- **The decision and its reason.** Measure each codec on each arm through the broker's own tools: a
  record that arrives proves nothing about compression, because an uncompressed batch arrives too.
  The third party is the stored log segment, read by `kafka-dump-log.sh --print-data-log`, which names
  the codec of every batch.
- librdkafka's own key is `compression.codec` with `compression.type` as its alias, and the Java client
  has only `compression.type` — so `compression.type` is the spelling that travels, as the contract
  already says. The item confirms it rather than changing it.
- The rejected alternative is reading values back and calling it done. Decompression that works is
  consistent with no compression having happened.
- Not covered: compression levels (`compression.level` and the Java client's per-codec keys), which
  are spelled differently per arm and are a separate portability question.

- AC: for each of `gzip`, `snappy`, `lz4`, `zstd` on each arm, `kafka-dump-log.sh` shows that codec on
  the batches written, and `kafka-console-consumer` reads the values back byte for byte.
- AC: an unknown codec is refused at construction on both arms, with the key named.
- AC: the contract's list either keeps `compression.type` with a measured date beside it, or loses it.
- Anchors: `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/CompressionTest.kt`,
  `ci/b-26/run.sh`, `docs/api/producer-contract.md`, `ci/harness/broker.sh`.

## What happened

**Every codec, on both arms, is what the broker stored.** `ci/b-26/run.sh`, 2026-09-24, a fresh
three-partition topic: `CompressionTest` sends 300 compressible records per codec per arm, each value
stamped with the arm and the codec, and the runner reads every log segment with the broker's own
`kafka-dump-log.sh --print-data-log` — 49 batches, 3000 records — matching each record to the batch
it landed in.

| | `none` | `gzip` | `snappy` | `lz4` | `zstd` |
|---|---|---|---|---|---|
| jvm | 300 of 300, stored `none` | 300, `gzip` | 300, `snappy` | 300, `lz4` | 300, `zstd` |
| linuxX64 | 300 of 300, stored `none` | 300, `gzip` | 300, `snappy` | 300, `lz4` | 300, `zstd` |

**The control is the `none` row and the check is that five labels come back as five different codecs
on disk**, per arm. A reader that reported the codec it was told to expect would make every row agree
with its label; it cannot make five labels produce five distinct values. Every value also read back
byte for byte through `kafka-console-consumer`.

**The refusal was measured too, and one arm's was wrong.** `compression.type=brotli`:

* jvm — *"Invalid value brotli for configuration compression.type: String must be one of: none, gzip,
  snappy, lz4, zstd"*;
* linuxX64, before — *"**unknown** producer configuration: compression.type (Invalid value "brotli"
  for configuration property "**compression.codec**")"*.

The key is known and the value is wrong, so "unknown" sent a caller looking for a typo in a key they
spelled correctly; and librdkafka's own sentence names the canonical key, not the alias the caller
typed. librdkafka already separates the two — `RD_KAFKA_CONF_UNKNOWN` against `RD_KAFKA_CONF_INVALID`
— and the native arm now does too: *"producer configuration compression.type refuses the value
'brotli' (…)"*. `CompressionTest` asserts the refusal names the key and does not call it unknown; it
was red on the native arm first.

**The runner failed once, for a reason worth keeping.** Given two test tasks and one `--tests` filter
on one command line, Gradle filtered one and ran the whole suite on the other — which then failed on
`AccountingTest`, correctly refusing to run without the topic its own runner creates (B-24). Each task
now has its own invocation. A filter that silently covers half of what it was given makes the other
half fail for somebody else's reason.
