---
id: B-26
title: "compression.type is named portable in the contract and no test has ever set it"
status: wip
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
- Anchors: `docs/api/producer-contract.md`, `ci/harness/broker.sh`.
