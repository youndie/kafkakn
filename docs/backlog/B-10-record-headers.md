---
id: B-10
title: "Record headers without rd_kafka_producev"
status: done
priority: P2
size: M
stage: stage-2-real-use
blocked_by: [B-07]
---

# B-10 — Record headers without `rd_kafka_producev`

Headers are how tracing and schema identifiers travel, so a producer without them is not usable in
most deployments. The natural vehicle on the native side is `rd_kafka_producev`, which is variadic
and therefore unusable through cinterop ([research §1.5](../research/research-architecture.md)).

- **The decision and its reason.** Settle **H2**: find the mechanism librdkafka offers that is not
  variadic — `rd_kafka_headers_new` plus a produce path that accepts them — and check the arms agree
  on what the broker stores.
- The rejected alternative is a small C shim compiled into the cinterop bundle that wraps `producev`.
  It works and it adds C of our own to a project whose whole argument is that it writes none; kept
  as a fallback if the non-variadic path does not exist.
- Not covered: header-based partitioning and any interceptor mechanism.

- AC: **H2 settled in writing** in [research §3](../research/research-architecture.md), either way.
- AC: a record with headers round-trips, and an independent reader sees the same header bytes.
- AC: both arms agree on header ordering and on what a duplicate key does.
- Anchors: `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerRecord.kt`,
  `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt`.

## Outcome — 2026-09-17

**H2 is settled, and the answer removes the fallback.** `rd_kafka_produceva(rk, vus, cnt)` takes the
same tagged fields `rd_kafka_producev` takes, as an **array** of `rd_kafka_vu_t`, and is an ordinary
function. The C shim this item kept in reserve is not needed; the project still contains no C of its
own. Written up as [research §2.10](../research/research-architecture.md), and the H2 row in §3 now
points at it.

§1.5 was being read more widely than it says. It rules out `rd_kafka_producev`, which is variadic;
`rd_kafka_produceva` is a different function and was there all along.

Measured by `ci/b-10/run.sh`, suites **21 tests on jvm, 23 on linuxX64, 0 failures**, arms agreeing
on all 17 shared observations:

| Sent | Stored, read by `kafka-console-consumer` |
|---|---|
| `trace=1`, `schema=kafkakn.v1`, `trace=2` | `trace:1,schema:kafkakn.v1,trace:2` on **both** arms |
| `absent` (no value), `empty` (zero bytes) | `absent:null,empty:` on **both** arms |

So a duplicate name survives **in order**, and a null value stays distinguishable from an empty one
— through the broker, on both arms. The script asserts the second as a **difference** rather than as
a spelling: a client that collapsed null into empty would render the two identically, and that is
the check, not what either one looks like.

**The whole produce path moved**, not only the header case — two paths, one taken only by callers
who use headers, is how the less-travelled one rots. That also retired the topic-handle cache
(`produceva` names the topic) and with it the last of the machinery behind
[research §2.8](../research/research-architecture.md).

**Not covered, as the item says.** Header-based partitioning and any interceptor mechanism. Also not
covered: headers whose bytes are not printable — the third-party reader available here renders them
as text, so a binary round-trip would be checked by the same client that wrote it, which is the one
thing this suite does not do.
