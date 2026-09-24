---
id: B-41
title: "Metrics an operator can read — without the library counting its own successes"
status: open
priority: P3
size: M
stage: stage-6-real-deployments
---

# B-41 — metrics an operator can read

Every mainstream client exposes metrics, and both of ours do — `Producer.metrics()` on the JVM, a JSON
document every `statistics.interval.ms` through `rd_kafka_conf_set_stats_cb` on native
([research §1.8](../research/research-architecture.md)). The two shapes have nothing in common.

- **The decision and its reason, and the constraint that shapes it.** This project refuses to let
  the library count its own deliveries, and a build gate enforces it: a delivered-records counter is
  exactly the number that read "100% success" while a quarter of the input was lost (§1.4). So the
  first decision is **what may be exposed at all** — queue depth, records in flight, broker round-trip
  time and connection state describe the machinery; a count of successes describes a claim, and
  stays out.
- A small portable set, each metric defined once in the contract and read from each arm's own source,
  with the two readings compared under the same load.
- The rejected alternative is passing each arm's native metrics through. It is the fastest thing to
  build, and it would give operators two different dashboards for one library.

- AC: the contract names each exposed metric and where each arm reads it from.
- AC: under one load, the two arms' readings of each metric are within a stated tolerance, or the
  difference is written down as the arms'.
- AC: `scripts/no_delivery_counters.py` still passes, and its self-test still fails the counter it
  exists to refuse.
- Anchors: `scripts/no_delivery_counters.py`, `docs/api/producer-contract.md`.
