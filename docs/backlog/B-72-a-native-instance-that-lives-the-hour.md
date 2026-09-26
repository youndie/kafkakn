---
id: B-72
title: "A native instance that lives the whole hour: its memory, measured"
status: wip
priority: P3
size: S
stage: stage-15-a-consumer-of-our-own
blocked_by: [B-70]
---

# B-72 — a native instance that lives the whole hour: its memory, measured

[B-70](B-70-kafkakn-soak.md) sampled every process's RSS for an hour. Its chaos killed or froze a random
instance every 45 s, so no process lived longer than about 16 minutes. The native ones stayed near 17 MB, but
a leak slower than a quarter of an hour cannot show in lives that short.

- **The decision and its reason.** Keep one native instance out of the chaos, so it lives the whole run and
  goes through every rebalance the others cause. Sample its RSS every minute, and fit the trend over the
  hour after its first ten minutes. The chaos stays on the others: a rebalance-heavy hour is where a leak in
  the rebalance or transaction path would show.
- A flat line is the expected result. A line that keeps rising is a finding for the native arm, and the
  first thing to check is what [B-53](B-53-consumer-lag-in-metrics.md) keeps: the statistics document,
  replaced every second.

- AC: `ci/b-70/run.sh` can exempt a slot from the chaos, and a one-hour run reports that instance's RSS at
  10 minutes, at the end, and the slope between them.
- Anchors: `ci/b-70/run.sh`.
