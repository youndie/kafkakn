---
id: B-72
title: "A native instance that lives the whole hour: its memory, measured"
status: done
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

## Findings (2026-09-26)

- **AC: a slot exempt from the chaos, with its RSS at 10 minutes, at the end, and the slope.** In
  `EXEMPT=n1 ci/b-70/run.sh`, the chaos picks only among the other slots. The report fits two slopes:
  - one over the raw samples after the first ten minutes;
  - one over the **floor**, the least RSS in each five-minute window.

  A leak raises the floor the process returns to. The raw slope follows transient peaks, such as a batch
  picked up after a rebalance, 22 to 44 MB for about a minute.
- **The result: no leak visible in an hour.** Two runs of the native instance, each living through every
  rebalance the other three caused:

  | Run | Life | Floor per five minutes | Floor slope | Raw slope |
  |---|---|---|---|---|
  | 1 | 66.7 min | 16.8 to 17.4 MB | −0.34 MB/hour | −1.46 MB/hour |
  | 2 | 60.0 min | 16.8 to 17.3 MB | +0.15 MB/hour | +2.65 MB/hour |

  An earlier run reported a raw slope of +14 MB/hour over only 27 minutes. That was the peaks, not the floor,
  which stayed at 16 to 17 MB there too, and it is why the floor is reported.
- **Found in the runner, twice, both about B-70:**
  - **The chaos did not last `DURATION`.** The input was sized to a rate measured on 1 000 records, the
    sustained rate was higher, and the loop ended with the input: 37 minutes, in B-70's run and in this
    item's first. The chaos now lasts `DURATION`, with twice the input the rate would need, stopped when it
    is up. B-70's findings are corrected where they said an hour.
  - **The trickle was not stopped.** Killing `broker.sh` left its `java` child writing. The input kept
    growing after the chaos, so the lag never reached zero, and the second run was counted 258 records
    short. Every one of those was beyond the group's commit, and none below it: unprocessed, not lost. The
    runner now kills the trickle's process tree.
- **The genuine hour, green:** 60 minutes of chaos (35 kills, 42 freezes, 33 exits of the instances' own,
  every one of them `StaleGroupMetadataException` now, B-71), **609 627 input records, 609 627 output
  records, missing 0, more than once 0**, commits at every end, and no trickle left running.
