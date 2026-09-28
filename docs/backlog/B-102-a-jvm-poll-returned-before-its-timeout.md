---
id: B-102
title: "A JVM poll(3 s) returned empty after 2.54 s by the monotonic clock (one suite run on the Linux box)"
status: done
priority: P1
size: S
stage: stage-18-what-the-consumer-and-the-harness-found
blocked_by: []
---

# B-102 — a JVM `poll(3 s)` returned empty after 2.54 s

Found by B-100's whole-suite run on the Linux box (2026-09-29): `ConsumerTest.a_waiting_poll_does_not_hold_the_callers_dispatcher`
failed on the JVM with *"the fixture stopped working: 0 records after 2.535969499s"*. The test assigns a partition,
seeks to its end, and polls for `WAIT` = 3 s; it expects nothing back and at least 3 s gone, measured with
`TimeSource.Monotonic`.

**Hypothesis, not checked:** kafka-clients times `poll` with `Time.SYSTEM.milliseconds()`, the wall clock, and this
box's wall clock is known to jump forward (by +2.93 s, seen in another project) while its monotonic clock lags. A
poll timed by one clock and checked by the other then ends early.

- **The decision and its reason.** Read, in kafka-clients 4.3.1, which clock `KafkaConsumer.poll(Duration)`'s timer
  uses, and record the wall-clock and monotonic time around the poll when it fails. If it is the wall clock, the
  test's promise ("a poll waits its timeout") holds only on a host whose wall clock does not jump, and the contract
  says so; the test measures by both clocks. If not, the early return is a finding about the JVM arm.
- AC: the clock kafka-clients uses is named, with the class and line.
- AC: a failing run records both clocks, or the test no longer depends on a clock the client does not use.
- Anchors: `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/ConsumerTest.kt`.

## Findings

- **Done, 2026-09-29. The hypothesis holds, read and measured.**
  - kafka-clients 4.3.1 counts `poll`'s timeout on the wall clock: `ClassicKafkaConsumer.java:175`
    (`this.time = Time.SYSTEM`), `:641` (`poll(time.timer(timeout))`); `SystemTime.java:35-36`
    (`milliseconds()` is `System.currentTimeMillis()`); `Timer.java:142-143` (`update()` reads `time.milliseconds()`).
    From `kafka-clients-4.3.1-sources.jar`.
  - The build box's wall clock jumps: `ci/b-102/Clocks.java`, 60 s sampled every 100 ms, gave one step of +820 ms wall
    against +100 ms monotonic (load average 0.4). A 3 s poll that such a step lands in ends that much early by the
    monotonic clock, which is what the suite saw: 2.54 s.
  - Not forced: making the wall clock jump would mean changing the host's time, which is not the loop's to do. The
    positive evidence is the source and the measured jump, not a reproduction.
- **The test measures the wait by both clocks** and records both (`consume.wait.ms`): each arm's client counts on its
  own. Green on both arms (3037/3036 ms on the JVM, 3004/3004 on native). Mutant, the JVM `poll` asking for half its
  timeout: `ConsumerTest.a_waiting_poll_does_not_hold_the_callers_dispatcher[jvm]` failed by name, 1.89 s by both
  clocks, so a poll that is short by both still fails.
- **The contract says it** (§1, the `poll` promise): the JVM arm's timeout is its client's, on the wall clock, and the
  library does not correct it.
