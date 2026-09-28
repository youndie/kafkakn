---
id: B-102
title: "A JVM poll(3 s) returned empty after 2.54 s by the monotonic clock (one suite run on the Linux box)"
status: wip
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
