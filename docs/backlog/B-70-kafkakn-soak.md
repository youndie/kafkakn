---
id: B-70
title: "kafkakn-soak: a service built on kafkakn, run under chaos for an hour"
status: wip
priority: P2
size: L
stage: stage-15-a-consumer-of-our-own
---

# B-70 — `kafkakn-soak`: a service built on kafkakn, run under chaos for an hour

Every measurement so far is a test or a runner written around one promise: a few minutes each, against
one feature at a time. Nothing has *used* the library the way a service does: for an hour, with every
feature at once, while its instances are killed and frozen. That is the one place left where kafkakn can
be found wrong by something other than its own suite. There is no outside consumer, so the owner asked for
one of our own (2026-09-26).

- **The decision and its reason.** A module, `kafkakn-soak`, holding one service in common code. It is the
  exactly-once read-process-write loop of [B-38](B-38-exactly-once-read-process-write.md), as a program
  rather than a test:
  - read the input in a group;
  - for each record, write one to the output inside a transaction;
  - commit the input's progress in that transaction.

  It is built twice, as a native executable and as a JVM program. It is **crash-only**: any failure ends
  the process with a non-zero status, and the runner starts it again. That exercises every recovery path
  the library has (fencing, lost membership, aborted transactions) without the service deciding which
  errors are safe to continue past.
- `ci/b-70/run.sh` runs four instances, two per arm, in one group, for an hour by default. It kills one at
  random with `SIGKILL` or freezes it past its session with `SIGSTOP`, every 45 s. The input trickles
  throughout. Everything around the service is the Java client's, not ours, as in every oracle here: the
  generator and the final reading of the output (`ci/harness/Records.java`).
- **What is checked:**
  - every input record is in the output **exactly once**, under `read_committed`;
  - the group's commits reach every input partition's end;
  - each process's RSS, sampled every minute, with the growth over each native process's lifetime reported.
    A growth that does not flatten is a finding.
- **The shared build box is protected.** Each instance runs in a `systemd-run --user --scope` with its
  own `MemoryMax` and `TasksMax`. A watchdog stops the whole run if `MemAvailable` falls below 1.5 GB.
  Gradle runs without the daemon of another session (B-68's finding).
- Not covered: publishing the module (it is not published), a second broker, network faults, the
  KIP-848 protocol (a later run can switch it on).

- AC: `kafkakn-soak` builds a native executable and a JVM program from one common service.
- AC: a one-hour run under chaos ends with every input record in the output exactly once, and the
  group's commits at every end, counted by the Java client.
- AC: the report gives each instance's restarts, and each native process's RSS at start, at end and at its
  peak.
- Anchors: `kafkakn-soak/build.gradle.kts`, `kafkakn-soak/src/commonMain/kotlin/io/github/youndie/kafkakn/soak/Soak.kt`,
  `ci/b-70/run.sh`.
