---
id: B-70
title: "kafkakn-soak: a service built on kafkakn, run under chaos for an hour"
status: done
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

## Findings (2026-09-26)

- **AC: one common service, built on both arms.** `kafkakn-soak` holds `runSoak` in common code, with a
  native entry point and a JVM one. The native executable is 10.9 MB, release. The JVM program is launched
  from the classpath `soakJvmLaunch` writes, so the runner kills and freezes the process itself, not Gradle.
- **AC: an hour under chaos, every input record exactly once.** One run, `ci/b-70/run.sh`, `DURATION=3600`.
  **Corrected by B-72: the chaos lasted 37 minutes, not the hour.** The runner sized the input to a rate
  measured on 1 000 records, the sustained rate was higher, and the chaos stopped when the input did. The
  hour this AC asks for was then run by B-72: 60 minutes of chaos, 609 627 records, exactly once.
  - **input 388 800 records, output 388 800: missing 0, more than once 0, unknown 0**, under
    `read_committed`, counted by the Java client;
  - the group's commits at `64800/64800` on all six input partitions;
  - the service went through 23 kills, 26 freezes past its 10 s session, and 17 exits of its own, each
    followed by a restart.
- **AC: restarts and RSS per process.**

  | Slot | Killed | Frozen | Exited by itself |
  |---|---|---|---|
  | n1 (native) | 3 | 5 | 1 |
  | n2 (native) | 8 | 7 | 2 |
  | j1 (JVM) | 8 | 9 | 9 |
  | j2 (JVM) | 4 | 5 | 5 |

  - Native RSS sat at 16.6 to 18.1 MB. A process picking up a backlog after a restart peaked at 38 to
    48 MB, and came back to about 17 MB within minutes. The two longest native lives, 16.3 and 15.8 minutes,
    ended at 17.2 and 17.1 MB.
  - The JVM processes grew from about 100 MB to 150 to 206 MB within minutes, as a JVM heap does.
  - **What this run cannot say:** chaos every 45 s kept every process short-lived. A leak slower than a
    quarter of an hour is out of its reach. That is [B-72](B-72-a-native-instance-that-lives-the-hour.md).
- **Found: the two arms fail the same situation in two ways, neither of them kafkakn's.** An instance frozen
  past its session wakes mid-transaction. The group has moved on, and it calls `sendOffsetsToTransaction`
  with its old group metadata:
  - the JVM throws `CommitFailedException`, the Java client's own type (*"consumer group metadata
    mismatch"*), 14 times;
  - native throws `KafkaProduceException` with `ILLEGAL_GENERATION` or `UNKNOWN_MEMBER_ID`, marked
    *abortable*, 3 times.

  A caller cannot catch both with one `catch`, and the contract does not say what to do next. That is
  [B-71](B-71-send-offsets-after-the-group-moved-on.md). The counts differ as well: each JVM freeze ended in
  an exit, and 3 of 12 native freezes did. Whether that is where the freezes landed, or the arms behaving
  differently, is part of B-71.
- **Found in the runner, by the two-minute smoke run before the hour:** an instance that exited and was not
  yet waited for is a zombie, and `kill -0` still answers for it, so the exit went uncounted. The runner now
  checks the process state.
- `ci/harness/Records.java`'s `dump` waits up to 10 minutes, not 30 s, so an hour of output is read whole.
