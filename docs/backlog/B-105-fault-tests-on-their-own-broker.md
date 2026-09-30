---
id: B-105
title: "The fault tests run in the suite on linuxX64, each on a broker of its own"
status: done
priority: P1
size: M
stage: stage-19-the-suite-in-ci
blocked_by: []
---

# B-105 — the fault tests run in the suite on linuxX64, each on a broker of its own

Seven tests pause or stop the broker (`EnqueueTest`, `CancelledSendTest`, `StoppedBrokerTest`,
`CloseWithBrokerGoneTest`). The broker they break is the one the whole suite uses, so they are gated behind
`KAFKAKN_BROKER_CONTROL` / `KAFKAKN_BROKER_STOP`, and `ci/suite/run.sh` never sets either: the suite that gates
every merge has never run them.

- **The decision:** on `linuxX64` a fault test gets a broker of its own from
  [kontainer](https://github.com/youndie/kontainer) (`io.github.youndie.kontainer:kontainer`, published for
  linuxX64): `ci/broker/fault-broker.compose.yml` under a compose project and a host port of its own, ready when
  it answers ApiVersions, removed after the test. It runs with no switch. Its observations are compared with
  the JVM arm only when the switch has made the JVM arm run it too; otherwise they are kept as this arm's facts,
  since a comparison with an arm that did not run would be a disagreement about nothing.
- **Not changed:** the JVM, a Mac and arm64 keep the shared broker behind the switches. kontainer has no target
  for them yet.
- **Rejected:** giving the shared compose file an override — it has a fixed container name and fixed ports,
  which two brokers on one box cannot share.
- **Not covered:** `CloseWithBrokerGoneTest`'s two tests are measurements with nothing asserted, run per
  `KAFKAKN_CLOSE_VARIANT` and minutes long; they get an owned broker when a variant is named, but stay out of the
  suite.

- AC: `ci/suite/run.sh` runs `EnqueueTest`, `CancelledSendTest` and `StoppedBrokerTest` on `linuxX64` with no
  switch, on brokers of their own, and the arms still agree.
- AC: the shared broker the rest of the suite uses is never paused or stopped during it.
- Anchors: `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/FaultBroker.kt`,
  `kafkakn-core/src/linuxX64Test/kotlin/io/github/youndie/kafkakn/FaultBroker.linuxX64.kt`,
  `ci/broker/fault-broker.compose.yml`

## Findings (2026-09-30)

- **`ci/suite/run.sh` on the build box:** both arms green (`jvmTest` 178, `linuxX64Test` 169), and the arms agree
  on all 120 observations. The fault tests ran on `linuxX64` with no switch, on owned brokers: the native arm's
  facts carry `enqueue.full`, `enqueue.unknown`, `cancel.queued`, `cancel.room` and `stopped.*` from a broker on
  a host port kontainer chose (`127.0.0.1:59736` in the stopped test's refusal), while the JVM arm's read
  `not asked`.
- **The shared broker was never touched:** `docker events` for `kafkakn-broker` over the run shows no `pause`,
  `stop`, `kill` or `die`, and no owned fixture was left behind.
- **Five of the seven run in the suite.** `CloseWithBrokerGoneTest`'s two are measurements with nothing
  asserted, one per `KAFKAKN_CLOSE_VARIANT`, and minutes long; they get an owned broker on `linuxX64` when a
  variant is named, and otherwise stay out, as before.
- **Observations are compared only when both arms ran:** without the switch the native arm keeps the fault
  tests' observations as its own facts, so the comparison is not handed an arm that did not run.
