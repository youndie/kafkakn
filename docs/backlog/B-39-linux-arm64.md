---
id: B-39
title: "linuxArm64: settle H5 — does a second native target cost a matrix row and no code?"
status: open
priority: P2
size: M
stage: stage-9-targets
---

# B-39 — linuxArm64

D6 says `linuxArm64` is designed for and not built, and H5 claims it costs a build-matrix row and no
code. Every other client this project is compared with runs on arm64 servers; kafkakn claims it could
and has never tried.

- **The decision and its reason.** Build it, and settle H5 in writing whichever way it goes. The C
  bundle is the hard part: it is built in `manylinux2014` for glibc 2.17 (D4), and the arm64 image of
  the same family is the first thing to try, including whether the local patch applies there — the
  B-22 check will say which of its three answers it gets.
- **Run, not only link.** A binary that links for a target nobody ran is B-15's lesson again. Under
  emulation on the build machine at first, on real arm64 if one is available, and the write-up says
  which.
- The rejected alternative is claiming the target from a successful cross-compile. It is the claim D6
  was written to avoid.
- Not covered: publishing it — that follows from a green here and is decided then.

- AC: a `linuxArm64` klib carries its C, a downstream build links a binary against it with no
  configuration of its own, and the binary produces to the broker.
- AC: the glibc floor for the arm64 binary is measured the way `ci/b-16/run.sh` measures it for x64.
- AC: H5 is marked settled in the research, with what it actually cost.
- Anchors: `ci/librdkafka/build.sh`, `kafkakn-core/build.gradle.kts`, `ci/b-16/run.sh`.
