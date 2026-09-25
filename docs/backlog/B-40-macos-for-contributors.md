---
id: B-40
title: "macOS, so a contributor can run the native arm without the Linux box"
status: done
priority: P3
size: M
stage: stage-9-targets
---

# B-40 — macOS for contributors

The README's out-of-scope table says it plainly: without a macOS target, a contributor on a Mac cannot
run the native arm locally. Every native change today needs a Linux machine, and the one this project
uses has been restarted twice in a day.

- **The decision and its reason.** `macosArm64` for the test suite first — a C bundle built on macOS
  and the native suite passing against the Dockerised broker — and **not** as a published target
  until someone asks for one. The measure of this item is a contributor's loop, not a user's platform.
- The C bundle cannot come from `manylinux2014`, so D4 does not apply: macOS has no glibc floor, and
  the question becomes which OpenSSL the bundle links and whether the result is still one binary.
- The rejected alternative is Homebrew's librdkafka. It is a system dependency — the shape §1.1 found
  in the prior art and this project exists not to have.
- Not covered: Windows, `musl`, iOS.

- AC: on a Mac, the native suite runs against the broker and agrees with the JVM arm, the same
  differential that runs on Linux.
- AC: the README's out-of-scope row changes to say what is and is not supported, measured.
- Anchors: `ci/librdkafka/build.sh`, `kafkakn-core/build.gradle.kts`.

## Findings (2026-09-25)

**Measured, `ci/b-40/run.sh`, on an arm64 Mac (macOS 27.0).** The broker ran on the Linux box and was
reached through an SSH tunnel on the listeners' own ports, because they advertise 127.0.0.1.
- `macosArm64Test`: 92 tests, 0 failures, TLS, mTLS and SASL included, with the fixture's
  certificates copied over.
- The JVM arm ran on the Linux box against the same broker, and `compare-arms.sh` found the two arms
  agreeing on all 17 observations.

**The bundle.** `ci/librdkafka/build-macos.sh` builds it on the Mac from the same four source tarballs
as the Linux bundle, copied from the Linux box's cache with matching checksums. OpenSSL is
`darwin64-arm64-cc`, `configure` selects SSL, zlib, zstd and SCRAM, and the output lives in its own
path, `librdkafka-<v>-macosArm64`. The glibc patch is not applied: macOS has `sys/random.h`.
`otool -L` shows no librdkafka or OpenSSL library in the test binary.

**Not published, by construction.** The target is declared only when Gradle runs on a Mac, and
publishing happens on the Linux box, so no macOS publication can be produced there.

**The cost** is in research §2.27: no production code changed; the build and the test source sets did.
