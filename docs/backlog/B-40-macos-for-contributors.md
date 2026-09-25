---
id: B-40
title: "macOS, so a contributor can run the native arm without the Linux box"
status: wip
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
