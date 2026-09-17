---
id: B-03
title: "The C bundle, built against glibc 2.17, consumed by cinterop"
status: wip
priority: P0
size: M
stage: stage-0-it-builds
blocked_by: [B-01]
---

# B-03 — The C bundle, built against glibc 2.17, consumed by cinterop

librdkafka, OpenSSL, zlib and zstd as static archives, built in `manylinux2014` (glibc 2.17) so that
nothing references a symbol newer than the 2.19 Kotlin/Native ships
([research §1.3](../research/research-architecture.md)), then handed to cinterop.

- **The decision and its reason.** [D4](../research/research-architecture.md): the old-glibc image
  rather than three `-Xoverride-konan-properties` keys. The argument is where the breakage lands — an
  unstable toolchain key breaks on a Kotlin upgrade at a moment nobody chose; a patch against a
  pinned librdkafka breaks when this project bumps librdkafka, deliberately.
- The rejected alternative is the override route. It was measured and it works; it is not chosen.
- Not covered: `linuxArm64` (the same script with a second image), and reporting the patch upstream —
  nothing from this project goes upstream.

- AC: a script produces the four archives into a cache **outside** the source tree and prints, for
  each, whether any post-2.19 symbol remains — asking the pair (defined anywhere in this archive
  versus referenced and undefined) with anchored names, because `nm -u` on an archive counts
  intra-archive references and reads as a false dependency.
- AC: the one-line patch to `rdrand.c` lives as a **file** in this repository, not as a `sed` inside
  a script, so that a conflict on a librdkafka bump is visible.
- AC: `configure`'s output is checked to confirm the fallbacks were **selected** (`c11threads`,
  `getentropy`, `strlcpy`), not silently skipped.
- AC: the Kotlin binary links with **no** `-Xoverride-konan-properties` and no extra `-L`, and its
  `ldd` set equals that of a binary built without the Kafka dependency.
- Anchors: `ci/librdkafka/build.sh`, `ci/librdkafka/oldglibc.Dockerfile`,
  `ci/librdkafka/patches/`, `kafkakn-core/src/nativeInterop/cinterop/rdkafka.def`.
