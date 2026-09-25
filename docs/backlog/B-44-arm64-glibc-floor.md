---
id: B-44
title: "linuxArm64 binaries need glibc 2.25 because one weak OpenSSL symbol is bound"
status: open
priority: P3
size: S
stage: stage-9-targets
---

# B-44 — the arm64 glibc floor is 2.25, not 2.17

Found by [B-39](B-39-linux-arm64.md), measured rather than read. A linuxArm64 binary that links kafkakn
needs glibc 2.25. On `manylinux2014_aarch64` (glibc 2.17) it does not start:
`version 'GLIBC_2.25' not found`. The x64 floor is 2.17 ([B-16](B-16-readme-says-what-was-measured.md)).

**The cause is one symbol.** `readelf --dyn-syms` lists exactly one reference above 2.17:
`getentropy@GLIBC_2.25`, `WEAK UND`, from `libcrypto.a`. OpenSSL declares `getentropy` weak and calls
it only when it resolves. Kotlin/Native's aarch64 sysroot is glibc 2.25, which has it, so the link
binds it. The x64 sysroot is 2.19, which does not, so on x64 the reference stays empty and costs
nothing. lld did not mark the version requirement weak, although every reference to it is, so the
loader enforces it.

- **The decision this item asks for:** whether 2.25 is acceptable for arm64. It rules out CentOS 7 and
  Amazon Linux 2 on arm64, and admits Debian 10+, Ubuntu 18.04+ and RHEL 8+. If it is not acceptable,
  lower it. The candidates, none tried yet:
  - build OpenSSL so it does not reference `getentropy` at all. It then seeds from `getrandom(2)` or
    `/dev/urandom`, which it already falls back to.
  - mark the version need weak when linking.
- Not covered: publishing linuxArm64. That is a separate decision, and this floor is an input to it.

- AC: the floor `ci/b-39/run.sh` pins is what the documents state. If it is lowered, the binary starts
  on `manylinux2014_aarch64`, and the downstream build still produces to the broker from there.
- Anchors: `ci/b-39/run.sh`, `ci/librdkafka/inside.sh`.
