---
id: B-44
title: "linuxArm64 binaries need glibc 2.25 because one weak OpenSSL symbol is bound"
status: done
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

## Decision (2026-09-25, the owner): lower it to 2.17

Asked interactively, with three options: lower it, accept 2.25, or later. Lowered, for parity with x64.
The acceptance above is the one that counts: the binary starts on `manylinux2014_aarch64` and produces
to the broker from there.

## Iteration 1 (2026-09-25): lowered to 2.17, and run there

- **The fix is a Configure option, not a patch.** The aarch64 bundle configures OpenSSL with
  `--with-rand-seed=devrandom`. The default `os` source compiles both the `getrandom` path, which declares
  `getentropy` weak, and `/dev/urandom`. `devrandom` keeps only the second, and OpenSSL still waits for
  `/dev/random` to be ready before it trusts `/dev/urandom`. x64 keeps `os`: on its 2.19 sysroot the
  weak reference stays empty, and its bundle is unchanged. `ci/librdkafka/build.sh` on the Linux box
  still passes its checks.
- **The old guard could not see it.** The `nm -u` pair in `inside.sh` looks for `U`, and a weak
  reference is `w`. A check on aarch64 now refuses any archive that references `getentropy` in either
  form. Its control: it rejected the previous `libcrypto.a`, which had one reference.
- **AC: the floor the runner pins is what the documents state.** `ci/b-39/run.sh` pins `GLIBC_2.17` for
  both the downstream and the test binary, and README, the service document and research §2.28 say
  2.17.
- **AC: the binary starts on `manylinux2014_aarch64` and produces to the broker from there.** It runs on
  glibc 2.17 (the same binary stopped there before with `version 'GLIBC_2.25' not found`), sends 50
  records, and the broker's consumer counts all 50. The same run keeps everything B-39 showed: the suite
  passes on arm64, 93 of 93, TLS included, with the new seed source, and agrees with the JVM arm on all
  17 observations.
- **A wording corrected on the way.** The build file and the research said that CI has no arm64 Docker.
  What is true is that this repository's CI does not run one. GitHub's `ubuntu-24.04-arm` runner is free
  for a public repository and would.
