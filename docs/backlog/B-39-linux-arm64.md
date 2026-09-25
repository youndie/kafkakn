---
id: B-39
title: "linuxArm64: settle H5 — does a second native target cost a matrix row and no code?"
status: done
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

## Iteration 1 (2026-09-25) — stopped on the environment, and a question

**What was read, before anything was built.** H5 says the second target costs "a matrix row and no
code". Four places already name x86_64 or assume one architecture, so the claim does not hold as
written:

| Where | What is x86_64 |
|---|---|
| `ci/librdkafka/oldglibc.Dockerfile` | `FROM quay.io/pypa/manylinux2014_x86_64` |
| `ci/librdkafka/inside.sh` | OpenSSL `./Configure linux-x86_64` |
| `ci/librdkafka/build.sh`, `kafkakn-core/build.gradle.kts` | one bundle path, `~/.cache/kafkakn/librdkafka-<version>`, with no architecture in it: an arm64 bundle would overwrite the x64 one |
| `kafkakn-core/build.gradle.kts` | the cinterop is declared on `linuxX64 { }` only |

Each is small. Together they are code in the build, which is not what H5 said, and research D6 should
say so whatever this item decides.

**What stopped it.** The item asks for the binary to *run* ("run, not only link", B-15's lesson), and
the build box cannot run an arm64 binary today. `/proc/sys/fs/binfmt_misc` has no `qemu-aarch64`
entry, and no qemu-user is installed. Building the C bundle in `manylinux2014_aarch64` needs that
emulation too. Registering it means running a privileged container that changes the host kernel's
binfmt table (`tonistiigi/binfmt --install arm64`; the image is already on the box). That is a
system-level change to the owner's machine, and the loop does not make it on its own.

**The question: how should an arm64 binary be run?** A person decides.

1. **Register arm64 emulation on the build box.** Run
   `docker run --privileged --rm tonistiigi/binfmt --install arm64` once per WSL start, since binfmt
   registrations do not survive a restart. The item then proceeds under emulation, and the write-up
   says so.
2. **A real arm64 host**, if one is available. The xyk bench hosts are borrowed only on request, and
   are IPv6-only.
3. **Cross-build without running.** Use a cross toolchain for the C bundle and a linked but never-run
   binary. This is the alternative the item rejects, so choosing it means changing the item.
4. **Leave linuxArm64 unbuilt for now**, correct H5's "no code" in research D6 with the table above,
   and close the item as `dropped`.

Until then the loop moves on to the next pickable item.

## Decision (2026-09-25, the owner): real arm64, on the Mac's Docker

Asked interactively. Emulation was chosen first, and the registration then ran on the Mac rather than
on the build box. That showed what the four options had missed: Docker Desktop on the Mac **is** an
arm64 Linux host (`docker info`: `aarch64`, kernel `6.12.54-linuxkit`, 4 CPUs, 8 GB). This is option 2,
available all along, with no change to any host. The owner chose it over emulation on WSL.

So the item proceeds as follows:

- The `linuxArm64` Kotlin side is cross-compiled on the build box, as every other native build is.
- The C bundle is built in `manylinux2014_aarch64` on the Mac's Docker. It is the one arm64 build, and
  the build box cannot run it without the emulation this decision avoids.
- The test binary runs in an arm64 container on the Mac, against the broker on the build box, through
  the same tunnel `ci/b-40/run.sh` uses.

The write-up says **real arm64 hardware (Apple silicon), in a linuxkit VM**, not emulation.

## Iteration 2 (2026-09-25): built, published to disk, and run on arm64 hardware

Everything below was measured by `ci/b-39/run.sh`, run on the Mac.

- **AC: the klib carries its C, and a downstream build links with no configuration of its own.**
  `kafkakn-core-linuxarm64` is published to a repository on disk. Its cinterop klib carries
  `librdkafka-static.a libssl.a libcrypto.a libz.a libzstd.a`. `ci/downstream` links from that artefact
  alone, with a Gradle home purged of the group: an AArch64 executable of 9.8 MB. The binary produced 50
  records on arm64, and the broker's own consumer read all 50, with the header.
- **AC: the glibc floor, measured as `ci/b-16/run.sh` measures it.** The floor is `GLIBC_2.25`, not
  2.17. One weak OpenSSL reference, `getentropy`, is bound against Kotlin/Native's aarch64 sysroot.
  Checked by running the binary: on `manylinux2014_aarch64` (glibc 2.17) it stops with `version
  'GLIBC_2.25' not found`. The runner pins the number, and [B-44](B-44-arm64-glibc-floor.md) asks whether
  to lower it.
- **AC: H5 settled in the research, with what it cost.** See research §2.28. In short: no code in the
  library, but code in the build, and three traps, each found by a failure:
  - outline atomics;
  - a cinterop task that did not see its archives as inputs;
  - the floor above.
- **Beyond the AC: the native suite on arm64.** It passes, 93 of 93, against the broker on the Linux box,
  and agrees with the JVM arm on all 17 observations.

**Two assumptions of the suite gave way once it ran on a second machine.** Both are fixed here, because
without the fixes the item could not be verified:
- the log-append scenario read the broker's clock on the test's own clock. It is now bracketed by two
  records the broker stamps. `ci/b-28/run.sh` is green on both Linux arms after the change, and a mutant
  reporting no timestamp is caught by name;
- the native observation writer returned silently when `build/` did not exist. It now creates the
  directory and fails when it cannot write.

**Not done, on purpose:** publishing `linuxArm64`. Its bundle needs an arm64 Docker, which neither CI
nor the build box has, and its floor is still an open question (B-44).
