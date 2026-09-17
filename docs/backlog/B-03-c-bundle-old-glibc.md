---
id: B-03
title: "The C bundle, built against glibc 2.17, consumed by cinterop"
status: done
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

---

## Findings — 2026-09-17

**Done.** The bundle builds against glibc 2.17, links into a Kotlin/Native binary with **no**
toolchain override, and adds nothing to what that binary resolves at runtime.

| | |
|---|---|
| `configure` selected | `WITH_SSL=y`, `WITH_ZLIB=y`, `WITH_ZSTD=y`; **no `WITH_C11THREADS`**, so the bundled tinycthread links |
| post-2.19 symbols | `mtx_lock`, `thrd_create`, `cnd_signal` **defined inside** `librdkafka-static.a`; no archive expects one from libc |
| the binary | `linuxX64Test` links and **runs**: `CinteropLinkTest` calls `rd_kafka_version_str()` and gets `2.…` |
| `ldd` | **identical** to the Kafka-free binary from the same build — 11 entries, all glibc family |
| overrides | `-Xoverride-konan-properties` outside comments: **0** |

### The comparison is against a real Kafka-free binary, not an allowlist

`-Pkafkakn.noKafkaC` drops the cinterop, the linker options **and** the source directory holding
`CinteropLinkTest`, so the baseline genuinely contains none of it. The script asserts both halves —
the cinterop test present in the first build's results and **absent** from the second — and that the
two binaries have different md5s. Otherwise "identical `ldd`" could mean the switch did nothing.

### Two guards against the route changing quietly

`ci/librdkafka/patches/0001-guard-sys-random-include.patch` is a **file** rather than a `sed`: on a
librdkafka bump, a patch that no longer applies is a conflict somebody sees, while a `sed` that no
longer matches is a silent no-op followed by a confusing compile error.

`inside.sh` **fails** if `WITH_C11THREADS` ever appears. The route depends on librdkafka not finding
C11 threads and falling back to what it bundles; if a base image grows them, `mtx_lock` returns as a
libc dependency and only the symbol table would show it.

### Three silent no-ops in one item, and the last one reached `main`

1. The check for "no override is used" was `grep -c` and counted **1** — its own comment saying that
   none is used. Fifth unanchored pattern in this project to read documentation as code; the
   comment-stripping is now `ci/lib/token_in_code.py` instead of being improvised again.
2. Two attempts to fix that died without running — one on the shell parsing the command before
   Python started, one on failing to find text the first was supposed to have written — and both
   were reported as applied.
3. **This item's own closure was lost the same way.** The commit-message hook rejected the whole
   command for a 73-character subject, so the status change and the index regeneration that were
   chained in front of the commit never ran either; retrying only the commit merged the work with
   the item still `wip`. The hook intercepts **before** execution, so anything chained after a bad
   message silently does not happen.

The habit that catches all three is the same and is now the rule here: **read the file back instead
of believing the edit**, and check the item's status on `main` after a merge.

### Not covered

`linuxArm64` (the same script with a second image), `macosArm64`, `musl`, and reporting the patch
upstream — nothing from this project goes upstream. No release binary: the test executable carries
the link today, and [B-07](B-07-native-actual.md) replaces it with a real producer.
