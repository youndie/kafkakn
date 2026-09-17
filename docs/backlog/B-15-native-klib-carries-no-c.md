---
id: B-15
title: "The published native klib does not carry its C dependency"
status: done
priority: P0
size: M
stage: stage-3-usable-by-others
blocked_by: [B-12]
---

# B-15 — The published native klib does not carry its C dependency

**A stranger cannot link `kafkakn-core-linuxx64` today.** Found by [B-13](B-13-external-consumer-acceptance.md)
on the first run of a consumer that is not part of this build: it resolved the published artefact,
compiled against it, and then the link failed with

```
ld.lld: error: undefined symbol: rd_kafka_produceva
ld.lld: error: undefined symbol: rd_kafka_poll
ld.lld: error: undefined symbol: rd_kafka_error_code
```

and six more. The same source builds and runs on the **jvm** arm from the same published version, so
this is not the API: it is the native artefact not carrying the thing it is a binding to.

**Why it was invisible from inside.** The archives are named in `kafkakn-core/build.gradle.kts`, as
`linkerOpts` on **this project's own test binaries**, pointing at absolute paths in
`~/.cache/kafkakn`. The suite therefore links, every item so far has been green, and nothing in the
published klib says a word about `librdkafka-static.a`. `rdkafka.def` carries `includeDirs` and no
`staticLibraries`, so the klib is a set of bindings with no implementation attached.

- **The decision to take.** Which of these the library does, and it is a real choice with a cost
  either way:

  1. **`staticLibraries` + `libraryPaths` in `rdkafka.def`** — cinterop copies the archives *into*
     the klib, and a downstream link then works with no configuration at all. Self-contained, which
     is the whole argument of a single-binary story; the klib grows by the bundle (the measured
     delta is 8.26 MB of which 5.71 MB is TLS+zlib+zstd,
     [research §1.2](../research/research-architecture.md)).
  2. **Publish the C bundle as its own artefact** and have consumers add it. Keeps the klib small,
     adds a second coordinate and a step every consumer must not forget.
  3. **Document that a consumer supplies `linkerOpts`.** Free here, and it makes every consumer
     solve the problem this project exists to have solved. It is also unverifiable: the only thing
     that would catch a broken instruction is a consumer, which is B-13.

  Recommended: **1**, because the alternative to a self-contained klib is a README nobody reads at
  link time, and because the size is the size the binary was always going to have — it is the same
  archives, moved from this repository's build file into the artefact.
- The rejected alternative is `linkerOpts` inside `rdkafka.def`. Absolute paths cannot travel, and
  `-lrdkafka` means the consumer's machine must already have a librdkafka — which is the one thing
  the old-glibc build exists to avoid ([D4](../research/research-architecture.md)).
- Not covered: `linuxArm64`, and any change to how the bundle itself is built.

- AC: the consumer in `ci/consumer`, with **no linker configuration of its own**, links and runs a
  `linuxX64` binary against the published artefact.
- AC: the same consumer is shown **failing** against the artefact published before the fix, so the
  check is known to catch what it exists for.
- AC: whatever the klib grows by is measured and written down, not estimated.
- Anchors: `kafkakn-core/src/nativeInterop/cinterop/rdkafka.def`, `kafkakn-core/build.gradle.kts`,
  `ci/consumer/build.gradle.kts`.

## Outcome — 2026-09-17

`staticLibraries = librdkafka-static.a libssl.a libcrypto.a libz.a libzstd.a` in `rdkafka.def`, with
the two directories passed from Gradle as `-libraryPath` so the file stays machine-independent.
Option 1, as recommended.

**The same consumer, the same version string, two repositories:**

| | the server, pre-fix | the candidate, post-fix |
|---|---|---|
| `linkReleaseExecutableLinuxX64` | **14 undefined symbols** | a 9.6 MB binary |
| running it | nothing to run | 50/50 records, headers intact, both arms |
| `…-cinterop-rdkafka.klib` | 76 952 bytes | 11 280 633 bytes |

That A/B is the positive control, and it cost nothing: the server still held the artefact this item
exists to condemn. An earlier attempt at a control — pinning the consumer to the previous
**timestamped** snapshot — failed with `Could not find …:0.1.0-20260917.151134-1`, which is red for
the wrong reason and was not counted.

**The estimate in the item above was the wrong quantity.** It quoted 8.26 MB from research §1.2 —
that is the measured *binary* delta after the linker discards what is unused, not the size of the
archives. The archives are 46 MB on disk and 11 MB inside the klib, which is a zip.

**The guard is a removal.** `linkerOpts` is gone from `build.gradle.kts`, so this project's test
binaries link exactly the way a stranger's does; an artefact that cannot be linked now fails the
suite. While they were there, eleven items passed over a published artefact nobody could use.
