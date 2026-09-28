---
id: B-99
title: "Publish kafkakn-schema-registry beside kafkakn-core"
status: done
priority: P2
size: S
stage: stage-21-schema-registry
epic: feature-schema-registry
blocked_by: [B-94]
---

# B-99 — publish `kafkakn-schema-registry` beside `kafkakn-core`

The publish workflow and `ci/publish/run.sh` name `kafkakn-core`'s three coordinates only. A second module must be
published under the same version, checked coordinate by coordinate, and resolved back from the network by a build
that knows only a coordinate and a URL, as B-12 does for the core.

- AC: `kafkakn-schema-registry{,-jvm,-linuxx64}` are published with each numbered version. The preflight covers
  them (the token's route, and the version unpublished). `verify-published.sh` compiles against them.
- Anchors: `.github/workflows/publish.yaml`, `ci/publish/`.

## Findings

- **Done, 2026-09-29, the half that can run without the credentials.** The module has `sborka.publish` and the local
  repository. `ci/publish/run.sh` on the Linux box published and named all six coordinates, each with its `.module` and
  `.pom`, every file named after the version; the downstream probe, which knows only a coordinate and a URL, compiled on
  `jvm`, `linuxX64` and the common metadata against both modules (`RegistryProbe.kt` names the registry, a subject and
  both serdes); and it failed against an empty repository.
- **Mutant:** `run.sh` without the module's publish task reported `MISSING kafkakn-schema-registry`, `-jvm`,
  `-linuxx64` and exited 1.
- **The upload half runs on the next publish the owner starts** (`publish` is `workflow_dispatch` only). The preflight
  needs no change: it asks the token about every coordinate the local publication holds, so a token whose route does
  not cover the new module refuses before anything is written. `verify-published.sh` compiles the same probe from the
  network. Neither has run for this module yet.
- **Corrected on the way, because publishing makes it read:** the service document and the README said a native
  caller reaches HTTPS through `HttpClient(Curl)`; B-98 found that such a binary does not link beside `kafkakn-core`.
  Both now say HTTPS from native is B-98's open question. The README's "Schema Registry: not planned" row predated
  stage 21.
