---
id: B-99
title: "Publish kafkakn-schema-registry beside kafkakn-core"
status: wip
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
