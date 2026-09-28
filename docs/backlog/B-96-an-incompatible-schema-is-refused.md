---
id: B-96
title: "A schema the subject's compatibility refuses is one typed exception, before any record is sent"
status: open
priority: P2
size: S
stage: stage-21-schema-registry
epic: feature-schema-registry
blocked_by: [B-94]
---

# B-96 — a schema the subject's compatibility refuses is one typed exception, before any record is sent

Schema evolution is what the registry is for. A type changed in a way the subject's compatibility level
(`BACKWARD` by default) refuses must fail when its serializer registers, with the registry's reason, and never
produce a record the other side cannot read.

- AC: adding an optional field is accepted, and a required field removed under `BACKWARD` is refused, on both arms,
  with the registry's 409 and its message in one kafkakn type.
- **From B-93:** with open JSON Schemas, even an added optional property is refused under `BACKWARD`. The item measures
  evolution on the closed schemas B-94 generates, and states what an open one would have cost.
- Anchors: `kafkakn-schema-registry/`.
