---
id: B-96
title: "A schema the subject's compatibility refuses is one typed exception, before any record is sent"
status: done
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

## Findings

- **Done, 2026-09-28.** `IncompatibleSchemaException` (a `SchemaRegistryException`, status 409, with the subject) and
  `register()` on both serdes. `SchemaEvolutionTest`, five tests, green on `jvm` and `linuxX64` against the fixture
  registry at `BACKWARD`.
- **What the registry says**, measured: a required field removed from a closed JSON Schema is
  `PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL`; from the generated proto2 `.proto`, where a field with no default is
  `required`, it is `REQUIRED_FIELD_REMOVED`. An optional field added is accepted in both formats, and the new type
  decodes what the old one wrote.
- **What an open schema would have cost:** its first optional property is refused
  (`OPTIONAL_PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL`), so a type with an open schema could not evolve at all under the
  default. B-94's closed schemas are what makes the accepted case possible.
- **Every version keeps one serial name** (`@SerialName("Item")`) in the test, as a type does when it evolves in a
  service. A renamed class is a renamed Protobuf message, which is a different question and not measured here.
- **Red before green, which is the mutant:** with the class and `register()` in place and the 409 not yet mapped, the
  three refusal tests failed by name on `jvm`, each with `SchemaRegistryException` where the type was expected.
