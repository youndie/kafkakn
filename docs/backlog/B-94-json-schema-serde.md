---
id: B-94
title: "A @Serializable type as JSON Schema, in the registry's wire format, read by its official deserializer"
status: done
priority: P1
size: M
stage: stage-21-schema-registry
epic: feature-schema-registry
blocked_by: [B-93]
---

# B-94 — a @Serializable type as JSON Schema, in the registry's wire format, read by its official deserializer

`registry.serializer<Order>(topic, SchemaFormat.Json)`: the JSON Schema generated from `Order`'s descriptor is
registered once, and each value is the wire format around `Json.encodeToString`. Decoding reads the id, checks it
names a schema for this type, and decodes.

- AC: bytes kafkakn writes are read by Confluent's `KafkaJsonSchemaDeserializer` in the harness, and bytes it writes
  are decoded by kafkakn, on both arms.
- AC: the generated schema for a type with nested classes, lists, nullable and default fields is what the registry
  accepts, and a record that does not match it is refused before it is sent.
- **From B-93:** the generated schema must be closed (`"additionalProperties": false`). An open one cannot gain even
  an optional property under `BACKWARD`, which is refused as `OPTIONAL_PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL`.
- Anchors: `kafkakn-schema-registry/`.

## Findings (2026-09-28)

- `registry.jsonSchemaSerde<T>(subject)`: a draft-07 JSON Schema generated from `T`'s descriptor, closed, requiring
  what has no default, with nullable fields admitting `null`, enums as `enum`, lists as `array`, and string-keyed maps
  as `additionalProperties`. Polymorphic, contextual and recursive types are refused when the serde is made.
- *Measured*, `ci/b-94/run.sh`: Confluent's `KafkaJsonSchemaDeserializer` with `json.fail.invalid.schema=true` read
  3 of 3 records kafkakn encoded, equal to their JSON, on both arms. kafkakn decoded the 3 records
  `KafkaJsonSchemaSerializer` wrote under the generated schema into the same values, on both arms.
- **One AC was met another way.** *"A record that does not match [the schema] is refused before it is sent"* cannot
  happen for a value of `T`: the schema is generated from `T` itself, so every value matches. What is refused is what
  can go wrong: a type with no schema here, when the serde is made, and bytes not in the wire format, or a schema id
  that is not JSON Schema, when decoding.
- **Mutant: the id in little-endian order.** This module's own round trip still passed, because the mistake is
  symmetric. The oracle caught it both ways on both arms: *"the deserializer refused kafkakn's bytes"*, and
  `JsonSchemaOracleTest` failed to decode Confluent's.
