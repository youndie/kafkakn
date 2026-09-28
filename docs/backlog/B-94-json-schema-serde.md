---
id: B-94
title: "A @Serializable type as JSON Schema, in the registry's wire format, read by its official deserializer"
status: open
priority: P1
size: M
stage: stage-21-schema-registry
epic: feature-produce-a-record
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
