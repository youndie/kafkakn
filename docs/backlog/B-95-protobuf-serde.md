---
id: B-95
title: "A @Serializable type as Protobuf, in the registry's wire format, read by its official deserializer"
status: open
priority: P2
size: M
stage: stage-21-schema-registry
epic: feature-schema-registry
blocked_by: [B-93]
---

# B-95 — a @Serializable type as Protobuf, in the registry's wire format, read by its official deserializer

The same shape as B-94 with `kotlinx-serialization-protobuf`. The `.proto` comes from `ProtoBufSchemaGenerator`,
and the message-index list goes before the payload.

- AC: bytes kafkakn writes are read by Confluent's `KafkaProtobufDeserializer` in the harness, and the reverse, on both
  arms.
- AC: what `ProtoBufSchemaGenerator` cannot express, if anything, is refused when the serializer is made, not when a
  record is sent.
- Anchors: `kafkakn-schema-registry/`.
