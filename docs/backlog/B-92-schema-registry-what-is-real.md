---
id: B-92
title: "Schema Registry: what is real on both targets, measured before anything is built"
status: open
priority: P1
size: M
stage: stage-21-schema-registry
epic: feature-produce-a-record
blocked_by: []
---

# B-92 — schema Registry: what is real on both targets, measured before anything is built

The owner asked on 2026-09-28 for Schema Registry support, and for an elegant one: a `@Serializable` type in, bytes
the registry's other clients can read out. Before any code, the questions below are answered by measurement or by
reading the artefact at its pinned version. Each answer goes into the research document with its address, and
the items after this one are adjusted to what is found.

- **Formats.** JSON Schema through `kotlinx-serialization-json`, with the schema generated from the
  `SerialDescriptor`. Protobuf through `kotlinx-serialization-protobuf`, whose `ProtoBufSchemaGenerator` writes a
  `.proto` from a descriptor. Avro has no Kotlin Multiplatform library known here, and this item says whether one
  exists at a version that builds for `linuxX64`. The hypothesis is JSON Schema and Protobuf now, Avro out.
- **The wire format**, read in Confluent's own serializer source: magic byte 0, a 4-byte schema id, and for Protobuf
  the message-index list before the payload.
- **The oracle.** The registry's official serializers on the JVM (`KafkaJsonSchemaSerializer`,
  `KafkaProtobufSerializer`, and the deserializers) in a harness program of their own, like
  `ci/harness/Records.java`. Bytes kafkakn writes must be read by them, and theirs by kafkakn. Their licence and
  the Maven repository they come from are recorded, and they stay test-only.
- **The registry fixture.** An image beside the broker, pinned, and what `broker.sh up` needs to start it.
- **HTTP on native.** Which Ktor client engine speaks HTTP, and HTTPS, on `linuxX64`, and what it adds to the
  binary and its `ldd`. The registry is plain HTTP in the fixture and HTTPS in real use.
- **The module.** A separate `kafkakn-schema-registry`, so that `kafkakn-core` does not gain an HTTP client.
- AC: every point above answered in `docs/research/research-architecture.md` with the address it was read at or the
  run it was measured in, and the items after this one adjusted to it.
- Anchors: `docs/research/research-architecture.md`.
