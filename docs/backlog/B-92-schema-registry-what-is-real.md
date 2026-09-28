---
id: B-92
title: "Schema Registry: what is real on both targets, measured before anything is built"
status: done
priority: P1
size: M
stage: stage-21-schema-registry
epic: feature-schema-registry
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

## Findings (2026-09-28)

Everything the item asked is answered in the research document, §2.29, with addresses. In short: JSON Schema and
Protobuf through kotlinx on both targets (`ProtoBufSchemaGenerator` is common code, experimental); Avro is out, since
its Kotlin library is JVM-only; the wire format is magic 0 and a 4-byte id, plus message indexes for Protobuf; the
oracle is Confluent's Apache-2.0 serializers in a harness; the registry fixture runs; the code goes in a module of
its own. And one finding that changes the next items: **Ktor's CIO client has no TLS on Kotlin/Native.**

## Question for the owner

A registry served over HTTPS cannot be reached from native through CIO. Which transport?

1. **The module takes a Ktor `HttpClient`, with CIO as the default** (recommended). The module depends on
   `ktor-client-core` and CIO, which covers HTTP on both arms and HTTPS on the JVM. A native caller who needs HTTPS
   passes `HttpClient(Curl)` and takes libcurl as a runtime dependency by their own choice. The suite runs HTTPS on
   native through Curl, as a test dependency only. kafkakn's own binaries gain nothing.
2. **Curl by default on native.** HTTPS works out of the box. Every native binary that uses the module then needs
   libcurl on the target, and its `ldd` grows by libcurl and whatever TLS library that libcurl brings.
3. **HTTPS written here over the OpenSSL already linked into the native bundle.** No new runtime dependency and the
   same `ldd`. But it is an HTTP client and certificate verification of this project's own, the kind of
   security-critical code the rest of kafkakn has avoided writing by delegating to the clients underneath.

## Decision (the owner, 2026-09-28)

**Option 1.** The module takes a Ktor `HttpClient`, with CIO as the default. A native caller who needs HTTPS passes
`HttpClient(Curl)`. HTTPS on both arms is [B-98](B-98-https-to-the-registry.md).
