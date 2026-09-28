---
id: feature-schema-registry
title: A @Serializable type in the registry's wire format
type: feature
status: active
owner: unassigned
involved_services:
  - kafkakn-schema-registry
  - test-broker
client_entries: []
api: []
tags: [schema-registry, serialization]
---

# A @Serializable type in the registry's wire format

## 1. Overview

A service writes records that other services, in any language, read through a Schema Registry, and reads theirs.
kafkakn does it through kotlinx.serialization: a `@Serializable` type in, bytes in the registry's wire format out,
with the schema generated from the type and registered once
([stage 21](../../backlog.md); research §2.29).

**Built and measured:** the registry client ([B-93](../backlog/B-93-a-registry-client.md)) and JSON Schema
([B-94](../backlog/B-94-json-schema-serde.md)) and Protobuf ([B-95](../backlog/B-95-protobuf-serde.md)), on both
arms. ***Target***: evolution (B-96), HTTPS on native (B-98), and the module's publication (B-99).

## 2. Business rules

- The oracle is the registry's own serializers on the JVM, not this module's round trip. A symmetric mistake, such as
  the id in the wrong byte order, passes a round trip and fails the oracle, as a mutant showed.
- A generated JSON Schema is closed (`additionalProperties: false`), so that a type can gain an optional field under
  the registry's default `BACKWARD` compatibility (B-93).
- A type with no schema here (polymorphic, contextual, recursive) is refused when its serde is made, not at the first
  record.
- Written as magic `0x00` and a 4-byte big-endian id. Read the same way, and anything else is refused by name.
- Protobuf adds the message indexes after the id, always `[0]`: the generated `.proto` has the root type's message
  first. A record naming any other message is refused, because this reader's type is the root.

## 3. Code anchors

| What | Where |
|---|---|
| the client, the schema generator and the serde | `kafkakn-schema-registry/src/commonMain/kotlin/io/github/youndie/kafkakn/schema/` |
| the suite | `kafkakn-schema-registry/src/commonTest/kotlin/io/github/youndie/kafkakn/schema/` |
| the oracle harness, Confluent's serializers with no kafkakn | `ci/b-94/oracle/` |

## 4. Scenarios (BDD / test cases)

### Scenario: A schema is registered once and read back by its id
* **Given:** the fixture registry.
* **When:** a schema is registered twice under one subject, then under a second subject, then read by id with a fresh
  client.
* **Then:** one request for the first two, two across the two subjects, and the same schema read back, on both arms.
* **Automated:** `SchemaRegistryTest`, counted in the registry's own request log by `ci/b-93/run.sh`.

### Scenario: A @Serializable type round-trips as JSON Schema
* **Given:** `Order`, with nested classes, a list, a nullable field, an enum with a default, and a map.
* **When:** it is encoded and decoded through `registry.jsonSchemaSerde<Order>(valueSubject(topic))`.
* **Then:** the bytes start with magic 0 and the id the registry gave the generated schema, and decode to the same
  value. The schema is closed and requires only what has no default.
* **Automated:** `JsonSchemaSerdeTest`, in the suite.

### Scenario: The registry's own serializers read kafkakn's bytes, and kafkakn reads theirs
* **Given:** three `Order`s.
* **When:** kafkakn encodes them and Confluent's `KafkaJsonSchemaDeserializer` reads them, validating against the
  schema; and Confluent's `KafkaJsonSchemaSerializer` writes them under the generated schema, and kafkakn decodes them.
* **Then:** equal both ways, on both arms.
* **Automated:** `JsonSchemaOracleTest` with `ci/b-94/oracle`, by `ci/b-94/run.sh`.

### Scenario: A @Serializable type round-trips as Protobuf
* **Given:** the same `Order`.
* **When:** it is encoded and decoded through `registry.protobufSerde<Order>(valueSubject(topic))`.
* **Then:** the bytes are magic 0, the id the registry gave the generated `.proto`, the message index `0x00`, and the
  payload, and decode to the same value. The root type is the schema's first message. A sealed type is refused when
  the serde is made.
* **Automated:** `ProtobufSerdeTest`, in the suite.

### Scenario: The registry's own Protobuf serializers read kafkakn's bytes, and kafkakn reads theirs
* **Given:** three `Order`s.
* **When:** kafkakn encodes them and Confluent's `KafkaProtobufDeserializer` reads them; and Confluent's
  `KafkaProtobufSerializer` writes them as messages of the generated `.proto`, and kafkakn decodes them.
* **Then:** equal both ways, on both arms, where an empty list or map equals an absent one: Protobuf gives such a
  field no presence.
* **Automated:** `ProtobufOracleTest` with `ci/b-94/oracle`, by `ci/b-95/run.sh`.

## 5. Out of scope

Avro, which has no Kotlin Multiplatform library (B-92). Schema ids by GUID or in a record header, which the registry
offers and does not default to. Polymorphic types, in either format, until a caller needs one. A Protobuf record
naming a message other than the schema's first.
