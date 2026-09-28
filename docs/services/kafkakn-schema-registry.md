---
id: kafkakn-schema-registry
title: kafkakn-schema-registry — Schema Registry support
type: service
repo_url: https://github.com/youndie/kafkakn
module: kafkakn-schema-registry
tech_stack: [Kotlin Multiplatform, Ktor client, kotlinx.serialization]
owner: unassigned
depends_on:
  - test-broker
publishes:
  - io.github.youndie.kafkakn:kafkakn-schema-registry (reposilite, numbered 0.1.0.<run>, beside kafkakn-core)
---

# kafkakn-schema-registry

A client for a Schema Registry's REST API, and, from stage 21's later items, serializers that put a `@Serializable`
type into the registry's wire format. It is a module of its own so that `kafkakn-core` gains no HTTP client
([B-92](../backlog/B-92-schema-registry-what-is-real.md)). It is published beside `kafkakn-core`, under the same
numbered version and three coordinates of its own ([B-99](../backlog/B-99-publish-the-schema-registry-module.md)).

## Shape

- **`SchemaRegistry(url, httpClient = null)`** registers a schema under a subject and reads one back by id. Both
  answers are cached, so a record costs no request once its schema is known. Subjects follow `TopicNameStrategy`
  (`valueSubject`, `keySubject`).
- **The transport is a Ktor `HttpClient`, CIO by default.** CIO speaks HTTP on both targets and HTTPS on the JVM.
  It has no TLS on Kotlin/Native (measured in B-92). **HTTPS from native is an open question**
  ([B-98](../backlog/B-98-https-to-the-registry.md)): `HttpClient(Curl)` works in a binary without `kafkakn-core`, and
  a binary with both does not link, because Ktor's Curl engine carries its own static OpenSSL (3.6.3) beside the one
  in kafkakn's C bundle (3.0.13): `ld.lld: duplicate symbol`. Until it is decided, a native service reaches a registry
  over HTTP.
- **A refusal is `SchemaRegistryException`,** with the HTTP status and the registry's own error code: `42201` for a
  schema it cannot read, `40403` for an id it does not have. A registration the subject's compatibility refuses (409)
  is its subclass `IncompatibleSchemaException`, with the subject and the registry's reason
  ([B-96](../backlog/B-96-an-incompatible-schema-is-refused.md)). Each serde's `register()` meets it at startup; its
  first `encode` meets it otherwise, before a record exists.
- *Measured* (`ci/b-93/run.sh`, both arms, counted in the registry's own request log): one schema registered twice
  is one request, and under two subjects it is two. A mutant that ignored the cache was caught by that count on both
  arms.

- **`registry.jsonSchemaSerde<T>(subject)`** ([B-94](../backlog/B-94-json-schema-serde.md)) generates a closed
  draft-07 JSON Schema from `T`'s descriptor when it is made, registers it on the first `encode`, and frames `Json`'s
  output in the wire format. `decode` reads the id, checks the schema it names is JSON Schema, and decodes with the
  reader's type. *Measured* (`ci/b-94/run.sh`): Confluent's `KafkaJsonSchemaDeserializer`, validating, reads kafkakn's
  bytes, and kafkakn reads what `KafkaJsonSchemaSerializer` writes, on both arms.
- **`registry.protobufSerde<T>(subject)`** ([B-95](../backlog/B-95-protobuf-serde.md)) registers the proto2 `.proto`
  that kotlinx's `ProtoBufSchemaGenerator` writes for `T`, and frames `ProtoBuf`'s output with the message indexes
  `[0]`. The overload that takes a `ProtoBuf` is `@ExperimentalSerializationApi`, as kotlinx marks it; the default one
  asks nothing of the caller. *Measured* (`ci/b-95/run.sh`): Confluent's `KafkaProtobufDeserializer` reads kafkakn's
  bytes, and kafkakn reads what `KafkaProtobufSerializer` writes, on both arms.

## Quirks

- **Under `BACKWARD`, the registry's default, a property added to an open JSON Schema is incompatible**
  (`OPTIONAL_PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL`, found by B-93's first control). A schema without
  `"additionalProperties": false` can therefore not gain even an optional field. B-94's generated schemas have to be
  closed for a type to evolve. B-96 measured both: a closed schema gains an optional field; an open one is refused
  (`SchemaEvolutionTest.an_open_json_schema_cannot_gain_an_optional_property`).
- **A sealed root leaves no mark in the generated `.proto`.** The generator writes a `KotlinxSerializationPolymorphic`
  message only for polymorphism below the root, so a check on the schema's text let a sealed type through (B-95).
  The serde walks the descriptor instead.
- **The registry leaves `schemaType` out for Avro,** its default, so a schema read back without one is Avro, which
  this module does not read. It says so rather than guessing.

## Code anchors

| What | Where |
|---|---|
| the client | `kafkakn-schema-registry/src/commonMain/kotlin/io/github/youndie/kafkakn/schema/SchemaRegistry.kt` |
| its suite | `kafkakn-schema-registry/src/commonTest/kotlin/io/github/youndie/kafkakn/schema/` |
| the runner that counts requests at the registry | `ci/b-93/run.sh` |
| the JSON Schema generator and serde | `kafkakn-schema-registry/src/commonMain/kotlin/io/github/youndie/kafkakn/schema/JsonSchemaGenerator.kt`, `JsonSchemaSerde.kt` |
| the Protobuf serde | `kafkakn-schema-registry/src/commonMain/kotlin/io/github/youndie/kafkakn/schema/ProtobufSerde.kt` |
| the oracle harness, both formats | `ci/b-94/oracle/` |
| the fixture registry | `ci/broker/docker-compose.yml` |
