---
id: B-95
title: "A @Serializable type as Protobuf, in the registry's wire format, read by its official deserializer"
status: done
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

## Findings

- **Done, 2026-09-28.** `ProtobufSerde` and `registry.protobufSerde<T>(subject)`. `ci/b-95/run.sh` on the Linux box:
  the registry's deserializer read kafkakn's three records as written, and kafkakn read the three the registry's
  serializer wrote, on both arms. The module suite is green on `jvm` and `linuxX64`.
- **What the generator cannot express is refused when the serde is made:** a polymorphic or contextual type, anywhere
  in the tree. The first version looked for `KotlinxSerializationPolymorphic` in the schema's text, and its test was
  red: for a sealed root the generator writes no such message. The check reads the descriptor now.
- **The first oracle run was red on an empty list.** Protobuf gives a repeated field no presence, so the registry's
  JSON printer leaves it out, and `Json` leaves out a map equal to its default. The comparison drops empty lists and
  maps on both sides. Making the printer print them only moved the difference to the other side.
- **Mutant:** the `0x00` message index left out of `encode`. Killed by
  `ProtobufSerdeTest.a_serializable_type_round_trips_through_the_registry_as_protobuf` (jvm) and by the deserializer
  refusing the bytes on both arms in `ci/b-95/run.sh`.
- The overload that takes a `ProtoBuf` is `@ExperimentalSerializationApi`, because kotlinx marks the parameter so; the
  default one needs no opt-in.
