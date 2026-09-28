package io.github.youndie.kafkakn.schema

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

/**
 * [B-95](../../../../../../../docs/backlog/B-95-protobuf-serde.md): a `@Serializable` type as Protobuf in the registry's
 * wire format, in the suite. What the registry's own serializers make of the bytes is `ProtobufOracleTest`'s, run by
 * `ci/b-95/run.sh`.
 */
class ProtobufSerdeTest {
    @Test
    fun a_serializable_type_round_trips_through_the_registry_as_protobuf() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val registry = SchemaRegistry(registryUrl)
                try {
                    val orders = registry.protobufSerde<Order>(valueSubject("kafkakn-b95-round-$arm-$run"))
                    for (order in SAMPLES) {
                        val bytes = orders.encode(order)
                        assertEquals(0, bytes[0].toInt(), "magic")
                        assertEquals(0, bytes[5].toInt(), "message indexes [0], one zigzag varint 0")
                        assertEquals(order, orders.decode(bytes))
                    }
                } finally {
                    registry.close()
                }
            }
        }

    @Test
    fun the_root_type_is_the_schemas_first_message() {
        val registry = SchemaRegistry(registryUrl)
        try {
            val schema = registry.protobufSerde<Order>(valueSubject("kafkakn-b95-schema")).schema
            val first = Regex("""^message (\w+) \{""", RegexOption.MULTILINE).find(schema)?.groupValues?.get(1)
            assertEquals("Order", first, schema)
            assertTrue("required int64 id" in schema, schema)
        } finally {
            registry.close()
        }
    }

    @Test
    fun a_polymorphic_type_is_refused_when_the_serde_is_made() {
        val registry = SchemaRegistry(registryUrl)
        try {
            assertFailsWith<IllegalArgumentException> {
                registry.protobufSerde<Event>(
                    valueSubject("kafkakn-b95-poly"),
                )
            }
        } finally {
            registry.close()
        }
    }

    @Test
    fun message_indexes_are_read_as_confluent_writes_them() {
        // [0] as one varint 0; [1, 2] as a count and each index, all zigzag.
        assertEquals(listOf(0) to 1, MessageIndexes.read(byteArrayOf(0), 0))
        assertEquals(listOf(1, 2) to 3, MessageIndexes.read(byteArrayOf(4, 2, 4), 0))
        assertFailsWith<SchemaWireException> { MessageIndexes.read(byteArrayOf(4, 2), 0) }
    }
}

/**
 * [B-95](../../../../../../../docs/backlog/B-95-protobuf-serde.md): the registry's own Protobuf serializers as the
 * oracle. Only when `ci/b-95/run.sh` asks, in the phases `JsonSchemaOracleTest` has, with files in `KAFKAKN_B95_DIR`.
 */
class ProtobufOracleTest {
    @Test
    fun what_the_registrys_own_protobuf_serializers_make_of_it() =
        runTest(timeout = 2.minutes) {
            val dir = env("KAFKAKN_B95_DIR") ?: return@runTest
            withContext(Dispatchers.Default) {
                val registry = SchemaRegistry(registryUrl)
                try {
                    val orders = registry.protobufSerde<Order>(valueSubject("kafkakn-b95-$arm-$run"))
                    when (env("KAFKAKN_B95_PHASE")) {
                        "encode" -> {
                            writeLines(
                                "$dir/kafkakn-$arm.lines",
                                SAMPLES.map {
                                    "${orders.encode(
                                        it,
                                    ).toHex()}\t${Json.encodeToString(Order.serializer(), it)}"
                                },
                            )
                            writeLines("$dir/schema.proto", orders.schema.lines())
                        }

                        "decode" -> {
                            val decoded =
                                readLines(
                                    "$dir/confluent.lines",
                                ).filter { it.isNotBlank() }.map { orders.decode(it.fromHex()) }
                            assertEquals(SAMPLES, decoded)
                        }

                        else -> {
                            error("KAFKAKN_B95_PHASE must be encode or decode")
                        }
                    }
                } finally {
                    registry.close()
                }
            }
        }
}
