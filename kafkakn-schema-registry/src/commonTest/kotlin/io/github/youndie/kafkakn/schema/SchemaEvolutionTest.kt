package io.github.youndie.kafkakn.schema

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

// Three versions of one type, under one serial name, as a type evolves in a service: the name is the schema's.

/** The first version. */
@Serializable
@SerialName("Item")
data class ItemV1(
    val id: Long,
    val name: String,
)

/** An optional field added: what `BACKWARD` accepts, a reader of this version reading what [ItemV1] wrote. */
@Serializable
@SerialName("Item")
data class ItemV2(
    val id: Long,
    val name: String,
    val colour: String? = null,
)

/** A required field removed: what `BACKWARD` refuses for a closed JSON Schema. */
@Serializable
@SerialName("Item")
data class ItemV3(
    val id: Long,
)

/**
 * [B-96](../../../../../../../docs/backlog/B-96-an-incompatible-schema-is-refused.md): a type evolves under the
 * registry's default compatibility, `BACKWARD` — a new schema must read what the previous one wrote. What it refuses is
 * one kafkakn type, [IncompatibleSchemaException], thrown when the serde registers, before a record exists.
 */
class SchemaEvolutionTest {
    @Test
    fun an_optional_field_added_is_accepted_and_reads_what_the_old_type_wrote_as_json_schema() =
        registry { registry ->
            val subject = valueSubject("kafkakn-b96-json-add-$arm-$run")
            val old = registry.jsonSchemaSerde<ItemV1>(subject)
            val written = old.encode(ItemV1(1, "cup"))
            val new = registry.jsonSchemaSerde<ItemV2>(subject)
            val id = new.register()
            assertTrue(id != Wire.unframe(written).first, "a second version, with an id of its own")
            assertEquals(ItemV2(1, "cup"), new.decode(written))
        }

    @Test
    fun a_required_field_removed_is_refused_under_backward_as_json_schema() =
        registry { registry ->
            val subject = valueSubject("kafkakn-b96-json-remove-$arm-$run")
            registry.jsonSchemaSerde<ItemV1>(subject).register()
            val new = registry.jsonSchemaSerde<ItemV3>(subject)
            val refused = assertFailsWith<IncompatibleSchemaException> { new.register() }
            assertEquals(subject, refused.subject)
            assertEquals(409, refused.status)
            assertTrue("incompatible" in refused.message.orEmpty(), "the registry's own reason: ${refused.message}")
            val again = assertFailsWith<IncompatibleSchemaException> { new.encode(ItemV3(1)) }
            assertEquals(subject, again.subject, "encode registers first, so no record is made")
        }

    @Test
    fun an_optional_field_added_is_accepted_and_reads_what_the_old_type_wrote_as_protobuf() =
        registry { registry ->
            val subject = valueSubject("kafkakn-b96-proto-add-$arm-$run")
            val old = registry.protobufSerde<ItemV1>(subject)
            val written = old.encode(ItemV1(1, "cup"))
            val new = registry.protobufSerde<ItemV2>(subject)
            val id = new.register()
            assertTrue(id != Wire.unframe(written).first, "a second version, with an id of its own")
            assertEquals(ItemV2(1, "cup"), new.decode(written))
        }

    @Test
    fun a_required_field_removed_is_refused_under_backward_as_protobuf() =
        registry { registry ->
            val subject = valueSubject("kafkakn-b96-proto-remove-$arm-$run")
            registry.protobufSerde<ItemV1>(subject).register()
            val new = registry.protobufSerde<ItemV3>(subject)
            val refused = assertFailsWith<IncompatibleSchemaException> { new.register() }
            assertEquals(subject, refused.subject)
            assertEquals(409, refused.status)
        }

    /**
     * What a closed schema buys, measured on an open one (B-93): under `BACKWARD` an open JSON Schema cannot gain even
     * an optional property, so a type written that way could never evolve.
     */
    @Test
    fun an_open_json_schema_cannot_gain_an_optional_property() =
        registry { registry ->
            val subject = valueSubject("kafkakn-b96-open-$arm-$run")
            registry.register(subject, OPEN_V1, SchemaType.JSON)
            val refused =
                assertFailsWith<IncompatibleSchemaException> { registry.register(subject, OPEN_V2, SchemaType.JSON) }
            assertTrue(
                "OPTIONAL_PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL" in refused.message.orEmpty(),
                "the registry's reason: ${refused.message}",
            )
        }

    private fun registry(block: suspend (SchemaRegistry) -> Unit) =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val registry = SchemaRegistry(registryUrl)
                try {
                    block(registry)
                } finally {
                    registry.close()
                }
            }
        }

    private companion object {
        const val OPEN_V1 = """{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"""
        const val OPEN_V2 =
            """{"type":"object","properties":{"id":{"type":"integer"},"colour":{"type":"string"}},"required":["id"]}"""
    }
}
