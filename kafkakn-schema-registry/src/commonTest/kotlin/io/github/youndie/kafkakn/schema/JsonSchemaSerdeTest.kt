package io.github.youndie.kafkakn.schema

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes

@Serializable
enum class Status { NEW, PAID }

@Serializable
data class Customer(
    val name: String,
    val vip: Boolean,
)

@Serializable
data class Line(
    val sku: String,
    val quantity: Int,
    val price: Double,
)

@Serializable
data class Order(
    val id: Long,
    val customer: Customer,
    val lines: List<Line>,
    val note: String? = null,
    val status: Status = Status.NEW,
    val tags: Map<String, Int> = emptyMap(),
)

@Serializable
sealed interface Event

/** The values the oracle is asked about, in order: defaults left out, set, and a nullable set. */
internal val SAMPLES =
    listOf(
        Order(1, Customer("ann", false), listOf(Line("a-1", 2, 9.5))),
        Order(
            2,
            Customer("bo", true),
            listOf(Line("b-1", 1, 0.25), Line("b-2", 3, 12.0)),
            note = "gift",
            status = Status.PAID,
        ),
        Order(3, Customer("cy", false), emptyList(), tags = mapOf("rush" to 1, "fragile" to 2)),
    )

/**
 * [B-94](../../../../../../../docs/backlog/B-94-json-schema-serde.md): a `@Serializable` type as JSON Schema in the
 * registry's wire format. These run in the suite against the fixture registry. What the registry's own serializers
 * make of the bytes is `JsonSchemaOracleTest`'s, run by `ci/b-94/run.sh`.
 */
class JsonSchemaSerdeTest {
    @Test
    fun a_serializable_type_round_trips_through_the_registry() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val registry = SchemaRegistry(registryUrl)
                try {
                    val orders = registry.jsonSchemaSerde<Order>(valueSubject("kafkakn-b94-round-$arm-$run"))
                    for (order in SAMPLES) {
                        val bytes = orders.encode(order)
                        assertEquals(0, bytes[0].toInt(), "magic")
                        val id = registry.register(orders.subject, orders.schema, SchemaType.JSON)
                        assertEquals(id, Wire.unframe(bytes).first, "the id the registry gave the generated schema")
                        assertEquals(order, orders.decode(bytes))
                    }
                } finally {
                    registry.close()
                }
            }
        }

    @Test
    fun the_generated_schema_is_closed_and_requires_what_has_no_default() {
        val schema = Json.parseToJsonElement(JsonSchemaGenerator.generate(Order.serializer().descriptor)).jsonObject
        assertEquals(JsonPrimitive("object"), schema["type"])
        assertEquals(JsonPrimitive(false), schema["additionalProperties"])
        assertEquals(JsonArray(listOf("id", "customer", "lines").map { JsonPrimitive(it) }), schema["required"])
        val properties = schema.getValue("properties").jsonObject
        assertEquals(
            JsonArray(listOf(JsonPrimitive("string"), JsonPrimitive("null"))),
            properties.getValue("note").jsonObject["type"],
        )
        assertEquals(
            JsonArray(listOf(JsonPrimitive("NEW"), JsonPrimitive("PAID"))),
            properties.getValue("status").jsonObject["enum"],
        )
        val line =
            properties
                .getValue("lines")
                .jsonObject
                .getValue("items")
                .jsonObject
        assertEquals(JsonPrimitive(false), line["additionalProperties"], "nested objects are closed too")
        assertEquals(
            JsonPrimitive("integer"),
            (properties.getValue("tags").jsonObject["additionalProperties"] as JsonObject)["type"],
        )
    }

    @Test
    fun a_polymorphic_type_is_refused_when_the_serde_is_made() {
        val registry = SchemaRegistry(registryUrl)
        try {
            assertFailsWith<IllegalArgumentException> {
                registry.jsonSchemaSerde<Event>(
                    valueSubject("kafkakn-b94-poly"),
                )
            }
        } finally {
            registry.close()
        }
    }

    @Test
    fun bytes_not_in_the_wire_format_are_refused_by_name() {
        assertFailsWith<SchemaWireException> { Wire.unframe(byteArrayOf(0, 0, 1)) }
        val guid = assertFailsWith<SchemaWireException> { Wire.unframe(ByteArray(21).also { it[0] = 1 }) }
        assertEquals(true, guid.message!!.contains("GUID"), guid.message)
        assertFailsWith<SchemaWireException> { Wire.unframe(ByteArray(8).also { it[0] = 7 }) }
    }
}
