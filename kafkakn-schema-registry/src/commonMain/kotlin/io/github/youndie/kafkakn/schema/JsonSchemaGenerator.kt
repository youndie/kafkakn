package io.github.youndie.kafkakn.schema

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/**
 * A JSON Schema (draft-07) for what `Json` writes for a [SerialDescriptor]
 * ([B-94](../../../../../../../docs/backlog/B-94-json-schema-serde.md)). No library does this for kotlinx, and the
 * descriptor holds what a schema needs: names, kinds, element types, nullability, optional elements.
 *
 * **Every object is closed** (`"additionalProperties": false`). Under the registry's default compatibility,
 * `BACKWARD`, an open schema cannot gain even an optional property: measured in B-93 as
 * `OPTIONAL_PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL`. A closed one can, which is how a `@Serializable` class evolves.
 *
 * An element with a default is optional, because `Json` leaves a default out unless told to encode it. A nullable
 * element admits `null`. Polymorphic, contextual and recursive types are refused here, when the serializer is made,
 * not when a record is sent: none of them is a shape this generator has been measured on.
 */
@OptIn(ExperimentalSerializationApi::class)
internal object JsonSchemaGenerator {
    private const val DRAFT = "http://json-schema.org/draft-07/schema#"

    fun generate(descriptor: SerialDescriptor): String {
        val root = schemaOf(descriptor, visiting = emptySet())
        return JsonObject(mapOf("\$schema" to JsonPrimitive(DRAFT)) + root).toString()
    }

    private fun schemaOf(
        descriptor: SerialDescriptor,
        visiting: Set<String>,
    ): JsonObject {
        if (descriptor.isInline) return schemaOf(descriptor.getElementDescriptor(0), visiting)
        val plain = plainSchemaOf(descriptor, visiting)
        return if (descriptor.isNullable) nullable(plain) else plain
    }

    private fun plainSchemaOf(
        descriptor: SerialDescriptor,
        visiting: Set<String>,
    ): JsonObject =
        when (val kind = descriptor.kind) {
            PrimitiveKind.STRING, PrimitiveKind.CHAR -> {
                type("string")
            }

            PrimitiveKind.BOOLEAN -> {
                type("boolean")
            }

            PrimitiveKind.BYTE, PrimitiveKind.SHORT, PrimitiveKind.INT, PrimitiveKind.LONG -> {
                type("integer")
            }

            PrimitiveKind.FLOAT, PrimitiveKind.DOUBLE -> {
                type("number")
            }

            SerialKind.ENUM -> {
                buildJsonObject {
                    put("type", "string")
                    put(
                        "enum",
                        JsonArray(
                            (0 until descriptor.elementsCount).map { JsonPrimitive(descriptor.getElementName(it)) },
                        ),
                    )
                }
            }

            StructureKind.LIST -> {
                buildJsonObject {
                    put("type", "array")
                    put("items", schemaOf(descriptor.getElementDescriptor(0), visiting))
                }
            }

            StructureKind.MAP -> {
                val key = descriptor.getElementDescriptor(0)
                require(key.kind == PrimitiveKind.STRING || key.kind == SerialKind.ENUM) {
                    "${descriptor.serialName}: a map's keys must be strings for JSON Schema, not ${key.kind}"
                }
                buildJsonObject {
                    put("type", "object")
                    put("additionalProperties", schemaOf(descriptor.getElementDescriptor(1), visiting))
                }
            }

            StructureKind.CLASS, StructureKind.OBJECT -> {
                objectSchema(descriptor, visiting)
            }

            is PolymorphicKind, SerialKind.CONTEXTUAL -> {
                throw IllegalArgumentException(
                    "${descriptor.serialName}: $kind types have no JSON Schema here yet; a concrete class does",
                )
            }
        }

    private fun objectSchema(
        descriptor: SerialDescriptor,
        visiting: Set<String>,
    ): JsonObject {
        val name = descriptor.serialName.removeSuffix("?")
        require(name !in visiting) { "$name: a recursive type has no JSON Schema here yet" }
        val inside = visiting + name
        val properties =
            (0 until descriptor.elementsCount).associate { index ->
                descriptor.getElementName(index) to schemaOf(descriptor.getElementDescriptor(index), inside)
            }
        val required =
            (0 until descriptor.elementsCount)
                .filterNot {
                    descriptor.isElementOptional(it)
                }.map { descriptor.getElementName(it) }
        return buildJsonObject {
            put("type", "object")
            put("title", name.substringAfterLast('.'))
            put("properties", JsonObject(properties))
            if (required.isNotEmpty()) put("required", JsonArray(required.map { JsonPrimitive(it) }))
            put("additionalProperties", false)
        }
    }

    /** `["string", "null"]` for a primitive; `oneOf` null and the schema for anything else. */
    private fun nullable(schema: JsonObject): JsonObject {
        val type = schema["type"]
        return if (type is JsonPrimitive && schema.keys == setOf("type")) {
            JsonObject(mapOf("type" to JsonArray(listOf(type, JsonPrimitive("null")))))
        } else {
            JsonObject(mapOf("oneOf" to JsonArray(listOf<JsonElement>(type("null"), schema))))
        }
    }

    private fun type(name: String): JsonObject = JsonObject(mapOf("type" to JsonPrimitive(name)))
}
