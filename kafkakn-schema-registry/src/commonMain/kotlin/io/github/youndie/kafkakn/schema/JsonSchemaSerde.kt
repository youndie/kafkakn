package io.github.youndie.kafkakn.schema

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer

/**
 * Bytes that are not in the registry's wire format, or not in the part of it this module reads: magic `0x00`, then
 * the schema id as a 4-byte big-endian int, then the payload (B-92). Magic `0x01` (a GUID) is the registry's too, and
 * is refused by name rather than misread.
 */
public class SchemaWireException(
    message: String,
) : IllegalArgumentException(message)

/** The registry's wire format, in the one variant this module writes and reads (research §2.29). */
internal object Wire {
    private const val MAGIC: Byte = 0x00
    private const val GUID_MAGIC: Byte = 0x01
    private const val HEADER = 5

    fun frame(
        id: Int,
        payload: ByteArray,
    ): ByteArray =
        ByteArray(HEADER + payload.size).also { framed ->
            framed[0] = MAGIC
            framed[1] = (id ushr 24).toByte()
            framed[2] = (id ushr 16).toByte()
            framed[3] = (id ushr 8).toByte()
            framed[4] = id.toByte()
            payload.copyInto(framed, HEADER)
        }

    /** The schema id and the payload's first index. */
    fun unframe(bytes: ByteArray): Pair<Int, Int> {
        val short = bytes.size < HEADER
        if (short) throw SchemaWireException("${bytes.size} bytes: too short for the registry's wire format")
        when (bytes[0]) {
            MAGIC -> {}

            GUID_MAGIC -> {
                throw SchemaWireException("magic 0x01: a schema GUID, which this module does not read")
            }

            else -> {
                throw SchemaWireException("magic ${bytes[0]}: not the registry's wire format")
            }
        }
        val id =
            ((bytes[1].toInt() and 0xff) shl 24) or ((bytes[2].toInt() and 0xff) shl 16) or
                ((bytes[3].toInt() and 0xff) shl 8) or (bytes[4].toInt() and 0xff)
        return id to HEADER
    }
}

/**
 * A `@Serializable` type as JSON Schema, in the registry's wire format
 * ([B-94](../../../../../../../docs/backlog/B-94-json-schema-serde.md)): what the registry's own
 * `KafkaJsonSchemaSerializer` writes, and what its deserializer reads.
 *
 * [schema] is generated from the type's descriptor when this is made, so a type with no JSON Schema here fails now
 * rather than at the first record. The first [encode] registers it under [subject], and every later one uses the
 * cached id. [decode] reads the id, asks the registry what it names (cached too), and refuses a schema that is not
 * JSON Schema. It decodes with the reader's own type: the writer's schema may be an older or newer version of it.
 */
public class JsonSchemaSerde<T> internal constructor(
    private val registry: SchemaRegistry,
    public val subject: String,
    private val serializer: KSerializer<T>,
    private val json: Json,
) {
    public val schema: String = JsonSchemaGenerator.generate(serializer.descriptor)

    /**
     * Registers [schema] under [subject], once, and returns its id: what the first [encode] does, for a caller who
     * would rather find out at startup that the subject refuses it ([IncompatibleSchemaException]).
     */
    public suspend fun register(): Int = registry.register(subject, schema, SchemaType.JSON)

    /** [value] as the registry's wire format: magic, the schema's id, and the JSON `Json` writes for it. */
    public suspend fun encode(value: T): ByteArray {
        val id = register()
        return Wire.frame(id, json.encodeToString(serializer, value).encodeToByteArray())
    }

    /** A value of this type from bytes in the registry's wire format, whose schema is JSON Schema. */
    public suspend fun decode(bytes: ByteArray): T {
        val (id, from) = Wire.unframe(bytes)
        val written = registry.schema(id)
        if (written.type != SchemaType.JSON) {
            throw SchemaWireException("schema $id is ${written.type}, not JSON Schema")
        }
        return json.decodeFromString(serializer, bytes.decodeToString(from, bytes.size))
    }
}

/** A JSON Schema serde for [serializer]'s type under [subject]: `valueSubject(topic)` for a topic's values. */
public fun <T> SchemaRegistry.jsonSchemaSerde(
    subject: String,
    serializer: KSerializer<T>,
    json: Json = Json,
): JsonSchemaSerde<T> = JsonSchemaSerde(this, subject, serializer, json)

/** A JSON Schema serde for [T] under [subject]: `registry.jsonSchemaSerde<Order>(valueSubject("orders"))`. */
public inline fun <reified T> SchemaRegistry.jsonSchemaSerde(
    subject: String,
    json: Json = Json,
): JsonSchemaSerde<T> = jsonSchemaSerde(subject, serializer<T>(), json)
