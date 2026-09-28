package io.github.youndie.kafkakn.schema

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.protobuf.ProtoBuf
import kotlinx.serialization.protobuf.schema.ProtoBufSchemaGenerator
import kotlinx.serialization.serializer

/**
 * A `@Serializable` type as Protobuf, in the registry's wire format
 * ([B-95](../../../../../../../docs/backlog/B-95-protobuf-serde.md)): what the registry's own `KafkaProtobufSerializer`
 * writes, and what its deserializer reads.
 *
 * [schema] is the `.proto` kotlinx's `ProtoBufSchemaGenerator` writes for the type, proto2, made when this is made. The
 * generator writes the root type's message first (it walks the types breadth-first from the root), so the record's
 * message indexes are `[0]`, which the wire format writes as one `0x00` between the id and the payload (research
 * §2.29). A polymorphic type, which the generator can only express as an artificial wrapper message, is refused.
 */
@OptIn(ExperimentalSerializationApi::class)
public class ProtobufSerde<T> internal constructor(
    private val registry: SchemaRegistry,
    public val subject: String,
    private val serializer: KSerializer<T>,
    private val protoBuf: ProtoBuf,
) {
    public val schema: String = ProtoBufSchemaGenerator.generateSchemaText(serializer.descriptor)

    init {
        require(POLYMORPHIC !in schema) {
            "${serializer.descriptor.serialName}: a polymorphic type has no Protobuf schema here yet; a concrete class does"
        }
    }

    /** [value] as the registry's wire format: magic, the schema's id, the message indexes `[0]`, and its Protobuf bytes. */
    public suspend fun encode(value: T): ByteArray {
        val id = registry.register(subject, schema, SchemaType.PROTOBUF)
        return Wire.frame(id, byteArrayOf(ROOT_INDEX) + protoBuf.encodeToByteArray(serializer, value))
    }

    /** A value of this type from bytes in the registry's wire format, whose schema is Protobuf and message the root. */
    public suspend fun decode(bytes: ByteArray): T {
        val (id, from) = Wire.unframe(bytes)
        val written = registry.schema(id)
        if (written.type != SchemaType.PROTOBUF) {
            throw SchemaWireException("schema $id is ${written.type}, not Protobuf")
        }
        val (indexes, payload) = MessageIndexes.read(bytes, from)
        if (indexes != listOf(0)) {
            throw SchemaWireException("message indexes $indexes: this reader's type is the schema's first message, [0]")
        }
        return protoBuf.decodeFromByteArray(serializer, bytes.copyOfRange(payload, bytes.size))
    }

    private companion object {
        /** The name of the artificial message the generator writes for a polymorphic type. */
        const val POLYMORPHIC = "KotlinxSerializationPolymorphic"
        const val ROOT_INDEX: Byte = 0
    }
}

/**
 * Confluent's message indexes: which message of the schema a record is, before its payload
 * (`MessageIndexes.java`, kafka-protobuf-provider 8.3.2, lines 27–58). `[0]` is one zigzag varint `0`; anything else
 * is its count and then each index, as zigzag varints.
 */
internal object MessageIndexes {
    /** The indexes starting at [from], and where the payload starts after them. */
    fun read(
        bytes: ByteArray,
        from: Int,
    ): Pair<List<Int>, Int> {
        var at = from

        fun next(): Int {
            var value = 0
            var shift = 0
            while (true) {
                if (at >= bytes.size) throw SchemaWireException("message indexes run past the end of the record")
                val byte = bytes[at++].toInt()
                value = value or ((byte and 0x7f) shl shift)
                if (byte and 0x80 == 0) break
                shift += 7
                if (shift > MAX_SHIFT) throw SchemaWireException("a message index longer than a 32-bit varint")
            }
            return (value ushr 1) xor -(value and 1)
        }
        val count = next()
        if (count == 0) return listOf(0) to at
        if (count < 0) throw SchemaWireException("a negative count of message indexes: $count")
        return List(count) { next() } to at
    }

    private const val MAX_SHIFT = 28
}

/** A Protobuf serde for [serializer]'s type under [subject]: `valueSubject(topic)` for a topic's values. */
@OptIn(ExperimentalSerializationApi::class)
public fun <T> SchemaRegistry.protobufSerde(
    subject: String,
    serializer: KSerializer<T>,
    protoBuf: ProtoBuf = ProtoBuf,
): ProtobufSerde<T> = ProtobufSerde(this, subject, serializer, protoBuf)

/** A Protobuf serde for [T] under [subject]: `registry.protobufSerde<Order>(valueSubject("orders"))`. */
@OptIn(ExperimentalSerializationApi::class)
public inline fun <reified T> SchemaRegistry.protobufSerde(
    subject: String,
    protoBuf: ProtoBuf = ProtoBuf,
): ProtobufSerde<T> = protobufSerde(subject, serializer<T>(), protoBuf)
