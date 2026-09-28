package io.github.youndie.kafkakn.schema

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.minutes

/**
 * [B-94](../../../../../../../docs/backlog/B-94-json-schema-serde.md): the registry's own serializers as the oracle.
 * Only when `ci/b-94/run.sh` asks, in two phases, with files in `KAFKAKN_B94_DIR`:
 * - `encode`: [SAMPLES] encoded by kafkakn, one line each, the hex bytes and the JSON the value is. The harness reads
 *   them with Confluent's `KafkaJsonSchemaDeserializer`, validating against the schema, and the runner compares.
 * - `decode`: the bytes Confluent's `KafkaJsonSchemaSerializer` wrote for the same values, decoded by kafkakn into the
 *   same [SAMPLES].
 */
class JsonSchemaOracleTest {
    @Test
    fun what_the_registrys_own_serializers_make_of_it() =
        runTest(timeout = 2.minutes) {
            val dir = env("KAFKAKN_B94_DIR") ?: return@runTest
            withContext(Dispatchers.Default) {
                val registry = SchemaRegistry(registryUrl)
                try {
                    val orders = registry.jsonSchemaSerde<Order>(valueSubject("kafkakn-b94-$arm-$run"))
                    when (env("KAFKAKN_B94_PHASE")) {
                        "encode" -> {
                            writeLines(
                                "$dir/kafkakn-$arm.lines",
                                SAMPLES.map {
                                    "${orders.encode(
                                        it,
                                    ).toHex()}\t${Json.encodeToString(Order.serializer(), it)}"
                                },
                            )
                            writeLines("$dir/schema.json", listOf(orders.schema))
                        }

                        "decode" -> {
                            val decoded =
                                readLines(
                                    "$dir/confluent.lines",
                                ).filter { it.isNotBlank() }.map { orders.decode(it.fromHex()) }
                            assertEquals(SAMPLES, decoded)
                        }

                        else -> {
                            error("KAFKAKN_B94_PHASE must be encode or decode")
                        }
                    }
                } finally {
                    registry.close()
                }
            }
        }
}

internal fun ByteArray.toHex(): String = joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }

internal fun String.fromHex(): ByteArray = chunked(2).map { it.toInt(16).toByte() }.toByteArray()
