package io.github.youndie.kafkakn.schema

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes

/**
 * [B-93](../../../../../../../docs/backlog/B-93-a-registry-client.md): the registry client, against the fixture
 * registry. That a cached schema costs no request is counted by `ci/b-93/run.sh` in the registry's own request log,
 * not by the client: the subjects are named after the run, so the runner finds this run's requests.
 */
class SchemaRegistryTest {
    @Test
    fun a_schema_is_registered_once_and_read_back_by_its_id() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val subject = valueSubject("kafkakn-sr-cached-$arm-$run")
                val registry = SchemaRegistry(registryUrl)
                val first: Int
                val again: Int
                try {
                    first = registry.register(subject, ORDER, SchemaType.JSON)
                    again = registry.register(subject, ORDER, SchemaType.JSON)
                } finally {
                    registry.close()
                }
                assertEquals(first, again, "the same schema, the same id")
                // Read by a client that has never seen it, so the answer is the registry's, not the cache's.
                val reader = SchemaRegistry(registryUrl)
                try {
                    val read = reader.schema(first)
                    assertEquals(SchemaType.JSON, read.type)
                    assertEquals(Json.parseToJsonElement(ORDER), Json.parseToJsonElement(read.schema))
                } finally {
                    reader.close()
                }
            }
        }

    /** The count's positive control: two different schemas under one subject are two requests, whatever the cache. */
    @Test
    fun two_different_schemas_are_two_registrations() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val subject = valueSubject("kafkakn-sr-control-$arm-$run")
                val registry = SchemaRegistry(registryUrl)
                try {
                    val first = registry.register(subject, ORDER, SchemaType.JSON)
                    val second = registry.register(subject, ORDER_WITH_NOTE, SchemaType.JSON)
                    assertEquals(true, first != second, "a different schema, a different id: $first and $second")
                } finally {
                    registry.close()
                }
            }
        }

    @Test
    fun a_schema_the_registry_cannot_read_is_its_error_in_one_type() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val registry = SchemaRegistry(registryUrl)
                try {
                    val refused =
                        assertFailsWith<SchemaRegistryException> {
                            registry.register(
                                valueSubject("kafkakn-sr-invalid-$arm-$run"),
                                "{\"type\": 42}",
                                SchemaType.JSON,
                            )
                        }
                    assertEquals(422, refused.status, refused.message)
                    assertEquals(42201, refused.errorCode, refused.message)
                    val unknown = assertFailsWith<SchemaRegistryException> { registry.schema(UNKNOWN_ID) }
                    assertEquals(404, unknown.status, unknown.message)
                    assertEquals(40403, unknown.errorCode, unknown.message)
                } finally {
                    registry.close()
                }
            }
        }

    private companion object {
        const val ORDER = """{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"""
        const val ORDER_WITH_NOTE =
            """{"type":"object","properties":{"id":{"type":"integer"},"note":{"type":"string"}},"required":["id"]}"""
        const val UNKNOWN_ID = 999_999
    }
}
