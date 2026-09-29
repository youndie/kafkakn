package io.github.youndie.kafkakn.schema

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.minutes

/**
 * [B-98](../../../../../../../docs/backlog/B-98-https-to-the-registry.md): a registry served over HTTPS, reached the
 * way a caller would reach one: CIO on the JVM, Curl on native (CIO has no TLS there, B-92), each trusting only the
 * fixture's CA. The fixture's HTTPS listener stands beside its plaintext one and serves a certificate the fixture CA
 * signed, for 127.0.0.1.
 */
class RegistryHttpsTest {
    @Test
    fun a_schema_is_registered_and_read_back_over_https_verifying_the_fixture_ca() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val subject = valueSubject("kafkakn-b98-$arm-$run")
                val id =
                    over(caPath) { registry -> registry.jsonSchemaSerde<ItemV1>(subject).register() }
                // A second client, so the read is a request and not the first client's cache.
                val read = over(caPath) { registry -> registry.schema(id) }
                assertEquals(SchemaType.JSON, read.type)
                assertTrue("\"title\":\"Item\"" in read.schema, "the schema registered, read back: ${read.schema}")
                // And the same id over plaintext: one registry behind both listeners, not two.
                val plain = SchemaRegistry(registryUrl)
                try {
                    assertEquals(read.schema, plain.schema(id).schema)
                } finally {
                    plain.close()
                }
            }
        }

    @Test
    fun a_registry_whose_certificate_another_ca_signed_is_refused() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val refused =
                    try {
                        over(
                            wrongCaPath,
                        ) { registry ->
                            registry.register(
                                valueSubject("kafkakn-b98-refused-$arm"),
                                SCHEMA,
                                SchemaType.JSON,
                            )
                        }
                        fail("a registry certificate the trusted CA did not sign was accepted")
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (thrown: Exception) {
                        thrown
                    }
                val said =
                    generateSequence<Throwable>(refused) {
                        it.cause
                    }.joinToString(" <- ") { "${it::class.simpleName}: ${it.message}" }
                recordRefusal(said)
                assertTrue("certificat" in said.lowercase(), "the refusal names certificate verification: $said")
            }
        }

    private suspend fun <T> over(
        ca: String,
        use: suspend (SchemaRegistry) -> T,
    ): T {
        val http = httpsClient(ca)
        try {
            val registry = SchemaRegistry(registryTlsUrl, http)
            return use(registry)
        } finally {
            http.close()
        }
    }

    /** What each arm said, for the item: the words differ by arm, the fact they share is the one asserted. */
    private fun recordRefusal(said: String) {
        println("B-98 $arm refusal: $said")
    }

    private companion object {
        const val SCHEMA = """{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}"""
    }
}
