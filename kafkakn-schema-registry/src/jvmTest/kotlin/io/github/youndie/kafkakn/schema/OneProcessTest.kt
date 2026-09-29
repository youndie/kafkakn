package io.github.youndie.kafkakn.schema

import io.github.youndie.kafkakn.ProducerConfig
import io.github.youndie.kafkakn.ProducerRecord
import io.github.youndie.kafkakn.kafkaProducer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

/**
 * [B-98](../../../../../../../docs/backlog/B-98-https-to-the-registry.md): what the module is for, in one process. A
 * record is encoded through the registry over HTTPS and produced to the broker over TLS.
 *
 * **On the JVM only.** On native the same test does not link: librdkafka's OpenSSL (3.0.13, in kafkakn's C bundle)
 * and the one Ktor's Curl engine carries (3.6.3) are both static, and `ld.lld` refuses the duplicate symbols. The owner
 * chose that a native service reaches a registry over HTTP until one needs HTTPS (B-98, option 3).
 */
class OneProcessTest {
    @Test
    fun a_record_encoded_over_https_is_produced_over_tls() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val http = httpsClient(caPath)
                val producer =
                    kafkaProducer(
                        ProducerConfig(
                            "bootstrap.servers" to (env("KAFKAKN_SSL_BOOTSTRAP") ?: "127.0.0.1:9094"),
                            "security.protocol" to "SSL",
                            "ssl.ca.location" to caPath,
                            "acks" to "all",
                        ),
                    )
                try {
                    val registry = SchemaRegistry(registryTlsUrl, http)
                    val items = registry.jsonSchemaSerde<ItemV1>(valueSubject("kafkakn-b98-one-$arm-$run"))
                    val bytes = items.encode(ItemV1(7, "lamp"))
                    val written = producer.send(ProducerRecord(env("KAFKAKN_TOPIC") ?: "kafkakn", bytes))
                    assertTrue(written.offset >= 0, "the broker's offset for the record: ${written.offset}")
                    assertEquals(ItemV1(7, "lamp"), items.decode(bytes))
                } finally {
                    producer.close()
                    http.close()
                }
            }
        }
}
