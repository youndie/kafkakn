package io.github.youndie.kafkakn

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import rdkafka.rd_kafka_version_str
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * The C library is linked in, and it runs.
 *
 * A klib that merely compiles proves nothing about linking; this test lives in the `linuxX64Test`
 * **executable**, so its existence means the archives resolved, and its passing means the resulting
 * binary starts and can call into them.
 *
 * It is in a source directory added only when the cinterop is enabled, so that
 * `-Pkafkakn.noKafkaC` produces a genuinely Kafka-free binary to compare `ldd` against.
 */
@OptIn(ExperimentalForeignApi::class)
class CinteropLinkTest {
    @Test
    fun librdkafka_is_linked_and_answers() {
        val version = rd_kafka_version_str()?.toKString()
        assertTrue(version != null && version.isNotEmpty(), "librdkafka reported no version")
        assertTrue(version!!.startsWith("2."), "unexpected librdkafka version: $version")
    }
}
