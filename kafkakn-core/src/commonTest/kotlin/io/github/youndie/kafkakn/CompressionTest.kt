package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * [The producer contract, *Configuration*](../../../../../../../docs/api/producer-contract.md) names
 * `compression.type` as a key that travels to both clients, and until
 * [B-26](../../../../../../../docs/backlog/B-26-compression-was-never-measured.md) nothing had set it.
 *
 * **Half of the assertion is deliberately not here**, the same way [AccountingTest]'s is not. A record
 * that arrives says nothing about compression — an uncompressed batch arrives too, and reads back
 * identically. The third party is the broker's stored log segment: `ci/b-26/run.sh` reads it with
 * `kafka-dump-log.sh`, which names the codec of every batch, and matches each record here to the
 * batch it landed in by the stamp recorded below.
 *
 * `none` is a row of its own and it is the control. If whatever reads the segments reported the codec
 * it was told to expect rather than the one on disk, five rows would all agree with their labels; the
 * script requires five different codecs back, one per row.
 */
class CompressionTest {
    private suspend fun sendWith(codec: String) {
        val stamp = "comp-$armName-$codec-${randomSuffix()}"
        val producer =
            kafkaProducer(
                ProducerConfig(
                    "bootstrap.servers" to bootstrap,
                    "compression.type" to codec,
                ),
            )
        try {
            withContext(Dispatchers.Default) {
                // Concurrent, so that records share batches: a batch of one compresses to nothing
                // worth measuring, and it is the batch that carries the codec.
                coroutineScope {
                    (0 until RECORDS)
                        .map { index ->
                            val value = "$stamp:$index:$PADDING".encodeToByteArray()
                            async { producer.send(ProducerRecord(testTopic, value)) }
                        }.awaitAll()
                }
                producer.flush()
            }
        } finally {
            producer.close()
        }
        recordArmFact("compression.$codec.stamp", stamp)
        recordArmFact("compression.$codec.count", RECORDS.toString())
    }

    @Test
    fun none_is_the_control() = runTest { sendWith("none") }

    @Test
    fun gzip() = runTest { sendWith("gzip") }

    @Test
    fun snappy() = runTest { sendWith("snappy") }

    @Test
    fun lz4() = runTest { sendWith("lz4") }

    @Test
    fun zstd() = runTest { sendWith("zstd") }

    @Test
    fun an_unknown_codec_is_refused_and_the_key_is_named() {
        val failure =
            assertFails {
                kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "compression.type" to "brotli"))
            }
        // Recorded as well as asserted: the two clients word this differently, and whether either calls
        // a known key with a bad value "unknown" is worth being able to read.
        val said = failure.message.orEmpty()
        recordArmFact("compression.refusal", said.replace('\n', ' ').take(REFUSAL_CHARS))
        assertTrue(
            said.contains("compression.type"),
            "the refusal must name the key the caller typed. It said: $said",
        )
        // The key is real and the VALUE is wrong. The native arm used to call every refusal "unknown
        // producer configuration", which sends a caller looking for a typo in a key they spelled
        // correctly - measured the first time this test ran.
        assertFalse(
            said.lowercase().contains("unknown"),
            "a known key with a bad value is not an unknown key. It said: $said",
        )
    }

    private companion object {
        const val RECORDS = 300
        const val REFUSAL_CHARS = 300

        /** Compressible on purpose: a codec that has nothing to remove is hard to tell from none. */
        val PADDING = "kafkakn".repeat(40)
    }
}
