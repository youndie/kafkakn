package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * [feature-secure-connection](../../../../../../../docs/features/feature-secure-connection.md).
 *
 * **Nothing about TLS is proved by the fact that OpenSSL is linked.** A linked library that was
 * never called is the same evidence as one that does not work, and a size figure for TLS was
 * published before anything here had ever opened a TLS connection.
 *
 * The three scenarios are one claim each, and the second and third are what make the first mean
 * anything: a client that skips verification, and a client that quietly fell back to plaintext, both
 * make the happy path green.
 *
 * Configuration is Kafka's own on both arms — `security.protocol=SSL`, `ssl.ca.location` — and the
 * JVM arm translates the CA path into a PEM trust store (`translateForJava`). That translation is
 * the only thing this library does to the oracle, and it is here that it is checked to be faithful.
 */
class TlsTest {
    @Test
    fun a_record_reaches_the_broker_over_tls() =
        runTest {
            val stamp = "tls-$armName-${randomSuffix()}"
            val producer = kafkaProducer(ProducerConfig(secureConfig(caPath, sslBootstrap)))
            try {
                withContext(Dispatchers.Default) {
                    coroutineScope {
                        (0 until RECORDS)
                            .map { index ->
                                async { producer.send(ProducerRecord(testTopic, "$stamp:$index".encodeToByteArray())) }
                            }.awaitAll()
                    }
                    producer.flush()
                }
            } finally {
                producer.close()
            }
            // Counted by the harness OVER THE PLAINTEXT LISTENER: the path that verifies the claim must
            // not be the path the claim is about.
            recordArmFact("tls.stamp", stamp)
            recordArmFact("tls.count", RECORDS.toString())
        }

    @Test
    fun an_untrusted_peer_is_refused_and_says_why() =
        runTest {
            val text = failureText(secureConfig(wrongCaPath, sslBootstrap), "wrong-ca")

            // Both arms have to NAME the certificate. "Broker transport failure" and "Local: Message
            // timed out" are true, unhelpful, and identical to what a closed port says - and a caller
            // who reads that will go looking at firewalls while the answer is a CA file.
            assertTrue(
                text.lowercase().contains("certif"),
                "the failure must name certificate verification, not merely that the broker is " +
                    "unreachable. It said: $text",
            )
        }

    @Test
    fun the_plaintext_listener_is_not_silently_accepted_as_tls() =
        runTest {
            // The SAME TLS configuration, pointed at the port that speaks no TLS. Without this scenario
            // "TLS worked" cannot be told from "TLS was quietly not used".
            val text = failureText(secureConfig(caPath, bootstrap), "tls-to-plaintext")
            assertTrue(text.isNotBlank(), "a TLS handshake against a plaintext listener must fail loudly")
        }

    /** Sends one record, requires it to fail, and returns everything the failure said. */
    private suspend fun failureText(
        properties: Map<String, String>,
        what: String,
    ): String {
        val producer = kafkaProducer(ProducerConfig(properties))
        val failure =
            try {
                withContext(Dispatchers.Default) {
                    producer.send(ProducerRecord(testTopic, "$what-$armName".encodeToByteArray()))
                }
                null
            } catch (cancellation: CancellationException) {
                // RETHROWN, AND THIS IS NOT A FORMALITY. `runTest` cancels the body when it times
                // out, so a catch that swallows cancellation records the TIMEOUT as the producer's
                // failure - which is exactly what happened while B-11 was being written: the arm
                // file read `tls.wrong-ca.failure=CancellationException: The test timed out`, a
                // sentence about the harness filed as a measurement of the library.
                throw cancellation
            } catch (thrown: Throwable) {
                thrown
            } finally {
                try {
                    producer.close()
                } catch (cancellation: CancellationException) {
                    throw cancellation
                } catch (onClose: Throwable) {
                    // The subject is what `send` did. A producer that cannot close after refusing a
                    // peer is worth seeing, but it is not what this test asserts - so it is printed
                    // rather than thrown, and never silently dropped.
                    println("$what: close() after the failure threw ${onClose::class.simpleName}: ${onClose.message}")
                }
            }
        if (failure == null) {
            fail("$what: the record was accepted. A producer that connects here is not verifying anything.")
        }
        val text = failure.chainText()
        // Recorded per arm rather than compared: the two clients word this differently and always
        // will, so a shared observation file would report a disagreement on every run and stop
        // being read. A person reads both, which is the point of writing them down.
        recordArmFact("tls.$what.failure", text.replace('\n', ' ').take(400))
        return text
    }

    private fun secureConfig(
        ca: String,
        servers: String,
    ): Map<String, String> =
        buildMap {
            put("bootstrap.servers", servers)
            put("security.protocol", "SSL")
            put("ssl.ca.location", ca)
            put("acks", "all")
            putAll(failFastConfig())
        }

    /**
     * The message plus every cause's.
     *
     * The Java client's top-level message is `SSL handshake failed`; the sentence naming the
     * certificate is two causes down. Asserting on the top message alone would have failed a client
     * that says exactly the right thing.
     */
    private fun Throwable.chainText(): String {
        val parts = mutableListOf<String>()
        var current: Throwable? = this
        var depth = 0
        while (current != null && depth < CAUSE_DEPTH) {
            parts += "${current::class.simpleName}: ${current.message}"
            current = current.cause
            depth++
        }
        return parts.joinToString(" <- ")
    }

    private companion object {
        const val RECORDS = 500
        const val CAUSE_DEPTH = 10
    }
}
