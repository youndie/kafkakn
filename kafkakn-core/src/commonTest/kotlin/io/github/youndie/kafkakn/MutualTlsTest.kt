package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertFails
import kotlin.test.assertTrue

/**
 * [B-31](../../../../../../../docs/backlog/B-31-client-certificates.md): a broker that asks who is
 * connecting gets an answer — [feature-secure-connection](../../../../../../../docs/features/feature-secure-connection.md).
 *
 * The shape is [TlsTest]'s, turned round. There the client checks the broker; here the broker, on a
 * listener set to `ssl.client.auth=required`, checks the client. The two refusals are what make the
 * acceptance mean anything: a listener that quietly does not ask connects the right certificate, and
 * so does one that only checks that *a* certificate was presented.
 *
 * The keys are librdkafka's — `ssl.certificate.location`, `ssl.key.location`, `ssl.key.password` —
 * because the contract has spelled TLS that way since B-11, and the JVM arm turns them into a PEM key
 * store (`translateForJava`, asserted directly in `TranslateForJavaTest`).
 *
 * The listener's own tools asked the broker the same three questions before any of this ran
 * (`broker.sh mtls-selftest`); a refusal read here is the broker's, not a fixture that was down.
 */
class MutualTlsTest {
    @Test
    fun the_right_client_certificate_connects() =
        runTest {
            val stamp = "mtls-$armName-${randomSuffix()}"
            val producer = kafkaProducer(ProducerConfig(mutualConfig(clientCertPath, clientKeyPath, clientKeyPassword)))
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
            // Counted by the harness over the PLAINTEXT listener, as for TLS.
            recordArmFact("mtls.stamp", stamp)
            recordArmFact("mtls.count", RECORDS.toString())
        }

    @Test
    fun no_certificate_is_refused_and_the_message_says_so() =
        runTest {
            // Exactly the configuration that connects to the TLS listener next door: the broker is
            // trusted, and the only thing missing is the answer to the question this listener asks.
            val text = refusalText(mutualConfig(certificate = null, key = null, password = null), "mtls-no-certificate")
            assertNamesTheCertificate(text)
        }

    @Test
    fun a_certificate_from_the_wrong_authority_is_refused() =
        runTest {
            val wrong = mutualConfig(wrongClientCertPath, wrongClientKeyPath, password = null)
            val text = refusalText(wrong, "mtls-wrong-authority")
            assertNamesTheCertificate(text)
        }

    @Test
    fun a_certificate_without_its_key_is_refused_at_construction_on_both_arms() {
        // Half a key store. librdkafka constructs a producer from a certificate with no key - measured
        // 2026-09-24, it checks the pair only when a key is set - and that producer presents nothing,
        // so the refusal would arrive minutes later, at the first handshake. The Java client refuses
        // at construction. Asserted on the message, because "it threw" alone was green on the JVM
        // arm for a different reason before this item: it did not know either key.
        val halves =
            listOf(
                mutualConfig(clientCertPath, key = null, password = null),
                mutualConfig(certificate = null, key = clientKeyPath, password = clientKeyPassword),
            )
        for (half in halves) {
            val refused =
                assertFails("constructed with ${half.keys.filter { it.startsWith("ssl.") }.sorted()}") {
                    kafkaProducer(ProducerConfig(half)).also { runTest { it.close() } }
                }
            assertTrue(
                refused.message.orEmpty().contains("come together"),
                "refused, but not by the pair rule: ${refused.message}",
            )
        }
    }

    /**
     * A refusal that does not mention the certificate is the same sentence a closed port produces,
     * and a caller who reads it will look at the network while the answer is a file.
     */
    private fun assertNamesTheCertificate(text: String) {
        assertTrue(
            text.lowercase().contains("certif"),
            "the refusal must say it was about the certificate, not merely that the broker is " +
                "unreachable. It said: $text",
        )
    }

    private fun mutualConfig(
        certificate: String?,
        key: String?,
        password: String?,
    ): Map<String, String> =
        buildMap {
            put("bootstrap.servers", mtlsBootstrap)
            put("security.protocol", "SSL")
            put("ssl.ca.location", caPath)
            certificate?.let { put("ssl.certificate.location", it) }
            key?.let { put("ssl.key.location", it) }
            password?.let { put("ssl.key.password", it) }
            put("acks", "all")
            putAll(failFastConfig())
        }

    private companion object {
        const val RECORDS = 200
    }
}
