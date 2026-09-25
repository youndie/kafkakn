package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * [B-42](../../../../../../../docs/backlog/B-42-a-pkcs1-key-works-on-one-arm.md): a client key in
 * OpenSSL's traditional PKCS#1 form is refused at construction on BOTH arms, and the message says how
 * to convert it.
 *
 * Measured before this rule (`logs/b-42/`): the native arm constructed a producer with either form and
 * sent over the listener that requires a client certificate; the JVM arm refused both at construction
 * with *"Invalid PEM keystore configs"* over an `IOException` — a sentence that does not say the
 * problem is the key's form. One key file, two answers, and the helpful one on neither.
 */
class Pkcs1KeyTest {
    @Test
    fun a_pkcs1_key_is_refused_at_construction_on_both_arms_with_the_way_out() =
        runTest {
            for ((name, key) in listOf("plain" to clientPkcs1KeyPath, "encrypted" to clientPkcs1EncryptedKeyPath)) {
                val refused =
                    assertFailsWith<IllegalArgumentException>("$name PKCS#1 key constructed") { construct(key) }
                val said = refused.message.orEmpty()
                assertTrue(said.contains("PKCS#8") && said.contains("openssl pkcs8 -topk8"), "$name: $said")
                recordArmFact("pkcs1.$name", said.take(REASON))
            }
        }

    @Test
    fun the_pkcs8_key_the_suite_already_uses_is_still_accepted() =
        runTest {
            // The positive control: the rule reads the key's form, and the form it must let through is
            // the encrypted PKCS#8 key every client-certificate test uses.
            construct(clientKeyPath)
        }

    private suspend fun construct(key: String) {
        val producer =
            kafkaProducer(
                ProducerConfig(
                    "bootstrap.servers" to mtlsBootstrap,
                    "security.protocol" to "SSL",
                    "ssl.ca.location" to caPath,
                    "ssl.certificate.location" to clientCertPath,
                    "ssl.key.location" to key,
                    "ssl.key.password" to clientKeyPassword,
                ),
            )
        withContext(Dispatchers.Default) { producer.close() }
    }

    private companion object {
        const val REASON = 300
    }
}
