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
 * [B-32](../../../../../../../docs/backlog/B-32-sasl-plain-and-scram.md): SASL PLAIN and SCRAM —
 * [feature-secure-connection](../../../../../../../docs/features/feature-secure-connection.md).
 *
 * The keys are `sasl.mechanism`, `sasl.username`, `sasl.password`: the first is the Java client's own
 * name and an alias librdkafka accepts, the other two are librdkafka's, and the JVM arm builds
 * `sasl.jaas.config` from them. That string is where the translation can go wrong, and only there —
 * so the password the JAAS format has to escape is a scenario of its own, and the native arm, which
 * sends it raw, is what proves the broker holds the password the test means.
 *
 * The broker's own tools refused the wrong password for PLAIN and SCRAM before any of this ran
 * (`broker.sh sasl-selftest`). Records are counted over the PLAINTEXT listener by `ci/b-32/run.sh`.
 */
class SaslTest {
    @Test
    fun plain_connects() = connects("plain", saslConfig("PLAIN"))

    @Test
    fun scram_sha_256_connects() = connects("scram256", saslConfig("SCRAM-SHA-256"))

    @Test
    fun scram_sha_512_connects() = connects("scram512", saslConfig("SCRAM-SHA-512"))

    @Test
    fun scram_sha_512_over_tls_connects() =
        connects(
            "ssl-scram512",
            saslConfig("SCRAM-SHA-512", protocol = "SASL_SSL", servers = saslSslBootstrap) +
                ("ssl.ca.location" to caPath),
        )

    @Test
    fun a_password_with_a_double_quote_and_a_backslash_connects() =
        connects("quoted", saslConfig("PLAIN", user = QUOTED_USER, password = QUOTED_PASSWORD))

    @Test
    fun the_wrong_password_is_refused_and_the_message_names_authentication() =
        runTest {
            for (mechanism in listOf("PLAIN", "SCRAM-SHA-256")) {
                val wrong = saslConfig(mechanism, password = "not-the-password")
                val text = refusalText(wrong, "sasl-wrong-${mechanism.lowercase()}")
                assertTrue(
                    text.lowercase().contains("authentic"),
                    "$mechanism: the refusal must say it was authentication, not merely that the broker " +
                        "is unreachable. It said: $text",
                )
            }
        }

    @Test
    fun a_sasl_protocol_without_a_mechanism_is_refused_at_construction_on_both_arms() {
        // Both clients default to GSSAPI, which is not in the native bundle: the default would be a
        // failure on one arm and a Kerberos attempt on the other.
        for (protocol in listOf("SASL_PLAINTEXT", "SASL_SSL")) {
            val unnamed = saslConfig("PLAIN", protocol = protocol) - "sasl.mechanism"
            val refused = assertFails("$protocol constructed with no mechanism") { construct(unnamed) }
            assertTrue(
                refused.message.orEmpty().contains("sasl.mechanism"),
                "$protocol: refused, but the message does not name the key: ${refused.message}",
            )
        }
    }

    @Test
    fun a_username_without_its_password_is_refused_at_construction_on_both_arms() {
        for (half in listOf(saslConfig("PLAIN") - "sasl.password", saslConfig("PLAIN") - "sasl.username")) {
            val refused =
                assertFails(
                    "constructed with ${half.keys.filter { it.startsWith("sasl.") }.sorted()}",
                ) { construct(half) }
            assertTrue(
                refused.message.orEmpty().contains("come together"),
                "refused, but not by the pair rule: ${refused.message}",
            )
        }
    }

    @Test
    fun credentials_for_a_mechanism_that_takes_none_are_refused_at_construction_on_both_arms() {
        // GSSAPI takes a Kerberos principal, not a password: librdkafka would drop the pair, and the
        // JVM arm has no login module to put it in. No mechanism at all means GSSAPI to both clients.
        val cases =
            listOf(
                saslConfig("GSSAPI"),
                saslConfig("PLAIN", protocol = "PLAINTEXT", servers = bootstrap) - "sasl.mechanism",
            )
        for (case in cases) {
            val refused = assertFails("constructed with ${case["sasl.mechanism"]}") { construct(case) }
            assertTrue(
                refused.message.orEmpty().contains("are the credentials of"),
                "refused, but not by the mechanism rule: ${refused.message}",
            )
        }
    }

    private fun construct(properties: Map<String, String>) {
        kafkaProducer(ProducerConfig(properties)).also { runTest { it.close() } }
    }

    private fun connects(
        what: String,
        properties: Map<String, String>,
    ) = runTest {
        val stamp = "sasl-$what-$armName-${randomSuffix()}"
        val producer = kafkaProducer(ProducerConfig(properties))
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
        recordArmFact("sasl.$what.stamp", stamp)
        recordArmFact("sasl.$what.count", RECORDS.toString())
    }

    private fun saslConfig(
        mechanism: String,
        protocol: String = "SASL_PLAINTEXT",
        servers: String = saslBootstrap,
        user: String = SASL_USER,
        password: String = SASL_PASSWORD,
    ): Map<String, String> =
        buildMap {
            put("bootstrap.servers", servers)
            put("security.protocol", protocol)
            put("sasl.mechanism", mechanism)
            put("sasl.username", user)
            put("sasl.password", password)
            put("acks", "all")
            putAll(failFastConfig())
        }

    private companion object {
        const val RECORDS = 100
    }
}
