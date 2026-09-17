package io.github.youndie.kafkakn

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFails
import kotlin.test.assertTrue

/**
 * [The producer contract, *Configuration*](../../../../../../../docs/api/producer-contract.md), and
 * [B-18](../../../../../../../docs/backlog/B-18-verification-cannot-be-turned-off.md).
 *
 * **Two keys of the same family, decided in opposite directions**, and the difference is whether an
 * oracle can see what they do.
 *
 * `enable.ssl.certificate.verification` is librdkafka's alone. `kafka-clients` has no equivalent —
 * `ssl.endpoint.identification.algorithm` disables hostname checking, never trust — so by this
 * contract's own configuration rule it is a *platform* key, and the platform it belongs to is the
 * one without a witness. A caller who turned trust off would be alone with the implementation this
 * whole project exists to check. It is refused on **both** arms, so that the two say the same thing
 * and the README's sentence is true.
 *
 * `ssl.endpoint.identification.algorithm` exists on both arms, carries Kafka's own name, and
 * whatever it does happens where the oracle can see it. It passes — and the contract names it as the
 * one remaining way to weaken TLS through this API, so that "verification is on and cannot be turned
 * off" is read exactly as far as it is true.
 *
 * **The arms disagree about its value, which is not what B-18 expected.** Measured 2026-09-17: given
 * the empty string that `kafka-clients` documents as "off", librdkafka answers *"cannot be set to
 * empty value"*. So the contract's spelling is `none`, the JVM arm translates it, and an empty value
 * is refused on both — it is also what a variable that expanded to nothing looks like.
 */
class VerificationTest {
    @Test
    fun turning_certificate_verification_off_is_refused_at_construction() {
        val failure =
            assertFails {
                kafkaProducer(
                    ProducerConfig(
                        "bootstrap.servers" to bootstrap,
                        "security.protocol" to "SSL",
                        "enable.ssl.certificate.verification" to "false",
                    ),
                )
            }

        // The message has to NAME the key. A caller who set it is looking for the word they typed,
        // and "invalid configuration" sends them to read the whole map.
        assertTrue(
            failure.message.orEmpty().contains("enable.ssl.certificate.verification"),
            "the refusal must name the key. It said: ${failure.message}",
        )
    }

    @Test
    fun the_key_is_refused_even_where_it_asks_for_verification_to_stay_on() {
        // `=true` is the default and changes nothing, which is exactly why it is still refused: the
        // key is not portable, and a configuration that constructs on one arm and throws on the
        // other is the defect, not the value it carries. Refusing only `false` would also leave the
        // caller free to flip it later in a file this library never sees.
        assertFails {
            kafkaProducer(
                ProducerConfig(
                    "bootstrap.servers" to bootstrap,
                    "security.protocol" to "SSL",
                    "enable.ssl.certificate.verification" to "true",
                ),
            )
        }
    }

    @Test
    fun hostname_verification_is_still_reachable_on_both_arms() =
        runTest {
            // The knob that survives, and it has to construct on BOTH arms or the contract's claim
            // that it is portable is a guess. No `ssl.ca.location`: this asks whether the key is
            // accepted, and dragging a certificate file in would make the test fail for a second
            // reason on a machine where the broker fixture has not run.
            val producer =
                kafkaProducer(
                    ProducerConfig(
                        "bootstrap.servers" to bootstrap,
                        "security.protocol" to "SSL",
                        "ssl.endpoint.identification.algorithm" to "none",
                    ),
                )
            producer.close()
        }

    @Test
    fun the_default_spelling_of_hostname_verification_is_accepted_too() =
        runTest {
            // `https` is both clients' default and travels untranslated. Without this the suite
            // would only ever exercise the value that goes through a translation, and a rule that
            // accepts exactly one value is indistinguishable from one that accepts none.
            val producer =
                kafkaProducer(
                    ProducerConfig(
                        "bootstrap.servers" to bootstrap,
                        "security.protocol" to "SSL",
                        "ssl.endpoint.identification.algorithm" to "https",
                    ),
                )
            producer.close()
        }

    @Test
    fun the_empty_value_kafka_clients_documents_is_refused_on_both_arms() {
        // The JVM would take it and the native arm cannot, so accepting it would be a configuration
        // that works on the arm the caller runs locally and fails on the one they ship.
        val failure =
            assertFails {
                kafkaProducer(
                    ProducerConfig(
                        "bootstrap.servers" to bootstrap,
                        "security.protocol" to "SSL",
                        "ssl.endpoint.identification.algorithm" to "",
                    ),
                )
            }

        assertTrue(
            failure.message.orEmpty().contains("ssl.endpoint.identification.algorithm"),
            "the refusal must name the key. It said: ${failure.message}",
        )
    }
}
