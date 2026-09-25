package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.incrementAndFetch
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-33](../../../../../../../docs/backlog/B-33-sasl-oauthbearer.md): SASL/OAUTHBEARER with a token the
 * caller supplies — the only shape both arms can honour, since the native bundle has no OIDC fetcher.
 *
 * The broker validates with Kafka's unsecured validator, so the tokens here are unsigned JWTs; what is
 * under test is the plumbing from a Kotlin function into each client, not a signature. The broker also
 * makes an OAUTHBEARER connection re-authenticate every ten seconds (`ci/broker/docker-compose.tls.yml`),
 * which is what turns "the provider was called again" into "the new token was used".
 */
@OptIn(ExperimentalAtomicApi::class, ExperimentalEncodingApi::class)
class OAuthBearerTest {
    @Test
    fun a_producer_with_a_token_provider_connects() =
        runTest(timeout = 2.minutes) {
            val stamp = "oauth-$armName-${randomSuffix()}"
            val producer = kafkaProducer(oauthConfig { token(lifetime = 60.seconds) })
            try {
                withContext(Dispatchers.Default) {
                    repeat(RECORDS) { index ->
                        producer.send(ProducerRecord(testTopic, "$stamp:$index".encodeToByteArray()))
                    }
                }
            } finally {
                withContext(Dispatchers.Default) { producer.close() }
            }
            recordArmFact("oauth.stamp", stamp)
            recordArmFact("oauth.count", RECORDS.toString())
        }

    @Test
    fun a_provider_that_throws_fails_with_its_own_exception_visible() =
        runTest(timeout = 2.minutes) {
            // Construction or the first send: the Java client logs in while it constructs, librdkafka
            // asks for a token when it first connects. Either is fine; what matters is what the caller reads.
            val said =
                try {
                    val producer = kafkaProducer(oauthConfig(failFastConfig()) { throw NoTokenForYou() })
                    try {
                        val never = ProducerRecord(testTopic, "never".encodeToByteArray())
                        withContext(Dispatchers.Default) { producer.send(never) }
                        fail("a producer whose provider throws sent a record")
                    } finally {
                        withContext(Dispatchers.Default) { producer.close() }
                    }
                } catch (cancellation: CancellationException) {
                    throw cancellation
                } catch (failed: Exception) {
                    failed.chainText()
                }
            recordArmFact("oauth.provider-throws", said.replace('\n', ' ').take(REASON))
            assertTrue(said.contains(NoTokenForYou.WORDS), "the provider's exception is not visible: $said")
        }

    @Test
    fun an_expiring_token_is_refreshed_through_the_provider_without_the_caller_doing_anything() =
        runTest(timeout = 3.minutes) {
            // Tokens that live twelve seconds, and sends for thirty: both clients refresh at about eighty
            // per cent of a token's life, and the broker re-authenticates every ten seconds - so a client
            // that did not refresh would present an expired token and be refused.
            val issued = AtomicInt(0)
            val producer =
                kafkaProducer(
                    oauthConfig {
                        issued.incrementAndFetch()
                        token(lifetime = 12.seconds)
                    },
                )
            var sent = 0
            try {
                withContext(Dispatchers.Default) {
                    val until = TimeSource.Monotonic.markNow() + 30.seconds
                    while (until.hasNotPassedNow()) {
                        producer.send(ProducerRecord(testTopic, "oauth-refresh-$armName-$sent".encodeToByteArray()))
                        sent++
                        delay(250.milliseconds)
                    }
                }
            } finally {
                withContext(Dispatchers.Default) { producer.close() }
            }
            recordArmFact("oauth.refresh.tokens", issued.load().toString())
            recordArmFact("oauth.refresh.sent", sent.toString())
            assertTrue(issued.load() >= 2, "one token in thirty seconds of twelve-second tokens: nothing refreshed")
        }

    @Test
    fun a_consumer_or_an_admin_client_refuses_oauthbearer_for_now() {
        // Only the producer takes a provider; the other two have nowhere to put one, and a mechanism with
        // no token source is a connection that can never authenticate - refused, not attempted.
        val oauth =
            mapOf(
                "bootstrap.servers" to saslBootstrap,
                "security.protocol" to "SASL_PLAINTEXT",
                "sasl.mechanism" to "OAUTHBEARER",
            )
        val consumer =
            assertFailsWith<IllegalArgumentException> {
                kafkaConsumer(
                    ConsumerConfig(oauth + ("group.id" to "g")),
                )
            }
        val admin = assertFailsWith<IllegalArgumentException> { kafkaAdmin(AdminConfig(oauth)) }
        for (refused in listOf(consumer, admin)) {
            assertTrue(refused.message.orEmpty().contains("OAuthBearerTokenProvider"), refused.message)
        }
    }

    private class NoTokenForYou : RuntimeException(WORDS) {
        companion object {
            const val WORDS = "the identity provider said no to kafkakn"
        }
    }

    private fun oauthConfig(
        extra: Map<String, String> = emptyMap(),
        provider: OAuthBearerTokenProvider,
    ) = ProducerConfig(
        mapOf(
            "bootstrap.servers" to saslBootstrap,
            "security.protocol" to "SASL_PLAINTEXT",
            "sasl.mechanism" to "OAUTHBEARER",
        ) + extra,
        oauthBearerTokenProvider = provider,
    )

    /** An unsigned JWT - `alg: none` - which is what the broker's unsecured validator accepts. */
    @Suppress("ktlint:kapkan:wall-clock", "the token's own claims are wall-clock time by definition")
    private fun token(lifetime: kotlin.time.Duration): OAuthBearerToken {
        val now = Clock.System.now().toEpochMilliseconds()
        val expires = now + lifetime.inWholeMilliseconds

        fun part(json: String) = Base64.UrlSafe.encode(json.encodeToByteArray()).trimEnd('=')
        val value =
            part("""{"alg":"none"}""") + "." + part("""{"sub":"alice","iat":${now / 1000},"exp":${expires / 1000}}""") +
                "."
        return OAuthBearerToken(value = value, principal = "alice", expiresAtMillis = expires)
    }

    private companion object {
        const val RECORDS = 50
        const val REASON = 400
    }
}
