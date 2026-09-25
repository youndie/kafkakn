package io.github.youndie.kafkakn

import kotlinx.coroutines.runBlocking
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.auth.SaslExtensions
import org.apache.kafka.common.security.auth.SaslExtensionsCallback
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import javax.security.auth.callback.Callback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.AppConfigurationEntry
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken as ApacheToken

/**
 * The bridge from a Kotlin function into the Java client's OAUTHBEARER login (B-33) — and the design
 * problem the item named. The Java client configures its login callback handler **by class name** and
 * instantiates it itself, so a caller's provider cannot be handed to it; it has to be found.
 *
 * It is found through the JAAS options, the one per-client channel from configuration into the
 * handler's `configure`: the producer registers its provider here under a fresh id, writes that id
 * into `sasl.jaas.config` as `kafkakn.provider`, and removes it on `close`.
 */
internal object OAuthBearerProviders {
    const val OPTION = "kafkakn.provider"
    private val providers = ConcurrentHashMap<String, OAuthBearerTokenProvider>()

    fun register(provider: OAuthBearerTokenProvider): String =
        UUID.randomUUID().toString().also {
            providers[it] =
                provider
        }

    fun unregister(id: String) {
        providers.remove(id)
    }

    fun find(id: String?): OAuthBearerTokenProvider? = id?.let { providers[it] }

    /** The properties that route the Java client's OAUTHBEARER login to the provider registered as [id]. */
    fun properties(id: String): Map<String, String> =
        mapOf(
            "sasl.jaas.config" to
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required $OPTION=${jaasQuoted(
                    id,
                )};",
            "sasl.login.callback.handler.class" to KafkaknOAuthBearerLoginHandler::class.java.name,
        )
}

/**
 * Instantiated by the Java client, by name, once per login context; asks the caller's provider for a
 * token on every login — the first, and every refresh the client's refreshing login schedules.
 *
 * The provider suspends and this is a blocking callback on the client's own login thread, so it is run
 * with `runBlocking` there: that thread exists to wait for exactly this.
 *
 * **A provider that throws is reported through `OAuthBearerTokenCallback.error`, not by throwing.**
 * Measured, and then read in `OAuthBearerLoginModule.identifyToken` (4.3.1): an exception out of the
 * handler is logged and replaced by *"An internal error occurred while retrieving token from callback
 * handler"*, which says nothing about the provider; the callback's error description is what becomes
 * the `LoginException`'s message. So the provider's words reach the caller that way — its words, not
 * the exception object, which the Java client's login does not carry.
 */
public class KafkaknOAuthBearerLoginHandler : AuthenticateCallbackHandler {
    private companion object {
        /** The error code the Java client requires beside a description; RFC 6749's closest is not ours. */
        const val PROVIDER_FAILED = "kafkakn_token_provider_failed"
    }

    private var provider: OAuthBearerTokenProvider? = null

    override fun configure(
        configs: MutableMap<String, *>,
        saslMechanism: String,
        jaasConfigEntries: MutableList<AppConfigurationEntry>,
    ) {
        val id = jaasConfigEntries.firstNotNullOfOrNull { it.options[OAuthBearerProviders.OPTION] as String? }
        provider =
            OAuthBearerProviders.find(id)
                ?: throw IllegalStateException("no OAUTHBEARER token provider is registered as '$id'")
    }

    override fun handle(callbacks: Array<Callback>) {
        for (callback in callbacks) {
            when (callback) {
                is OAuthBearerTokenCallback -> fetch(callback)

                // No extensions unless the token carries them; asked for on every login.
                is SaslExtensionsCallback -> callback.extensions(SaslExtensions.empty())

                else -> throw UnsupportedCallbackException(callback)
            }
        }
    }

    private fun fetch(callback: OAuthBearerTokenCallback) {
        val token =
            try {
                runBlocking { provider!!.token() }
            } catch (failed: Exception) {
                callback.error(PROVIDER_FAILED, "OAUTHBEARER token provider failed: ${failed.message}", null)
                return
            }
        callback.token(
            object : ApacheToken {
                override fun value() = token.value

                override fun scope() = emptySet<String>()

                override fun lifetimeMs() = token.expiresAtMillis

                override fun principalName() = token.principal

                // Optional in the interface, and the provider did not say: no second clock read here.
                override fun startTimeMs(): Long? = null
            },
        )
    }

    override fun close() {
        provider = null
    }
}
