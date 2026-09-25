package io.github.youndie.kafkakn

/**
 * Producer configuration, with Kafka's own key names.
 *
 * The keys are **not** renamed into a Kotlin vocabulary: an operator reading a kafkakn
 * configuration should be able to search Kafka's own documentation for the key in front of them,
 * and a wrapper that invents `bootstrapServers` breaks that for no gain.
 *
 * A key neither arm honours is a failure at construction rather than a silently ignored entry — an
 * option accepted and dropped behaves exactly like one that worked, until it matters. That
 * validation is not here yet; it arrives with the first arm that can say what it honours.
 */
public class ProducerConfig(
    public val properties: Map<String, String>,
    /**
     * The caller's source of SASL/OAUTHBEARER tokens, required when `sasl.mechanism` is `OAUTHBEARER`
     * and refused otherwise ([OAuthBearerTokenProvider], B-33).
     */
    public val oauthBearerTokenProvider: OAuthBearerTokenProvider? = null,
) {
    public constructor(vararg pairs: Pair<String, String>) : this(pairs.toMap())

    public operator fun get(key: String): String? = properties[key]

    override fun toString(): String = "ProducerConfig(${properties.keys.sorted()})"
}
