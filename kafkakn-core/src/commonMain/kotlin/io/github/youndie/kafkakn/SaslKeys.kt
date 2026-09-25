package io.github.youndie.kafkakn

/**
 * The SASL keys the contract carries (B-32). `sasl.mechanism` is the Java client's name and an alias
 * librdkafka accepts for its own `sasl.mechanisms`; `sasl.username` and `sasl.password` are
 * librdkafka's, and the JVM arm turns them into `sasl.jaas.config`.
 */
internal const val SASL_MECHANISM: String = "sasl.mechanism"
internal const val SASL_USERNAME: String = "sasl.username"
internal const val SASL_PASSWORD_KEY: String = "sasl.password"

/** librdkafka's own spelling of [SASL_MECHANISM], a platform key the native arm may still be given. */
private const val SASL_MECHANISMS_NATIVE: String = "sasl.mechanisms"

/** The mechanisms a username and a password are the credentials of. */
internal val PASSWORD_MECHANISMS: Set<String> = setOf("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")

private val SASL_PROTOCOLS = setOf("SASL_PLAINTEXT", "SASL_SSL")

/**
 * Refuses the SASL configuration that each client would refuse in its own words, or not at all.
 *
 * Both actuals call it. Each rule below was watched failing differently per arm before it existed:
 * librdkafka answered the missing mechanism with *"No provider for SASL mechanism GSSAPI"* and the
 * missing password with *"sasl.username and sasl.password must be set"*, while the JVM arm had not
 * heard of either key — three sentences for two mistakes, none naming the key the caller forgot.
 */
internal fun ProducerConfig.checkSaslKeys() {
    val protocol = properties["security.protocol"]?.uppercase()
    val mechanism = properties[SASL_MECHANISM] ?: properties[SASL_MECHANISMS_NATIVE]
    // Both clients default to GSSAPI, which the native bundle does not have: the default is a failure
    // on one arm and a Kerberos attempt on the other. So the default is not taken at all.
    require(protocol !in SASL_PROTOCOLS || mechanism != null) {
        "$SASL_MECHANISM must be set when security.protocol is $protocol: both clients default to " +
            "GSSAPI, which this library cannot offer on both arms. Name PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 or OAUTHBEARER"
    }
    // OAUTHBEARER and a token provider come together (B-33). A provider is the only way either arm
    // gets a token here, so the mechanism without one is a connection that can never authenticate;
    // and a provider for any other mechanism would be a function nothing ever calls.
    val oauth = mechanism?.uppercase() == OAUTHBEARER
    require(oauth == (oauthBearerTokenProvider != null)) {
        if (oauth) {
            "$SASL_MECHANISM=$OAUTHBEARER needs an OAuthBearerTokenProvider in ProducerConfig: the " +
                "library does not fetch tokens itself, on either arm"
        } else {
            "an OAuthBearerTokenProvider is for $SASL_MECHANISM=$OAUTHBEARER, and $SASL_MECHANISM is ${mechanism ?: "not set"}"
        }
    }
    val hasUser = SASL_USERNAME in properties
    val hasPassword = SASL_PASSWORD_KEY in properties
    require(hasUser == hasPassword) {
        "$SASL_USERNAME and $SASL_PASSWORD_KEY come together, and only " +
            "${if (hasUser) SASL_USERNAME else SASL_PASSWORD_KEY} is set"
    }
    // A username handed to a mechanism that takes none - or to no mechanism, which means GSSAPI - is
    // dropped by librdkafka and cannot be put in any login module on the JVM arm: accepted on one arm,
    // meaningless on both.
    require(!hasUser || mechanism?.uppercase() in PASSWORD_MECHANISMS) {
        "$SASL_USERNAME and $SASL_PASSWORD_KEY are the credentials of " +
            "${PASSWORD_MECHANISMS.sorted().joinToString()}, and $SASL_MECHANISM is ${mechanism ?: "not set"}"
    }
}
