package io.github.youndie.kafkakn

/**
 * librdkafka's switch for certificate trust. **Refused, on both arms.**
 *
 * `kafka-clients` has no equivalent — `ssl.endpoint.identification.algorithm` turns off *hostname*
 * checking and never trust — so by the contract's own configuration rule this is a platform key, and
 * the platform it belongs to is the one with no oracle. A caller who turned trust off would be alone
 * with the implementation this whole project exists to check.
 */
internal const val CERTIFICATE_VERIFICATION: String = "enable.ssl.certificate.verification"

/** Hostname verification, which both clients have. The one remaining way to weaken TLS here. */
internal const val HOSTNAME_VERIFICATION: String = "ssl.endpoint.identification.algorithm"

/**
 * **`none` is this library's spelling of "off", and it is librdkafka's.**
 *
 * The two clients disagree about the value, not the key: librdkafka refuses an empty one outright
 * (*"cannot be set to empty value"*, measured 2026-09-17), and `kafka-clients` documents the empty
 * string as the way to disable. One of the two spellings has to be the contract's, and an empty
 * string is also exactly what an environment variable that expanded to nothing produces — a way to
 * switch off a security check by accident. So `none` travels, and the JVM arm translates it.
 */
internal val HOSTNAME_VERIFICATION_VALUES: Set<String> = setOf("none", "https")

/**
 * Refuses the configuration this library will not carry, before either arm has touched a socket.
 *
 * Both actuals call it, which is the point: a rule enforced on one arm is a rule the caller meets
 * for the first time on the platform they do not run locally.
 */
internal fun ProducerConfig.checkTlsKeys() {
    require(CERTIFICATE_VERIFICATION !in properties) {
        "$CERTIFICATE_VERIFICATION is refused on both arms: it exists only in librdkafka, so it " +
            "could only ever be honoured on the arm with no oracle. Certificate trust is not " +
            "configurable through this API — see docs/api/producer-contract.md"
    }
    val hostname = properties[HOSTNAME_VERIFICATION] ?: return
    require(hostname in HOSTNAME_VERIFICATION_VALUES) {
        "$HOSTNAME_VERIFICATION must be ${HOSTNAME_VERIFICATION_VALUES.sorted().joinToString(" or ")}, " +
            "not '$hostname'. The two clients spell 'off' differently and this library spells it " +
            "'none'; an empty value is refused because it is also what a variable that expanded to " +
            "nothing looks like"
    }
}
