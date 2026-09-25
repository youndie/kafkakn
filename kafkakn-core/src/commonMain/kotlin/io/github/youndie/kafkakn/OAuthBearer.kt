package io.github.youndie.kafkakn

/**
 * A SASL/OAUTHBEARER token as the caller obtained it ([B-33](../../../../../../../docs/backlog/B-33-sasl-oauthbearer.md)).
 *
 * [value] is the token itself, sent to the broker as it is; [principal] is who it names; [expiresAtMillis]
 * is when it stops being valid, in milliseconds since the epoch — both clients refresh through the
 * provider before then, at about eighty per cent of the token's life.
 */
public class OAuthBearerToken(
    public val value: String,
    public val principal: String,
    public val expiresAtMillis: Long,
    public val extensions: Map<String, String> = emptyMap(),
) {
    init {
        require(value.isNotEmpty()) { "an OAUTHBEARER token needs a value" }
        require(principal.isNotEmpty()) { "an OAUTHBEARER token needs a principal" }
    }

    override fun toString(): String = "OAuthBearerToken(principal=$principal, expiresAtMillis=$expiresAtMillis)"
}

/**
 * Where the caller's tokens come from. **The only OAUTHBEARER shape both arms can honour**: the native
 * bundle carries the OAUTHBEARER mechanism and not librdkafka's OIDC fetcher (it is built without curl),
 * so fetching is the caller's, and the library asks for a token when one is needed or about to expire.
 *
 * A provider that throws fails the connection, and its exception is what the caller sees — not a
 * generic authentication error. It may suspend; neither arm calls it on the caller's dispatcher.
 */
public fun interface OAuthBearerTokenProvider {
    public suspend fun token(): OAuthBearerToken
}

/** The mechanism a token provider serves. */
internal const val OAUTHBEARER: String = "OAUTHBEARER"
