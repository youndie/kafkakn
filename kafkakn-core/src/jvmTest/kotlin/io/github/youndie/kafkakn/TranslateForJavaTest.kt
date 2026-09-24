package io.github.youndie.kafkakn

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The one place the oracle arm is not a plain delegate, asserted directly.
 *
 * Construction succeeding proves the Java client accepted whatever it was handed; it does not prove
 * the translation produced the value that actually turns hostname checking off. `""` and `"none"`
 * are both accepted by `kafka-clients` — the first disables the check, the second is a name JSSE
 * does not know and silently does not enforce — so a test that only watched the producer construct
 * would pass on the untranslated map as well.
 */
class TranslateForJavaTest {
    @Test
    fun `none becomes the empty string kafka-clients documents`() {
        val translated = translateForJava(mapOf(HOSTNAME_VERIFICATION to "none"))

        assertEquals("", translated[HOSTNAME_VERIFICATION])
    }

    @Test
    fun `https travels untouched`() {
        val translated = translateForJava(mapOf(HOSTNAME_VERIFICATION to "https"))

        assertEquals("https", translated[HOSTNAME_VERIFICATION])
    }

    @Test
    fun `the certificate authority becomes a PEM trust store`() {
        val translated = translateForJava(mapOf("ssl.ca.location" to "/etc/ca.pem"))

        assertEquals("/etc/ca.pem", translated["ssl.truststore.location"])
        assertEquals("PEM", translated["ssl.truststore.type"])
        assertEquals(null, translated["ssl.ca.location"])
    }

    @Test
    fun `both translations survive each other`() {
        // They used to be one `return` apart: the CA branch returned early, so a map carrying both
        // would have had the hostname value translated and then thrown away, or the reverse.
        val translated =
            translateForJava(
                mapOf("ssl.ca.location" to "/etc/ca.pem", HOSTNAME_VERIFICATION to "none"),
            )

        assertEquals("/etc/ca.pem", translated["ssl.truststore.location"])
        assertEquals("", translated[HOSTNAME_VERIFICATION])
    }

    // B-31. The Java client's PEM key store takes a PATH only as one file holding the chain AND the
    // key (DefaultSslEngineFactory.FileBasedPemStore hands the same contents to both), while
    // librdkafka's spelling is two paths. Two separate files reach it only as their CONTENTS, through
    // `ssl.keystore.certificate.chain` and `ssl.keystore.key` - read out of kafka-clients 4.3.1, not
    // assumed. The files are read by the fake below rather than the disk: what is asserted is where
    // each one's contents go.
    private val files =
        mapOf(
            "/etc/client.pem" to "-----BEGIN CERTIFICATE-----\nchain\n-----END CERTIFICATE-----\n",
            "/etc/client.key" to "-----BEGIN ENCRYPTED PRIVATE KEY-----\nkey\n-----END ENCRYPTED PRIVATE KEY-----\n",
        )

    private fun read(path: String): String = files.getValue(path)

    @Test
    fun `a certificate and its key become a PEM key store holding their contents`() {
        val translated =
            translateForJava(
                mapOf("ssl.certificate.location" to "/etc/client.pem", "ssl.key.location" to "/etc/client.key"),
                ::read,
            )

        assertEquals("PEM", translated["ssl.keystore.type"])
        assertEquals(files["/etc/client.pem"], translated["ssl.keystore.certificate.chain"])
        assertEquals(files["/etc/client.key"], translated["ssl.keystore.key"])
        // Not a PATH as well: kafka-clients refuses a key store location beside a separate key
        // ("Both SSL key store location and separate private key are specified").
        assertEquals(null, translated["ssl.keystore.location"])
        assertEquals(null, translated["ssl.certificate.location"])
        assertEquals(null, translated["ssl.key.location"])
    }

    @Test
    fun `the key password travels untouched, because both clients spell it the same`() {
        val translated =
            translateForJava(
                mapOf(
                    "ssl.certificate.location" to "/etc/client.pem",
                    "ssl.key.location" to "/etc/client.key",
                    "ssl.key.password" to "secret",
                ),
                ::read,
            )

        assertEquals("secret", translated["ssl.key.password"])
    }

    @Test
    fun `half a pair is translated as half a pair, for the Java client to refuse`() {
        // Not completed, not dropped: dropping the lone half would construct a producer with no client
        // certificate at all, which fails only at the handshake, against the one listener that asks.
        val translated = translateForJava(mapOf("ssl.certificate.location" to "/etc/client.pem"), ::read)

        assertEquals(files["/etc/client.pem"], translated["ssl.keystore.certificate.chain"])
        assertEquals(null, translated["ssl.keystore.key"])
        assertEquals(null, translated["ssl.certificate.location"])
    }

    @Test
    fun `all three translations survive each other`() {
        val translated =
            translateForJava(
                mapOf(
                    "ssl.ca.location" to "/etc/ca.pem",
                    HOSTNAME_VERIFICATION to "none",
                    "ssl.certificate.location" to "/etc/client.pem",
                    "ssl.key.location" to "/etc/client.key",
                ),
                ::read,
            )

        assertEquals("/etc/ca.pem", translated["ssl.truststore.location"])
        assertEquals("", translated[HOSTNAME_VERIFICATION])
        assertEquals(files["/etc/client.key"], translated["ssl.keystore.key"])
    }
}
