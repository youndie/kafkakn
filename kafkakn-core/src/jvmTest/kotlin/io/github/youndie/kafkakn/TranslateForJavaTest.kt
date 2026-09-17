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
}
