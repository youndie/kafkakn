package io.github.youndie.kafkakn.schema

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import java.io.File
import java.security.KeyStore
import java.security.cert.CertificateFactory
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager

internal actual val arm: String = "jvm"

internal actual fun env(name: String): String? = System.getenv(name)

internal actual fun readLines(path: String): List<String> = java.io.File(path).readLines()

internal actual fun writeLines(
    path: String,
    lines: List<String>,
) {
    java.io.File(path).writeText(lines.joinToString("\n", postfix = "\n"))
}

/** CIO, trusting only the CA in [caPemPath]: the JVM's own TLS, with hostname verification on, as by default. */
internal actual fun httpsClient(caPemPath: String): HttpClient {
    val ca = File(caPemPath).inputStream().use { CertificateFactory.getInstance("X.509").generateCertificate(it) }
    val store =
        KeyStore.getInstance(KeyStore.getDefaultType()).apply {
            load(null, null)
            setCertificateEntry("fixture-ca", ca)
        }
    val trust =
        TrustManagerFactory
            .getInstance(TrustManagerFactory.getDefaultAlgorithm())
            .apply { init(store) }
            .trustManagers
            .filterIsInstance<X509TrustManager>()
            .single()
    return HttpClient(CIO) { engine { https { trustManager = trust } } }
}
