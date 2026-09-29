package io.github.youndie.kafkakn.schema

import io.ktor.client.HttpClient

/** The arm this suite runs on, for names that must not collide between the two. */
internal expect val arm: String

/** An environment variable, for what the runner hands the suite. */
internal expect fun env(name: String): String?

/** The fixture registry, which `ci/harness/broker.sh up` brings with the broker. */
internal val registryUrl: String get() = env("KAFKAKN_REGISTRY") ?: "http://127.0.0.1:18081"

/** This run's name, so the runner can find the run's own requests in the registry's log. */
internal val run: String get() = env("KAFKAKN_RUN") ?: "local"

/** A file's lines, for what the runner hands between the suite and the oracle harness. */
internal expect fun readLines(path: String): List<String>

/** Writes [lines] to [path], replacing it. */
internal expect fun writeLines(
    path: String,
    lines: List<String>,
)

/** The fixture registry's HTTPS listener (B-98), serving a certificate the fixture's CA signed. */
internal val registryTlsUrl: String get() = env("KAFKAKN_REGISTRY_TLS") ?: "https://127.0.0.1:18082"

/** The fixture's CA, and the one that signed nothing: `ci/broker/certs.sh` writes both. */
internal val caPath: String get() = env("KAFKAKN_CA") ?: "${env("HOME")}/.cache/kafkakn/tls/ca.pem"
internal val wrongCaPath: String get() = env("KAFKAKN_WRONG_CA") ?: "${env("HOME")}/.cache/kafkakn/tls/wrong-ca.pem"

/**
 * An HTTP client that trusts only the CA in [caPemPath], as a caller who needs HTTPS makes one: CIO on the JVM, Curl on
 * native, where CIO has no TLS (B-92).
 */
internal expect fun httpsClient(caPemPath: String): HttpClient
