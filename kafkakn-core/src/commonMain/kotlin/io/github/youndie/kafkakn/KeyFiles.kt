package io.github.youndie.kafkakn

/**
 * The text of a PEM file the caller named, or an [IllegalArgumentException] naming [key] and [path].
 *
 * The one piece of file reading common code needs (B-42): the form of a client key is a rule both
 * arms enforce, and a rule written once has to read the file once.
 */
internal expect fun readPemFile(
    key: String,
    path: String,
): String

/**
 * The client key must be PKCS#8 — `BEGIN PRIVATE KEY` or `BEGIN ENCRYPTED PRIVATE KEY` — on both arms
 * (B-42). Measured before this rule: librdkafka hands the file to OpenSSL and took the traditional
 * PKCS#1 form (`BEGIN RSA PRIVATE KEY`), plain and encrypted, and sent; the Java client refused both at
 * construction with "Invalid PEM keystore configs", which does not say the form is the problem. Refusing
 * on both, with the conversion, was chosen over converting on the JVM arm: that would be this library's
 * own cryptography, OpenSSL's traditional encryption included, in the arm whose value is having none.
 */
internal fun requirePkcs8(
    key: String,
    path: String,
) {
    val pem = readPemFile(key, path)
    if (PKCS8_MARKERS.any { it in pem }) return
    val found = BEGIN.find(pem)?.value ?: "no PEM private key at all"
    throw IllegalArgumentException(
        "$key '$path' holds $found, and both arms need a PKCS#8 key (BEGIN PRIVATE KEY or BEGIN ENCRYPTED " +
            "PRIVATE KEY). Convert it with: openssl pkcs8 -topk8 -in $path -out <new file> -v2 aes-256-cbc " +
            "(or -nocrypt for an unencrypted key)",
    )
}

private val PKCS8_MARKERS = listOf("-----BEGIN PRIVATE KEY-----", "-----BEGIN ENCRYPTED PRIVATE KEY-----")
private val BEGIN = Regex("-----BEGIN [A-Z ]*PRIVATE KEY-----")
