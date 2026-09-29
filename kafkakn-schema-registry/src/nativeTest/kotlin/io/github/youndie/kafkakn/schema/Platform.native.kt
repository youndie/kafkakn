@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn.schema

import io.ktor.client.HttpClient
import io.ktor.client.engine.curl.Curl
import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.toKString
import platform.posix.fclose
import platform.posix.fgets
import platform.posix.fopen
import platform.posix.fputs
import platform.posix.getenv

internal actual val arm: String = "linuxX64"

internal actual fun env(name: String): String? = getenv(name)?.toKString()

internal actual fun readLines(path: String): List<String> {
    val file = fopen(path, "r") ?: error("cannot open $path")
    try {
        val lines = mutableListOf<String>()
        memScoped {
            val buffer = allocArray<ByteVar>(LINE)
            while (fgets(buffer, LINE, file) != null) lines += buffer.toKString().trimEnd('\n')
        }
        return lines
    } finally {
        fclose(file)
    }
}

internal actual fun writeLines(
    path: String,
    lines: List<String>,
) {
    val file = fopen(path, "w") ?: error("cannot open $path")
    try {
        lines.forEach { fputs("$it\n", file) }
    } finally {
        fclose(file)
    }
}

/** Curl, trusting only the CA in [caPemPath]: libcurl verifies the peer and the host name by default. */
internal actual fun httpsClient(caPemPath: String): HttpClient = HttpClient(Curl) { engine { caInfo = caPemPath } }

/** Longer than any line the oracle writes: a hex-encoded record of a few hundred bytes. */
private const val LINE = 1 shl 16
