@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.readBytes
import kotlinx.cinterop.toKString
import platform.posix.errno
import platform.posix.fclose
import platform.posix.fopen
import platform.posix.fread
import platform.posix.strerror

internal actual fun readPemFile(
    key: String,
    path: String,
): String {
    val file =
        fopen(path, "r")
            ?: throw IllegalArgumentException("$key: cannot read '$path': ${strerror(errno)?.toKString()}")
    try {
        val text = StringBuilder()
        memScoped {
            val chunk = allocArray<ByteVar>(CHUNK)
            while (true) {
                val read = fread(chunk, 1.convert(), CHUNK.convert(), file).toInt()
                if (read <= 0) break
                text.append(chunk.readBytes(read).decodeToString())
            }
        }
        return text.toString()
    } finally {
        fclose(file)
    }
}

private const val CHUNK = 4096
