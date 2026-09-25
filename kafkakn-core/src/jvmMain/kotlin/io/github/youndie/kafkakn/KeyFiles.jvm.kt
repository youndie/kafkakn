package io.github.youndie.kafkakn

import java.io.File
import java.io.IOException

internal actual fun readPemFile(
    key: String,
    path: String,
): String =
    try {
        File(path).readText()
    } catch (unreadable: IOException) {
        throw IllegalArgumentException("$key: cannot read '$path': ${unreadable.message}", unreadable)
    }
