@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn.soak

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import kotlinx.coroutines.runBlocking
import platform.posix.getenv
import kotlin.system.exitProcess

/** The native instance. Any failure is the end of the process (crash-only, see [runSoak]). */
public fun main() {
    try {
        runBlocking { runSoak { getenv(it)?.toKString() } }
    } catch (failure: Throwable) {
        say("failed: ${failure::class.simpleName}: ${failure.message}")
        exitProcess(1)
    }
}
