@file:JvmName("SoakMain")

package io.github.youndie.kafkakn.soak

import kotlinx.coroutines.runBlocking
import kotlin.system.exitProcess

/** The JVM instance. Any failure is the end of the process (crash-only, see [runSoak]). */
public fun main() {
    try {
        runBlocking { runSoak(System::getenv) }
    } catch (failure: Throwable) {
        say("failed: ${failure::class.simpleName}: ${failure.message}")
        exitProcess(1)
    }
}
