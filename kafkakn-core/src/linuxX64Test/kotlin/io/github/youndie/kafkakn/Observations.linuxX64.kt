@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import platform.posix.fclose
import platform.posix.fopen
import platform.posix.fputs
import platform.posix.getenv
import platform.posix.mkdir

internal actual val armName: String = "linuxX64"

internal actual fun recordObservation(key: String, value: String) = append("$armName.txt", key, value)

internal actual fun recordArmFact(key: String, value: String) = append("$armName-local.txt", key, value)

private fun append(name: String, key: String, value: String) {
    // 0x1FF is 0777; the process umask narrows it. mkdir failing because the directory already
    // exists is the ordinary case and is ignored deliberately.
    mkdir("build/observations", 0x1FFu)
    val file = fopen("build/observations/$name", "a") ?: return
    fputs("$key=$value\n", file)
    fclose(file)
}

internal actual fun testEnv(name: String): String? = getenv(name)?.toKString()

internal actual fun skewedArm(): String? = testEnv("KAFKAKN_SKEW_ARM")

internal actual fun randomSuffix(): String = platform.posix.time(null).toString() +
    platform.posix.getpid().toString(36)


internal actual fun smallQueueConfig(): Map<String, String> = mapOf(
    "queue.buffering.max.messages" to "100",
)

internal actual fun backpressureWaitCount(): Long = backpressureWaits.value
