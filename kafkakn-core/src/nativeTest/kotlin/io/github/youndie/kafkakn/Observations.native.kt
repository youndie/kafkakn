@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.convert
import kotlinx.cinterop.toKString
import platform.posix.fclose
import platform.posix.fopen
import platform.posix.fputs
import platform.posix.getenv
import platform.posix.mkdir

/**
 * The native target this binary was built for: `linuxX64` on the Linux box, `macosArm64` on a
 * contributor's Mac (B-40), `linuxArm64` in an arm64 container (B-39). Named rather than assumed,
 * because the arm's name is the file its observations go to and the stamp its records carry.
 */
@OptIn(kotlin.experimental.ExperimentalNativeApi::class)
internal actual val armName: String =
    when {
        Platform.osFamily == OsFamily.MACOSX -> "macosArm64"
        Platform.cpuArchitecture == CpuArchitecture.ARM64 -> "linuxArm64"
        else -> "linuxX64"
    }

internal actual fun recordObservation(
    key: String,
    value: String,
) = append("$armName.txt", key, value)

internal actual fun recordArmFact(
    key: String,
    value: String,
) = append("$armName-local.txt", key, value)

private fun append(
    name: String,
    key: String,
    value: String,
) {
    // 0x1FF is 0777; the process umask narrows it. mkdir failing because the directory already
    // exists is the ordinary case and is ignored deliberately. Both levels: `build` is there when Gradle
    // runs the suite, and was not when B-39 ran the binary in a container's empty directory.
    mkdir("build", 0x1FF.convert())
    mkdir("build/observations", 0x1FF.convert())
    // Loud, not `?: return`: that was here, and an arm whose observations went nowhere passed its whole
    // suite. Only compare-arms, finding no file, could tell that the arm had not been compared at all.
    val file = fopen("build/observations/$name", "a") ?: error("cannot append to build/observations/$name")
    fputs("$key=$value\n", file)
    fclose(file)
}

internal actual fun testEnv(name: String): String? = getenv(name)?.toKString()

internal actual fun skewedArm(): String? = testEnv("KAFKAKN_SKEW_ARM")

internal actual fun randomSuffix(): String =
    platform.posix.time(null).toString() +
        platform.posix.getpid().toString(36)

internal actual fun smallQueueConfig(): Map<String, String> =
    mapOf(
        "queue.buffering.max.messages" to "100",
    )

internal actual fun backpressureWaitCount(): Long = backpressureWaits.value

internal actual fun givenUpMidDrain(): String = "${rebalancesMidDrain.value}/${recordsGivenUpMidDrain.value}"

internal actual fun parkedSendCount(): Int = parkedSends()

internal actual fun brokerPaused(paused: Boolean) {
    val exit = platform.posix.system("docker ${if (paused) "pause" else "unpause"} kafkakn-broker > /dev/null 2>&1")
    // Unpausing twice is harmless and fails in docker's words: only a pause that did not happen is an error.
    check(!paused || exit == 0) { "docker could not pause the broker" }
}

internal actual fun adminFailFastConfig(): Map<String, String> = mapOf("socket.timeout.ms" to "5000")

internal actual fun failFastConfig(): Map<String, String> =
    mapOf(
        // librdkafka gives the record back with `Local: Message timed out` after this, which is the
        // moment the caller finds out anything at all went wrong.
        "message.timeout.ms" to "20000",
        "socket.timeout.ms" to "5000",
    )

/** librdkafka's own reading of the key, off the handle the producer actually built. */
internal actual fun effectiveIdempotence(config: ProducerConfig): Boolean {
    val producer = NativeKafkaProducer(config)
    try {
        return producer.effectiveConfig("enable.idempotence") == "true"
    } finally {
        kotlinx.coroutines.runBlocking { producer.close() }
    }
}
