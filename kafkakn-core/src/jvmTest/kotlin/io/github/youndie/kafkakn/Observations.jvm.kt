package io.github.youndie.kafkakn

import java.io.File

internal actual val armName: String = "jvm"

internal actual fun recordObservation(
    key: String,
    value: String,
) = append("$armName.txt", key, value)

internal actual fun recordArmFact(
    key: String,
    value: String,
) = append("$armName-local.txt", key, value)

private fun append(
    file: String,
    key: String,
    value: String,
) {
    val dir = File("build/observations").apply { mkdirs() }
    File(dir, file).appendText("$key=$value\n")
}

// The ENVIRONMENT, not a system property: `-Dfoo` on the Gradle command line sets a property on the
// Gradle process, not on the forked test JVM, so the skew never reached the test and the comparison
// happily reported agreement. An environment variable is inherited by both arms' test processes and
// is one mechanism instead of two.
internal actual fun testEnv(name: String): String? = System.getenv(name)

internal actual fun skewedArm(): String? = testEnv("KAFKAKN_SKEW_ARM")

internal actual fun randomSuffix(): String = System.nanoTime().toString(36)

internal actual fun smallQueueConfig(): Map<String, String> =
    mapOf(
        // The Java client has no record-count bound: it bounds the buffer in BYTES and blocks up to
        // max.block.ms waiting for room. 32 KiB is small enough that a few thousand 1 KiB records
        // cannot all fit at once.
        "buffer.memory" to "32768",
        "max.block.ms" to "60000",
    )

// The JVM client does its waiting inside send(); there is no counter to read, and inventing one
// would mean adding logic to the arm whose value is that it is not ours.
internal actual fun backpressureWaitCount(): Long = -1

internal actual fun failFastConfig(): Map<String, String> =
    mapOf(
        "max.block.ms" to "20000",
        "request.timeout.ms" to "5000",
        "delivery.timeout.ms" to "25000",
        "retries" to "1",
    )

/**
 * Builds the real producer first, so every refusal kafkakn or the Java client makes happens exactly as
 * it would for a caller, then reads the value the Java client itself settled on. `KafkaProducer` does
 * not expose its configuration; `ProducerConfig` over the same properties is the object its
 * constructor builds, so this is its answer rather than a re-derivation of it.
 */
internal actual fun effectiveIdempotence(config: ProducerConfig): Boolean {
    val producer = kafkaProducer(config)
    kotlinx.coroutines.runBlocking { producer.close() }
    val properties =
        translateForJava(config.properties) +
            mapOf(
                "key.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
                "value.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
            )
    return org.apache.kafka.clients.producer
        .ProducerConfig(properties)
        .getBoolean("enable.idempotence")
}
