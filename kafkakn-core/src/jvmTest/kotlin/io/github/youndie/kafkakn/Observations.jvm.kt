package io.github.youndie.kafkakn

import java.io.File

internal actual val armName: String = "jvm"

internal actual fun recordObservation(key: String, value: String) {
    val dir = File("build/observations").apply { mkdirs() }
    File(dir, "$armName.txt").appendText("$key=$value\n")
}

// The ENVIRONMENT, not a system property: `-Dfoo` on the Gradle command line sets a property on the
// Gradle process, not on the forked test JVM, so the skew never reached the test and the comparison
// happily reported agreement. An environment variable is inherited by both arms' test processes and
// is one mechanism instead of two.
internal actual fun testEnv(name: String): String? = System.getenv(name)

internal actual fun skewedArm(): String? = testEnv("KAFKAKN_SKEW_ARM")

internal actual fun randomSuffix(): String = System.nanoTime().toString(36)
