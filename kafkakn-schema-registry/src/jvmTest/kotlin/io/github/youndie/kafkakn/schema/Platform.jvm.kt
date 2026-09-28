package io.github.youndie.kafkakn.schema

internal actual val arm: String = "jvm"

internal actual fun env(name: String): String? = System.getenv(name)
