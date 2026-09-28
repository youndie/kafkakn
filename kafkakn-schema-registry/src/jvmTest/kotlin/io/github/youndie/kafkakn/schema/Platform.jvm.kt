package io.github.youndie.kafkakn.schema

internal actual val arm: String = "jvm"

internal actual fun env(name: String): String? = System.getenv(name)

internal actual fun readLines(path: String): List<String> = java.io.File(path).readLines()

internal actual fun writeLines(
    path: String,
    lines: List<String>,
) {
    java.io.File(path).writeText(lines.joinToString("\n", postfix = "\n"))
}
