@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn.schema

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import platform.posix.getenv

internal actual val arm: String = "linuxX64"

internal actual fun env(name: String): String? = getenv(name)?.toKString()
