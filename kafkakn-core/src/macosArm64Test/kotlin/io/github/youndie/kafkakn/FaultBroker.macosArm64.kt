package io.github.youndie.kafkakn

// No own broker here: kontainer is published for linuxX64 only, so this arm keeps the shared one (B-105).
internal actual suspend fun <T> withFaultBroker(
    switch: String,
    block: suspend (FaultBroker) -> T,
): T? = withSharedFaultBroker(switch, block)
