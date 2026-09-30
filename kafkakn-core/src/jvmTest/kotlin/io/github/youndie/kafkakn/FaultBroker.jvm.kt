package io.github.youndie.kafkakn

internal actual suspend fun <T> withFaultBroker(
    switch: String,
    block: suspend (FaultBroker) -> T,
): T? = withSharedFaultBroker(switch, block)
