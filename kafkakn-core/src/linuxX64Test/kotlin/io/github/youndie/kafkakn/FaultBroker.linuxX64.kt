package io.github.youndie.kafkakn

import io.github.youndie.kontainer.Fixture
import io.github.youndie.kontainer.Probe
import kotlin.time.Duration.Companion.seconds

/**
 * The test's own broker, from `ci/broker/fault-broker.compose.yml`, on a host port kontainer chooses. Always
 * run: nobody else uses it. Compared with the JVM arm only when the switch asks the JVM to run as well.
 */
internal actual suspend fun <T> withFaultBroker(
    switch: String,
    block: suspend (FaultBroker) -> T,
): T? {
    val file = testEnv("KAFKAKN_FAULT_BROKER_COMPOSE") ?: "../ci/broker/fault-broker.compose.yml"
    val fixture = Fixture.owned(listOf(file), ports = mapOf(SERVICE to listOf(PORT)))
    try {
        fixture.up()
        fixture.awaitReady(SERVICE, PORT, Probe.kafka(), timeout = READY)
        return block(OwnedFaultBroker(fixture, compared = testEnv(switch) != null))
    } finally {
        fixture.down()
    }
}

private class OwnedFaultBroker(
    private val fixture: Fixture,
    override val compared: Boolean,
) : FaultBroker {
    override val bootstrap: String = "127.0.0.1:${fixture.port(SERVICE, PORT)}"

    // Unpausing twice is harmless here as it was with `docker unpause`: kontainer decides by the state.
    override suspend fun paused(paused: Boolean) = if (paused) fixture.pause(SERVICE) else fixture.unpause(SERVICE)

    override suspend fun stopped(stopped: Boolean) {
        if (stopped) {
            // The same grace as `docker stop -t 1` on the shared broker.
            fixture.stop(SERVICE, grace = 1.seconds)
        } else {
            fixture.start(SERVICE)
            fixture.awaitReady(SERVICE, PORT, Probe.kafka(), timeout = READY)
        }
    }
}

private const val SERVICE = "broker"
private const val PORT = 9092
private val READY = 90.seconds
