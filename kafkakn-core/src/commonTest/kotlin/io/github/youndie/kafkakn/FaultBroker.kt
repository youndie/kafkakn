package io.github.youndie.kafkakn

/**
 * A broker a fault test may pause or stop (B-105).
 *
 * On `linuxX64` it is the test's own, brought up for it through kontainer and gone after it: breaking it
 * breaks nobody else, so the test runs in the whole suite with no switch. Elsewhere — the JVM, a Mac, arm64 —
 * it is the shared fixture broker, and the test runs only when its switch asks, as before.
 */
internal interface FaultBroker {
    val bootstrap: String

    /**
     * Whether the other arm runs this test too. Only then are its observations compared
     * (`ci/harness/compare-arms.sh`); otherwise they are kept as this arm's facts, since a comparison with
     * an arm that did not run is a disagreement about nothing.
     */
    val compared: Boolean

    suspend fun paused(paused: Boolean)

    suspend fun stopped(stopped: Boolean)
}

/** An observation of a fault test: compared when both arms ran it, kept as this arm's fact otherwise. */
internal fun FaultBroker.observe(
    key: String,
    value: String,
) = if (compared) recordObservation(key, value) else recordArmFact(key, value)

/** Runs [block] against a broker it may break, or returns null when this arm is not asked to ([switch] unset). */
internal expect suspend fun <T> withFaultBroker(
    switch: String,
    block: suspend (FaultBroker) -> T,
): T?

/** The shared fixture broker, behind [switch]: what every arm without its own broker uses. */
internal suspend fun <T> withSharedFaultBroker(
    switch: String,
    block: suspend (FaultBroker) -> T,
): T? {
    if (testEnv(switch) == null) return null
    return block(SharedFaultBroker)
}

private val sharedBootstrap: String get() = bootstrap

private object SharedFaultBroker : FaultBroker {
    override val bootstrap: String get() = sharedBootstrap
    override val compared: Boolean = true

    override suspend fun paused(paused: Boolean) = brokerPaused(paused)

    override suspend fun stopped(stopped: Boolean) = brokerStopped(stopped)
}
