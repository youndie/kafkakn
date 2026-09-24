package io.github.youndie.kafkakn

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails

/**
 * [B-25](../../../../../../../docs/backlog/B-25-the-arms-disagree-on-idempotence.md) and
 * [research §1.8](../../../../../../../docs/research/research-architecture.md).
 *
 * **The arms disagreed by default**: `enable.idempotence` is `true` in `kafka-clients` 4.3.1 and
 * `false` in librdkafka 2.13.0, read out of both artefacts. So a retried record whose acknowledgement
 * was lost could be written twice by one arm and once by the other, each arm consistent with the
 * broker — the partitioner finding of §2.2 again.
 *
 * **The expected column is not a design, it is a measurement of the reference arm.** "Default on"
 * is only half of what the Java client does: it turns idempotence off **silently** when the caller
 * set a key that conflicts with it, and refuses outright for others. Measured against
 * `kafka-clients-4.3.1.jar` by constructing its own `ProducerConfig` and reading the value back:
 *
 * | the caller set | the Java client |
 * |---|---|
 * | nothing | on |
 * | `acks=1` | off, silently |
 * | `retries=0` | off, silently |
 * | `enable.idempotence=false` | off |
 * | `acks=1` and `enable.idempotence=true` | refuses to construct |
 * | `max.in.flight.requests.per.connection=10` | refuses to construct, even with idempotence unset |
 *
 * A native default that was only "on" would reject `acks=1`, which the reference accepts — a
 * configuration that works on the arm a caller runs locally and fails on the one they ship. Every row
 * is held against both arms, and the value is read from each client's own effective configuration
 * rather than from what kafkakn asked for.
 */
class IdempotenceTest {
    private fun config(vararg extra: Pair<String, String>) =
        ProducerConfig(mapOf("bootstrap.servers" to bootstrap) + extra.toMap())

    private fun on(
        label: String,
        config: ProducerConfig,
    ): Boolean = effectiveIdempotence(config).also { recordArmFact("idempotence.$label", it.toString()) }

    @Test
    fun with_nothing_set_the_producer_is_idempotent() {
        assertEquals(true, on("default", config()), "the default is not idempotent on this arm")
    }

    @Test
    fun acks_other_than_all_turns_it_off_without_a_word() {
        assertEquals(false, on("acks-1", config("acks" to "1")))
    }

    @Test
    fun retries_zero_turns_it_off_without_a_word() {
        assertEquals(false, on("retries-0", config("retries" to "0")))
    }

    @Test
    fun an_explicit_false_is_kept() {
        assertEquals(false, on("explicit-false", config("enable.idempotence" to "false")))
    }

    @Test
    fun an_explicit_true_is_kept_too() {
        // Not redundant with the default: this is the row that proves a caller's value is read
        // rather than overwritten, in the direction the default cannot show.
        assertEquals(true, on("explicit-true", config("enable.idempotence" to "true")))
    }

    @Test
    fun acks_1_with_idempotence_asked_for_is_refused() {
        assertFails { effectiveIdempotence(config("acks" to "1", "enable.idempotence" to "true")) }
    }

    @Test
    fun more_than_five_in_flight_is_refused_even_when_nobody_asked_for_idempotence() {
        // The surprising row, and the one most likely to split the arms: the Java client refuses this
        // with idempotence UNSET, because its default is on. A native arm that only turned the default
        // on and did not refuse here would accept a configuration the reference rejects.
        assertFails { effectiveIdempotence(config("max.in.flight.requests.per.connection" to "10")) }
    }
}
