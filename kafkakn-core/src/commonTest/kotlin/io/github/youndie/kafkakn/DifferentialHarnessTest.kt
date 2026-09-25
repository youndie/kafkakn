package io.github.youndie.kafkakn

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The mechanism the project rests on, kept as its own control.
 *
 * It was written before there was a producer, so its observations are about the surface rather than
 * about Kafka. The point is not what is recorded but that **both arms record it and a step outside
 * them compares the two files**. The observations that are about Kafka use the same path: the
 * partitioner's choice for each key is recorded by `PartitionerAgreementTest`, and the others by the
 * tests that make them.
 *
 * `KAFKAKN_SKEW_ARM=<arm>` makes the named arm record a different value. That is how the comparison
 * is shown failing (`ci/b-05/run.sh`); a differential oracle nobody has seen disagree is a diagram.
 */
class DifferentialHarnessTest {
    @Test
    fun both_arms_record_what_they_see_of_the_surface() {
        val config = ProducerConfig("bootstrap.servers" to "127.0.0.1:9092", "acks" to "all")
        val record = ProducerRecord(topic = "t", value = byteArrayOf(1, 2, 3), key = byteArrayOf(9))

        // Values every arm must agree on, because they come from common code: the control. Where the
        // arms can genuinely differ is recorded by the tests about that behaviour.
        recordObservation("config.acks", config["acks"] ?: "<absent>")
        recordObservation("config.unknown-key-is-null", (config["bootstrapServers"] == null).toString())
        recordObservation("record.value.size", record.value?.size.toString())
        recordObservation("record.key.size", (record.key?.size ?: 0).toString())
        recordObservation("record.topic", skewed(record.topic))

        assertEquals("all", config["acks"])
    }

    /**
     * Returns [value] unchanged, unless this arm was named by `kafkakn.skewArm` — in which case it
     * returns something else, so the comparison between the arms must fail.
     */
    private fun skewed(value: String): String = if (skewedArm() == armName) "$value-skewed" else value
}

internal expect fun skewedArm(): String?
