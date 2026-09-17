package io.github.youndie.kafkakn

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The scenario the whole two-arm design exists for.
 *
 * Which partition a key lands on is the **client's** choice, not the broker's, so no amount of
 * asking the broker tells you whether the two implementations agree — each would simply report where
 * its own record went, and both would be internally consistent. The arms are compared by recording
 * the answer on each and diffing the files afterwards (`ci/harness/compare-arms.sh`).
 *
 * A wrong partitioner is invisible to a single implementation. This is the cheapest place in the
 * project where that stops being true.
 */
class PartitionerAgreementTest {
    @Test
    fun the_partition_chosen_for_each_key_is_recorded_for_comparison() =
        runTest {
            val producer =
                kafkaProducer(
                    ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"),
                )
            try {
                KEYS.forEach { key ->
                    val metadata =
                        producer.send(
                            ProducerRecord(testTopic, "partitioner:$key".encodeToByteArray(), key.encodeToByteArray()),
                        )
                    recordObservation("partitioner.$key", metadata.partition.toString())
                }
                producer.flush()
            } finally {
                producer.close()
            }
        }

    @Test
    fun the_same_key_goes_to_the_same_partition_twice() =
        runTest {
            val producer =
                kafkaProducer(
                    ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"),
                )
            try {
                val first =
                    producer.send(
                        ProducerRecord(testTopic, "once".encodeToByteArray(), "stable".encodeToByteArray()),
                    )
                val second =
                    producer.send(
                        ProducerRecord(testTopic, "twice".encodeToByteArray(), "stable".encodeToByteArray()),
                    )
                assertEquals(first.partition, second.partition)
            } finally {
                producer.close()
            }
        }

    private companion object {
        // Enough keys that an accidental agreement is unlikely, few enough that the run stays short.
        val KEYS = listOf("alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta")
    }
}
