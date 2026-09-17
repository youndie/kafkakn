package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * [feature-produce-a-record](../../../../../../../docs/features/feature-produce-a-record.md).
 *
 * What a test can assert here is what the **client** returns. What actually landed on the broker is
 * the broker's to say, and it is asked by `ci/b-06/run.sh` afterwards — each test records the stamp
 * and the count it used, and the script holds the topic's end offsets and an independent consumer
 * against them. A producer verified by its own library can be wrong in both directions at once.
 */
class ProduceTest {
    @Test
    fun a_record_with_no_key_reaches_the_topic() =
        runTest {
            val stamp = "nokey-$armName-${randomSuffix()}"
            val producer = kafkaProducer(producerConfig())
            try {
                repeat(RECORDS) { index ->
                    val metadata =
                        producer.send(
                            ProducerRecord(testTopic, "$stamp:$index".encodeToByteArray()),
                        )
                    assertEquals(testTopic, metadata.topic)
                    assertTrue(metadata.partition in 0..2, "unexpected partition ${metadata.partition}")
                    assertTrue(metadata.offset >= 0, "unexpected offset ${metadata.offset}")
                }
                producer.flush()
            } finally {
                producer.close()
            }
            recordArmFact("produce.no-key.stamp", stamp)
            recordArmFact("produce.no-key.count", RECORDS.toString())
        }

    @Test
    fun records_with_the_same_key_land_on_one_partition() =
        runTest {
            val stamp = "samekey-$armName-${randomSuffix()}"
            val key = "k".encodeToByteArray()
            val producer = kafkaProducer(producerConfig())
            val partitions = mutableSetOf<Int>()
            try {
                repeat(KEYED_RECORDS) { index ->
                    val metadata =
                        producer.send(
                            ProducerRecord(testTopic, "$stamp:$index".encodeToByteArray(), key),
                        )
                    partitions += metadata.partition
                }
                producer.flush()
            } finally {
                producer.close()
            }
            assertEquals(1, partitions.size, "one key landed on partitions $partitions")
            recordArmFact("produce.same-key.stamp", stamp)
            recordArmFact("produce.same-key.count", KEYED_RECORDS.toString())
            // The partition a key maps to is the CLIENT's choice, not the broker's, so it is the kind of
            // observation the two arms have to be compared on rather than each checked alone.
            recordObservation("partitioner.key-k.partition", partitions.single().toString())
        }

    @Test
    fun a_topic_that_does_not_exist_is_an_error_not_a_silence() =
        runTest {
            val producer = kafkaProducer(producerConfig())
            val absent = "kafkakn-absent-${randomSuffix()}"
            try {
                producer.send(ProducerRecord(absent, "x".encodeToByteArray()))
                fail("sending to $absent should have thrown; auto-creation is off on the test broker")
            } catch (cancellation: CancellationException) {
                // A cancelled test is not a producer that threw. Without this the timeout is caught
                // below and asserted against, and the assertion's message names the topic rather
                // than the harness.
                throw cancellation
            } catch (expected: Throwable) {
                assertTrue(
                    expected.message?.contains(absent) == true ||
                        expected.cause?.message?.contains(absent) == true,
                    "the failure should name the topic, but said: ${expected.message}",
                )
            } finally {
                producer.close()
            }
        }

    @Test
    fun acks_reaches_the_broker_and_is_honoured() =
        runTest {
            // A producer whose `acks` was silently dropped behaves identically to one that honoured it,
            // until something goes wrong. The only proof is a refusal only the BROKER can produce.
            //
            // An invalid value does not prove it: kafka-clients rejects `acks=99` at construction with
            // its own ConfigException, before any broker is contacted (measured in B-06). So the probe
            // is a VALID value the broker cannot satisfy - `acks=all` against a topic whose
            // min.insync.replicas is 2 on a single-broker cluster.
            val strict = testEnv("KAFKAKN_STRICT_TOPIC") ?: "kafkakn-strict"

            val refusing = kafkaProducer(producerConfig("acks" to "all"))
            try {
                refusing.send(ProducerRecord(strict, "needs-two-replicas".encodeToByteArray()))
                fail("acks=all against min.insync.replicas=2 on one broker should have been refused")
            } catch (cancellation: CancellationException) {
                // A cancelled test is not a producer that threw. Without this the timeout is caught
                // below and asserted against, and the assertion's message names the topic rather
                // than the harness.
                throw cancellation
            } catch (expected: Throwable) {
                val text = "${expected.message} ${expected.cause?.message}"
                assertTrue(
                    text.contains("REPLICAS", ignoreCase = true) ||
                        text.contains("NotEnoughReplicas", ignoreCase = true),
                    "the refusal should come from the broker and name the replicas, but said: $text",
                )
            } finally {
                refusing.close()
            }

            // The control: the same topic accepts acks=1, so the refusal above was about acks and not
            // about the topic being unusable.
            val lenient = kafkaProducer(producerConfig("acks" to "1"))
            try {
                val metadata = lenient.send(ProducerRecord(strict, "one-replica-is-enough".encodeToByteArray()))
                assertEquals(strict, metadata.topic)
            } finally {
                lenient.close()
            }
        }

    @Test
    fun a_configuration_key_nobody_honours_fails_at_construction() {
        // Not a warning in a log nobody reads. An option accepted and dropped behaves exactly like
        // one that worked, until it matters - so an unknown key is refused where the caller is
        // still holding it.
        val failure =
            assertFailsWith<IllegalArgumentException> {
                kafkaProducer(producerConfig("bootstrapServers" to "h:9092"))
            }
        assertTrue(
            failure.message?.contains("bootstrapServers") == true,
            "the failure should name the offending key, but said: ${failure.message}",
        )
    }

    private fun producerConfig(vararg extra: Pair<String, String>) =
        ProducerConfig(
            buildMap {
                put("bootstrap.servers", bootstrap)
                put("acks", "all")
                putAll(extra)
            },
        )

    private companion object {
        const val RECORDS = 100
        const val KEYED_RECORDS = 50
    }
}

/** A short unique-enough suffix; the suite needs distinguishable stamps, not randomness. */
internal expect fun randomSuffix(): String
