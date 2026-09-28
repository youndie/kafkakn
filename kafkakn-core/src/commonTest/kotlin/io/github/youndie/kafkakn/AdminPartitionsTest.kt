package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-62](../../../../../../../docs/backlog/B-62-create-partitions.md): add partitions to an existing topic.
 * `ci/b-62/run.sh` holds each arm's grown topic against `kafka-topics.sh --describe`.
 */
class AdminPartitionsTest {
    @Test
    fun a_topic_grows_and_the_same_keys_written_after_it_land_elsewhere() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-grown-$armName-${randomSuffix()}"
                withAdmin { admin ->
                    admin.createTopics(listOf(NewTopic(topic, 1, 1)))
                    val before = partitionsOf(topic)
                    assertEquals(List(KEYS.size) { 0 }, before, "one partition: every key in it")

                    admin.createPartitions(topic, GROWN_TO)
                    val started = TimeSource.Monotonic.markNow()
                    describedOnce(admin, topic) { it.size == GROWN_TO }
                    recordArmFact("partitions.visible.after.ms", started.elapsedNow().inWholeMilliseconds.toString())

                    // A new producer, so its metadata is the grown topic's. The same keys, now spread by four.
                    val after = partitionsOf(topic)
                    recordArmFact("partitions.topic", topic)
                    recordObservation("partitions.keys.after", after.joinToString(" "))
                    assertTrue(after.any { it != 0 }, "no key moved: $after")
                    assertTrue(after.all { it in 0 until GROWN_TO }, "a key outside the grown topic: $after")
                }
            }
        }

    @Test
    fun a_count_that_does_not_grow_the_topic_is_refused_alike_on_both_arms() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-not-grown-$armName-${randomSuffix()}"
                withAdmin { admin ->
                    admin.createTopics(listOf(NewTopic(topic, 3, 1)))
                    val refusals =
                        listOf(
                            "equal" to outcome { admin.createPartitions(topic, 3) },
                            "smaller" to outcome { admin.createPartitions(topic, 2) },
                        )
                    refusals.forEach { (what, said) ->
                        recordArmFact("partitions.refused.$what.said", said)
                        recordObservation("partitions.refused.$what", said.substringBefore(":"))
                    }
                    assertEquals(3, admin.describeTopics(listOf(topic)).getValue(topic).size, "still three")
                    assertEquals(
                        refusals.map { it.first to "threw IllegalArgumentException" },
                        refusals.map { (what, said) -> what to said.substringBefore(":") },
                    )
                }
            }
        }

    /**
     * [B-87](../../../../../../../docs/backlog/B-87-create-partitions-with-a-replica-assignment.md): new partitions
     * with a replica assignment. The fixture has one broker, id [BROKER], so `[[BROKER]]` is the assignment it can
     * check, and one that names a broker the cluster does not have is the refusal worth measuring.
     */
    @Test
    fun new_partitions_land_on_the_broker_named_and_an_unknown_broker_is_refused() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-assigned-$armName-${randomSuffix()}"
                withAdmin { admin ->
                    admin.createTopics(listOf(NewTopic(topic, 1, 1)))
                    admin.createPartitions(topic, 2, assignment = listOf(listOf(BROKER)))
                    // The topic and its growth reach the broker's description a moment after the controller answered:
                    // partition 1 was missing on the JVM in one CI run (B-90), the topic itself on native in another
                    // (B-101).
                    val grown = describedOnce(admin, topic) { described -> described.any { it.partition == 1 } }
                    val added = grown.single { it.partition == 1 }
                    val refused = outcome { admin.createPartitions(topic, 3, assignment = listOf(listOf(NO_BROKER))) }
                    val after = admin.describeTopics(listOf(topic)).getValue(topic).size
                    recordArmFact("partitions.assigned.topic", topic)
                    recordArmFact("partitions.assigned.refused.said", refused)
                    recordObservation("partitions.assigned.replicas", added.replicas.toString())
                    recordObservation("partitions.assigned.refused", refused.substringBefore(":"))
                    recordObservation("partitions.assigned.after", after.toString())
                    assertEquals(listOf(BROKER), added.replicas)
                    assertEquals("threw IllegalArgumentException", refused.substringBefore(":"))
                    assertEquals(2, after, "the refused growth changed nothing")
                }
            }
        }

    /** The partition each of [KEYS] went to, written by a producer made now. */
    private suspend fun partitionsOf(topic: String): List<Int> {
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
        try {
            return KEYS.map { key ->
                producer.send(ProducerRecord(topic, "v".encodeToByteArray(), key = key.encodeToByteArray())).partition
            }
        } finally {
            producer.close()
        }
    }

    private suspend fun withAdmin(use: suspend (KafkaAdmin) -> Unit) {
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            use(admin)
        } finally {
            admin.close()
        }
    }

    /**
     * The topic's description once [agreed] holds. `createTopics` and `createPartitions` return on the controller's
     * answer, and the broker's description shows them a moment later: at once, 199 of 200 new topics were unknown on
     * both arms, and visible within 134 ms (B-101). So an unknown topic is waited for too, within [VISIBLE_WITHIN].
     */
    private suspend fun describedOnce(
        admin: KafkaAdmin,
        topic: String,
        agreed: (List<PartitionInfo>) -> Boolean,
    ): List<PartitionInfo> {
        val started = TimeSource.Monotonic.markNow()
        while (true) {
            val described =
                try {
                    admin.describeTopics(listOf(topic)).getValue(topic)
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (thrown: Exception) {
                    if (!thrown.isUnknownTopic()) throw thrown
                    null
                }
            if (described != null && agreed(described)) return described
            check(started.elapsedNow() < VISIBLE_WITHIN) { "$topic: not as expected after $VISIBLE_WITHIN: $described" }
            delay(RETRY)
        }
    }

    /** "done", or "threw <type>: <message>". */
    private suspend fun outcome(call: suspend () -> Unit): String =
        try {
            call()
            "done"
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (thrown: Exception) {
            "threw ${thrown::class.simpleName}: ${thrown.message}"
        }

    private companion object {
        const val GROWN_TO = 4

        /** The fixture's one broker. */
        const val BROKER = 1
        const val NO_BROKER = 99
        val KEYS = (0 until 8).map { "key-$it" }
        val VISIBLE_WITHIN = 10.seconds
        val RETRY = 100.milliseconds
    }
}
