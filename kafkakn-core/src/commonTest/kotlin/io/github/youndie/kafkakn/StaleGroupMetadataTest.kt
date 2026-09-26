package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-71](../../../../../../../docs/backlog/B-71-send-offsets-after-the-group-moved-on.md): offsets handed to a
 * transaction with the group metadata of a membership the group has moved past. B-70's soak under chaos met it as an
 * instance waking from a freeze mid-transaction; here it is made on purpose, two ways:
 * - the group rebalanced (a second member joined) after the metadata was taken: its generation is old;
 * - the member left after the metadata was taken: the coordinator no longer knows it.
 *
 * Each refusal must be the same on both arms, the transaction must abort cleanly after it, and the same member
 * with fresh metadata must commit: the positive control that the refusal is about staleness and nothing else.
 */
class StaleGroupMetadataTest {
    @Test
    fun offsets_with_the_metadata_of_an_older_generation_are_refused_alike() =
        runTest(timeout = 3.minutes) {
            withContext(Dispatchers.Default) {
                val topic = seededTopic("kafkakn-stale-generation")
                val group = topic
                val a = member(group)
                val b = member(group)
                val producer = transactional(group)
                try {
                    a.subscribe(listOf(topic))
                    pollUntil(a) { a.assignment().size == PARTITIONS }
                    val stale = a.groupMetadata()
                    // B joins, and both poll until the rebalance has given B its share: A's generation is newer now.
                    b.subscribe(listOf(topic))
                    val until = TimeSource.Monotonic.markNow() + WITHIN
                    while ((b.assignment().isEmpty() || a.assignment().size == PARTITIONS) && until.hasNotPassedNow()) {
                        a.poll(POLL)
                        b.poll(POLL)
                    }
                    check(b.assignment().isNotEmpty()) { "B was never assigned" }
                    val held = a.assignment().first()

                    producer.beginTransaction()
                    val refused = outcome { producer.sendOffsetsToTransaction(mapOf(held to 1L), stale) }
                    val aborted = outcome { producer.abortTransaction() }
                    // The control: the same member, the same partition, metadata taken now.
                    producer.beginTransaction()
                    val fresh =
                        outcome {
                            producer.sendOffsetsToTransaction(mapOf(held to 1L), a.groupMetadata())
                            producer.commitTransaction()
                        }
                    recordArmFact("stale.generation.said", refused)
                    recordObservation("stale.generation", refused.substringBefore(":"))
                    recordObservation("stale.generation.aborted", aborted.substringBefore(":"))
                    recordObservation("stale.generation.fresh", fresh.substringBefore(":"))
                    assertEquals(
                        listOf("threw StaleGroupMetadataException", "done", "done"),
                        listOf(refused, aborted, fresh).map { it.substringBefore(":") },
                        "stale refused, then aborted, then fresh committed",
                    )
                } finally {
                    producer.close()
                    a.close()
                    b.close()
                }
            }
        }

    @Test
    fun offsets_with_the_metadata_of_a_member_that_left_are_refused_alike() =
        runTest(timeout = 3.minutes) {
            withContext(Dispatchers.Default) {
                val topic = seededTopic("kafkakn-stale-member")
                val group = topic
                val a = member(group)
                val producer = transactional(group)
                try {
                    a.subscribe(listOf(topic))
                    pollUntil(a) { a.assignment().isNotEmpty() }
                    val partition = a.assignment().first()
                    val stale = a.groupMetadata()
                    a.close()
                    producer.beginTransaction()
                    val refused = outcome { producer.sendOffsetsToTransaction(mapOf(partition to 1L), stale) }
                    val aborted = outcome { producer.abortTransaction() }
                    recordArmFact("stale.member.said", refused)
                    recordObservation("stale.member", refused.substringBefore(":"))
                    recordObservation("stale.member.aborted", aborted.substringBefore(":"))
                    assertEquals(
                        listOf("threw StaleGroupMetadataException", "done"),
                        listOf(refused, aborted).map { it.substringBefore(":") },
                        "a member that left: refused, then aborted",
                    )
                } finally {
                    producer.close()
                }
            }
        }

    private fun member(group: String) =
        kafkaConsumer(
            ConsumerConfig(
                "bootstrap.servers" to bootstrap,
                "group.id" to group,
                "auto.offset.reset" to "earliest",
                "partition.assignment.strategy" to "range",
            ),
        )

    private suspend fun transactional(group: String): KafkaProducer {
        val producer =
            kafkaProducer(
                ProducerConfig(
                    "bootstrap.servers" to bootstrap,
                    "transactional.id" to "$group-txn",
                    "acks" to "all",
                ),
            )
        producer.initTransactions()
        return producer
    }

    private suspend fun pollUntil(
        consumer: KafkaConsumer,
        done: suspend () -> Boolean,
    ) {
        val until = TimeSource.Monotonic.markNow() + WITHIN
        while (!done() && until.hasNotPassedNow()) consumer.poll(POLL)
        check(done()) { "not reached in $WITHIN" }
    }

    private suspend fun seededTopic(prefix: String): String {
        val topic = "$prefix-$armName-${randomSuffix()}"
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            admin.createTopics(listOf(NewTopic(topic, PARTITIONS, 1)))
        } finally {
            admin.close()
        }
        return topic
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
        const val PARTITIONS = 2
        val POLL = 200.milliseconds
        val WITHIN = 30.seconds
    }
}
