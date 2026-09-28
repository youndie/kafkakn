package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-76](../../../../../../../docs/backlog/B-76-enqueue-without-metadata.md): the other half of the contract's
 * `RecordNotQueuedException` row, *no metadata within `max.block.ms`*. B-74 measured the queue-full half.
 *
 * The Java client waits for a topic's metadata inside `send`, up to `max.block.ms`. librdkafka queues the record
 * at once and looks for the metadata in the background, so the native `enqueue` answered "queued" for a record
 * that had nowhere to go, and `close` then waited out `message.timeout.ms` for it. Here both arms must answer
 * "never queued", and a topic whose metadata arrives while `enqueue` waits must be queued as it was before.
 */
class EnqueueMetadataTest {
    @Test
    fun a_record_for_a_cluster_that_does_not_answer_is_not_queued_within_max_block_ms() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                // Nothing listens on port 1: no broker, so no metadata, ever.
                val producer =
                    kafkaProducer(
                        ProducerConfig(
                            "bootstrap.servers" to NOWHERE,
                            "max.block.ms" to "$MAX_BLOCK_MS",
                        ),
                    )
                val started = TimeSource.Monotonic.markNow()
                val said = outcome { producer.enqueue(ProducerRecord("kafkakn-nowhere", "lost".encodeToByteArray())) }
                val after = started.elapsedNow().inWholeMilliseconds
                recordArmFact("metadata.nowhere.said", said)
                recordArmFact("metadata.nowhere.after.ms", after.toString())
                recordObservation("metadata.nowhere", said.substringBefore(":"))
                if (said == "queued") {
                    // Not closed: a producer holding that record waits out message.timeout.ms (300 s) to close.
                    fail("a record with no metadata was queued, after $after ms")
                }
                val closing = TimeSource.Monotonic.markNow()
                producer.close()
                val closed = closing.elapsedNow().inWholeMilliseconds
                recordArmFact("metadata.nowhere.close.ms", closed.toString())
                recordObservation("metadata.nowhere.closed.promptly", (closed < CLOSE_MS).toString())
                assertEquals("threw RecordNotQueuedException", said.substringBefore(":"))
                assertTrue(after >= MAX_BLOCK_MS, "refused after $after ms, before max.block.ms $MAX_BLOCK_MS")
                assertTrue(closed < CLOSE_MS, "close took $closed ms with nothing queued")
            }
        }

    @Test
    fun a_record_for_a_topic_that_does_not_exist_is_not_queued() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                // The test broker has auto-creation off, so this topic stays missing.
                val topic = "kafkakn-no-such-topic-$armName-${randomSuffix()}"
                val producer =
                    kafkaProducer(
                        ProducerConfig(
                            "bootstrap.servers" to bootstrap,
                            "max.block.ms" to "$MAX_BLOCK_MS",
                        ),
                    )
                val started = TimeSource.Monotonic.markNow()
                val said = outcome { producer.enqueue(ProducerRecord(topic, "lost".encodeToByteArray())) }
                val after = started.elapsedNow().inWholeMilliseconds
                recordArmFact("metadata.missing.topic", topic)
                recordArmFact("metadata.missing.said", said)
                recordArmFact("metadata.missing.after.ms", after.toString())
                recordObservation("metadata.missing", said.substringBefore(":"))
                if (said == "queued") fail("a record for a topic that does not exist was queued, after $after ms")
                producer.close()
                assertEquals("threw RecordNotQueuedException", said.substringBefore(":"))
            }
        }

    @Test
    fun a_record_for_a_topic_created_while_enqueue_waits_is_queued_and_lands() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-created-while-waiting-$armName-${randomSuffix()}"
                val producer =
                    kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "max.block.ms" to "$LONG_BLOCK_MS"))
                try {
                    val started = TimeSource.Monotonic.markNow()
                    val (queuedAfter, landed) =
                        coroutineScope {
                            val queued =
                                async {
                                    val delivery = producer.enqueue(ProducerRecord(topic, "late".encodeToByteArray()))
                                    started.elapsedNow().inWholeMilliseconds to delivery
                                }
                            delay(CREATE_AFTER)
                            val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
                            try {
                                admin.createTopics(listOf(NewTopic(topic, 1, 1)))
                            } finally {
                                admin.close()
                            }
                            val (after, delivery) = queued.await()
                            after to delivery.await()
                        }
                    recordArmFact("metadata.created.queued.after.ms", queuedAfter.toString())
                    recordObservation(
                        "metadata.created.waited",
                        (queuedAfter >= CREATE_AFTER.inWholeMilliseconds).toString(),
                    )
                    recordObservation("metadata.created.landed", "${landed.partition}:${landed.offset}")
                    assertTrue(
                        queuedAfter >= CREATE_AFTER.inWholeMilliseconds,
                        "queued after $queuedAfter ms, before the topic existed",
                    )
                    assertEquals(0L, landed.offset)
                } finally {
                    producer.close()
                }
            }
        }

    /** "queued", or "threw <type>: <message>". */
    private suspend fun outcome(call: suspend () -> Unit): String =
        try {
            call()
            "queued"
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (thrown: Exception) {
            "threw ${thrown::class.simpleName}: ${thrown.message}"
        }

    private companion object {
        const val NOWHERE = "127.0.0.1:1"
        const val MAX_BLOCK_MS = 1_000L
        const val LONG_BLOCK_MS = 20_000L
        const val CLOSE_MS = 10_000L
        val CREATE_AFTER = 2.seconds
    }
}
