package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-74](../../../../../../../docs/backlog/B-74-a-cut-wait-says-whether-the-record-was-queued.md): a caller whose wait
 * ends early can tell "never queued" from "queued, outcome unknown", on both arms, by which step ended it:
 * - [KafkaProducer.enqueue] throws [RecordNotQueuedException] when the queue had no room within `max.block.ms`, and
 *   that record is not in the topic after the queue drains;
 * - once it returned, the record is queued: a wait on [Delivery.await] cut short leaves it going on, and it lands.
 *
 * The broker is paused (`docker pause`) so that nothing drains, only when `ci/b-74/run.sh` asks.
 */
class EnqueueTest {
    @Test
    fun a_record_the_queue_has_no_room_for_within_max_block_ms_is_not_queued() =
        runTest(timeout = 3.minutes) {
            if (testEnv("KAFKAKN_BROKER_CONTROL") == null) {
                recordArmFact("enqueue.full", "not asked")
                return@runTest
            }
            withContext(Dispatchers.Default) {
                val topic = oneTopic("kafkakn-enqueue-full")
                val producer =
                    kafkaProducer(
                        ProducerConfig(
                            mapOf("bootstrap.servers" to bootstrap, "acks" to "all") + smallQueueConfig() +
                                ("max.block.ms" to MAX_BLOCK_MS.toString()),
                        ),
                    )
                try {
                    producer.send(ProducerRecord(topic, "warm".encodeToByteArray()))
                    val deliveries = mutableListOf<Delivery>()
                    var refused: String? = null
                    var refusedAfter = 0L
                    var refusedIndex = -1
                    brokerPaused(true)
                    try {
                        // Queue until the queue is full and one more cannot be queued within max.block.ms.
                        for (index in 0 until LIMIT) {
                            val started = TimeSource.Monotonic.markNow()
                            try {
                                deliveries += producer.enqueue(ProducerRecord(topic, value(index)))
                            } catch (cancelled: CancellationException) {
                                throw cancelled
                            } catch (thrown: Exception) {
                                refused = "threw ${thrown::class.simpleName}: ${thrown.message}"
                                refusedAfter = started.elapsedNow().inWholeMilliseconds
                                refusedIndex = index
                                break
                            }
                        }
                    } finally {
                        brokerPaused(false)
                    }
                    deliveries.forEach { it.await() }
                    producer.flush()
                    val end = endOffset(topic)
                    recordArmFact("enqueue.full.topic", topic)
                    recordArmFact("enqueue.full.refused.said", refused.toString())
                    recordArmFact("enqueue.full.refused.after.ms", refusedAfter.toString())
                    recordArmFact("enqueue.full.refused.value", "r-$refusedIndex")
                    recordArmFact("enqueue.full.queued", deliveries.size.toString())
                    recordObservation("enqueue.full.refused", refused.toString().substringBefore(":"))
                    recordObservation("enqueue.full.end.is.warm.plus.queued", (end == 1L + deliveries.size).toString())
                    assertEquals("threw RecordNotQueuedException", refused.toString().substringBefore(":"))
                    assertTrue(
                        refusedAfter in (MAX_BLOCK_MS - 500)..(MAX_BLOCK_MS * 3),
                        "refused after $refusedAfter ms",
                    )
                    assertEquals(1L + deliveries.size, end, "warm and every queued record, and not the refused one")
                } finally {
                    producer.close()
                }
            }
        }

    @Test
    fun a_record_queued_and_cut_while_awaiting_is_outcome_unknown_and_lands() =
        runTest(timeout = 3.minutes) {
            if (testEnv("KAFKAKN_BROKER_CONTROL") == null) {
                recordArmFact("enqueue.unknown", "not asked")
                return@runTest
            }
            withContext(Dispatchers.Default) {
                val topic = oneTopic("kafkakn-enqueue-unknown")
                val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
                try {
                    producer.send(ProducerRecord(topic, "warm".encodeToByteArray()))
                    brokerPaused(true)
                    val started = TimeSource.Monotonic.markNow()
                    val delivery: Delivery
                    val cut: RecordMetadata?
                    val queuedAfter: Long
                    try {
                        delivery = producer.enqueue(ProducerRecord(topic, "unknown".encodeToByteArray()))
                        queuedAfter = started.elapsedNow().inWholeMilliseconds
                        cut = withTimeoutOrNull(CUT) { delivery.await() }
                    } finally {
                        brokerPaused(false)
                    }
                    // The same delivery, awaited again once the broker answers: the record went on.
                    val landed = delivery.await()
                    val end = endOffset(topic)
                    recordArmFact("enqueue.unknown.topic", topic)
                    recordArmFact("enqueue.unknown.queued.after.ms", queuedAfter.toString())
                    recordObservation(
                        "enqueue.unknown.queued.promptly",
                        (queuedAfter < CUT.inWholeMilliseconds).toString(),
                    )
                    recordObservation("enqueue.unknown.cut", (cut == null).toString())
                    recordObservation("enqueue.unknown.landed", "${landed.offset} end=$end")
                    assertTrue(
                        queuedAfter < CUT.inWholeMilliseconds,
                        "queued after $queuedAfter ms, with the broker paused",
                    )
                    assertEquals(null, cut, "the wait on the broker was cut")
                    assertEquals(1L, landed.offset, "the cut record landed after warm")
                    assertEquals(2L, end)
                } finally {
                    producer.close()
                }
            }
        }

    private fun value(index: Int): ByteArray = "r-$index".encodeToByteArray() + ByteArray(RECORD_BYTES)

    private suspend fun oneTopic(prefix: String): String {
        val topic = "$prefix-$armName-${randomSuffix()}"
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            admin.createTopics(listOf(NewTopic(topic, 1, 1)))
        } finally {
            admin.close()
        }
        return topic
    }

    private suspend fun endOffset(topic: String): Long {
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            return admin
                .listOffsets(
                    listOf(TopicPartition(topic, 0)),
                    OffsetSpec.Latest,
                ).getValue(TopicPartition(topic, 0))!!
        } finally {
            admin.close()
        }
    }

    private companion object {
        const val MAX_BLOCK_MS = 2_000L
        const val LIMIT = 1_000
        const val RECORD_BYTES = 1024
        val CUT = 1.seconds
    }
}
