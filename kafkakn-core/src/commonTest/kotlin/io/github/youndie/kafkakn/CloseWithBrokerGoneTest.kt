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
 * [B-83](../../../../../../../docs/backlog/B-83-close-with-records-queued-and-the-broker-gone.md): a MEASUREMENT of
 * how long `close` takes with records queued and the broker gone, and what each queued record's `Delivery` then
 * answers. The contract says only that `close` flushes. A consumer measured 300 200 ms on native for one record.
 *
 * Two ways the broker is gone, as `KAFKAKN_CLOSE_VARIANT` says:
 * - `paused` (`docker pause`): connections stay open and nothing is answered;
 * - `stopped` (`docker stop`): connections are refused. The producer keeps its topics
 *   (`metadata.recovery.strategy=none`, B-80), so the records are queued rather than refused.
 *
 * Nothing is asserted: each arm's times and answers are recorded, and the contract states them.
 */
class CloseWithBrokerGoneTest {
    @Test
    fun close_with_records_queued_and_the_broker_gone() =
        runTest(timeout = 12.minutes) {
            val variant = testEnv("KAFKAKN_CLOSE_VARIANT")
            if (testEnv("KAFKAKN_BROKER_STOP") == null || variant == null) {
                recordArmFact("close.gone", "not asked")
                return@runTest
            }
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-close-gone-$variant-$armName-${randomSuffix()}"
                val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
                try {
                    admin.createTopics(listOf(NewTopic(topic, 1, 1)))
                } finally {
                    admin.close()
                }
                val producer =
                    kafkaProducer(
                        ProducerConfig(
                            "bootstrap.servers" to bootstrap,
                            "acks" to "all",
                            "metadata.recovery.strategy" to "none",
                        ),
                    )
                producer.send(ProducerRecord(topic, "warm".encodeToByteArray()))
                val deliveries = mutableListOf<Delivery>()
                var closedAfter = -1L
                try {
                    if (variant == "paused") brokerPaused(true) else brokerStopped(true)
                    for (index in 0 until RECORDS) {
                        deliveries += producer.enqueue(ProducerRecord(topic, "r-$index".encodeToByteArray()))
                    }
                    val closing = TimeSource.Monotonic.markNow()
                    try {
                        producer.close()
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (thrown: Exception) {
                        recordArmFact("close.$variant.threw", "${thrown::class.simpleName}: ${thrown.message}")
                    }
                    closedAfter = closing.elapsedNow().inWholeMilliseconds
                } finally {
                    if (variant == "paused") brokerPaused(false) else brokerStopped(false)
                }
                recordArmFact("close.$variant.topic", topic)
                recordArmFact("close.$variant.ms", closedAfter.toString())
                deliveries.forEachIndexed { index, delivery ->
                    val answer =
                        try {
                            withTimeoutOrNull(ANSWERED) { delivery.await() }?.let { "landed at ${it.offset}" }
                                ?: "no answer after close"
                        } catch (cancelled: CancellationException) {
                            throw cancelled
                        } catch (thrown: Exception) {
                            "failed ${thrown::class.simpleName}: ${thrown.message}"
                        }
                    recordArmFact("close.$variant.r-$index", answer.replace('\n', ' ').take(REASON))
                }
            }
        }

    /**
     * [B-91](../../../../../../../docs/backlog/B-91-close-with-a-timeout.md): the same situation, with `close(timeout)`.
     * It returns within the timeout and a bounded margin, and every record not acknowledged fails its `await()` with
     * [ClosedBeforeAcknowledgedException]. What the topic then holds is read by `ci/b-91/run.sh`: a record in flight may
     * have been written anyway.
     */
    @Test
    fun close_with_a_timeout_gives_up_on_what_is_not_acknowledged() =
        runTest(timeout = 3.minutes) {
            val variant = testEnv("KAFKAKN_CLOSE_VARIANT")
            if (testEnv("KAFKAKN_BROKER_STOP") == null || variant == null) {
                recordArmFact("close.timeout.gone", "not asked")
                return@runTest
            }
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-close-timeout-$variant-$armName-${randomSuffix()}"
                val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
                try {
                    admin.createTopics(listOf(NewTopic(topic, 1, 1)))
                } finally {
                    admin.close()
                }
                val producer =
                    kafkaProducer(
                        ProducerConfig(
                            "bootstrap.servers" to bootstrap,
                            "acks" to "all",
                            "metadata.recovery.strategy" to "none",
                        ),
                    )
                producer.send(ProducerRecord(topic, "warm".encodeToByteArray()))
                val deliveries = mutableListOf<Delivery>()
                val closedAfter: Long
                try {
                    if (variant == "paused") brokerPaused(true) else brokerStopped(true)
                    for (index in 0 until RECORDS) {
                        deliveries += producer.enqueue(ProducerRecord(topic, "t-$index".encodeToByteArray()))
                    }
                    val closing = TimeSource.Monotonic.markNow()
                    producer.close(CLOSE_TIMEOUT)
                    closedAfter = closing.elapsedNow().inWholeMilliseconds
                } finally {
                    if (variant == "paused") brokerPaused(false) else brokerStopped(false)
                }
                val answers =
                    deliveries.map { delivery ->
                        try {
                            withTimeoutOrNull(ANSWERED) { delivery.await() }?.let { "landed" } ?: "no answer"
                        } catch (cancelled: CancellationException) {
                            throw cancelled
                        } catch (thrown: Exception) {
                            thrown::class.simpleName ?: "?"
                        }
                    }
                recordArmFact("close.timeout.$variant.topic", topic)
                recordArmFact("close.timeout.$variant.ms", closedAfter.toString())
                recordArmFact("close.timeout.$variant.answers", answers.joinToString(" "))
                recordObservation("close.timeout.$variant.answers", answers.distinct().joinToString(" "))
                recordObservation(
                    "close.timeout.$variant.bounded",
                    (closedAfter < BOUND.inWholeMilliseconds).toString(),
                )
                assertTrue(closedAfter < BOUND.inWholeMilliseconds, "close($CLOSE_TIMEOUT) took $closedAfter ms")
                assertEquals(List(RECORDS) { "ClosedBeforeAcknowledgedException" }, answers)
            }
        }

    private companion object {
        const val RECORDS = 5
        const val REASON = 200
        val ANSWERED = 2.seconds
        val CLOSE_TIMEOUT = 3.seconds

        /** The timeout and a margin for the purge and the release that follow it. */
        val BOUND = 10.seconds
    }
}
