package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-91](../../../../../../../docs/backlog/B-91-close-with-a-timeout.md): `close(timeout)` with a broker that answers.
 * It is a bound, not a wait: with nothing outstanding it returns at once, and records that can be acknowledged in
 * time are. What it gives up on when the broker does not answer is `CloseWithBrokerGoneTest`'s, run by
 * `ci/b-91/run.sh`.
 */
class CloseWithTimeoutTest {
    @Test
    fun close_with_a_timeout_and_nothing_outstanding_returns_at_once() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
                producer.send(ProducerRecord(testTopic, "close-idle-$armName".encodeToByteArray()))
                val closing = TimeSource.Monotonic.markNow()
                producer.close(TIMEOUT)
                val closed = closing.elapsedNow()
                recordArmFact("close.timeout.idle.ms", closed.inWholeMilliseconds.toString())
                recordObservation("close.timeout.idle.prompt", (closed < PROMPT).toString())
                assertTrue(closed < PROMPT, "close($TIMEOUT) with nothing outstanding took $closed")
            }
        }

    @Test
    fun records_that_can_be_acknowledged_in_time_land() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
                val deliveries =
                    (0 until RECORDS).map {
                        producer.enqueue(
                            ProducerRecord(testTopic, "close-timeout-$armName-$it".encodeToByteArray()),
                        )
                    }
                producer.close(TIMEOUT)
                val landed = deliveries.count { runCatching { it.await() }.isSuccess }
                recordObservation("close.timeout.landed", "$landed of $RECORDS")
                assertEquals(RECORDS, landed, "every record acknowledged before close($TIMEOUT) returned")
            }
        }

    private companion object {
        const val RECORDS = 20
        val TIMEOUT = 10.seconds
        val PROMPT = 2.seconds
    }
}
