package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-54](../../../../../../../docs/backlog/B-54-a-flow-over-poll.md): a `Flow` of records, built on `poll`,
 * in common code.
 */
class RecordsFlowTest {
    private val partition = TopicPartition(consumeTopic, 0)

    @Test
    fun collecting_returns_the_records_in_order() =
        runTest(timeout = 1.minutes) {
            withContext(Dispatchers.Default) {
                val consumer = kafkaConsumer(ConsumerConfig("bootstrap.servers" to bootstrap))
                try {
                    consumer.assign(listOf(partition))
                    consumer.seek(partition, SeekTo.Beginning)
                    val read = consumer.records().take(CONSUME_COUNT).toList()
                    assertEquals((0L until CONSUME_COUNT).toList(), read.map { it.offset }, "offsets, in order")
                    recordObservation("flow.read", read.size.toString())
                } finally {
                    consumer.close()
                }
            }
        }

    @Test
    fun cancelling_the_collector_returns_promptly_and_the_consumer_stays_usable() =
        runTest(timeout = 1.minutes) {
            withContext(Dispatchers.Default) {
                val consumer = kafkaConsumer(ConsumerConfig("bootstrap.servers" to bootstrap))
                try {
                    consumer.assign(listOf(partition))
                    consumer.seek(partition, SeekTo.End)
                    // Nothing to read at the end: the collector is waiting inside poll when it is cancelled. In a
                    // scope of its own, and the join bounded: a collector that does not stop would otherwise
                    // keep this test's scope waiting forever, and the run hangs instead of failing by name.
                    val collecting = CoroutineScope(Dispatchers.Default).launch { consumer.records().collect { } }
                    delay(WAIT_FIRST)
                    val cancelledAt = TimeSource.Monotonic.markNow()
                    collecting.cancel()
                    val stopped = withTimeoutOrNull(STOP_WITHIN) { collecting.join() } != null
                    val took = cancelledAt.elapsedNow()
                    assertTrue(stopped, "the collector did not stop within $STOP_WITHIN of being cancelled")
                    consumer.seek(partition, SeekTo.Beginning)
                    val after = consumer.records().take(1).toList()
                    assertTrue(took < PROMPT, "cancelling took $took")
                    assertEquals(0L, after.single().offset, "the consumer after a cancelled collector")
                    recordArmFact("flow.cancel.ms", took.inWholeMilliseconds.toString())
                } finally {
                    consumer.close()
                }
            }
        }

    /**
     * The eviction the `Flow` makes easy: while the collector is busy, nothing polls. Here it is busy for
     * longer than `max.poll.interval.ms`, deliberately. B-54 measured the arms parting at the next poll:
     * the JVM rejoined and native threw. [B-64](../../../../../../../docs/backlog/B-64-native-poll-throws-where-the-jvm-rejoins.md)
     * holds both to the reference's behaviour, asserted here.
     */
    @Test
    fun a_collector_slower_than_max_poll_interval_is_evicted_and_the_caller_is_told() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val events = mutableListOf<String>()
                val listener =
                    object : RebalanceListener {
                        override fun onAssigned(partitions: List<TopicPartition>) {
                            events += "+${partitions.map { it.partition }}"
                        }

                        override fun onRevoked(
                            partitions: List<TopicPartition>,
                            scope: RebalanceScope,
                        ) {
                            events += "-${partitions.map { it.partition }}"
                        }

                        override fun onLost(partitions: List<TopicPartition>) {
                            events += "!${partitions.map { it.partition }}"
                        }
                    }
                val consumer =
                    kafkaConsumer(
                        ConsumerConfig(
                            "bootstrap.servers" to bootstrap,
                            "group.id" to "kafkakn-flow-slow-$armName-${randomSuffix()}",
                            "auto.offset.reset" to "earliest",
                            "max.poll.interval.ms" to "6000",
                            "session.timeout.ms" to "6000",
                        ),
                    )
                val seen = mutableListOf<Long>()
                val outcome =
                    try {
                        consumer.subscribe(listOf(consumeTopic), listener)
                        consumer.records().take(RECORDS_AFTER).collect { record ->
                            seen += record.offset
                            if (seen.size == 1) delay(SLOWER_THAN_THE_INTERVAL)
                        }
                        "completed"
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (thrown: Exception) {
                        "threw ${thrown::class.simpleName}: ${thrown.message}"
                    } finally {
                        consumer.close()
                    }
                recordArmFact("flow.slow.outcome", outcome)
                recordArmFact("flow.slow.events", events.joinToString(" "))
                recordArmFact("flow.slow.seen", seen.joinToString(","))
                // B-64: one behaviour, the reference's. The eviction is told to the listener, the next poll
                // rejoins, and with nothing committed the records come again from the start.
                assertEquals("completed", outcome, "the collector after an eviction")
                val lost = events.indexOfFirst { it.startsWith("!") }
                assertTrue(
                    lost >= 0 && events.drop(lost + 1).any { it.startsWith("+") },
                    "lost, then assigned again: $events",
                )
                assertEquals(
                    (0L until CONSUME_COUNT).toList() + (0L until RECORDS_AFTER - CONSUME_COUNT),
                    seen,
                    "records",
                )
                recordObservation("flow.slow.outcome", outcome)
            }
        }

    private companion object {
        const val RECORDS_AFTER = 25
        val WAIT_FIRST = 500.milliseconds
        val PROMPT = 2.seconds
        val STOP_WITHIN = 10.seconds
        val SLOWER_THAN_THE_INTERVAL = 10.seconds
    }
}
