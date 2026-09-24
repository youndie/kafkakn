package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-36](../../../../../../../docs/backlog/B-36-assign-and-poll.md): assign and poll, against
 * [consumer-contract](../../../../../../../docs/api/consumer-contract.md).
 *
 * **The records were written by a third party**, not by kafkakn: the Kafka distribution's own client
 * (`ci/harness/Records.java`, through `broker.sh up`) — null keys, a tombstone, bytes that are not UTF-8,
 * duplicate header names, empty next to null. What each arm reads is recorded in the dump format that
 * tool prints, and `ci/b-36/run.sh` compares the three, byte for byte.
 *
 * The last three tests are H7 (research §2.24): overlapping calls, a cancelled `poll`, the dispatcher.
 */
class ConsumerTest {
    private val partition = TopicPartition(consumeTopic, 0)

    @Test
    fun both_arms_read_every_record_of_an_assigned_partition_in_order() =
        runTest(timeout = TIMEOUT) {
            val read = mutableListOf<ConsumerRecord>()
            withConsumer { consumer ->
                consumer.assign(listOf(partition))
                consumer.seek(partition, SeekTo.Beginning)
                val deadline = TimeSource.Monotonic.markNow() + READ_FOR
                while (read.size < CONSUME_COUNT && deadline.hasNotPassedNow()) {
                    read += consumer.poll(POLL)
                }
            }
            assertEquals(CONSUME_COUNT, read.size, "records read")
            assertEquals((0L until CONSUME_COUNT).toList(), read.map { it.offset }, "offsets, in order")
            recordArmFact("consume.dump", read.joinToString(";") { it.dumpLine() })
        }

    @Test
    fun seeking_lands_where_the_broker_says() =
        runTest(timeout = TIMEOUT) {
            withConsumer { consumer ->
                consumer.assign(listOf(partition))
                // Each landing is recorded; `ci/b-36/run.sh` asks `kafka-get-offsets.sh` where each
                // should have been.
                for ((name, to) in listOf(
                    "beginning" to SeekTo.Beginning,
                    "offset" to SeekTo.Offset(SEEK_OFFSET),
                    "timestamp" to SeekTo.Timestamp(SEEK_TIMESTAMP),
                )) {
                    consumer.seek(partition, to)
                    recordArmFact("consume.seek.$name", firstOffset(consumer).toString())
                }
                consumer.seek(partition, SeekTo.End)
                val afterEnd = consumer.poll(QUIET)
                recordArmFact("consume.seek.end.read", afterEnd.size.toString())
                assertTrue(afterEnd.isEmpty(), "a seek to the end read ${afterEnd.size} records")
            }
        }

    @Test
    fun overlapping_calls_on_one_consumer_are_queued_not_interleaved() =
        runTest(timeout = TIMEOUT) {
            // H7, first measurement: the Java client throws ConcurrentModificationException on
            // overlapping calls. Eight coroutines polling one consumer at once on a pool, twenty times
            // each - nothing may throw, and nothing may be read twice.
            val read = mutableListOf<Long>()
            val guard = Mutex()
            withConsumer { consumer ->
                consumer.assign(listOf(partition))
                consumer.seek(partition, SeekTo.Beginning)
                withContext(Dispatchers.Default) {
                    coroutineScope {
                        repeat(CALLERS) {
                            launch {
                                repeat(CALLS) {
                                    val got = consumer.poll(SHORT)
                                    guard.withLock { read += got.map { it.offset } }
                                }
                            }
                        }
                    }
                }
            }
            assertEquals(read.distinct().size, read.size, "a record was returned twice to overlapping callers")
            recordArmFact("consume.overlap.read", read.size.toString())
        }

    @Test
    fun cancelling_a_waiting_poll_returns_promptly_and_the_consumer_stays_usable() =
        runTest(timeout = TIMEOUT) {
            // H7, second measurement. At the end of the partition a poll has nothing to return and waits
            // its whole timeout - a minute here - unless cancellation reaches it.
            withConsumer { consumer ->
                consumer.assign(listOf(partition))
                consumer.seek(partition, SeekTo.End)
                val waited =
                    withContext(Dispatchers.Default) {
                        val poll = launch { consumer.poll(1.minutes) }
                        delay(CANCEL_AFTER)
                        val cancelled = TimeSource.Monotonic.markNow()
                        poll.cancel()
                        poll.join()
                        cancelled.elapsedNow()
                    }
                recordArmFact("consume.cancel.ms", waited.inWholeMilliseconds.toString())
                assertTrue(waited < PROMPT, "a cancelled poll took $waited to return")
                // Usable afterwards: the next poll neither throws nor returns something stale.
                consumer.seek(partition, SeekTo.Beginning)
                assertEquals(0L, firstOffset(consumer), "the consumer after a cancelled poll")
            }
        }

    /** H7, third measurement — the seam, with the mark taken before the call (research §2.22). */
    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun a_waiting_poll_does_not_hold_the_callers_dispatcher() =
        runTest(timeout = TIMEOUT) {
            val lane = Dispatchers.Default.limitedParallelism(1)
            var longestSilence = Duration.ZERO
            var returned = -1
            val started = TimeSource.Monotonic.markNow()
            withConsumer { consumer ->
                consumer.assign(listOf(partition))
                consumer.seek(partition, SeekTo.End)
                withContext(lane) {
                    coroutineScope {
                        var lastTick = TimeSource.Monotonic.markNow()

                        fun tick() {
                            val gap = lastTick.elapsedNow()
                            if (gap > longestSilence) longestSilence = gap
                            lastTick = TimeSource.Monotonic.markNow()
                        }

                        val ticker =
                            launch {
                                while (isActive) {
                                    tick()
                                    delay(TICK)
                                }
                            }
                        returned = consumer.poll(WAIT).size
                        tick()
                        ticker.cancel()
                    }
                }
            }
            val waited = started.elapsedNow()
            assertTrue(returned == 0 && waited > WAIT, "the fixture stopped working: $returned records after $waited")
            assertTrue(longestSilence < TOLERATED_SILENCE, "the caller's dispatcher was held for $longestSilence")
        }

    private suspend fun firstOffset(consumer: KafkaConsumer): Long {
        val deadline = TimeSource.Monotonic.markNow() + READ_FOR
        while (deadline.hasNotPassedNow()) {
            consumer.poll(POLL).firstOrNull()?.let { return it.offset }
        }
        return -1
    }

    private suspend fun withConsumer(use: suspend (KafkaConsumer) -> Unit) {
        val consumer = kafkaConsumer(ConsumerConfig("bootstrap.servers" to bootstrap))
        try {
            use(consumer)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } finally {
            withContext(Dispatchers.Default) { consumer.close() }
        }
    }

    private companion object {
        val TIMEOUT = 3.minutes
        val READ_FOR = 30.seconds
        val POLL = 1.seconds
        val QUIET = 2.seconds
        val SHORT = 10.milliseconds
        val WAIT = 3.seconds
        val CANCEL_AFTER = 500.milliseconds
        val PROMPT = 2.seconds
        val TICK = 2.milliseconds
        val TOLERATED_SILENCE = 500.milliseconds
        const val CALLERS = 8
        const val CALLS = 20
        const val SEEK_OFFSET = 7L

        /** Between record 5 (…5 000) and record 6 (…6 000): the broker's answer is offset 6. */
        const val SEEK_TIMESTAMP = 1_700_000_005_500L
    }
}

/** The dump format `ci/harness/Records.java` prints: `offset/timestamp/key/value/headers`. */
internal fun ConsumerRecord.dumpLine(): String {
    fun ByteArray?.dumped() =
        if (this ==
            null
        ) {
            "~"
        } else {
            "x" + joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }
        }
    return "$offset/$timestamp/${key.dumped()}/${value.dumped()}/" +
        headers.joinToString(",") { "${it.name}:${it.value.dumped()}" }
}
