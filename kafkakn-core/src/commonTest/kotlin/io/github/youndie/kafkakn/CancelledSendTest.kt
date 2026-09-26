package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
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
 * [B-73](../../../../../../../docs/backlog/B-73-a-cancelled-send.md): what a cancelled `send` means for its record,
 * on each arm, against the broker's end offsets.
 *
 * The broker is paused (`docker pause`, so its connections stay open and nothing is answered) while a `send` is cut
 * with `withTimeoutOrNull`, then unpaused. Two moments:
 * - **after the record is queued**: the record goes on, and lands once the broker answers;
 * - **while it waits for room**, the queue at its bound: what each arm does is measured, not assumed. Native waits
 *   in a loop of its own, the JVM inside `kafka-clients`' blocking `send`.
 *
 * Only when `ci/b-73/run.sh` asks (`KAFKAKN_BROKER_CONTROL`): pausing the broker under other tests would fail them.
 */
class CancelledSendTest {
    @Test
    fun a_send_cancelled_after_its_record_was_queued_still_lands() =
        runTest(timeout = 3.minutes) {
            if (testEnv("KAFKAKN_BROKER_CONTROL") == null) {
                recordArmFact("cancel.queued", "not asked")
                return@runTest
            }
            withContext(Dispatchers.Default) {
                val topic = oneTopic("kafkakn-cancel-queued")
                val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
                try {
                    // Metadata and a connection first: the send cut below is queued at once, not held for either.
                    producer.send(ProducerRecord(topic, "warm".encodeToByteArray()))
                    val started = TimeSource.Monotonic.markNow()
                    brokerPaused(true)
                    var returnedAfter = 0.milliseconds
                    val cut =
                        try {
                            coroutineScope {
                                // Unpaused on a clock of its own, never after the cut send returns: a send that cannot
                                // return until the broker answers would otherwise wait for itself.
                                val unpause =
                                    launch {
                                        delay(PAUSE - started.elapsedNow())
                                        brokerPaused(false)
                                    }
                                val sent =
                                    withTimeoutOrNull(
                                        CUT,
                                    ) { producer.send(ProducerRecord(topic, "cut".encodeToByteArray())) }
                                returnedAfter = started.elapsedNow()
                                unpause.join()
                                sent
                            }
                        } finally {
                            brokerPaused(false)
                        }
                    producer.flush()
                    val end = endOffset(topic)
                    recordArmFact("cancel.queued.topic", topic)
                    recordArmFact("cancel.queued.returned.after.ms", returnedAfter.inWholeMilliseconds.toString())
                    recordArmFact("cancel.queued.parked", parkedSendCount().toString())
                    recordObservation("cancel.queued.cut", (cut == null).toString())
                    recordObservation("cancel.queued.end", end.toString())
                    assertEquals(null, cut, "the send was cut before the paused broker could answer")
                    assertEquals(2L, end, "warm and the cut record: a cancelled send does not recall a queued record")
                    assertTrue(parkedSendCount() <= 0, "a parked send left behind: ${parkedSendCount()}")
                } finally {
                    producer.close()
                }
            }
        }

    @Test
    fun a_send_cancelled_while_it_waits_for_room_is_measured() =
        runTest(timeout = 3.minutes) {
            if (testEnv("KAFKAKN_BROKER_CONTROL") == null) {
                recordArmFact("cancel.room", "not asked")
                return@runTest
            }
            withContext(Dispatchers.Default) {
                val topic = oneTopic("kafkakn-cancel-room")
                val producer =
                    kafkaProducer(
                        ProducerConfig(mapOf("bootstrap.servers" to bootstrap, "acks" to "all") + smallQueueConfig()),
                    )
                try {
                    producer.send(ProducerRecord(topic, "warm".encodeToByteArray()))
                    brokerPaused(true)
                    val started = TimeSource.Monotonic.markNow()
                    var returnedAfter = 0.milliseconds
                    val cut =
                        try {
                            coroutineScope {
                                // Unpaused on a clock of its own (see above): on the JVM the probe cannot return before.
                                val unpause =
                                    launch {
                                        delay(PAUSE - started.elapsedNow())
                                        brokerPaused(false)
                                    }
                                // The queue filled to its bound while nothing drains it. Native: 100 records, its
                                // queue.buffering.max.messages here. JVM: more 1 KiB records than 32 KiB of buffer holds,
                                // fewer than Dispatchers.IO has threads, so the probe below reaches kafka-clients' own
                                // wait for room rather than a queue for a thread.
                                val fill =
                                    (0 until FILL).map { index ->
                                        async {
                                            producer.send(
                                                ProducerRecord(topic, ByteArray(RECORD_BYTES) { index.toByte() }),
                                            )
                                        }
                                    }
                                delay(SETTLE)
                                val probeStarted = TimeSource.Monotonic.markNow()
                                val probe =
                                    withTimeoutOrNull(
                                        CUT,
                                    ) { producer.send(ProducerRecord(topic, "probe".encodeToByteArray())) }
                                returnedAfter = probeStarted.elapsedNow()
                                unpause.join()
                                fill.awaitAll()
                                probe
                            }
                        } finally {
                            brokerPaused(false)
                        }
                    producer.flush()
                    val end = endOffset(topic)
                    // What reached the topic beyond warm and the fill: the probe, or nothing.
                    val probeLanded = end - 1 - FILL
                    recordArmFact("cancel.room.topic", topic)
                    recordArmFact("cancel.room.returned.after.ms", returnedAfter.inWholeMilliseconds.toString())
                    recordArmFact("cancel.room.probe.landed", probeLanded.toString())
                    recordArmFact("cancel.room.parked", parkedSendCount().toString())
                    assertEquals(null, cut, "the probe was cut")
                    assertTrue(parkedSendCount() <= 0, "a parked send left behind: ${parkedSendCount()}")
                    // Measured 2026-09-26 (B-73), and held here so it cannot change unnoticed:
                    if (armName == "jvm") {
                        // kafka-clients' send blocks while it waits for room, and a coroutine cannot interrupt it. The
                        // cut caller gets its thread back only when the client lets go, here when the broker answered
                        // again, and by then the probe was queued: it is in the topic.
                        assertTrue(returnedAfter >= CUT * 3, "the JVM caller came back in $returnedAfter")
                        assertEquals(1L, probeLanded, "on the JVM, the cut probe was queued after all")
                    } else {
                        // Native waits for room in a loop of its own, and a cut there is clean: the caller is back at
                        // the deadline, and the probe was never queued.
                        assertTrue(returnedAfter < CUT * 2, "the native caller came back in $returnedAfter")
                        assertEquals(0L, probeLanded, "on native, the cut probe was never queued")
                    }
                } finally {
                    producer.close()
                }
            }
        }

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
        val CUT = 1.seconds
        val PAUSE = 6.seconds
        val SETTLE = 1.seconds
        const val RECORD_BYTES = 1024
        val FILL = if (armName == "jvm") 40 else 100
    }
}
