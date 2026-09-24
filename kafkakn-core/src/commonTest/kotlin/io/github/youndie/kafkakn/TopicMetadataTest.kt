package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

/**
 * [B-29](../../../../../../../docs/backlog/B-29-topic-metadata.md): `partitionsFor`, what a topic looks
 * like from the producer that writes to it — `Producer.partitionsFor` on the JVM, `rd_kafka_metadata`
 * on native.
 *
 * **Half of the first assertion is in `ci/b-29/run.sh`**: the answer is recorded here and compared
 * there with `kafka-topics.sh --describe`, the broker's own description of the same topic.
 */
class TopicMetadataTest {
    @Test
    fun the_partitions_agree_with_what_the_broker_describes() =
        runTest {
            val partitions =
                withProducer(ProducerConfig("bootstrap.servers" to bootstrap)) { it.partitionsFor(metadataTopic) }

            assertEquals((0 until metadataPartitions).toList(), partitions.map { it.partition })
            for (partition in partitions) {
                assertEquals(metadataTopic, partition.topic)
                assertNotNull(partition.leader, "partition ${partition.partition} has no leader")
                assertTrue(partition.replicas.isNotEmpty(), "partition ${partition.partition} has no replicas")
                assertTrue(partition.inSyncReplicas.isNotEmpty(), "partition ${partition.partition} has no ISR")
            }
            // `partition:leader:replicas:isr`, the shape `ci/b-29/run.sh` builds from the broker's output.
            recordArmFact(
                "metadata.describe",
                partitions.joinToString(";") {
                    "${it.partition}:${it.leader}:${it.replicas.joinToString(
                        ",",
                    )}:${it.inSyncReplicas.joinToString(",")}"
                },
            )
        }

    @Test
    fun an_unknown_topic_fails_and_each_arm_says_so_its_own_way() =
        runTest {
            val started = TimeSource.Monotonic.markNow()
            val failure =
                withProducer(ProducerConfig(mapOf("bootstrap.servers" to bootstrap) + failFastConfig())) { producer ->
                    assertFails { producer.partitionsFor("kafkakn-no-such-topic-${randomSuffix()}") }
                }
            // Recorded rather than compared: how each client reaches the refusal, and how long it takes,
            // is part of what a caller meets, and the item promises only that both refuse.
            recordArmFact("metadata.unknown.failure", "${failure::class.simpleName}: ${failure.message}".take(REASON))
            recordArmFact("metadata.unknown.ms", started.elapsedNow().inWholeMilliseconds.toString())
        }

    /**
     * The seam `JvmDispatcherSeamTest` holds for `send`, held for this call on BOTH arms — here, in
     * common code, because both clients block: the Java client inside `partitionsFor`, librdkafka inside
     * `rd_kafka_metadata`.
     *
     * One lane of `Dispatchers.Default`, and a port with no broker, so the call waits out its whole
     * bound. A ticker on the same lane that wants it every 2 ms and does not get it for seconds says the
     * lane was held; a gap is the measurement, not a count, for the reason that test gives.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun the_call_does_not_hold_the_callers_dispatcher() =
        runTest(timeout = TEST_TIMEOUT) {
            val lane = Dispatchers.Default.limitedParallelism(1)
            val nowhere = ProducerConfig(mapOf("bootstrap.servers" to "127.0.0.1:9099") + failFastConfig())
            // Plain variables, and that is safe here rather than careless: the ticker and the call share
            // one lane, which runs one of them at a time.
            var longestSilence = Duration.ZERO
            var outcome: String? = null
            val started = TimeSource.Monotonic.markNow()
            withProducer(nowhere) { producer ->
                withContext(lane) {
                    coroutineScope {
                        // The mark is taken HERE, before the call, and read again after it. The first
                        // version started it inside the ticker and passed against a blocking
                        // implementation: the ticker never got the lane until the call was over, so its
                        // clock started after the silence it was meant to measure, and it was cancelled
                        // before its first gap. Watched green on both arms against exactly the defect.
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
                        outcome =
                            try {
                                producer.partitionsFor(testTopic)
                                "answered"
                            } catch (cancellation: CancellationException) {
                                throw cancellation
                            } catch (refused: Throwable) {
                                "${refused::class.simpleName}: ${refused.message}"
                            }
                        // The gap still open when the call returns: on a held lane, the only one there is.
                        tick()
                        ticker.cancel()
                    }
                }
            }
            val waited = started.elapsedNow()
            recordArmFact("metadata.nowhere.ms", waited.inWholeMilliseconds.toString())

            // The fixture has to have arranged a wait, or the silence below measures nothing.
            assertTrue(
                outcome != "answered" && waited > MEANINGFUL_WAIT,
                "the fixture stopped working: the call returned '$outcome' after $waited, and it " +
                    "should have waited for a broker that is not there. Nothing below measures what it claims to.",
            )
            assertTrue(
                longestSilence < TOLERATED_SILENCE,
                "the caller's dispatcher was held for $longestSilence while partitionsFor waited " +
                    "$waited on a broker that is not there: a suspend signature over a blocking call.",
            )
        }

    private suspend fun <T> withProducer(
        config: ProducerConfig,
        use: suspend (KafkaProducer) -> T,
    ): T {
        val producer = kafkaProducer(config)
        try {
            return use(producer)
        } finally {
            withContext(Dispatchers.Default) { producer.close() }
        }
    }

    private companion object {
        const val REASON = 300
        val TICK = 2.milliseconds
        val TOLERATED_SILENCE = 500.milliseconds
        val MEANINGFUL_WAIT = 2000.milliseconds
        val TEST_TIMEOUT = 120_000.milliseconds
    }
}
