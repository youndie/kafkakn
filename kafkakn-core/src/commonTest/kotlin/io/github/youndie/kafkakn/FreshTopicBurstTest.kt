package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-79](../../../../../../../docs/backlog/B-79-poll-after-rebalance-under-load.md): a MEASUREMENT. `PollAfterRebalanceTest`'s
 * fill expired its records on the JVM, and the broker's log showed why: an idempotent producer re-sent sequence 0 of a
 * partition it had already written 0 to 999 into, 107 times, `OutOfOrderSequenceException` each time, for the two
 * minutes of `delivery.timeout.ms`. It began 0.1 s after the topic was created.
 *
 * So the question: does a burst of concurrent sends into a topic created a moment before get stuck, and does writing
 * one record per partition first prevent it? `KAFKAKN_FRESH_ROUNDS` rounds, each on a new topic, with and without
 * that first record, as `KAFKAKN_FRESH_WARM` says. Nothing is asserted: each round's counts are recorded.
 */
class FreshTopicBurstTest {
    @Test
    fun a_burst_into_a_topic_created_a_moment_before() =
        runTest(timeout = 60.minutes) {
            val rounds = testEnv("KAFKAKN_FRESH_ROUNDS")?.toIntOrNull()
            if (rounds == null) {
                recordArmFact("fresh", "not asked")
                return@runTest
            }
            val warm = testEnv("KAFKAKN_FRESH_WARM") == "1"
            withContext(Dispatchers.Default) {
                for (round in 0 until rounds) {
                    val topic = "kafkakn-fresh-$armName-${randomSuffix()}"
                    val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
                    try {
                        admin.createTopics(listOf(NewTopic(topic, PARTITIONS, 1)))
                    } finally {
                        admin.close()
                    }
                    val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "linger.ms" to "5"))
                    val started = TimeSource.Monotonic.markNow()
                    var landed = 0
                    var failed = 0
                    var unanswered = 0
                    var firstFailure: String? = null
                    try {
                        if (warm) {
                            for (partition in 0 until PARTITIONS) {
                                producer.send(ProducerRecord(topic, "warm".encodeToByteArray(), partition = partition))
                            }
                        }
                        val deliveries =
                            coroutineScope {
                                (0 until BURST)
                                    .flatMap { index ->
                                        (0 until PARTITIONS).map { partition ->
                                            async {
                                                producer.enqueue(
                                                    ProducerRecord(
                                                        topic,
                                                        ByteArray(RECORD_BYTES) { index.toByte() },
                                                        partition = partition,
                                                    ),
                                                )
                                            }
                                        }
                                    }.awaitAll()
                            }
                        for (delivery in deliveries) {
                            try {
                                if (withTimeoutOrNull(ANSWER) { delivery.await() } == null) unanswered++ else landed++
                            } catch (cancelled: CancellationException) {
                                throw cancelled
                            } catch (thrown: Exception) {
                                failed++
                                if (firstFailure ==
                                    null
                                ) {
                                    firstFailure = "${thrown::class.simpleName}: ${thrown.message}"
                                }
                            }
                        }
                    } finally {
                        producer.close()
                    }
                    recordArmFact(
                        "fresh.$round",
                        "warm=$warm landed=$landed failed=$failed unanswered=$unanswered " +
                            "ms=${started.elapsedNow().inWholeMilliseconds} topic=$topic first=${firstFailure?.take(
                                REASON,
                            )}",
                    )
                }
            }
        }

    private companion object {
        const val PARTITIONS = 2
        const val BURST = 1_000
        const val RECORD_BYTES = 1_000
        const val REASON = 200
        val ANSWER = 150.seconds
    }
}
