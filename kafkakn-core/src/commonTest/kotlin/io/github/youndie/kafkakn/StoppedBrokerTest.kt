package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * A stopped broker and a topic already written to: what `enqueue` answers, on both arms alike.
 *
 * [B-77](../../../../../../../docs/backlog/B-77-a-stopped-broker-and-a-known-topic.md) measured the arms disagreeing:
 * the Java client's `metadata.recovery.strategy`, `rebootstrap` by default, forgets a known topic when no broker is
 * reachable, so its `send` refuses at `max.block.ms`, while native queued.
 * [B-80](../../../../../../../docs/backlog/B-80-native-forgets-topics-when-every-broker-is-down.md) makes native
 * follow the oracle. So, with `enqueue` at 5 and 20 s after `docker stop`:
 * - as they ship, both arms refuse every record with [RecordNotQueuedException], and none is in the topic after;
 * - with `metadata.recovery.strategy=none` (`KAFKAKN_STOPPED_EXTRA`), both queue every record, and each lands;
 * - either way, once the broker is back, the topic is described again and the next record is queued and lands.
 *
 * A record at the moment of the stop is recorded and not asserted. Neither client has noticed yet that the brokers
 * are gone, and the JVM answered both ways at 0 s in two runs: refused on 2026-09-27 in B-77, and queued in B-80's
 * first run. No contract can promise an answer the oracle does not give consistently.
 *
 * Only when `ci/b-77/run.sh` asks, with `KAFKAKN_BROKER_STOP`: stopping the broker under other tests would fail them.
 */
class StoppedBrokerTest {
    @Test
    fun a_record_enqueued_while_every_broker_is_down_is_answered_alike_on_both_arms() =
        runTest(timeout = 8.minutes) {
            val extra =
                testEnv("KAFKAKN_STOPPED_EXTRA")?.takeIf { it.isNotBlank() }?.split("=", limit = 2)?.let {
                    it[0] to
                        it[1]
                }
            val keeps = extra == ("metadata.recovery.strategy" to "none")
            val ran =
                withFaultBroker("KAFKAKN_BROKER_STOP") { broker ->
                    withContext(Dispatchers.Default) {
                        val topic = "kafkakn-stopped-$armName-${randomSuffix()}"
                        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to broker.bootstrap))
                        try {
                            admin.createTopics(listOf(NewTopic(topic, 1, 1)))
                        } finally {
                            admin.close()
                        }
                        val producer =
                            kafkaProducer(
                                ProducerConfig(
                                    mapOf(
                                        "bootstrap.servers" to broker.bootstrap,
                                        "acks" to "all",
                                        "max.block.ms" to "$MAX_BLOCK_MS",
                                    ) +
                                        listOfNotNull(extra),
                                ),
                            )
                        val answers = mutableListOf<String>()
                        val queued = mutableListOf<Pair<String, Delivery>>()
                        try {
                            // The topic is known: one record written and acknowledged before the stop.
                            producer.send(ProducerRecord(topic, "warm".encodeToByteArray()))
                            broker.stopped(true)
                            val stopped = TimeSource.Monotonic.markNow()
                            try {
                                for ((index, at) in MOMENTS.withIndex()) {
                                    val wait = at - stopped.elapsedNow()
                                    if (wait > Duration.ZERO) delay(wait)
                                    val value = "r-$index"
                                    val started = TimeSource.Monotonic.markNow()
                                    val said =
                                        outcome {
                                            queued +=
                                                value to
                                                producer.enqueue(ProducerRecord(topic, value.encodeToByteArray()))
                                        }
                                    recordArmFact(
                                        "stopped.$index.after.ms",
                                        started.elapsedNow().inWholeMilliseconds.toString(),
                                    )
                                    recordArmFact("stopped.$index.said", said.replace('\n', ' ').take(REASON))
                                    if (at >= NOTICED) {
                                        answers += said.substringBefore(":")
                                        broker.observe("stopped.$index", said.substringBefore(":"))
                                    }
                                }
                            } finally {
                                broker.stopped(false)
                            }
                            // The broker is back: the topic is described again, and a record is queued. Tried until the broker
                            // answers, each try bounded by max.block.ms.
                            val back = TimeSource.Monotonic.markNow()
                            var backSaid = "not tried"
                            while (back.elapsedNow() < BACK) {
                                backSaid =
                                    outcome {
                                        queued +=
                                            "back" to
                                            producer.enqueue(ProducerRecord(topic, "back".encodeToByteArray()))
                                    }
                                if (backSaid == "queued") break
                            }
                            recordArmFact("stopped.back.said", backSaid.replace('\n', ' ').take(REASON))
                            broker.observe("stopped.back", backSaid.substringBefore(":"))
                            val fates =
                                queued.associate { (value, delivery) ->
                                    value to
                                        try {
                                            withTimeoutOrNull(LANDING) { delivery.await() }?.let { "landed" }
                                                ?: "no answer"
                                        } catch (cancelled: CancellationException) {
                                            throw cancelled
                                        } catch (thrown: Exception) {
                                            "failed ${thrown::class.simpleName}"
                                        }
                                }
                            fates.forEach { (value, fate) -> recordArmFact("stopped.$value.fate", fate) }
                            recordArmFact("stopped.topic", topic)
                            recordArmFact("stopped.extra", extra?.let { "${it.first}=${it.second}" } ?: "none")
                            val expected = if (keeps) "queued" else "threw RecordNotQueuedException"
                            assertEquals(MOMENTS.count { it >= NOTICED }, answers.size)
                            assertEquals(List(answers.size) { expected }, answers, "while every broker was down")
                            assertEquals("queued", backSaid, "once the broker was back")
                            assertTrue(fates.values.all { it == "landed" }, "every queued record lands: $fates")
                        } finally {
                            producer.close()
                        }
                    }
                }
            if (ran == null) recordArmFact("stopped", "not asked")
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
        const val MAX_BLOCK_MS = 5_000L
        const val REASON = 300
        val MOMENTS = listOf(0.seconds, 5.seconds, 20.seconds)

        /** From when an answer is asserted: after both clients have had time to see every broker gone. */
        val NOTICED = 5.seconds
        val BACK = 2.minutes
        val LANDING = 3.minutes
    }
}
