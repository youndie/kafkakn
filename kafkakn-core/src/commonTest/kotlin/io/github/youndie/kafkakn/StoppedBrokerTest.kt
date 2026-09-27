package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-77](../../../../../../../docs/backlog/B-77-a-stopped-broker-and-a-known-topic.md): a MEASUREMENT, not a promise.
 * A consumer saw the JVM refuse a record and native queue it, with the broker stopped and the topic already known.
 * Before either arm is changed, this records what each does, at three moments after the stop, and what became of
 * every record it queued once the broker is back.
 *
 * Nothing is asserted about the answers: the contract does not yet say which is right, and that is the question
 * this item ends in. `KAFKAKN_STOPPED_EXTRA` (`key=value`) adds one producer key, so the runner can ask the JVM arm
 * the same question with a key of its own.
 */
class StoppedBrokerTest {
    @Test
    fun a_record_enqueued_while_the_broker_is_stopped_and_its_topic_is_known() =
        runTest(timeout = 8.minutes) {
            if (testEnv("KAFKAKN_BROKER_STOP") == null) {
                recordArmFact("stopped", "not asked")
                return@runTest
            }
            val extra =
                testEnv("KAFKAKN_STOPPED_EXTRA")?.takeIf { it.isNotBlank() }?.split("=", limit = 2)?.let {
                    it[0] to
                        it[1]
                }
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-stopped-$armName-${randomSuffix()}"
                val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
                try {
                    admin.createTopics(listOf(NewTopic(topic, 1, 1)))
                } finally {
                    admin.close()
                }
                val producer =
                    kafkaProducer(
                        ProducerConfig(
                            mapOf(
                                "bootstrap.servers" to bootstrap,
                                "acks" to "all",
                                "max.block.ms" to "$MAX_BLOCK_MS",
                            ) +
                                listOfNotNull(extra),
                        ),
                    )
                val queued = mutableListOf<Pair<String, Delivery>>()
                try {
                    // The topic is known: one record written and acknowledged before the stop.
                    producer.send(ProducerRecord(topic, "warm".encodeToByteArray()))
                    brokerStopped(true)
                    val stopped = TimeSource.Monotonic.markNow()
                    try {
                        for ((index, at) in MOMENTS.withIndex()) {
                            val wait = at - stopped.elapsedNow()
                            if (wait > Duration.ZERO) delay(wait)
                            val value = "r-$index"
                            val started = TimeSource.Monotonic.markNow()
                            val said =
                                try {
                                    queued +=
                                        value to producer.enqueue(ProducerRecord(topic, value.encodeToByteArray()))
                                    "queued"
                                } catch (cancelled: CancellationException) {
                                    throw cancelled
                                } catch (thrown: Exception) {
                                    "threw ${thrown::class.simpleName}: ${thrown.message}"
                                }
                            recordArmFact("stopped.$index.at.ms", stopped.elapsedNow().inWholeMilliseconds.toString())
                            recordArmFact(
                                "stopped.$index.after.ms",
                                started.elapsedNow().inWholeMilliseconds.toString(),
                            )
                            recordArmFact("stopped.$index.said", said.replace('\n', ' ').take(REASON))
                        }
                    } finally {
                        brokerStopped(false)
                    }
                    // What became of every record queued while the broker was stopped.
                    for ((value, delivery) in queued) {
                        val fate =
                            try {
                                withTimeoutOrNull(LANDING) { delivery.await() }?.let { "landed at ${it.offset}" }
                                    ?: "no answer within $LANDING"
                            } catch (cancelled: CancellationException) {
                                throw cancelled
                            } catch (thrown: Exception) {
                                "failed ${thrown::class.simpleName}: ${thrown.message}"
                            }
                        recordArmFact("stopped.$value.fate", fate.replace('\n', ' ').take(REASON))
                    }
                    recordArmFact("stopped.topic", topic)
                    recordArmFact("stopped.extra", extra?.let { "${it.first}=${it.second}" } ?: "none")
                } finally {
                    producer.close()
                }
            }
        }

    private companion object {
        const val MAX_BLOCK_MS = 5_000L
        const val REASON = 300
        val MOMENTS = listOf(0.seconds, 5.seconds, 20.seconds)
        val LANDING = 3.minutes
    }
}
