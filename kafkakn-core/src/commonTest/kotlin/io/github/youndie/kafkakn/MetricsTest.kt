package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-41](../../../../../../../docs/backlog/B-41-metrics-an-operator-can-read.md): the producer's
 * machinery, read from each arm's own client, under one load.
 *
 * Three moments: at rest after the first record, during three seconds of continuous sending (sampled
 * every 100 ms), and after `flush`. The claims inside this test are the ones that must hold on each arm
 * alone; `ci/b-41/run.sh` holds the two arms' readings against each other, with the tolerance the
 * contract states.
 */
class MetricsTest {
    @Test
    fun the_machinery_reads_alike_on_both_arms_under_one_load() =
        runTest(timeout = 2.minutes) {
            val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
            val payload = ByteArray(RECORD_BYTES) { it.toByte() }
            try {
                withContext(Dispatchers.Default) {
                    producer.send(ProducerRecord(testTopic, payload))
                    // Long enough for the native arm's first statistics, which arrive once a second.
                    delay(SETTLE)
                    record("rest", producer.metrics())

                    var maxBuffered = 0L
                    var maxInFlight = 0
                    var loadRoundTrip: Double? = null
                    coroutineScope {
                        val sending =
                            launch {
                                val until = TimeSource.Monotonic.markNow() + LOAD
                                while (until.hasNotPassedNow()) {
                                    (0 until BURST)
                                        .map {
                                            async {
                                                producer.send(
                                                    ProducerRecord(testTopic, payload),
                                                )
                                            }
                                        }.awaitAll()
                                }
                            }
                        while (sending.isActive) {
                            val now = producer.metrics()
                            maxBuffered = maxOf(maxBuffered, now.bufferedBytes ?: 0)
                            maxInFlight = maxOf(maxInFlight, now.requestsInFlight ?: 0)
                            now.brokerRoundTripMillis?.let { loadRoundTrip = it }
                            delay(SAMPLE)
                        }
                    }
                    recordArmFact("metrics.load.maxBufferedBytes", maxBuffered.toString())
                    recordArmFact("metrics.load.maxRequestsInFlight", maxInFlight.toString())
                    recordArmFact("metrics.load.brokerRoundTripMillis", loadRoundTrip.toString())

                    producer.flush()
                    delay(SETTLE)
                    val after = producer.metrics()
                    record("after", after)

                    // What must hold on each arm alone: a flushed producer holds nothing and waits for
                    // nothing, it is connected, and under load it saw a round trip. Not after the flush:
                    // librdkafka's round trip covers the last statistics interval only, and an idle one
                    // has none (measured: null at rest and after, 1.66 ms on the JVM's thirty-second window).
                    assertEquals(0L, after.bufferedBytes, "bytes held after flush: $after")
                    assertEquals(0, after.requestsInFlight, "requests out after flush: $after")
                    assertTrue((after.openConnections ?: 0) >= 1, "not connected after sending: $after")
                    assertTrue((loadRoundTrip ?: 0.0) > 0.0, "three seconds of sending never showed a round trip")
                    assertTrue(maxBuffered > 0, "three seconds of sending never showed a byte buffered")
                }
            } finally {
                withContext(Dispatchers.Default) { producer.close() }
            }
        }

    private fun record(
        moment: String,
        metrics: ProducerMetrics,
    ) {
        recordArmFact("metrics.$moment.bufferedBytes", metrics.bufferedBytes.toString())
        recordArmFact("metrics.$moment.requestsInFlight", metrics.requestsInFlight.toString())
        recordArmFact("metrics.$moment.brokerRoundTripMillis", metrics.brokerRoundTripMillis.toString())
        recordArmFact("metrics.$moment.openConnections", metrics.openConnections.toString())
    }

    private companion object {
        const val RECORD_BYTES = 1024
        const val BURST = 200
        val SETTLE = 2500.milliseconds
        val LOAD = 3.seconds
        val SAMPLE = 100.milliseconds
    }
}
