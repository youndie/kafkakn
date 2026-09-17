package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * [feature-backpressure-and-accounting](../../../../../../../docs/features/feature-backpressure-and-accounting.md).
 *
 * The queue bound is lowered here on purpose. At the default of 100 000 records a short test never
 * reaches the case that matters and passes for the wrong reason — which is exactly how a measured
 * naive binding lost 264 826 records of 1 000 000 while every indicator stayed green.
 *
 * The sends are **concurrent**, and that is not a performance choice. `send` awaits the broker's
 * acknowledgement, so a caller that awaits each record before offering the next never has more than
 * one in flight and can never fill a queue of any size. The first version of this test was serial,
 * and the vacuity guard below is what said so: `backpressureWaitCount()` was zero.
 *
 * They also run on [Dispatchers.Default] rather than in `runTest`'s scheduler, because the producer
 * waits for room with `delay` and virtual time would skip exactly the wait being tested.
 */
class BackpressureTest {
    @Test
    fun producing_past_the_queue_bound_loses_nothing() =
        runTest {
            val stamp = "backpressure-$armName-${randomSuffix()}"
            val producer =
                kafkaProducer(
                    ProducerConfig(
                        buildMap {
                            put("bootstrap.servers", bootstrap)
                            put("acks", "all")
                            putAll(smallQueueConfig())
                        },
                    ),
                )
            var delivered = 0
            try {
                withContext(Dispatchers.Default) {
                    coroutineScope {
                        (0 until RECORDS)
                            .map { index ->
                                async {
                                    producer.send(ProducerRecord(testTopic, "$stamp:$index".encodeToByteArray()))
                                }
                            }.awaitAll()
                    }
                }
                delivered = RECORDS
                producer.flush()
            } finally {
                producer.close()
            }

            assertEquals(RECORDS, delivered, "every send must return or throw, never drop")
            recordArmFact("backpressure.stamp", stamp)
            recordArmFact("backpressure.count", RECORDS.toString())

            val waits = backpressureWaitCount()
            if (waits >= 0) {
                // Only the native arm can report this. Zero would mean the bound was never reached and
                // the test proved nothing about backpressure at all.
                assertTrue(waits > 0, "the queue bound was never reached - this test is vacuous")
            }
        }

    @Test
    fun flush_waits_for_the_queue_rather_than_for_a_return_code() =
        runTest {
            val stamp = "flush-$armName-${randomSuffix()}"
            val producer =
                kafkaProducer(
                    ProducerConfig(
                        buildMap {
                            put("bootstrap.servers", bootstrap)
                            put("acks", "all")
                            putAll(smallQueueConfig())
                        },
                    ),
                )
            try {
                withContext(Dispatchers.Default) {
                    coroutineScope {
                        (0 until FLUSH_RECORDS)
                            .map { index ->
                                async {
                                    producer.send(ProducerRecord(testTopic, "$stamp:$index".encodeToByteArray()))
                                }
                            }.awaitAll()
                    }
                    producer.flush()
                }
                // After flush every record is accounted for, which is what makes the next assertion -
                // made by the script against the broker - a statement about flush and not about timing.
            } finally {
                producer.close()
            }
            recordArmFact("flush.stamp", stamp)
            recordArmFact("flush.count", FLUSH_RECORDS.toString())
        }

    private companion object {
        // Enough to overrun a 100-record queue many times over, few enough to finish quickly.
        const val RECORDS = 3_000
        const val FLUSH_RECORDS = 500
    }
}
