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
 * Every record the caller handed in is on the broker — the guard this project is shaped around.
 *
 * Cites the first business rule of
 * [feature-backpressure-and-accounting](../../../../../../../docs/features/feature-backpressure-and-accounting.md):
 * *the unit of truth is what the caller asked to send* — not what was enqueued, and not what
 * produced a delivery report. The measured defect it exists for lost **264 826 of 1 000 000**
 * records with `failed = 0` and a successful flush
 * ([research §1.4](../../../../../../../docs/research/research-architecture.md)).
 *
 * **Half of the assertion is deliberately not here.** The end offsets are read by `ci/b-09/run.sh`
 * through the broker's own tools, because a producer verified by a consumer of ours can be wrong in
 * both directions at once and agree with itself — and this library has no consumer to be wrong with
 * in the first place. What the test owns is the numerator: how many records were handed in, and how
 * many of them `send` answered without sending. The script holds that against the topic's end
 * offsets, which is the only party to this that is neither arm.
 *
 * **It is run twice.** The second pass sets `KAFKAKN_NAIVE=1`, which swaps in [NaiveProducer], and
 * the script requires that pass to be **red**. A guard nobody has watched fail is a guard whose
 * failure mode is unknown.
 */
class AccountingTest {
    @Test
    fun the_broker_holds_every_record_the_caller_handed_in() =
        runTest {
            val naive = naiveProducerRequested()
            val topic = accountingTopic
            val real =
                kafkaProducer(
                    ProducerConfig(
                        buildMap {
                            put("bootstrap.servers", bootstrap)
                            put("acks", "all")
                            putAll(smallQueueConfig())
                        },
                    ),
                )
            val producer: KafkaProducer = if (naive) NaiveProducer(real, NAIVE_BOUND) else real

            val results =
                try {
                    withContext(Dispatchers.Default) {
                        val sent =
                            coroutineScope {
                                (0 until RECORDS)
                                    .map { index ->
                                        async {
                                            producer.send(
                                                ProducerRecord(topic, "acct-$armName:$index".encodeToByteArray()),
                                            )
                                        }
                                    }.awaitAll()
                            }
                        producer.flush()
                        sent
                    }
                } finally {
                    producer.close()
                }

            val dropped = results.count { it.partition == NAIVE_DROP_PARTITION }
            recordArmFact("accounting.topic", topic)
            recordArmFact("accounting.handed_in", RECORDS.toString())
            recordArmFact("accounting.dropped", dropped.toString())
            recordArmFact("accounting.naive", naive.toString())

            assertEquals(RECORDS, results.size, "every send must answer, one answer per record")

            // The vacuity guard, and it is the reason the sends above are concurrent. `send` awaits the
            // acknowledgement, so a caller that awaits each record has exactly one in flight and cannot
            // fill a queue of any size - the first version of the sibling backpressure test was serial
            // and proved nothing at all (research §2.5).
            val waits = backpressureWaitCount()
            if (waits >= 0 && !naive) {
                assertTrue(waits > 0, "the queue bound was never reached - this run accounts for nothing")
            }

            assertTrue(
                dropped == 0,
                "$dropped of $RECORDS records were answered with a result and never sent - " +
                    "this is the shape that lost 264 826 records with every indicator green",
            )
        }

    private companion object {
        /** Enough to overrun a queue bound of 100 many times over, few enough to finish. */
        const val RECORDS = 3_000

        /** The permit count [NaiveProducer] refuses past, mirroring the lowered queue bound. */
        const val NAIVE_BOUND = 100
    }
}
