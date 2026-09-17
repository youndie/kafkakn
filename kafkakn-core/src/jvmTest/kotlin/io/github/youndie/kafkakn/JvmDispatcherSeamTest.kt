package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * The platform seam the contract flattens: **`kafka-clients` blocks, librdkafka refuses.**
 *
 * `producer-contract` says behaviour at the queue bound is "suspends" on both arms, and that is a
 * claim about the CALLER'S THREAD, not only about the outcome. `org.apache.kafka.clients.producer
 * .KafkaProducer.send` waits inside the client - for metadata, or for room in the record accumulator
 * up to `max.block.ms` - and `flush` and `close` wait outright. Called straight from a coroutine,
 * each of those holds the dispatcher's thread, and "suspends" becomes a blocked thread wearing a
 * suspend signature.
 *
 * It lives in `jvmTest`, and that is the third kind of assertion rather than a leak: it is about the
 * seam, not about what a caller sees. The native arm's side of the same claim is the vacuity guard
 * in `BackpressureTest`.
 */
class JvmDispatcherSeamTest {
    @Test
    fun producing_does_not_hold_the_callers_dispatcher() =
        runTest {
            // ONE thread, deliberately. On a pool the blocking is invisible until every thread is
            // taken, which is how it got past the concurrent backpressure test: 3 000 sends on
            // Dispatchers.Default kept finishing because the broker drained faster than the pool
            // filled.
            val single = Executors.newSingleThreadExecutor { runnable -> Thread(runnable, "seam") }
            val dispatcher = single.asCoroutineDispatcher()

            // A PORT WITH NOTHING ON IT, and the fixture took two attempts to get right. The
            // ordinary topic proved nothing - the broker acknowledged faster than records could pile
            // up. The strict topic (`min.insync.replicas=2`, where nothing can be acknowledged)
            // proved nothing either, because the broker REFUSES those records at once and a refusal
            // drains the accumulator as well as an acknowledgement does.
            //
            // What actually holds the client is metadata it cannot get: with no broker at the
            // address, `send` waits `max.block.ms` and then throws. Same wait, no broker needed.
            val producer =
                kafkaProducer(
                    ProducerConfig(
                        "bootstrap.servers" to "127.0.0.1:9099",
                        "acks" to "all",
                        "max.block.ms" to BLOCK_MS.toString(),
                    ),
                )

            // THE GAP, not the count. Between two blocking calls the dispatcher comes free for a
            // moment and a ticker gets its turn, so "it ran more than once" is true of a thread that
            // was held for six seconds - measured, on the first version of this test. What a held
            // thread cannot hide is the silence: a ticker that wants the thread every 2 ms and does
            // not get it for two seconds says so in one number.
            val longestSilence = AtomicLong(0)
            val failures = AtomicLong(0)
            val firstFailure = AtomicReference<String?>(null)
            try {
                withContext(dispatcher) {
                    coroutineScope {
                        val ticker =
                            launch {
                                var last = System.nanoTime()
                                while (isActive) {
                                    val now = System.nanoTime()
                                    longestSilence.updateAndGet { maxOf(it, (now - last) / 1_000_000) }
                                    last = now
                                    delay(TICK_MS)
                                }
                            }
                        coroutineScope {
                            repeat(RECORDS) { index ->
                                launch {
                                    try {
                                        producer.send(
                                            ProducerRecord(testTopic, ByteArray(RECORD_BYTES) { index.toByte() }),
                                        )
                                    } catch (cancellation: CancellationException) {
                                        throw cancellation
                                    } catch (refused: Throwable) {
                                        // Named, not swallowed: every record here is expected to
                                        // fail - there is no broker - but a run where none did, or
                                        // where they failed for some other reason, would mean the
                                        // fixture stopped arranging the wait this test is about.
                                        failures.incrementAndGet()
                                        firstFailure.compareAndSet(
                                            null,
                                            "${refused::class.simpleName}: ${refused.message}",
                                        )
                                    }
                                }
                            }
                        }
                        ticker.cancel()
                    }
                }
            } finally {
                producer.close()
                dispatcher.close()
                single.shutdown()
            }

            assertTrue(
                failures.get() == RECORDS.toLong(),
                "the fixture stopped working: ${failures.get()} of $RECORDS sends failed, and all of " +
                    "them should - there is no broker at that address. The first said " +
                    "${firstFailure.get()}. Nothing below is measuring what it claims to.",
            )
            assertTrue(
                longestSilence.get() < TOLERATED_SILENCE_MS,
                "the caller's dispatcher was held for ${longestSilence.get()} ms while $RECORDS " +
                    "sends waited on metadata. A coroutine on that dispatcher asked for the thread " +
                    "every $TICK_MS ms and did not get it: `send` on this arm waits inside the " +
                    "client, so a suspend signature over it blocks the thread it was called on.",
            )
        }

    private companion object {
        const val RECORDS = 3
        const val RECORD_BYTES = 64
        const val TICK_MS = 2L
        const val BLOCK_MS = 2000

        /** Generous: the point is a two-second stall, not a scheduling hiccup on a shared box. */
        const val TOLERATED_SILENCE_MS = 500
    }
}
