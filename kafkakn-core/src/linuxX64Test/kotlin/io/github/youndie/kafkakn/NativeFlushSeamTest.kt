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
import kotlin.concurrent.AtomicInt
import kotlin.concurrent.AtomicReference
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.TimeSource

/**
 * [B-43](../../../../../../../docs/backlog/B-43-native-flush-may-hold-the-callers-thread.md): the native
 * `flush` with records that cannot be delivered, held against a single-lane dispatcher — the seam
 * `JvmDispatcherSeamTest` holds for `send` on the other arm, with the mark taken before the call
 * (research §2.22).
 *
 * **A platform-seam test, and in `linuxX64Test` on purpose.** The fixture is a port with no broker:
 * librdkafka queues the records at once and keeps them until `message.timeout.ms`, so `flush` has
 * something to wait for. The Java client queues nothing without metadata, so the same fixture makes its
 * `flush` return at once — vacuous there, and that arm's `flush` runs on `Dispatchers.IO` by construction.
 * The strict topic was tried first and does not work on either arm: the broker refuses those records at
 * once, which drains the queue as surely as an acknowledgement (the lesson `JvmDispatcherSeamTest` records).
 */
class NativeFlushSeamTest {
    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun flush_with_records_pending_does_not_hold_the_callers_dispatcher() =
        runTest(timeout = 2.minutes) {
            val producer =
                kafkaProducer(
                    ProducerConfig(
                        "bootstrap.servers" to "127.0.0.1:9099",
                        "message.timeout.ms" to PENDING_MS.toString(),
                        "socket.timeout.ms" to "2000",
                    ),
                )
            val lane = Dispatchers.Default.limitedParallelism(1)
            var longestSilence = Duration.ZERO
            var waited = Duration.ZERO
            val timedOut = AtomicInt(0)
            val firstFailure = AtomicReference<String?>(null)
            try {
                withContext(Dispatchers.Default) {
                    coroutineScope {
                        // The sends suspend until their records time out; they are not what is measured.
                        val sends =
                            List(RECORDS) { index ->
                                launch {
                                    try {
                                        producer.send(ProducerRecord(testTopic, "flush-$index".encodeToByteArray()))
                                    } catch (cancellation: CancellationException) {
                                        throw cancellation
                                    } catch (expected: Exception) {
                                        // `Local: Message timed out`: the fixture working as intended -
                                        // counted, and every one of them has to happen.
                                        timedOut.incrementAndGet()
                                        firstFailure.compareAndSet(null, expected.message)
                                    }
                                }
                            }
                        delay(SETTLE)
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
                                val started = TimeSource.Monotonic.markNow()
                                producer.flush()
                                waited = started.elapsedNow()
                                tick()
                                ticker.cancel()
                            }
                        }
                        sends.forEach { it.join() }
                    }
                }
            } finally {
                withContext(Dispatchers.Default) { producer.close() }
            }
            recordArmFact("flush.pending.waited.ms", waited.inWholeMilliseconds.toString())
            recordArmFact("flush.pending.silence.ms", longestSilence.inWholeMilliseconds.toString())
            assertTrue(
                timedOut.value == RECORDS,
                "the fixture stopped working: ${timedOut.value} of $RECORDS sends failed, and all should - " +
                    "there is no broker. The first said ${firstFailure.value}",
            )
            assertTrue(
                waited > MEANINGFUL_WAIT,
                "the fixture stopped working: flush returned after $waited with records pending",
            )
            assertTrue(
                longestSilence < TOLERATED_SILENCE,
                "the caller's dispatcher was held for $longestSilence while flush waited $waited",
            )
        }

    private companion object {
        const val RECORDS = 5
        const val PENDING_MS = 6_000L
        val SETTLE = 500.milliseconds
        val TICK = 2.milliseconds
        val TOLERATED_SILENCE = 500.milliseconds
        val MEANINGFUL_WAIT = 3_000.milliseconds
    }
}
