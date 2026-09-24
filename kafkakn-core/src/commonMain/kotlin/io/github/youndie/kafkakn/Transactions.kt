package io.github.youndie.kafkakn

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

/**
 * Another producer with the same `transactional.id` has taken over, and this one is finished: every
 * later call fails, and the only thing left to do with it is [KafkaProducer.close].
 *
 * **One exception for both arms** ([B-30](../../../../../../../docs/backlog/B-30-transactions.md)).
 * The Java client throws its own `ProducerFencedException`; librdkafka reports `_FENCED` as a fatal
 * error. What each said is kept as the [cause] and recorded by the suite, and the type is what a
 * caller can rely on.
 */
public class ProducerFencedException(
    message: String,
    cause: Throwable? = null,
) : IllegalStateException(message, cause)

/**
 * Runs [block] inside a transaction: commits when it returns, aborts when it throws, and rethrows.
 *
 * **Built on the four calls, not instead of them.** A caller who has to decide when to abort — on a
 * validation failure halfway through, say — needs [KafkaProducer.abortTransaction] itself; this is the
 * common shape, not the only one.
 *
 * The abort runs even when the failure is a cancellation, in a non-cancellable context, because an
 * open transaction left behind by a cancelled coroutine holds `read_committed` readers back until it
 * times out. If the abort fails too — it will, on a fenced producer — that failure is attached to the
 * original as suppressed rather than replacing it. A failing commit is not retried or aborted here:
 * whether it can be retried is the caller's decision, and the exception says which it was.
 */
public suspend fun <T> KafkaProducer.inTransaction(block: suspend () -> T): T {
    beginTransaction()
    val result =
        try {
            block()
        } catch (failure: Throwable) {
            withContext(NonCancellable) {
                try {
                    abortTransaction()
                } catch (abortFailed: Throwable) {
                    failure.addSuppressed(abortFailed)
                }
            }
            throw failure
        }
    commitTransaction()
    return result
}
