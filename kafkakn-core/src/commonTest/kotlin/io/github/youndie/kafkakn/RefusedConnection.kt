package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.test.fail

// What a refused connection said, shared by [TlsTest] and [MutualTlsTest]: both ask the same
// question of a producer - send one record where it must not be accepted, and keep every sentence the
// failure carried.

/** Sends one record, requires it to fail, and returns everything the failure said. */
internal suspend fun refusalText(
    properties: Map<String, String>,
    what: String,
): String {
    val producer = kafkaProducer(ProducerConfig(properties))
    val failure =
        try {
            withContext(Dispatchers.Default) {
                producer.send(ProducerRecord(testTopic, "$what-$armName".encodeToByteArray()))
            }
            null
        } catch (cancellation: CancellationException) {
            // RETHROWN, AND THIS IS NOT A FORMALITY. `runTest` cancels the body when it times
            // out, so a catch that swallows cancellation records the TIMEOUT as the producer's
            // failure - which is exactly what happened while B-11 was being written: the arm
            // file read `tls.wrong-ca.failure=CancellationException: The test timed out`, a
            // sentence about the harness filed as a measurement of the library.
            throw cancellation
        } catch (thrown: Throwable) {
            thrown
        } finally {
            try {
                producer.close()
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (onClose: Throwable) {
                // The subject is what `send` did. A producer that cannot close after refusing a
                // peer is worth seeing, but it is not what this test asserts - so it is printed
                // rather than thrown, and never silently dropped.
                println("$what: close() after the failure threw ${onClose::class.simpleName}: ${onClose.message}")
            }
        }
    if (failure == null) {
        fail("$what: the record was accepted. A producer that connects here is not verifying anything.")
    }
    val text = failure.chainText()
    // Recorded per arm rather than compared: the two clients word this differently and always
    // will, so a shared observation file would report a disagreement on every run and stop
    // being read. A person reads both, which is the point of writing them down.
    recordArmFact("tls.$what.failure", text.replace('\n', ' ').take(400))
    return text
}

/**
 * The message plus every cause's.
 *
 * The Java client's top-level message is `SSL handshake failed`; the sentence naming the
 * certificate is two causes down. Asserting on the top message alone would have failed a client
 * that says exactly the right thing.
 */
internal fun Throwable.chainText(): String {
    val parts = mutableListOf<String>()
    var current: Throwable? = this
    var depth = 0
    while (current != null && depth < CAUSE_DEPTH) {
        parts += "${current::class.simpleName}: ${current.message}"
        current = current.cause
        depth++
    }
    return parts.joinToString(" <- ")
}

private const val CAUSE_DEPTH = 10
