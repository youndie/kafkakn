package io.github.youndie.kafkakn

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * An unexpected failure on librdkafka's thread must reach the caller.
 *
 * The first version of this control expected the process to die, and measuring it showed something
 * worse: an exception thrown inside the callback **does not terminate the process and surfaces
 * nowhere**. It left the continuation parked and the caller suspended for ever — a hang, which
 * nothing reports, rather than a crash, which everything does.
 *
 * So the producer now unparks before anything else and wraps the rest in a catch, and this test
 * asserts the consequence: with the affordance on, `send` **throws** instead of hanging.
 */
class DeliveryCallbackCrashControlTest {

    @Test
    fun a_failure_on_librdkafkas_thread_reaches_the_caller_instead_of_hanging_it() = runTest {
        assertFalse(crashInDeliveryCallback.value, "the affordance leaked into a normal run")
        crashInDeliveryCallback.value = true
        try {
            val producer = kafkaProducer(
                ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"),
            )
            val failure = assertFailsWith<KafkaProduceException> {
                producer.send(ProducerRecord(testTopic, "callback-control".encodeToByteArray()))
            }
            assertTrue(
                failure.message?.contains("delivery callback failed") == true,
                "the failure should say where it came from, but said: ${failure.message}",
            )
        } finally {
            crashInDeliveryCallback.value = false
        }
    }
}
