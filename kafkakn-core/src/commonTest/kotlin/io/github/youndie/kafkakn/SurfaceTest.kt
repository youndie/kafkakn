package io.github.youndie.kafkakn

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * The surface exists, is reachable on both arms, and does nothing yet.
 *
 * Red for the right reason is the point of this file: the first real producer test has to fail
 * because the producer is wrong, not because the suite could not construct one.
 */
class SurfaceTest {
    @Test
    fun a_producer_can_be_constructed_on_this_arm() {
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to "127.0.0.1:9092"))
        assertNotNull(producer)
    }

    // The two assertions that stood here - that the factory returns the stub, and that `send` throws
    // NotImplementedError - were true on both arms when B-02 wrote them and are now true only on the
    // arm that is still a stub. An assertion about "no arm is implemented" becomes a lie the moment
    // one is, so it is gone rather than qualified: what an implemented arm does is asserted by its
    // own feature tests, and what an unimplemented one does is not interesting.

    @Test
    fun configuration_keeps_kafka_s_own_key_names() {
        val config = ProducerConfig("bootstrap.servers" to "h:9092", "acks" to "all")
        assertEquals("h:9092", config["bootstrap.servers"])
        assertEquals("all", config["acks"])
        assertNull(config["bootstrapServers"])
    }

    @Test
    fun a_record_carries_bytes_and_an_optional_key() {
        val record = ProducerRecord(topic = "t", value = byteArrayOf(0, -1, 2))
        assertNull(record.key)
        assertEquals(3, record.value?.size)
    }
}
